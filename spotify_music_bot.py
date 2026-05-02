"""
spotify_music_bot.py — Production-ready Spotify + Apple Music download bot.

Key behaviours
──────────────
• Playlist flow: download ALL tracks first, then upload ALL sequentially.
• Song index (index.py): every uploaded track's Telegram file_id is stored in
  MongoDB.  On subsequent requests for the same song the bot re-sends the
  cached file_id — no re-download needed.
• Log channel: every sent audio + every skipped/failed track is reported.
• Progress bar: at most one edit per 10 seconds (PROGRESS_UPDATE_INTERVAL).
  Phase-transition messages (e.g. "all downloaded, uploading now") are
  force-sent but only at natural boundaries, not per-track.
"""

from __future__ import annotations

import os
import re
import json
import asyncio
import base64
import tempfile
import time
import shutil
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

import httpx
import requests
from bs4 import BeautifulSoup
from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler, CallbackQueryHandler
from pyrogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyParameters,
)
from pyrogram.enums import ParseMode, ButtonStyle
from pyrogram.errors import FloodWait

import config
import mongodb
import index as song_index
from ap import (
    init_session, get_token, get_all_track_forms, get_links_for_track,
    HEADERS as AP_HEADERS, TIMEOUT as AP_TIMEOUT,
)


# ── Constants ─────────────────────────────────────────────────────────────────

DOWNLOAD_DIR = "./downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

SPOTI_BASE = "https://spotidown.app"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
)
CHUNK_SIZE = 1 * 1024 * 1024          # 1 MB
PLAYLIST_UPLOAD_DELAY = 4.0           # seconds between TG sends
PROGRESS_UPDATE_INTERVAL = 10.0       # seconds between progress bar edits

SPOTIFY_RE = re.compile(
    r"https?://open\.spotify\.com/(track|playlist|album)/([A-Za-z0-9]+)"
)
APPLE_MUSIC_RE = re.compile(r"https?://music\.apple\.com/\S+")


# ── Per-user locks & cancellation ─────────────────────────────────────────────

_USER_LOCKS:   dict[int, asyncio.Lock]  = {}
_CANCEL_FLAGS: dict[int, asyncio.Event] = {}


def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _USER_LOCKS:
        _USER_LOCKS[user_id] = asyncio.Lock()
    return _USER_LOCKS[user_id]


def _get_cancel_flag(user_id: int) -> asyncio.Event:
    if user_id not in _CANCEL_FLAGS:
        _CANCEL_FLAGS[user_id] = asyncio.Event()
    return _CANCEL_FLAGS[user_id]


def _reset_cancel(user_id: int):
    _get_cancel_flag(user_id).clear()


def _is_cancelled(user_id: int) -> bool:
    return _get_cancel_flag(user_id).is_set()


# ── Health-check server (Koyeb / Railway) ─────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *_):
        pass


def _start_health_server(port: int = 8080):
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"[health] listening on :{port}")


# ── Misc helpers ──────────────────────────────────────────────────────────────

def cleanup(path: str | None):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def user_tag(user) -> str:
    return f"@{user.username}" if user.username else f"<code>{user.id}</code>"


async def flood_safe(coro_func, *args, **kwargs):
    while True:
        try:
            return await coro_func(*args, **kwargs)
        except FloodWait as e:
            wait = e.value + 1
            print(f"[FloodWait] sleeping {wait}s")
            await asyncio.sleep(wait)


def start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Dev",     url=config.DEV_URL,          style=ButtonStyle.PRIMARY),
        InlineKeyboardButton("Credits", callback_data="credits",     style=ButtonStyle.PRIMARY),
    ]])


# ── Progress bar ──────────────────────────────────────────────────────────────

def _build_progress_bar(
    done: int,
    total: int,
    current_song: str,
    status_label: str,
    speed_mbps: float | None,
    engine: str,
    cached: bool = False,
) -> str:
    pct     = int((done / total) * 100) if total else 0
    bar_len = 17
    filled  = int(bar_len * done / total) if total else 0
    bar     = "█" * filled + "░" * (bar_len - filled)
    speed_str = f"{speed_mbps:.2f} MB/s" if speed_mbps is not None else "—"
    name_display = current_song if len(current_song) <= 35 else current_song[:32] + "…"
    cache_tag = "  <i>(cached ⚡)</i>" if cached else ""
    return (
        f"<blockquote>"
        f"<b>Progress 🎵 {done}/{total}</b>\n"
        f"<b>Track:</b> {name_display}{cache_tag}\n"
        f"╭────────────────────╮\n"
        f"│ {bar} │ {pct}%\n"
        f"╰────────────────────╯\n"
        f"<b>Status:</b> {status_label}\n"
        f"<b>Speed:</b> {speed_str}\n"
        f"<b>Engine:</b> {engine}"
        f"</blockquote>"
    )


# ── Throttled progress message ────────────────────────────────────────────────

class ThrottledProgress:
    """
    Edit a Telegram message at most once per PROGRESS_UPDATE_INTERVAL seconds.
    `force_update` bypasses the throttle but should only be used at natural
    phase transitions (not after every track) to avoid TG rate-limits.
    """

    def __init__(self, status_msg):
        self._msg       = status_msg
        self._last_edit = 0.0
        self._last_text = ""

    async def update(self, text: str):
        """Throttled — skips the edit if last edit was < 10 s ago."""
        now = time.monotonic()
        if (now - self._last_edit) < PROGRESS_UPDATE_INTERVAL:
            return
        await self._do_edit(text)

    async def force_update(self, text: str):
        """Always edits regardless of throttle window."""
        await self._do_edit(text)

    async def _do_edit(self, text: str):
        if text == self._last_text:
            return
        try:
            await self._msg.edit_text(text, parse_mode=ParseMode.HTML)
            self._last_edit = time.monotonic()
            self._last_text = text
        except Exception:
            pass

    async def delete(self):
        try:
            await self._msg.delete()
        except Exception:
            pass


# ════════════════════════════════════════════════════════════════════════════════
# LOGGING TO CHANNEL
# ════════════════════════════════════════════════════════════════════════════════

async def _log(bot: Client, text: str):
    """Send a plain text message to the log channel (best-effort)."""
    if not config.LOG_CHANNEL:
        return
    try:
        await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[log] {e}")


async def log_new_user(bot: Client, user) -> None:
    if not config.LOG_CHANNEL:
        return
    name     = user.first_name + (f" {user.last_name}" if user.last_name else "")
    username = f"@{user.username}" if user.username else "<i>None</i>"
    text = (
        "<blockquote>"
        "🆕 <b>New User</b>\n\n"
        f"<b>Name     :</b>  <b>{name}</b>\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n"
        f"<b>Username :</b>  {username}"
        "</blockquote>"
    )
    photos = []
    try:
        async for photo in bot.get_chat_photos(user.id, limit=1):
            photos.append(photo)
    except Exception:
        pass
    try:
        if photos:
            await bot.send_photo(config.LOG_CHANNEL, photos[0].file_id,
                                 caption=text, parse_mode=ParseMode.HTML)
        else:
            await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[log] new-user: {e}")


async def log_track_sent(
    bot: Client,
    user,
    song_name: str,
    idx: int,
    total: int,
    audio_path: str | None,       # None if served from cache
    thumb_path: str | None,
    cached: bool,
    file_id: str | None = None,   # Telegram file_id when cached
) -> None:
    """Log every successfully sent track to the log channel."""
    if not config.LOG_CHANNEL:
        return
    uname = user.first_name + (f" {user.last_name}" if user.last_name else "")
    cache_tag = "⚡ <i>from cache</i>" if cached else "⬇️ <i>downloaded</i>"
    caption = (
        "<blockquote>"
        f"🎵 <b>{song_name}</b>\n"
        f"<b>By      :</b> {uname} (<code>{user.id}</code>)\n"
        f"<b>Track   :</b> {idx}/{total}  •  {cache_tag}"
        "</blockquote>"
    )
    try:
        if cached and file_id:
            # Re-send the cached audio directly
            await bot.send_audio(
                config.LOG_CHANNEL,
                audio=file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
        else:
            if thumb_path and os.path.exists(thumb_path):
                await bot.send_photo(
                    config.LOG_CHANNEL, photo=thumb_path,
                    caption=caption, parse_mode=ParseMode.HTML,
                )
            if audio_path and os.path.exists(audio_path):
                await bot.send_audio(
                    config.LOG_CHANNEL,
                    audio=audio_path,
                    title=song_name,
                    file_name=f"{song_name}.mp3",
                    parse_mode=ParseMode.HTML,
                )
    except Exception as e:
        print(f"[log] track_sent: {e}")


async def log_track_failed(
    bot: Client,
    user,
    song_name: str,
    idx: int,
    total: int,
    reason: str,
) -> None:
    if not config.LOG_CHANNEL:
        return
    uname = user.first_name + (f" {user.last_name}" if user.last_name else "")
    await _log(bot, (
        "<blockquote>"
        f"⚠️ <b>Track failed / skipped</b>\n\n"
        f"<b>Song    :</b> {song_name}\n"
        f"<b>Track   :</b> {idx}/{total}\n"
        f"<b>User    :</b> {uname} (<code>{user.id}</code>)\n"
        f"<b>Reason  :</b> <i>{reason}</i>"
        "</blockquote>"
    ))


async def log_download_summary(
    bot: Client,
    user,
    first_track: str,
    total: int,
    sent: int,
    engine: str,
) -> None:
    if not config.LOG_CHANNEL:
        return
    uname    = user.first_name + (f" {user.last_name}" if user.last_name else "")
    username = f"@{user.username}" if user.username else "<i>None</i>"
    await _log(bot, (
        "<blockquote>"
        f"✅ <b>Download Complete  [{engine}]</b>\n\n"
        f"<b>User     :</b>  <b>{uname}</b>\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n"
        f"<b>Username :</b>  {username}\n"
        f"<b>Track    :</b>  {first_track}\n"
        f"<b>Sent     :</b>  {sent}/{total} track{'s' if total != 1 else ''}"
        "</blockquote>"
    ))


# ════════════════════════════════════════════════════════════════════════════════
# SEND-AUDIO HELPER  (handles cache lookup, upload, index save, log)
# ════════════════════════════════════════════════════════════════════════════════

async def _send_audio_with_cache(
    bot: Client,
    msg: Message,
    user,
    *,
    source: str,            # "spotify" or "apple_music"
    track_id: str,          # cache key
    song_name: str,
    title: str,
    artist: str,
    audio_path: str | None,  # None means we MUST have a cached file_id
    thumb_path: str | None,
    idx: int,
    total: int,
) -> bool:
    """
    1. Check song index for a cached file_id.
    2a. If cached → re-send file_id to user + log channel.
    2b. If not cached → upload audio_path, save returned file_id, log.
    Returns True on success.
    """
    # ── 1. Cache lookup ───────────────────────────────────────────────────────
    cached_doc = await song_index.get_cached_track(source, track_id)

    caption = f"<blockquote>🎵 <b>{song_name}</b></blockquote>"

    if cached_doc:
        file_id       = cached_doc["file_id"]
        thumb_file_id = cached_doc.get("thumb_file_id")
        print(f"[cache ⚡] {song_name} — serving from index")
        try:
            # Send thumbnail if we have one
            if thumb_file_id:
                await flood_safe(
                    bot.send_photo,
                    chat_id=msg.chat.id,
                    photo=thumb_file_id,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=msg.id),
                )
            await flood_safe(
                bot.send_audio,
                chat_id=msg.chat.id,
                audio=file_id,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )
            await log_track_sent(bot, user, song_name, idx, total,
                                 None, None, cached=True, file_id=file_id)
            return True
        except Exception as e:
            # Cached file_id may have expired — fall through to re-upload
            print(f"[cache] stale file_id for {song_name}: {e} — re-uploading")

    # ── 2. Fresh upload ───────────────────────────────────────────────────────
    if not audio_path or not os.path.exists(audio_path):
        print(f"[send] no audio_path for {song_name}, cannot upload")
        return False

    sent_thumb_file_id = None
    try:
        if thumb_path and os.path.exists(thumb_path):
            photo_msg = await flood_safe(
                bot.send_photo,
                chat_id=msg.chat.id,
                photo=thumb_path,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )
            # Grab the file_id of the uploaded photo for caching
            if photo_msg and photo_msg.photo:
                sent_thumb_file_id = photo_msg.photo.file_id

        audio_msg = await flood_safe(
            bot.send_audio,
            chat_id=msg.chat.id,
            audio=audio_path,
            title=title,
            performer=artist,
            file_name=f"{song_name}.mp3",
            reply_parameters=ReplyParameters(message_id=msg.id),
        )

        # ── Save to index ─────────────────────────────────────────────────────
        if audio_msg and audio_msg.audio:
            await song_index.save_track(
                source      = source,
                track_id    = track_id,
                name        = song_name,
                title       = title,
                artist      = artist,
                file_id     = audio_msg.audio.file_id,
                thumb_file_id = sent_thumb_file_id,
            )

        await log_track_sent(bot, user, song_name, idx, total,
                             audio_path, thumb_path, cached=False)
        return True

    except Exception as e:
        print(f"[send] upload failed for {song_name}: {e}")
        await log_track_failed(bot, user, song_name, idx, total, str(e))
        return False


# ════════════════════════════════════════════════════════════════════════════════
# SPOTIFY  —  scraper helpers
# ════════════════════════════════════════════════════════════════════════════════

def _make_spoti_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Referer":    SPOTI_BASE + "/en2",
        "X-Requested-With": "XMLHttpRequest",
    })
    r    = s.get(SPOTI_BASE + "/en2", timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = soup.find("input", {"type": "hidden", "name": re.compile(r"^_")})
    s._csrf = {hidden["name"]: hidden["value"]}
    return s


def _spoti_fetch_action(s: requests.Session, spotify_url: str) -> str:
    r    = s.post(SPOTI_BASE + "/action", data={
        "url": spotify_url,
        "g-recaptcha-response": "faketoken",
        **s._csrf,
    }, timeout=20)
    resp = r.json()
    if resp.get("error"):
        raise Exception(resp.get("message", "unknown error"))
    return resp["data"]


def _spoti_parse_forms(html: str):
    soup   = BeautifulSoup(html, "html.parser")
    forms  = soup.find_all("form", {"name": "submitspurl"})
    result = []
    for form in forms:
        fields = {}
        for inp in form.find_all("input"):
            if inp.get("name"):
                fields[inp["name"]] = inp.get("value", "")
        result.append(fields)
    img            = soup.find("img")
    fallback_thumb = img["src"] if img else None
    return result, fallback_thumb


def _spoti_download_thumb(url: str, name: str) -> str | None:
    if not url or not url.startswith("http"):
        return None
    try:
        safe = re.sub(r'[\\/*?:"<>|]', "", name)[:80]
        path = os.path.join(DOWNLOAD_DIR, f"{safe}_thumb.jpg")
        with requests.get(url, timeout=15, headers={"User-Agent": UA}) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                f.write(r.content)
        return path if os.path.getsize(path) > 0 else None
    except Exception:
        return None


def _spoti_download_file(url: str, name: str) -> tuple[str, float]:
    """Returns (local_path, speed_mbps)."""
    safe  = re.sub(r'[\\/*?:"<>|]', "", name)[:100]
    path  = os.path.join(DOWNLOAD_DIR, f"{safe}.mp3")
    start = time.monotonic()
    total_bytes = 0
    with requests.get(url, stream=True, timeout=120,
                      headers={"User-Agent": UA}) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(128 * 1024):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)
    elapsed = time.monotonic() - start
    speed   = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise RuntimeError("downloaded file is empty")
    return path, speed


def _spoti_fetch_one(
    s: requests.Session,
    form_data: dict,
    index: int,
    fallback_thumb: str | None = None,
) -> tuple:
    """
    Returns:
      (index, name, title, artist, track_id, local_path, local_thumb, err, speed)
    """
    track_id = None
    try:
        info      = json.loads(base64.b64decode(form_data.get("data", "")).decode())
        title     = info.get("name",   f"Track {index + 1}")
        artist    = info.get("artist", "")
        name      = f"{title} - {artist}" if artist else title
        thumb_url = info.get("cover") or info.get("image") or info.get("thumb") or fallback_thumb
        # Spotify embeds the track ID in the form data
        track_id  = info.get("id") or info.get("track_id")
    except Exception:
        title, artist, name, thumb_url = (
            f"Track {index + 1}", "", f"Track {index + 1}", fallback_thumb
        )

    r    = s.post(SPOTI_BASE + "/action/track", data=form_data, timeout=30)
    resp = r.json()
    if resp.get("error"):
        return index, name, title, artist, track_id, None, None, resp.get("message"), 0.0

    soup = BeautifulSoup(resp["data"], "html.parser")
    img  = soup.find("img")
    if img and not thumb_url:
        thumb_url = img.get("src")

    href = None
    a = soup.find("a", href=re.compile(r"/dl\?token=|rapid\.spotidown"))
    if a:
        href = a["href"]
        if href.startswith("/"):
            href = SPOTI_BASE + href
    else:
        for a in soup.find_all("a", href=re.compile(r"https?://")):
            href = a["href"]
            break

    if not href:
        return index, name, title, artist, track_id, None, None, "no link found", 0.0

    try:
        local_path, speed = _spoti_download_file(href, name)
    except Exception as e:
        return index, name, title, artist, track_id, None, None, f"download failed: {e}", 0.0

    local_thumb = _spoti_download_thumb(thumb_url, name)
    return index, name, title, artist, track_id, local_path, local_thumb, None, speed


def spotify_get_track(spotify_url: str) -> tuple:
    s             = _make_spoti_session()
    html          = _spoti_fetch_action(s, spotify_url)
    forms, fb_th  = _spoti_parse_forms(html)
    if not forms:
        raise Exception("no track found")
    _, name, title, artist, track_id, local_path, thumb, err, _ = _spoti_fetch_one(
        s, forms[0], 0, fb_th
    )
    if err:
        raise Exception(err)
    # Fallback track_id from URL
    if not track_id:
        m = SPOTIFY_RE.search(spotify_url)
        track_id = m.group(2) if m else spotify_url
    return name, title, artist, track_id, local_path, thumb


def spotify_get_playlist_all(
    spotify_url: str,
    on_progress=None,
) -> list[tuple]:
    """
    Download ALL tracks sequentially.
    on_progress(done, total, name, speed, err) called after each.
    Returns list of (index, name, title, artist, track_id,
                     local_path, thumb, err, speed, total).
    """
    s             = _make_spoti_session()
    html          = _spoti_fetch_action(s, spotify_url)
    forms, fb_th  = _spoti_parse_forms(html)
    total         = len(forms)
    results       = []

    for i, form in enumerate(forms):
        tup = _spoti_fetch_one(s, form, i, fb_th)
        # tup: (index, name, title, artist, track_id, path, thumb, err, speed)
        results.append(tup + (total,))
        if on_progress:
            _, name, _, _, _, _, _, err, speed = tup
            on_progress(i + 1, total, name, speed, err)

    return results


def spotify_type(url: str) -> str:
    if "/track/"    in url: return "track"
    if "/playlist/" in url: return "playlist"
    if "/album/"    in url: return "album"
    return "unknown"


# ════════════════════════════════════════════════════════════════════════════════
# APPLE MUSIC  —  download helpers
# ════════════════════════════════════════════════════════════════════════════════

def _parse_ap_filename(response: httpx.Response, fallback: str) -> str:
    cd = response.headers.get("content-disposition", "")
    m  = re.search(r"filename\*=UTF-8''([^\s;]+)", cd, re.IGNORECASE)
    if m:
        return urllib.parse.unquote(m.group(1))
    m = re.search(r"""filename=["']?([^"';]+)["']?""", cd, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return fallback


async def _ap_download_file(
    client: httpx.AsyncClient,
    url: str,
    dest_dir: str,
    fallback_name: str,
) -> tuple[bool, str, float]:
    try:
        start = time.monotonic()
        total_bytes = 0
        async with client.stream("GET", url, headers=AP_HEADERS,
                                 follow_redirects=True) as r:
            r.raise_for_status()
            fname = _parse_ap_filename(r, fallback_name)
            fname = re.sub(r'[\\/*?:"<>|]', "_", fname)
            dest  = os.path.join(dest_dir, fname)
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes(CHUNK_SIZE):
                    f.write(chunk)
                    total_bytes += len(chunk)
        elapsed = time.monotonic() - start
        speed   = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
        return True, dest, speed
    except Exception as e:
        print(f"[AP dl] {url} → {e}")
        return False, "", 0.0


_EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FFFF\U00002700-\U000027BF\u2600-\u26FF\u2300-\u23FF]+",
    flags=re.UNICODE,
)
_DOMAIN_PREFIX_RE = re.compile(
    r'^(?:[\w\-]+\.)+[a-zA-Z]{2,10}[\s\-\u2013\u2014]*', re.IGNORECASE,
)
_APLMATE_RE = re.compile(
    r'(?i)apl\s*mate\s*(?:\.com?)?\s*[-\u2013\u2014\s]*',
)


def _clean_ap_song_name(raw: str) -> str:
    name = _APLMATE_RE.sub('', raw).strip()
    for _ in range(2):
        name = _DOMAIN_PREFIX_RE.sub('', name).strip()
    name = re.sub(
        r'^[^\w\u0400-\u04FF]*Download\s+\S+\s*[-\u2013]?\s*',
        '', name, flags=re.IGNORECASE,
    ).strip()
    name = _EMOJI_RE.sub('', name).strip()
    name = re.sub(r'^[\s\-\u2013\u2014_•·]+', '', name).strip()
    name = re.sub(r'\s*_\s*', ' ', name).strip()
    return name


def _sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', '_', name).strip()


def _split_ap_links(links: list[dict]) -> tuple[dict | None, dict | None]:
    audio = cover = None
    for item in links:
        label = item["quality"].lower()
        if "cover" in label:
            cover = item
        elif audio is None:
            audio = item
    return audio, cover


async def _ap_download_track_to_dir(
    dl: httpx.AsyncClient,
    idx: int,
    total: int,
    links: list[dict],
    album_thumb_path: str | None,
    dest_dir: str,
) -> dict | None:
    """
    Download audio (+ optional cover art) for one Apple Music track.
    Returns a dict with metadata + local paths, or None on failure.
    """
    audio_entry, cover_entry = _split_ap_links(links)
    if not audio_entry:
        print(f"[AP dl] Track {idx}/{total}: no audio link")
        return None

    ok, raw_path, speed = await _ap_download_file(
        dl, audio_entry["link"], dest_dir, f"track_{idx}.mp3"
    )
    if not ok:
        return None

    raw_name  = os.path.splitext(os.path.basename(raw_path))[0]
    song_name = _clean_ap_song_name(raw_name)
    safe_name = _sanitize_filename(song_name) + ".mp3"
    audio_path = os.path.join(dest_dir, safe_name)
    if raw_path != audio_path:
        os.rename(raw_path, audio_path)

    thumb_path = album_thumb_path
    if cover_entry:
        c_ok, cover_path, _ = await _ap_download_file(
            dl, cover_entry["link"], dest_dir, f"cover_{idx}.jpg"
        )
        if c_ok:
            if not cover_path.lower().endswith((".jpg", ".jpeg")):
                jpg = cover_path + ".jpg"
                os.rename(cover_path, jpg)
                cover_path = jpg
            thumb_path = cover_path

    print(f"[AP dl] ✓ {idx}/{total}  {song_name}")
    return {
        "idx":        idx,
        "song_name":  song_name,
        "safe_fname": safe_name,
        "audio_path": audio_path,
        "thumb_path": thumb_path,
        "speed":      speed,
    }


# ════════════════════════════════════════════════════════════════════════════════
# BOT COMMAND HANDLERS
# ════════════════════════════════════════════════════════════════════════════════

async def cmd_start(bot: Client, msg: Message):
    user = msg.from_user
    try:
        is_new = await mongodb.add_user(
            user_id    = user.id,
            first_name = user.first_name,
            username   = user.username,
            dc_id      = user.dc_id,
        )
        if is_new:
            await log_new_user(bot, user)
    except Exception as e:
        print(f"[db] {e}")

    await msg.reply_text(
        "<blockquote>\n"
        "<b>Hey 👋</b>\n"
        "<b>I can download Spotify and Apple Music tracks for you.</b>\n\n"
        "<i>Just paste any Spotify or Apple Music link below — "
        "single track, album, or playlist.</i>\n"
        "</blockquote>",
        parse_mode   = ParseMode.HTML,
        reply_markup = start_keyboard(),
    )


async def cmd_cancel(bot: Client, msg: Message):
    user = msg.from_user
    lock = _get_user_lock(user.id)
    if not lock.locked():
        await msg.reply_text(
            "<blockquote>ℹ️ <b>No active download to cancel.</b></blockquote>",
            parse_mode         = ParseMode.HTML,
            reply_parameters   = ReplyParameters(message_id=msg.id),
        )
        return
    _get_cancel_flag(user.id).set()
    await msg.reply_text(
        "<blockquote>🛑 <b>Cancellation requested.</b>\n"
        "<i>The bot will stop after the current track finishes.</i></blockquote>",
        parse_mode       = ParseMode.HTML,
        reply_parameters = ReplyParameters(message_id=msg.id),
    )


async def cb_credits(_, cb: CallbackQuery):
    await cb.answer()
    await cb.message.reply_text(
        "<blockquote>\n"
        "<b>Credits</b>\n\n"
        "<i>This bot is made by</i> <b>Mr. D</b>, <b>Mark</b>, <b>Abhinai</b>.\n\n"
        "<i>Mr. D and Mark did most of the heavy lifting — "
        "if it helps you, just give credit. That's all.</i>\n"
        "</blockquote>",
        parse_mode = ParseMode.HTML,
    )


async def handle_message(bot: Client, msg: Message):
    text = msg.text.strip()
    user = msg.from_user

    if "music.apple.com" in text:
        await _handle_apple_music(bot, msg, text, user)
        return

    match = SPOTIFY_RE.search(text)
    if match:
        await _handle_spotify(bot, msg, match.group(0), user)
        return

    await msg.reply_text(
        "That doesn't look like a Spotify or Apple Music link.",
        parse_mode = ParseMode.HTML,
    )


# ════════════════════════════════════════════════════════════════════════════════
# APPLE MUSIC HANDLER
# ════════════════════════════════════════════════════════════════════════════════

async def _handle_apple_music(bot: Client, msg: Message, url: str, user):
    user_lock = _get_user_lock(user.id)
    if user_lock.locked():
        await msg.reply_text(
            "<blockquote>ᯓ➤<b>You already have a download in progress • • •</b>\n"
            "<i>Please wait or use /cancel to stop it.</i></blockquote>",
            parse_mode       = ParseMode.HTML,
            reply_parameters = ReplyParameters(message_id=msg.id),
        )
        return

    _reset_cancel(user.id)

    async with user_lock:
        # Register user
        try:
            is_new = await mongodb.add_user(
                user_id    = user.id,
                first_name = user.first_name,
                username   = user.username,
                dc_id      = user.dc_id,
            )
            if is_new:
                await log_new_user(bot, user)
        except Exception as e:
            print(f"[db] {e}")

        status_msg = await msg.reply_text(
            "<blockquote>ᯓ➤<b>Processing • • •</b></blockquote>",
            parse_mode       = ParseMode.HTML,
            reply_parameters = ReplyParameters(message_id=msg.id),
        )
        progress   = ThrottledProgress(status_msg)
        master_tmp = tempfile.mkdtemp()
        is_playlist = False
        first_track_name = None
        upload_done = 0
        total = 0

        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=AP_TIMEOUT
            ) as fetch_cl:
                await init_session(fetch_cl)
                token        = await get_token(fetch_cl, url)
                track_forms, album_thumb = await get_all_track_forms(
                    fetch_cl, url, token
                )
                total = len(track_forms)

                if total == 0:
                    await progress.force_update(
                        "<blockquote>❌ <b>No tracks found for that link.</b></blockquote>"
                    )
                    return

                is_playlist = total > 1
                label       = "playlist" if is_playlist else "track"
                phase_msg   = (
                    "\n<i>Downloading all tracks first — uploads begin after…</i>"
                    if is_playlist else ""
                )
                await progress.force_update(
                    f"<blockquote>•ᴗ•<b> Found {total} {label} track{'s' if total > 1 else ''}</b>"
                    f"{phase_msg}</blockquote>"
                )

                # ── Album thumbnail ───────────────────────────────────────────
                album_thumb_path = None
                if album_thumb:
                    try:
                        async with fetch_cl.stream("GET", album_thumb) as _r:
                            _r.raise_for_status()
                            _p = os.path.join(master_tmp, "album_thumb.jpg")
                            with open(_p, "wb") as _f:
                                async for _c in _r.aiter_bytes(CHUNK_SIZE):
                                    _f.write(_c)
                        album_thumb_path = _p
                    except Exception as e:
                        print(f"[AP thumb] {e}")

                async with httpx.AsyncClient(
                    follow_redirects=True,
                    timeout=AP_TIMEOUT,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                ) as dl:
                    per_sem = asyncio.Semaphore(3)

                    # ════════════════════════════════════════════════════════
                    # PHASE 1 — Download ALL tracks
                    # ════════════════════════════════════════════════════════
                    downloaded: list[dict | None] = []
                    dl_done    = 0
                    last_speed = None

                    for i, form in enumerate(track_forms):
                        if _is_cancelled(user.id):
                            break

                        # Throttled bar update before each download
                        await progress.update(
                            _build_progress_bar(
                                dl_done, total, f"Track {i + 1}",
                                "Downloading⬇️", last_speed, "Apple Music",
                            )
                        )

                        idx, links = await get_links_for_track(
                            fetch_cl, form, per_sem, i + 1
                        )

                        # Check cache BEFORE downloading
                        ap_key    = song_index.normalise_apple_url(url) + f":{idx}"
                        cached_doc = await song_index.get_cached_track(
                            "apple_music", ap_key
                        )
                        if cached_doc:
                            print(f"[cache ⚡] AP track {idx}/{total} in index — skip download")
                            downloaded.append({
                                "idx":        idx,
                                "song_name":  cached_doc["name"],
                                "safe_fname": _sanitize_filename(cached_doc["name"]) + ".mp3",
                                "audio_path": None,
                                "thumb_path": None,
                                "speed":      0.0,
                                "cached":     True,
                                "track_id":   ap_key,
                            })
                            dl_done += 1
                            await progress.update(
                                _build_progress_bar(
                                    dl_done, total, cached_doc["name"],
                                    "Cached ⚡", None, "Apple Music", cached=True,
                                )
                            )
                            continue

                        track_dir  = os.path.join(master_tmp, f"track_{i + 1}")
                        os.makedirs(track_dir, exist_ok=True)
                        track_data = await _ap_download_track_to_dir(
                            dl, idx, total, links, album_thumb_path, track_dir
                        )

                        if track_data:
                            track_data["cached"]   = False
                            track_data["track_id"] = ap_key
                            dl_done   += 1
                            last_speed = track_data["speed"]

                        downloaded.append(track_data)

                        # Throttled bar after download
                        song_hint = track_data["song_name"] if track_data else f"Track {i + 1}"
                        await progress.update(
                            _build_progress_bar(
                                dl_done, total, song_hint,
                                "Downloading⬇️", last_speed, "Apple Music",
                            )
                        )

                    # ── Cancelled during download phase ───────────────────────
                    if _is_cancelled(user.id):
                        shutil.rmtree(master_tmp, ignore_errors=True)
                        await flood_safe(
                            msg.reply_text,
                            "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                            parse_mode       = ParseMode.HTML,
                            reply_parameters = ReplyParameters(message_id=msg.id),
                        )
                        await progress.delete()
                        return

                    # ════════════════════════════════════════════════════════
                    # PHASE 2 — Upload ALL tracks
                    # ════════════════════════════════════════════════════════
                    if is_playlist:
                        await progress.force_update(
                            f"<blockquote>"
                            f"✅ <b>All {dl_done}/{total} tracks ready!</b>\n"
                            f"<i>Uploading to Telegram…</i>"
                            f"</blockquote>"
                        )

                    for i, track_data in enumerate(downloaded):
                        if _is_cancelled(user.id):
                            break

                        if track_data is None:
                            await log_track_failed(
                                bot, user, f"Track {i + 1}", i + 1, total,
                                "download failed"
                            )
                            if is_playlist:
                                await flood_safe(
                                    msg.reply_text,
                                    f"<blockquote>⚠️ <b>Track {i + 1}/{total}:"
                                    f"</b> Download failed, skipping.</blockquote>",
                                    parse_mode       = ParseMode.HTML,
                                    reply_parameters = ReplyParameters(message_id=msg.id),
                                )
                            continue

                        idx        = track_data["idx"]
                        song_name  = track_data["song_name"]
                        is_cached  = track_data.get("cached", False)
                        track_id   = track_data["track_id"]

                        if first_track_name is None:
                            first_track_name = song_name

                        # Throttled bar: uploading
                        if is_playlist:
                            await progress.update(
                                _build_progress_bar(
                                    upload_done, total, song_name,
                                    "Uploading⬆️" if not is_cached else "Sending ⚡",
                                    track_data["speed"], "Apple Music",
                                    cached=is_cached,
                                )
                            )

                        ok = await _send_audio_with_cache(
                            bot, msg, user,
                            source     = "apple_music",
                            track_id   = track_id,
                            song_name  = song_name,
                            title      = song_name,
                            artist     = "",
                            audio_path = track_data["audio_path"],
                            thumb_path = track_data["thumb_path"],
                            idx        = idx,
                            total      = total,
                        )
                        if ok:
                            upload_done += 1

                        # Throttled bar after upload
                        if is_playlist:
                            await progress.update(
                                _build_progress_bar(
                                    upload_done, total, song_name,
                                    "Done ✓", track_data["speed"],
                                    "Apple Music", cached=is_cached,
                                )
                            )

                        if is_playlist and (i + 1) < total:
                            await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await progress.force_update(
                f"<blockquote>❌ <b>Error:</b> <i>{e}</i></blockquote>"
            )
            return
        finally:
            shutil.rmtree(master_tmp, ignore_errors=True)

        # ── Final messages ────────────────────────────────────────────────────
        if _is_cancelled(user.id):
            await flood_safe(
                msg.reply_text,
                "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )
        elif is_playlist:
            await flood_safe(
                msg.reply_text,
                f"<blockquote>✅ <b>Playlist done!</b>\n\n"
                f"<b>Sent :</b> {upload_done}/{total} tracks 🎶</blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )

        await log_download_summary(
            bot, user, first_track_name or "Unknown",
            total, upload_done, "Apple Music"
        )
        await progress.delete()


# ════════════════════════════════════════════════════════════════════════════════
# SPOTIFY HANDLER
# ════════════════════════════════════════════════════════════════════════════════

async def _handle_spotify(bot: Client, msg: Message, url: str, user):
    stype = spotify_type(url)

    # ── Single track ──────────────────────────────────────────────────────────
    if stype == "track":
        m        = SPOTIFY_RE.search(url)
        track_id = m.group(2) if m else url

        # Cache check first
        cached_doc = await song_index.get_cached_track("spotify", track_id)
        if cached_doc:
            print(f"[cache ⚡] Spotify {track_id} in index")
            status = await msg.reply_text(
                "<blockquote>⚡ <b>Serving from cache…</b></blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )
            await _send_audio_with_cache(
                bot, msg, user,
                source     = "spotify",
                track_id   = track_id,
                song_name  = cached_doc["name"],
                title      = cached_doc["title"],
                artist     = cached_doc["artist"],
                audio_path = None,
                thumb_path = None,
                idx        = 1,
                total      = 1,
            )
            await status.delete()
            return

        status     = await msg.reply_text(
            "<blockquote>ᯓ➤<b>Fetching track • • •</b></blockquote>",
            parse_mode       = ParseMode.HTML,
            reply_parameters = ReplyParameters(message_id=msg.id),
        )
        local_path = thumb = None
        try:
            loop = asyncio.get_running_loop()
            name, title, artist, track_id, local_path, thumb = (
                await loop.run_in_executor(None, spotify_get_track, url)
            )
            await status.edit_text(
                "<blockquote>⬆️ <b>Uploading…</b></blockquote>",
                parse_mode = ParseMode.HTML,
            )
            await _send_audio_with_cache(
                bot, msg, user,
                source     = "spotify",
                track_id   = track_id,
                song_name  = name,
                title      = title,
                artist     = artist,
                audio_path = local_path,
                thumb_path = thumb,
                idx        = 1,
                total      = 1,
            )
            await status.delete()
        except Exception as e:
            await status.edit_text(
                f"<blockquote>❌ <b>Failed:</b> <code>{e}</code></blockquote>",
                parse_mode = ParseMode.HTML,
            )
        finally:
            cleanup(local_path)
            cleanup(thumb)
        return

    # ── Playlist / album ──────────────────────────────────────────────────────
    if stype not in ("playlist", "album"):
        await msg.reply_text("Unsupported Spotify link type.")
        return

    user_lock = _get_user_lock(user.id)
    if user_lock.locked():
        await msg.reply_text(
            "<blockquote>ᯓ➤<b>You already have a download in progress • • •</b>\n"
            "<i>Please wait or use /cancel to stop it.</i></blockquote>",
            parse_mode       = ParseMode.HTML,
            reply_parameters = ReplyParameters(message_id=msg.id),
        )
        return

    _reset_cancel(user.id)

    async with user_lock:
        status_msg = await msg.reply_text(
            "<blockquote>ᯓ➤<b>Processing playlist • • •</b>\n"
            "<i>Downloading all tracks first — uploads begin after…</i></blockquote>",
            parse_mode       = ParseMode.HTML,
            reply_parameters = ReplyParameters(message_id=msg.id),
        )
        progress   = ThrottledProgress(status_msg)
        main_loop  = asyncio.get_event_loop()
        total_ref  = [0]
        speed_ref  = [None]

        # ── Async queue drainer for download-phase progress ───────────────────
        _dl_q: asyncio.Queue = asyncio.Queue()

        def on_progress(done, total, name, speed, err):
            total_ref[0] = total
            if speed:
                speed_ref[0] = speed
            asyncio.run_coroutine_threadsafe(
                _dl_q.put((done, total, name, speed, err)), main_loop
            )

        # ════════════════════════════════════════════════════════════════════
        # PHASE 1 — Download ALL tracks (in thread executor)
        # ════════════════════════════════════════════════════════════════════
        try:
            loop = asyncio.get_running_loop()

            async def _drain():
                while True:
                    item = await _dl_q.get()
                    if item is None:
                        break
                    done, total, name, speed, err = item
                    # Throttled — only updates if 10 s have passed
                    await progress.update(
                        _build_progress_bar(
                            done, total, name,
                            "Downloading⬇️" if not err else "Failed⚠️",
                            speed, "Spotify",
                        )
                    )

            drain_task  = asyncio.ensure_future(_drain())
            all_results = await loop.run_in_executor(
                None,
                lambda: spotify_get_playlist_all(url, on_progress=on_progress),
            )
            await _dl_q.put(None)
            await drain_task

        except Exception as e:
            await progress.force_update(
                f"<blockquote>❌ <b>Error:</b> <code>{e}</code></blockquote>"
            )
            return

        # Cancelled during download?
        if _is_cancelled(user.id):
            for r in all_results:
                cleanup(r[5]); cleanup(r[6])   # local_path, thumb
            await flood_safe(
                msg.reply_text,
                "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )
            await progress.delete()
            return

        total      = total_ref[0]
        dl_success = sum(1 for r in all_results if r[7] is None)  # err @ index 7

        # Force-update once at phase transition
        await progress.force_update(
            f"<blockquote>"
            f"✅ <b>All {dl_success}/{total} tracks downloaded!</b>\n"
            f"<i>Uploading to Telegram…</i>"
            f"</blockquote>"
        )

        # ════════════════════════════════════════════════════════════════════
        # PHASE 2 — Upload ALL tracks
        # ════════════════════════════════════════════════════════════════════
        completed        = 0
        failed           = 0
        first_track_name = None

        for i, result in enumerate(all_results):
            if _is_cancelled(user.id):
                for r in all_results[i:]:
                    cleanup(r[5]); cleanup(r[6])
                break

            # Unpack: (index, name, title, artist, track_id,
            #          local_path, thumb, err, speed, total)
            (_, name, title, artist, track_id,
             local_path, thumb, err, speed, _total) = result

            if first_track_name is None:
                first_track_name = name

            # Use URL-extracted track_id as fallback
            if not track_id:
                m = SPOTIFY_RE.search(url)
                track_id = m.group(2) if m else url

            if err:
                print(f"[Spotify] skip {name}: {err}")
                failed += 1
                await log_track_failed(bot, user, name, i + 1, total, err)
                # Throttled bar
                await progress.update(
                    _build_progress_bar(
                        completed + failed, total, name,
                        "Skipped⚠️", speed_ref[0], "Spotify",
                    )
                )
                cleanup(local_path); cleanup(thumb)
                continue

            # Throttled bar: uploading
            await progress.update(
                _build_progress_bar(
                    completed + failed, total, name,
                    "Uploading⬆️", speed or speed_ref[0], "Spotify",
                )
            )

            ok = await _send_audio_with_cache(
                bot, msg, user,
                source     = "spotify",
                track_id   = track_id,
                song_name  = name,
                title      = title,
                artist     = artist,
                audio_path = local_path,
                thumb_path = thumb,
                idx        = i + 1,
                total      = total,
            )
            if ok:
                completed += 1
            else:
                failed += 1

            # Always clean up local files after send attempt
            cleanup(local_path)
            cleanup(thumb)

            # Throttled bar after upload
            await progress.update(
                _build_progress_bar(
                    completed + failed, total, name,
                    "Done ✓", speed or speed_ref[0], "Spotify",
                )
            )

            if (i + 1) < total:
                await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

        # ── Final summary ─────────────────────────────────────────────────────
        if _is_cancelled(user.id):
            await flood_safe(
                msg.reply_text,
                "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )
        else:
            await flood_safe(
                msg.reply_text,
                f"<blockquote>✅ <b>Playlist done!</b>\n\n"
                f"<b>Sent :</b> {completed}/{total} tracks 🎶</blockquote>",
                parse_mode       = ParseMode.HTML,
                reply_parameters = ReplyParameters(message_id=msg.id),
            )

        await log_download_summary(
            bot, user, first_track_name or "Unknown",
            total, completed, "Spotify"
        )
        await progress.delete()


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

async def main():
    _start_health_server(port=8080)

    bot = Client(
        "music_bot",
        api_id    = config.API_ID,
        api_hash  = config.API_HASH,
        bot_token = config.BOT_TOKEN,
    )

    bot.add_handler(MessageHandler(cmd_start,  filters.command("start")  & filters.private))
    bot.add_handler(MessageHandler(cmd_cancel, filters.command("cancel") & filters.private))
    bot.add_handler(CallbackQueryHandler(cb_credits, filters.regex("^credits$")))
    bot.add_handler(MessageHandler(
        handle_message,
        filters.text & filters.private & ~filters.command(["start", "cancel"]),
    ))

    # Connect MongoDB first, then reuse its client for the song index
    await mongodb.connect()
    await song_index.connect(client=mongodb.client)

    await bot.start()
    print("[bot] running — waiting for messages…")
    await idle()
    await bot.stop()

    await mongodb.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
