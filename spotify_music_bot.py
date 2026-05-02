import os
import re
import json
import asyncio
import base64
import tempfile
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
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
MAX_WORKERS = 5
CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB
PLAYLIST_UPLOAD_DELAY = 4.0

SPOTIFY_RE = re.compile(
    r"https?://open\.spotify\.com/(track|playlist|album)/[A-Za-z0-9]+"
)
APPLE_MUSIC_RE = re.compile(r"https?://music\.apple\.com/\S+")

# Progress bar update interval in seconds
PROGRESS_UPDATE_INTERVAL = 10.0

# ── Concurrency (Apple Music) ─────────────────────────────────────────────────

GLOBAL_AP_SEMAPHORE = asyncio.Semaphore(15)

_USER_LOCKS: dict[int, asyncio.Lock] = {}


def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _USER_LOCKS:
        _USER_LOCKS[user_id] = asyncio.Lock()
    return _USER_LOCKS[user_id]


# ── Cancellation tokens ───────────────────────────────────────────────────────

# Maps user_id → asyncio.Event  (set = cancelled)
_CANCEL_FLAGS: dict[int, asyncio.Event] = {}


def _get_cancel_flag(user_id: int) -> asyncio.Event:
    if user_id not in _CANCEL_FLAGS:
        _CANCEL_FLAGS[user_id] = asyncio.Event()
    return _CANCEL_FLAGS[user_id]


def _reset_cancel_flag(user_id: int):
    """Clear the cancel flag before starting a new download."""
    flag = _get_cancel_flag(user_id)
    flag.clear()


def _raise_if_cancelled(user_id: int):
    """Raise CancelledError if the user has requested cancellation."""
    if _get_cancel_flag(user_id).is_set():
        raise asyncio.CancelledError("Cancelled by user")


def _is_cancelled(user_id: int) -> bool:
    return _get_cancel_flag(user_id).is_set()


# ── Koyeb health-check server ─────────────────────────────────────────────────

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, *args):
        pass


def _start_health_server(port: int = 8080):
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[health] listening on port {port}")


# ── Shared helpers ────────────────────────────────────────────────────────────

def cleanup(path: str):
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
        InlineKeyboardButton("Dev", url=config.DEV_URL, style=ButtonStyle.PRIMARY),
        InlineKeyboardButton("Credits", callback_data="credits", style=ButtonStyle.PRIMARY),
    ]])


# ── Progress bar builder ──────────────────────────────────────────────────────

def _build_progress_bar(
    done: int,
    total: int,
    current_song: str,
    status_label: str,   # e.g. "Downloading⬇️" or "Uploading⬆️"
    speed_mbps: float | None,
    engine: str,         # "Spotify" or "Apple Music"
) -> str:
    pct = int((done / total) * 100) if total else 0
    bar_len = 17
    filled = int(bar_len * done / total) if total else 0
    bar = "█" * filled + "░" * (bar_len - filled)
    speed_str = f"{speed_mbps:.2f} MB/s" if speed_mbps is not None else "—"
    # Truncate long song names
    display_name = current_song if len(current_song) <= 35 else current_song[:32] + "…"
    return (
        f"<blockquote>"
        f"<b>Downloaded 🎵 {done}/{total}</b>\n"
        f"<b>Current Progress:</b> {display_name}\n"
        f"╭────────────────────╮\n"
        f"│ {bar} │ {pct}%\n"
        f"╰────────────────────╯\n"
        f"<b>Status:</b> {status_label}\n"
        f"<b>Speed:</b> {speed_str}\n"
        f"<b>Engine:</b> {engine}"
        f"</blockquote>"
    )


# ── Throttled progress updater ────────────────────────────────────────────────

class ThrottledProgress:
    """
    Wraps a Pyrogram message and exposes `update(...)` which only actually
    edits the message at most once every PROGRESS_UPDATE_INTERVAL seconds.
    """

    def __init__(self, status_msg):
        self._msg = status_msg
        self._last_edit: float = 0.0

    async def update(self, text: str, force: bool = False):
        now = time.monotonic()
        if not force and (now - self._last_edit) < PROGRESS_UPDATE_INTERVAL:
            return
        try:
            await self._msg.edit_text(text, parse_mode=ParseMode.HTML)
            self._last_edit = now
        except Exception:
            pass

    async def force_update(self, text: str):
        await self.update(text, force=True)

    async def delete(self):
        try:
            await self._msg.delete()
        except Exception:
            pass


# ── Logging ───────────────────────────────────────────────────────────────────

async def log_new_user(bot: Client, user) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
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


async def log_download(bot: Client, user, name: str) -> None:
    if not config.LOG_CHANNEL:
        return
    tag   = user_tag(user)
    uname = user.first_name + (f" {user.last_name}" if user.last_name else "")
    text  = (
        "<blockquote>"
        "🎵 <b>Track Downloaded</b>\n\n"
        f"<b>User     :</b>  <b>{uname}</b>  ({tag})\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n\n"
        f"<b>Track    :</b>  <i>{name}</i>"
        "</blockquote>"
    )
    try:
        await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[log] download: {e}")


async def log_download_summary(bot: Client, user, first_track: str, total: int) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    username = f"@{user.username}" if user.username else "<i>None</i>"
    text = (
        "<blockquote>"
        "⬇️ <b>Download Complete</b>\n\n"
        f"<b>User     :</b>  <b>{name}</b>\n"
        f"<b>ID       :</b>  <code>{user.id}</code>\n"
        f"<b>Username :</b>  {username}\n"
        f"<b>Track    :</b>  {first_track}\n"
        f"<b>Total    :</b>  {total} track{'s' if total > 1 else ''}"
        "</blockquote>"
    )
    try:
        await bot.send_message(config.LOG_CHANNEL, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[Log] Failed to send download summary: {e}")


async def log_track_to_channel(
    bot: Client, user, song_name: str, idx: int, total: int,
    audio_path: str, thumb_path: str | None,
) -> None:
    if not config.LOG_CHANNEL:
        return
    name = user.first_name + (f" {user.last_name}" if user.last_name else "")
    caption = (
        "<blockquote>"
        f"🎵 <b>{song_name}</b>\n"
        f"<b>By      :</b> {name} (<code>{user.id}</code>)\n"
        f"<b>Track   :</b> {idx}/{total}"
        "</blockquote>"
    )
    try:
        if thumb_path:
            await bot.send_photo(
                config.LOG_CHANNEL, photo=thumb_path,
                caption=caption, parse_mode=ParseMode.HTML,
            )
        await bot.send_audio(
            config.LOG_CHANNEL, audio=audio_path, title=song_name,
            file_name=f"{song_name}.mp3",
            caption=caption if not thumb_path else None,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        print(f"[Log] Failed to send track to log channel: {e}")


# ════════════════════════════════════════════════════════════════════════════════
# SPOTIFY
# ════════════════════════════════════════════════════════════════════════════════

def _make_spoti_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Referer": SPOTI_BASE + "/en2",
        "X-Requested-With": "XMLHttpRequest",
    })
    r = s.get(SPOTI_BASE + "/en2", timeout=15)
    soup = BeautifulSoup(r.text, "html.parser")
    hidden = soup.find("input", {"type": "hidden", "name": re.compile(r"^_")})
    s._csrf = {hidden["name"]: hidden["value"]}
    return s


def _spoti_fetch_action(s: requests.Session, spotify_url: str) -> str:
    r = s.post(SPOTI_BASE + "/action", data={
        "url": spotify_url,
        "g-recaptcha-response": "faketoken",
        **s._csrf,
    }, timeout=20)
    resp = r.json()
    if resp.get("error"):
        raise Exception(resp.get("message", "unknown error"))
    return resp["data"]


def _spoti_parse_forms(html: str):
    soup = BeautifulSoup(html, "html.parser")
    forms = soup.find_all("form", {"name": "submitspurl"})
    result = []
    for form in forms:
        fields = {}
        for inp in form.find_all("input"):
            if inp.get("name"):
                fields[inp["name"]] = inp.get("value", "")
        result.append(fields)
    img = soup.find("img")
    fallback_thumb = img["src"] if img else None
    return result, fallback_thumb


def _spoti_download_thumb(url: str, name: str):
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
    safe = re.sub(r'[\\/*?:"<>|]', "", name)[:100]
    path = os.path.join(DOWNLOAD_DIR, f"{safe}.mp3")
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
    speed = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise RuntimeError("downloaded file is empty")
    return path, speed


def _spoti_fetch_one(s: requests.Session, form_data: dict, index: int,
                     fallback_thumb: str | None = None):
    try:
        info      = json.loads(base64.b64decode(form_data.get("data", "")).decode())
        title     = info.get("name", f"Track {index + 1}")
        artist    = info.get("artist", "")
        name      = f"{title} - {artist}" if artist else title
        thumb_url = info.get("cover") or info.get("image") or info.get("thumb") or fallback_thumb
    except Exception:
        title, artist, name, thumb_url = f"Track {index + 1}", "", f"Track {index + 1}", fallback_thumb

    r = s.post(SPOTI_BASE + "/action/track", data=form_data, timeout=30)
    resp = r.json()
    if resp.get("error"):
        return index, name, title, artist, None, None, resp.get("message"), 0.0

    soup = BeautifulSoup(resp["data"], "html.parser")
    img = soup.find("img")
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
        return index, name, title, artist, None, None, "no link found", 0.0

    try:
        local_path, speed = _spoti_download_file(href, name)
    except Exception as e:
        return index, name, title, artist, None, None, f"download failed: {e}", 0.0

    local_thumb = _spoti_download_thumb(thumb_url, name)
    return index, name, title, artist, local_path, local_thumb, None, speed


def spotify_get_track(spotify_url: str):
    s = _make_spoti_session()
    html = _spoti_fetch_action(s, spotify_url)
    forms, fallback_thumb = _spoti_parse_forms(html)
    if not forms:
        raise Exception("no track found")
    _, name, title, artist, local_path, thumb, err, _ = _spoti_fetch_one(
        s, forms[0], 0, fallback_thumb
    )
    if err:
        raise Exception(err)
    return name, title, artist, local_path, thumb


def spotify_get_playlist_all(spotify_url: str, on_download_progress=None):
    """
    Downloads ALL tracks first one-by-one, collecting results.
    Calls on_download_progress(done, total, name, speed) after each download.
    Returns list of result tuples: (index, name, title, artist, local_path, thumb, err, speed)
    """
    s = _make_spoti_session()
    html = _spoti_fetch_action(s, spotify_url)
    forms, fallback_thumb = _spoti_parse_forms(html)
    total = len(forms)
    results = []

    for i, form in enumerate(forms):
        result = _spoti_fetch_one(s, form, i, fallback_thumb)
        results.append(result + (total,))  # append total as last element
        if on_download_progress:
            _, name, _, _, _, _, err, speed = result
            on_download_progress(i + 1, total, name, speed, err)

    return results


def spotify_type(url: str) -> str:
    if "/track/" in url:
        return "track"
    if "/playlist/" in url:
        return "playlist"
    if "/album/" in url:
        return "album"
    return "unknown"


# ════════════════════════════════════════════════════════════════════════════════
# APPLE MUSIC
# ════════════════════════════════════════════════════════════════════════════════

def _parse_ap_filename(response: httpx.Response, fallback: str) -> str:
    cd = response.headers.get("content-disposition", "")
    m = re.search(r"filename\*=UTF-8''([^\s;]+)", cd, re.IGNORECASE)
    if m:
        return urllib.parse.unquote(m.group(1))
    m = re.search(r"""filename=["']?([^"';]+)["']?""", cd, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return fallback


async def _ap_download_file(
    client: httpx.AsyncClient, url: str, dest_dir: str, fallback_name: str
) -> tuple[bool, str, float]:
    """Returns (success, dest_path, speed_mbps)."""
    try:
        start = time.monotonic()
        total_bytes = 0
        async with client.stream("GET", url, headers=AP_HEADERS, follow_redirects=True) as r:
            r.raise_for_status()
            fname = _parse_ap_filename(r, fallback_name)
            fname = re.sub(r'[\\/*?:"<>|]', "_", fname)
            dest = os.path.join(dest_dir, fname)
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes(CHUNK_SIZE):
                    f.write(chunk)
                    total_bytes += len(chunk)
        elapsed = time.monotonic() - start
        speed = (total_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0
        return True, dest, speed
    except Exception as e:
        print(f"[AP Download] {url} failed: {e}")
        return False, "", 0.0


_EMOJI_RE = re.compile(
    "["
    "\U0001F000-\U0001FFFF"
    "\U00002700-\U000027BF"
    "\u2600-\u26FF"
    "\u2300-\u23FF"
    "]+",
    flags=re.UNICODE,
)
_DOMAIN_PREFIX_RE = re.compile(
    r'^(?:[\w\-]+\.)+[a-zA-Z]{2,10}[\s\-\u2013\u2014]*',
    re.IGNORECASE,
)
_APLMATE_RE = re.compile(
    r'(?i)'
    r'apl\s*mate\s*'
    r'(?:\.com?)?\s*'
    r'[-\u2013\u2014\s]*',
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
    audio = None
    cover = None
    for item in links:
        label = item["quality"].lower()
        if "cover" in label:
            cover = item
        elif audio is None:
            audio = item
    return audio, cover


# ── Apple Music: download one track to a persistent temp dir ─────────────────

async def _ap_download_track_to_dir(
    dl: httpx.AsyncClient,
    idx: int,
    total: int,
    links: list[dict],
    album_thumb_path: str | None,
    dest_dir: str,
) -> dict | None:
    """
    Downloads audio (and optional cover) for a single AP track into dest_dir.
    Returns a dict with track info, or None on failure.
    """
    audio_entry, cover_entry = _split_ap_links(links)

    if not audio_entry:
        print(f"[AP Download] Track {idx}/{total}: No audio link found.")
        return None

    ok, raw_audio_path, dl_speed = await _ap_download_file(
        dl, audio_entry["link"], dest_dir, f"track_{idx}.mp3"
    )
    if not ok:
        print(f"[AP Download] Track {idx}/{total}: Audio download failed.")
        return None

    raw_name = os.path.splitext(os.path.basename(raw_audio_path))[0]
    song_name = _clean_ap_song_name(raw_name)
    print(f"[AP Download] Track {idx}/{total} Raw : {raw_name}")
    print(f"[AP Download] Track {idx}/{total} Name: {song_name}")

    safe_fname = _sanitize_filename(song_name) + ".mp3"
    audio_path = os.path.join(dest_dir, safe_fname)
    if raw_audio_path != audio_path:
        os.rename(raw_audio_path, audio_path)

    thumb_path = album_thumb_path
    if cover_entry:
        c_ok, cover_path, _ = await _ap_download_file(
            dl, cover_entry["link"], dest_dir, f"cover_{idx}.jpg"
        )
        if c_ok:
            if not cover_path.lower().endswith((".jpg", ".jpeg")):
                jpg_path = cover_path + ".jpg"
                os.rename(cover_path, jpg_path)
                cover_path = jpg_path
            thumb_path = cover_path

    return {
        "idx": idx,
        "song_name": song_name,
        "safe_fname": safe_fname,
        "audio_path": audio_path,
        "thumb_path": thumb_path,
        "speed": dl_speed,
    }


# ════════════════════════════════════════════════════════════════════════════════
# HANDLERS
# ════════════════════════════════════════════════════════════════════════════════

async def cmd_start(bot: Client, msg: Message):
    user = msg.from_user
    try:
        if await mongodb.is_new_user(user.id):
            await mongodb.add_user(
                user_id=user.id,
                first_name=user.first_name,
                username=user.username,
                dc_id=user.dc_id,
            )
            await log_new_user(bot, user)
    except Exception as e:
        print(f"[db] {e}")

    await msg.reply_text(
        "<blockquote>\n"
        "<b>Hey 👋</b>\n"
        "<b>I can download Spotify and Apple Music tracks for you.</b>\n\n"
        "<i>Just paste any Spotify or Apple Music link below — single track, album, or playlist.</i>\n"
        "</blockquote>",
        parse_mode=ParseMode.HTML,
        reply_markup=start_keyboard(),
    )


async def cmd_cancel(bot: Client, msg: Message):
    """Cancel any ongoing download for this user."""
    user = msg.from_user
    lock = _get_user_lock(user.id)

    if not lock.locked():
        await msg.reply_text(
            "<blockquote>ℹ️ <b>No active download to cancel.</b></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=msg.id),
        )
        return

    _get_cancel_flag(user.id).set()
    await msg.reply_text(
        "<blockquote>🛑 <b>Cancellation requested.</b>\n"
        "<i>The current download will stop after the track in progress finishes.</i></blockquote>",
        parse_mode=ParseMode.HTML,
        reply_parameters=ReplyParameters(message_id=msg.id),
    )


async def cb_credits(_, cb: CallbackQuery):
    await cb.answer()
    await cb.message.reply_text(
        "<blockquote>\n"
        "<b>Credits</b>\n\n"
        "<i>This bot is made by</i> <b>Mr. D</b>, <b>Mark</b>, <b>Abhinai</b>.\n\n"
        "<i>Mr. D and Mark did most of the heavy lifting - if it helps you, just give credit. That's all.</i>\n"
        "</blockquote>",
        parse_mode=ParseMode.HTML,
    )


async def handle_message(bot: Client, msg: Message):
    text = msg.text.strip()
    user = msg.from_user

    # ── Route: Apple Music ────────────────────────────────────────────────────
    if "music.apple.com" in text:
        await _handle_apple_music(bot, msg, text, user)
        return

    # ── Route: Spotify ────────────────────────────────────────────────────────
    match = SPOTIFY_RE.search(text)
    if match:
        await _handle_spotify(bot, msg, match.group(0), user)
        return

    await msg.reply_text(
        "That doesn't look like a Spotify or Apple Music link.",
        parse_mode=ParseMode.HTML,
    )


# ── Apple Music handler ───────────────────────────────────────────────────────

async def _handle_apple_music(bot: Client, msg: Message, url: str, user):
    user_lock = _get_user_lock(user.id)
    if user_lock.locked():
        await msg.reply_text(
            "<blockquote>ᯓ➤<b>You already have a download in progress • • •</b>\n"
            "<i>Please wait for it to finish or use /cancel to stop it.</i></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=msg.id),
        )
        return

    _reset_cancel_flag(user.id)

    async with user_lock:
        try:
            is_new = await mongodb.add_user(
                user_id=user.id,
                first_name=user.first_name,
                username=user.username,
                dc_id=user.dc_id,
            )
            if is_new:
                await log_new_user(bot, user)
        except Exception as e:
            print(f"[db] apple music add_user: {e}")

        status_msg = await msg.reply_text(
            "<blockquote>ᯓ➤<b>Processing • • •</b></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=msg.id),
        )
        progress = ThrottledProgress(status_msg)

        is_playlist = False
        # Persistent temp dir that lives across all download + upload phases
        master_tmp = tempfile.mkdtemp()

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=AP_TIMEOUT) as fetch_cl:
                await init_session(fetch_cl)
                token = await get_token(fetch_cl, url)
                track_forms, album_thumb = await get_all_track_forms(fetch_cl, url, token)

                total = len(track_forms)
                print(f"\n[AP Flow] URL: {url} | Tracks: {total} | Thumb: {album_thumb}")

                if total == 0:
                    await progress.force_update(
                        "<blockquote>❌ <b>No tracks found for that link.</b></blockquote>",
                    )
                    return

                is_playlist = total > 1
                label = "playlist" if is_playlist else "track"
                await progress.force_update(
                    f"<blockquote>•ᴗ•<b> Processing {label}</b> ◌ {total} track{'s' if total > 1 else ''}…"
                    + ("\n<i>Downloading all tracks first, uploads will begin after…</i>" if is_playlist else "")
                    + "</blockquote>",
                )

                # ── Download album thumbnail ───────────────────────────────────
                album_thumb_path = None
                if album_thumb:
                    try:
                        async with fetch_cl.stream("GET", album_thumb) as _r:
                            _r.raise_for_status()
                            _ap = os.path.join(master_tmp, "album_thumb.jpg")
                            with open(_ap, "wb") as _f:
                                async for _chunk in _r.aiter_bytes(CHUNK_SIZE):
                                    _f.write(_chunk)
                        album_thumb_path = _ap
                    except Exception as e:
                        print(f"[AP Thumb] Album art failed: {e}")

                async with httpx.AsyncClient(
                    follow_redirects=True,
                    timeout=AP_TIMEOUT,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                ) as dl:
                    per_request_sem = asyncio.Semaphore(3)

                    # ════════════════════════════════════════════════════
                    # PHASE 1 — Download ALL tracks sequentially
                    # ════════════════════════════════════════════════════
                    downloaded_tracks: list[dict | None] = []
                    dl_done = 0
                    last_speed = None

                    for i, form in enumerate(track_forms):
                        if _is_cancelled(user.id):
                            break

                        # Update progress bar: Downloading
                        if is_playlist:
                            bar_text = _build_progress_bar(
                                dl_done, total,
                                f"Track {i + 1}",
                                "Downloading⬇️",
                                last_speed, "Apple Music",
                            )
                            await progress.update(bar_text)

                        idx, links = await get_links_for_track(
                            fetch_cl, form, per_request_sem, i + 1
                        )

                        track_dir = os.path.join(master_tmp, f"track_{i + 1}")
                        os.makedirs(track_dir, exist_ok=True)

                        track_data = await _ap_download_track_to_dir(
                            dl, idx, total, links, album_thumb_path, track_dir
                        )

                        downloaded_tracks.append(track_data)

                        if track_data:
                            dl_done += 1
                            last_speed = track_data["speed"]

                        # Force-update bar after each download
                        if is_playlist:
                            song_hint = track_data["song_name"] if track_data else f"Track {i + 1}"
                            bar_text = _build_progress_bar(
                                dl_done, total,
                                song_hint,
                                "Downloading⬇️",
                                last_speed, "Apple Music",
                            )
                            await progress.force_update(bar_text)

                    if _is_cancelled(user.id):
                        # Clean up any downloaded files
                        import shutil
                        shutil.rmtree(master_tmp, ignore_errors=True)
                        await flood_safe(
                            msg.reply_text,
                            "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                            parse_mode=ParseMode.HTML,
                            reply_parameters=ReplyParameters(message_id=msg.id),
                        )
                        await progress.delete()
                        return

                    # ════════════════════════════════════════════════════
                    # PHASE 2 — Upload ALL tracks sequentially
                    # ════════════════════════════════════════════════════
                    if is_playlist:
                        await progress.force_update(
                            f"<blockquote>"
                            f"✅ <b>All {dl_done}/{total} tracks downloaded!</b>\n"
                            f"<i>Now uploading to Telegram…</i>"
                            f"</blockquote>"
                        )

                    first_track_name = None
                    upload_done = 0

                    for i, track_data in enumerate(downloaded_tracks):
                        if _is_cancelled(user.id):
                            break

                        if track_data is None:
                            print(f"[AP Upload] Track {i + 1}/{total} skipped (download failed).")
                            if is_playlist:
                                await flood_safe(
                                    msg.reply_text,
                                    f"<blockquote>⚠️ <b>Track {i + 1}/{total}:</b> Download had failed, skipping.</blockquote>",
                                    parse_mode=ParseMode.HTML,
                                    reply_parameters=ReplyParameters(message_id=msg.id),
                                )
                            continue

                        idx        = track_data["idx"]
                        song_name  = track_data["song_name"]
                        safe_fname = track_data["safe_fname"]
                        audio_path = track_data["audio_path"]
                        thumb_path = track_data["thumb_path"]

                        if first_track_name is None:
                            first_track_name = song_name

                        # Update progress bar: Uploading
                        if is_playlist:
                            bar_text = _build_progress_bar(
                                upload_done, total,
                                song_name,
                                "Uploading⬆️",
                                track_data["speed"], "Apple Music",
                            )
                            await progress.update(bar_text)

                        caption = (
                            f"<blockquote>"
                            f"🎵 <b>{song_name}</b>"
                            f"</blockquote>"
                        )

                        try:
                            if thumb_path and os.path.exists(thumb_path):
                                await flood_safe(
                                    bot.send_photo,
                                    chat_id=msg.chat.id,
                                    photo=thumb_path,
                                    caption=caption,
                                    parse_mode=ParseMode.HTML,
                                    reply_parameters=ReplyParameters(message_id=msg.id),
                                )

                            await flood_safe(
                                bot.send_audio,
                                chat_id=msg.chat.id,
                                audio=audio_path,
                                title=song_name,
                                file_name=safe_fname,
                                reply_parameters=ReplyParameters(message_id=msg.id),
                            )
                            print(f"[AP Upload] ✓ Track {idx}/{total} sent.")
                            upload_done += 1

                            # Force bar update after each upload
                            if is_playlist:
                                bar_text = _build_progress_bar(
                                    upload_done, total,
                                    song_name,
                                    "Uploading⬆️",
                                    track_data["speed"], "Apple Music",
                                )
                                await progress.force_update(bar_text)

                            await log_track_to_channel(bot, user, song_name, idx, total, audio_path, thumb_path)

                        except Exception as e:
                            print(f"[AP Upload] ✗ Track {idx}/{total} failed: {e}")
                            await flood_safe(
                                msg.reply_text,
                                f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> Upload failed — <i>{e}</i></blockquote>",
                                parse_mode=ParseMode.HTML,
                                reply_parameters=ReplyParameters(message_id=msg.id),
                            )

                        if is_playlist and (i + 1) < total:
                            await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            await progress.force_update(
                f"<blockquote>❌ <b>Failed.</b>\n<i>{e}</i></blockquote>",
            )
            return
        finally:
            # Clean up all temp files
            import shutil
            shutil.rmtree(master_tmp, ignore_errors=True)

        # ── Post-download messages ────────────────────────────────────────────
        if _is_cancelled(user.id):
            await flood_safe(
                msg.reply_text,
                "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )
            await progress.delete()
            return

        if is_playlist:
            await flood_safe(
                msg.reply_text,
                f"<blockquote>"
                f"✅ <b>Playlist done!</b>\n\n"
                f"<b>Total :</b>  {upload_done} track{'s' if upload_done != 1 else ''} sent 🎶"
                f"</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )

        await log_download_summary(bot, user, first_track_name or "Unknown", total)
        await progress.delete()


# ── Spotify handler ───────────────────────────────────────────────────────────

async def _handle_spotify(bot: Client, msg: Message, url: str, user):
    stype = spotify_type(url)

    if stype == "track":
        status = await msg.reply_text("fetching track...")
        local_path = thumb = None
        try:
            loop = asyncio.get_running_loop()
            name, title, artist, local_path, thumb = await loop.run_in_executor(
                None, spotify_get_track, url
            )
            await status.edit_text("uploading...")
            if thumb:
                await msg.reply_photo(photo=thumb, caption=f"<b>{name}</b>", parse_mode=ParseMode.HTML)
            await msg.reply_audio(
                audio=local_path,
                title=title,
                performer=artist,
                thumb=thumb,
                parse_mode=ParseMode.HTML,
            )
            await status.delete()
            await log_download(bot, user, name)

        except Exception as e:
            await status.edit_text(
                f"something went wrong\n\n<code>{e}</code>",
                parse_mode=ParseMode.HTML,
            )
        finally:
            cleanup(local_path)
            cleanup(thumb)

    elif stype in ("playlist", "album"):
        user_lock = _get_user_lock(user.id)
        if user_lock.locked():
            await msg.reply_text(
                "<blockquote>ᯓ➤<b>You already have a download in progress • • •</b>\n"
                "<i>Please wait for it to finish or use /cancel to stop it.</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )
            return

        _reset_cancel_flag(user.id)

        async with user_lock:
            status_msg = await msg.reply_text(
                "<blockquote>ᯓ➤<b>Processing playlist • • •</b>\n"
                "<i>Downloading all tracks first, uploads will begin after…</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )
            progress = ThrottledProgress(status_msg)

            main_loop  = asyncio.get_event_loop()
            total_ref  = [0]
            dl_done_ref = [0]
            speed_ref  = [None]

            # ── Thread-safe download progress callback ─────────────────────────
            # Called from executor thread after each track is downloaded.
            # Uses an event to signal the async side so the progress bar updates.
            _dl_progress_queue: asyncio.Queue = asyncio.Queue()

            def on_download_progress(done, total, name, speed, err):
                total_ref[0] = total
                if speed:
                    speed_ref[0] = speed
                if not err:
                    dl_done_ref[0] = done
                # Push update to async queue (thread-safe)
                asyncio.run_coroutine_threadsafe(
                    _dl_progress_queue.put((done, total, name, speed, err)),
                    loop=main_loop,
                )

            # ════════════════════════════════════════════════════════════
            # PHASE 1 — Download ALL tracks in executor thread
            # ════════════════════════════════════════════════════════════
            try:
                loop = asyncio.get_running_loop()

                # Spawn a task to drain progress updates while downloading
                async def _drain_dl_progress():
                    while True:
                        item = await _dl_progress_queue.get()
                        if item is None:
                            break
                        done, total, name, speed, err = item
                        bar_text = _build_progress_bar(
                            done, total, name,
                            "Downloading⬇️" if not err else "Failed⚠️",
                            speed, "Spotify",
                        )
                        await progress.force_update(bar_text)

                drain_task = asyncio.ensure_future(_drain_dl_progress())

                all_results = await loop.run_in_executor(
                    None,
                    lambda: spotify_get_playlist_all(url, on_download_progress=on_download_progress),
                )

                # Signal drain task to stop
                await _dl_progress_queue.put(None)
                await drain_task

            except Exception as e:
                await progress.force_update(
                    f"<blockquote>❌ something went wrong\n\n<code>{e}</code></blockquote>",
                )
                return

            if _is_cancelled(user.id):
                # Clean up all downloaded files
                for result in all_results:
                    index, name, title, artist, local_path, thumb, err, speed, total = result
                    cleanup(local_path)
                    cleanup(thumb)
                await flood_safe(
                    msg.reply_text,
                    "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=msg.id),
                )
                await progress.delete()
                return

            total = total_ref[0]
            dl_success = sum(1 for r in all_results if r[6] is None)  # err is index 6

            # Announce upload phase
            await progress.force_update(
                f"<blockquote>"
                f"✅ <b>All {dl_success}/{total} tracks downloaded!</b>\n"
                f"<i>Now uploading to Telegram…</i>"
                f"</blockquote>"
            )

            # ════════════════════════════════════════════════════════════
            # PHASE 2 — Upload ALL tracks sequentially
            # ════════════════════════════════════════════════════════════
            completed = 0
            failed    = 0

            for i, result in enumerate(all_results):
                if _is_cancelled(user.id):
                    # Clean up remaining files
                    for r in all_results[i:]:
                        cleanup(r[4])  # local_path
                        cleanup(r[5])  # thumb
                    break

                index, name, title, artist, local_path, thumb, err, speed, total = result

                if err:
                    print(f"[Spotify skip] {name}: {err}")
                    failed += 1
                    bar_text = _build_progress_bar(
                        completed + failed, total, name, "Skipped⚠️",
                        speed_ref[0], "Spotify",
                    )
                    await progress.update(bar_text)
                    continue

                # Update bar: uploading
                bar_text = _build_progress_bar(
                    completed + failed, total, name, "Uploading⬆️",
                    speed if speed else speed_ref[0], "Spotify",
                )
                await progress.update(bar_text)

                try:
                    await msg.reply_audio(
                        audio=local_path,
                        caption=f"<b>{name}</b>",
                        title=title,
                        performer=artist,
                        thumb=thumb,
                        parse_mode=ParseMode.HTML,
                    )
                    completed += 1
                    await log_download(bot, user, name)

                    bar_text = _build_progress_bar(
                        completed + failed, total, name, "Uploading⬆️",
                        speed if speed else speed_ref[0], "Spotify",
                    )
                    await progress.force_update(bar_text)

                except Exception as e:
                    print(f"[Spotify send] {name}: {e}")
                    failed += 1
                finally:
                    cleanup(local_path)
                    cleanup(thumb)

                if (i + 1) < total:
                    await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

            # ── Final summary ─────────────────────────────────────────────────
            if _is_cancelled(user.id):
                await flood_safe(
                    msg.reply_text,
                    "<blockquote>🛑 <b>Download cancelled.</b></blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=msg.id),
                )
            else:
                await flood_safe(
                    msg.reply_text,
                    f"<blockquote>"
                    f"✅ <b>Playlist done!</b>\n\n"
                    f"<b>Total :</b>  {completed} track{'s' if completed != 1 else ''} sent 🎶"
                    f"</blockquote>",
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=msg.id),
                )

            await progress.delete()

    else:
        await msg.reply_text("unsupported spotify link type.")


# ════════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════════

async def main():
    _start_health_server(port=8080)

    bot = Client(
        "music_bot",
        api_id=config.API_ID,
        api_hash=config.API_HASH,
        bot_token=config.BOT_TOKEN,
    )

    bot.add_handler(MessageHandler(cmd_start,  filters.command("start")  & filters.private))
    bot.add_handler(MessageHandler(cmd_cancel, filters.command("cancel") & filters.private))
    bot.add_handler(CallbackQueryHandler(cb_credits, filters.regex("^credits$")))
    bot.add_handler(MessageHandler(
        handle_message,
        filters.text & filters.private & ~filters.command(["start", "cancel"]),
    ))

    await mongodb.connect()
    await bot.start()
    print("[bot] running — waiting for messages...")
    await idle()
    await bot.stop()
    await mongodb.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
