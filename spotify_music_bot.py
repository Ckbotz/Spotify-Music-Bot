import os
import re
import json
import asyncio
import base64
import tempfile
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

# ── Concurrency (Apple Music) ─────────────────────────────────────────────────

GLOBAL_AP_SEMAPHORE = asyncio.Semaphore(15)

_USER_LOCKS: dict[int, asyncio.Lock] = {}


def _get_user_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _USER_LOCKS:
        _USER_LOCKS[user_id] = asyncio.Lock()
    return _USER_LOCKS[user_id]


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


def _spoti_download_file(url: str, name: str) -> str:
    safe = re.sub(r'[\\/*?:"<>|]', "", name)[:100]
    path = os.path.join(DOWNLOAD_DIR, f"{safe}.mp3")
    with requests.get(url, stream=True, timeout=120,
                      headers={"User-Agent": UA}) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(128 * 1024):
                if chunk:
                    f.write(chunk)
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        raise RuntimeError("downloaded file is empty")
    return path


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
        return index, name, title, artist, None, None, resp.get("message")

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
        return index, name, title, artist, None, None, "no link found"

    try:
        local_path = _spoti_download_file(href, name)
    except Exception as e:
        return index, name, title, artist, None, None, f"download failed: {e}"

    local_thumb = _spoti_download_thumb(thumb_url, name)
    return index, name, title, artist, local_path, local_thumb, None


def spotify_get_track(spotify_url: str):
    s = _make_spoti_session()
    html = _spoti_fetch_action(s, spotify_url)
    forms, fallback_thumb = _spoti_parse_forms(html)
    if not forms:
        raise Exception("no track found")
    _, name, title, artist, local_path, thumb, err = _spoti_fetch_one(s, forms[0], 0, fallback_thumb)
    if err:
        raise Exception(err)
    return name, title, artist, local_path, thumb


def spotify_get_playlist(spotify_url: str, on_result=None):
    s = _make_spoti_session()
    html = _spoti_fetch_action(s, spotify_url)
    forms, fallback_thumb = _spoti_parse_forms(html)
    total = len(forms)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_spoti_fetch_one, s, form, i, fallback_thumb): i
            for i, form in enumerate(forms)
        }
        for future in as_completed(futures):
            index, name, title, artist, local_path, thumb, err = future.result()
            if on_result:
                on_result(index, total, name, title, artist, local_path, thumb, err)


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
) -> tuple[bool, str]:
    try:
        async with client.stream("GET", url, headers=AP_HEADERS, follow_redirects=True) as r:
            r.raise_for_status()
            fname = _parse_ap_filename(r, fallback_name)
            fname = re.sub(r'[\\/*?:"<>|]', "_", fname)
            dest = os.path.join(dest_dir, fname)
            with open(dest, "wb") as f:
                async for chunk in r.aiter_bytes(CHUNK_SIZE):
                    f.write(chunk)
        return True, dest
    except Exception as e:
        print(f"[AP Download] {url} failed: {e}")
        return False, ""


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


async def _ap_handle_track(
    client: Client,
    message,
    dl: httpx.AsyncClient,
    idx: int,
    total: int,
    links: list[dict],
    album_thumb_path: str | None,
    first_track_ref: list,
    user,
):
    audio_entry, cover_entry = _split_ap_links(links)

    if not audio_entry:
        await flood_safe(
            message.reply_text,
            f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> No audio link found.</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=message.id),
        )
        return

    with tempfile.TemporaryDirectory() as tmp:
        print(f"\n[AP Upload] Track {idx}/{total} → downloading audio…")
        ok, raw_audio_path = await _ap_download_file(dl, audio_entry["link"], tmp, f"track_{idx}.mp3")
        if not ok:
            await flood_safe(
                message.reply_text,
                f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> Audio download failed.</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=message.id),
            )
            return

        raw_name = os.path.splitext(os.path.basename(raw_audio_path))[0]
        song_name = _clean_ap_song_name(raw_name)
        print(f"[AP Upload] Raw : {raw_name}")
        print(f"[AP Upload] Name: {song_name}")

        safe_fname = _sanitize_filename(song_name) + ".mp3"
        audio_path = os.path.join(tmp, safe_fname)
        os.rename(raw_audio_path, audio_path)

        if first_track_ref[0] is None:
            first_track_ref[0] = song_name

        thumb_path = album_thumb_path
        if cover_entry:
            c_ok, cover_path = await _ap_download_file(dl, cover_entry["link"], tmp, f"cover_{idx}.jpg")
            if c_ok:
                if not cover_path.lower().endswith((".jpg", ".jpeg")):
                    jpg_path = cover_path + ".jpg"
                    os.rename(cover_path, jpg_path)
                    cover_path = jpg_path
                thumb_path = cover_path

        caption = (
            f"<blockquote>"
            f"🎵 <b>{song_name}</b>"
            f"</blockquote>"
        )

        try:
            if thumb_path:
                await flood_safe(
                    client.send_photo,
                    chat_id=message.chat.id,
                    photo=thumb_path,
                    caption=caption,
                    parse_mode=ParseMode.HTML,
                    reply_parameters=ReplyParameters(message_id=message.id),
                )

            await flood_safe(
                client.send_audio,
                chat_id=message.chat.id,
                audio=audio_path,
                title=song_name,
                file_name=safe_fname,
                reply_parameters=ReplyParameters(message_id=message.id),
            )
            print(f"[AP Upload] ✓ Track {idx}/{total} sent.")

            await log_track_to_channel(client, user, song_name, idx, total, audio_path, thumb_path)

        except Exception as e:
            print(f"[AP Upload] ✗ Track {idx}/{total} failed: {e}")
            await flood_safe(
                message.reply_text,
                f"<blockquote>⚠️ <b>Track {idx}/{total}:</b> Upload failed — <i>{e}</i></blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=message.id),
            )


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
            "<i>Please wait for it to finish.</i></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=msg.id),
        )
        return

    async with user_lock:
        is_new = await mongodb.add_user(user.id)
        if is_new:
            await log_new_user(bot, user)

        status = await msg.reply_text(
            "<blockquote>ᯓ➤<b>Processing • • •</b></blockquote>",
            parse_mode=ParseMode.HTML,
            reply_parameters=ReplyParameters(message_id=msg.id),
        )

        is_playlist = False

        try:
            async with httpx.AsyncClient(follow_redirects=True, timeout=AP_TIMEOUT) as fetch_cl:
                await init_session(fetch_cl)
                token = await get_token(fetch_cl, url)
                track_forms, album_thumb = await get_all_track_forms(fetch_cl, url, token)

                total = len(track_forms)
                print(f"\n[AP Flow] URL: {url} | Tracks: {total} | Thumb: {album_thumb}")

                if total == 0:
                    await status.edit_text(
                        "<blockquote>❌ <b>No tracks found for that link.</b></blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                    return

                is_playlist = total > 1
                label = "playlist" if is_playlist else "track"
                await status.edit_text(
                    f"<blockquote>•ᴗ•<b> Processing {label}</b> ◌ {total} track{'s' if total > 1 else ''}…"
                    + ("\n<i>Playlist detected - please wait…</i>" if is_playlist else "")
                    + "</blockquote>",
                    parse_mode=ParseMode.HTML,
                )

                _thumb_tmp = tempfile.mkdtemp()
                album_thumb_path = None
                if album_thumb:
                    try:
                        async with fetch_cl.stream("GET", album_thumb) as _r:
                            _r.raise_for_status()
                            _ap = os.path.join(_thumb_tmp, "album_thumb.jpg")
                            with open(_ap, "wb") as _f:
                                async for _chunk in _r.aiter_bytes(CHUNK_SIZE):
                                    _f.write(_chunk)
                        album_thumb_path = _ap
                    except Exception as e:
                        print(f"[AP Thumb] Album art failed: {e}")

                per_request_sem = asyncio.Semaphore(3)

                async def _get_links_guarded(f, i):
                    async with GLOBAL_AP_SEMAPHORE:
                        return await get_links_for_track(fetch_cl, f, per_request_sem, i)

                link_tasks = [
                    asyncio.ensure_future(_get_links_guarded(f, i + 1))
                    for i, f in enumerate(track_forms)
                ]

                first_track_ref = [None]

                async with httpx.AsyncClient(
                    follow_redirects=True,
                    timeout=AP_TIMEOUT,
                    limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
                ) as dl:
                    for coro in asyncio.as_completed(link_tasks):
                        idx, links = await coro
                        await _ap_handle_track(
                            bot, msg, dl,
                            idx, total, links,
                            album_thumb_path, first_track_ref,
                            user,
                        )
                        if is_playlist and idx < total:
                            await asyncio.sleep(PLAYLIST_UPLOAD_DELAY)

        except Exception as e:
            await status.edit_text(
                f"<blockquote>❌ <b>Failed.</b>\n<i>{e}</i></blockquote>",
                parse_mode=ParseMode.HTML,
            )
            return

        if is_playlist:
            await flood_safe(
                msg.reply_text,
                f"<blockquote>"
                f"✅ <b>Playlist done!</b>\n\n"
                f"<b>Total :</b>  {total} track{'s' if total > 1 else ''} sent 🎶"
                f"</blockquote>",
                parse_mode=ParseMode.HTML,
                reply_parameters=ReplyParameters(message_id=msg.id),
            )

        await log_download_summary(bot, user, first_track_ref[0] or "Unknown", total)
        await status.delete()


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
        status = await msg.reply_text("fetching playlist...")
        completed = 0
        failed    = 0
        main_loop = asyncio.get_event_loop()

        def on_result(_, total, name, title, artist, local_path, thumb, err):
            nonlocal completed, failed
            if err:
                failed += 1
            else:
                completed += 1
            asyncio.run_coroutine_threadsafe(
                _spoti_send_track(
                    bot, msg, status,
                    name, title, artist, local_path, thumb, err,
                    completed, failed, total, user,
                ),
                loop=main_loop,
            )

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None,
                lambda: spotify_get_playlist(url, on_result=on_result),
            )
            try:
                await status.delete()
            except Exception:
                pass

        except Exception as e:
            await status.edit_text(
                f"something went wrong\n\n<code>{e}</code>",
                parse_mode=ParseMode.HTML,
            )

    else:
        await msg.reply_text("unsupported spotify link type.")


async def _spoti_send_track(
    bot, msg, status,
    name, title, artist, local_path, thumb, err,
    completed, failed, total, user,
):
    if err:
        print(f"[Spotify skip] {name}: {err}")
        return
    try:
        await msg.reply_audio(
            audio=local_path,
            caption=f"<b>{name}</b>",
            title=title,
            performer=artist,
            thumb=thumb,
            parse_mode=ParseMode.HTML,
        )
        await log_download(bot, user, name)
        try:
            await status.edit_text(
                f"<blockquote>📥 <b>{completed + failed}/{total}</b> done\n"
                f"✅ <b>{completed}</b> succeeded   ❌ <b>{failed}</b> failed</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass
    except Exception as e:
        print(f"[Spotify send] {name}: {e}")
    finally:
        cleanup(local_path)
        cleanup(thumb)


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

    bot.add_handler(MessageHandler(cmd_start,       filters.command("start") & filters.private))
    bot.add_handler(CallbackQueryHandler(cb_credits, filters.regex("^credits$")))
    bot.add_handler(MessageHandler(
        handle_message,
        filters.text & filters.private & ~filters.command(["start"]),
    ))

    await mongodb.connect()
    await bot.start()
    print("[bot] running — waiting for messages...")
    await idle()
    await bot.stop()
    await mongodb.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
