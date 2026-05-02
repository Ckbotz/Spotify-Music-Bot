"""
index.py — Song index / cache layer.

Each document in the "songs" collection stores the Telegram file_id of an
already-uploaded audio file so the bot can re-send it instantly instead of
re-downloading from the internet.

Schema
──────
{
    "_id":          ObjectId,
    "source":       "spotify" | "apple_music",
    "track_id":     str,          # Spotify track ID or normalised Apple Music URL
    "name":         str,          # "Title - Artist"
    "title":        str,
    "artist":       str,
    "file_id":      str,          # Telegram audio file_id  (primary cache key)
    "thumb_file_id":str | None,   # Telegram photo file_id  (optional)
    "added_at":     datetime,
}

Lookup is by (source, track_id) — both indexed.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from motor.motor_asyncio import AsyncIOMotorClient

import config

# ── Module-level handles (set by connect / injected from mongodb.connect) ─────

_client: AsyncIOMotorClient | None = None
_songs = None   # songs collection


# ── Lifecycle ─────────────────────────────────────────────────────────────────

async def connect(client: AsyncIOMotorClient | None = None):
    """
    Attach to MongoDB.  Pass an existing Motor client (from mongodb.py) to
    reuse the same connection pool, or leave it None to open a fresh one.
    """
    global _client, _songs

    if client:
        _client = client
    else:
        if not config.MONGO_URI:
            raise RuntimeError("MONGO_URI is not set")
        _client = AsyncIOMotorClient(config.MONGO_URI)

    db = _client[config.DB_NAME]
    _songs = db["songs"]

    # Compound unique index: one doc per (source, track_id)
    await _songs.create_index(
        [("source", 1), ("track_id", 1)],
        unique=True,
        name="source_track_unique",
    )
    print("[index] songs collection ready")


async def disconnect():
    global _client
    if _client:
        _client.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def extract_spotify_track_id(url: str) -> str | None:
    """Return the bare Spotify track ID from a full URL, or None."""
    m = re.search(r"/track/([A-Za-z0-9]+)", url)
    return m.group(1) if m else None


def normalise_apple_url(url: str) -> str:
    """
    Strip query-string / country-code variations so the same song always maps
    to the same key regardless of which region or referral link is used.

    Example:
        https://music.apple.com/us/album/blinding-lights/1498987325?i=1498987346
        → apple:1498987325:1498987346

    Falls back to the raw URL if we can't parse it cleanly.
    """
    # Try to extract album-id and track i= param
    m_album = re.search(r"/album/[^/]+/(\d+)", url)
    m_track = re.search(r"[?&]i=(\d+)", url)
    if m_album and m_track:
        return f"apple:{m_album.group(1)}:{m_track.group(1)}"
    if m_album:
        return f"apple:{m_album.group(1)}"
    return url


# ── Public API ────────────────────────────────────────────────────────────────

async def get_cached_track(source: str, track_id: str) -> dict | None:
    """
    Look up a previously-uploaded track.

    Returns the full document (with file_id / thumb_file_id) if found,
    otherwise None.

    source  — "spotify" or "apple_music"
    track_id — Spotify track ID or normalised Apple URL key
    """
    if _songs is None:
        return None
    return await _songs.find_one(
        {"source": source, "track_id": track_id},
        {"_id": 0},
    )


async def save_track(
    source: str,
    track_id: str,
    name: str,
    title: str,
    artist: str,
    file_id: str,
    thumb_file_id: str | None = None,
) -> None:
    """
    Upsert a track into the song index.

    We always overwrite file_id / thumb_file_id so stale Telegram file_ids
    (which expire) get refreshed automatically when the same song is
    re-uploaded later.
    """
    if _songs is None:
        return
    now = datetime.now(timezone.utc)
    await _songs.update_one(
        {"source": source, "track_id": track_id},
        {
            "$set": {
                "name":          name,
                "title":         title,
                "artist":        artist,
                "file_id":       file_id,
                "thumb_file_id": thumb_file_id,
                "updated_at":    now,
            },
            "$setOnInsert": {
                "added_at": now,
            },
        },
        upsert=True,
    )


async def total_indexed() -> int:
    """Return total number of cached songs (useful for /stats)."""
    if _songs is None:
        return 0
    return await _songs.count_documents({})
