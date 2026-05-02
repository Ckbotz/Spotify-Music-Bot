"""
mongodb.py — User tracking + shared Motor client.

We expose the raw AsyncIOMotorClient so index.py can reuse the same
connection pool instead of opening a second one.
"""

from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DB_NAME

# Exposed so index.py (and anything else) can share the same pool.
client: AsyncIOMotorClient | None = None

_users = None


async def connect():
    global client, _users

    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is not set — add it to your environment variables")

    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    _users = db["users"]

    await _users.create_index("user_id", unique=True)
    print(f"[mongodb] connected → {DB_NAME}")


async def disconnect():
    global client
    if client:
        client.close()
        print("[mongodb] disconnected")


async def is_new_user(user_id: int) -> bool:
    doc = await _users.find_one({"user_id": user_id}, {"_id": 1})
    return doc is None


async def add_user(
    user_id: int,
    first_name: str,
    username: str | None,
    dc_id: int | None,
) -> bool:
    """
    Upsert user.  Returns True if this was a brand-new insertion,
    False if the user already existed.
    """
    result = await _users.update_one(
        {"user_id": user_id},
        {
            "$setOnInsert": {
                "user_id":    user_id,
                "first_name": first_name,
                "username":   username,
                "dc_id":      dc_id,
            }
        },
        upsert=True,
    )
    return result.upserted_id is not None
