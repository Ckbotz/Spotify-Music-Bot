"""
We only use it to track unique users so we know when someone is new.
"""

from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DB_NAME

_client: AsyncIOMotorClient | None = None
_users = None


async def connect():
    global _client, _users

    if not MONGO_URI:
        raise RuntimeError("MONGO_URI is not set — add it to your environment variables")

    _client = AsyncIOMotorClient(MONGO_URI)
    db = _client[DB_NAME]
    _users = db["users"]

    # unique index so we never get duplicate entries
    await _users.create_index("user_id", unique=True)
    print(f"[mongodb] connected → {DB_NAME}")


async def disconnect():
    global _client
    if _client:
        _client.close()
        print("[mongodb] disconnected")


async def is_new_user(user_id: int) -> bool:
    doc = await _users.find_one({"user_id": user_id}, {"_id": 1})
    return doc is None


async def add_user(user_id: int, first_name: str, username: str | None, dc_id: int | None):
    # upsert so it's safe to call more than once without creating duplicates
    await _users.update_one(
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
