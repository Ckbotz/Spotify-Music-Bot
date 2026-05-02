"""
stats.py — /stats command for the music bot.

Shows:
  • Total registered users
  • Total songs indexed (cached) in the database
"""

from __future__ import annotations

from pyrogram import Client
from pyrogram.types import Message, ReplyParameters
from pyrogram.enums import ParseMode

import mongodb
import index as song_index


async def cmd_stats(bot: Client, msg: Message):
    status = await msg.reply_text(
        "<blockquote>📊 <b>Fetching stats • • •</b></blockquote>",
        parse_mode       = ParseMode.HTML,
        reply_parameters = ReplyParameters(message_id=msg.id),
    )

    try:
        total_users = await mongodb.total_users()
        total_songs = await song_index.total_indexed()

        await status.edit_text(
            "<blockquote>"
            "📊 <b>Bot Statistics</b>\n\n"
            f"👤 <b>Total Users   :</b>  <code>{total_users}</code>\n"
            f"🎵 <b>Songs Indexed :</b>  <code>{total_songs}</code>"
            "</blockquote>",
            parse_mode = ParseMode.HTML,
        )

    except Exception as e:
        await status.edit_text(
            f"<blockquote>❌ <b>Failed to fetch stats:</b> <code>{e}</code></blockquote>",
            parse_mode = ParseMode.HTML,
        )
