"""
Рекурсивный перезапуск Telethon‑методов при FloodWait.
"""
import asyncio
from telethon.errors import FloodWaitError

async def safe_request(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except FloodWaitError as e:
        await asyncio.sleep(e.seconds + 5)
        return await safe_request(func, *args, **kwargs)
