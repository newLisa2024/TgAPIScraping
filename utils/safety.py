# utils/safety.py
import asyncio
from telethon.errors import FloodWaitError
from utils.logger import logger

async def safe_request(func, *args, **kwargs):
    """
    Выполняет Telethon-запрос, при FloodWait ждёт e.seconds+5 и повторяет.
    """
    while True:
        try:
            return await func(*args, **kwargs)
        except FloodWaitError as e:
            wait = e.seconds + 5
            logger.warning(f"FloodWait: ждём {wait} сек...")
            await asyncio.sleep(wait)
