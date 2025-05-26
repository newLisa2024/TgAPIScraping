import asyncio
from telethon.errors import FloodWaitError

async def safe_request(func, *args, **kwargs):
    """Обработка Flood-ошибок и задержек."""
    try:
        return await func(*args, **kwargs)
    except FloodWaitError as e:
        wait = e.seconds + 10
        print(f"⚠️ Телеграм просит подождать {wait} сек.")
        await asyncio.sleep(wait)
        return await safe_request(func, *args, **kwargs)