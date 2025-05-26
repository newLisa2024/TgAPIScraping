# main.py

import asyncio
import csv
import os
import random
import requests
from telethon import TelegramClient, errors
from config import API_ID, API_HASH, PHONE, AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME
from utils.safety import safe_request
from utils.logger import setup_logger

# Быстрый принт для отладки запуска
print("▶️  Старт main.py")
print(f"🔧 Конфиг: BASE_ID={AIRTABLE_BASE_ID}, TABLE_NAME='{AIRTABLE_TABLE_NAME}'")

# ПАРАМЕТРЫ (для теста)
REQUEST_DELAY = 1       # задержка (сек)
MAX_POSTS = 2       # число последних постов
CHANNEL = "DeepLearning_ai"  # без @

# Настраиваем логер
logger = setup_logger()
# Создаём папки, если нет
os.makedirs("data/media", exist_ok=True)
os.makedirs("sessions", exist_ok=True)

# Инициализируем Telethon
client = TelegramClient("sessions/main_session", API_ID, API_HASH)

async def download_media(message):
    # Скачиваем медиа только для CSV, не для Airtable
    if message.media:
        dst = f"data/media/{message.id}_{message.date:%Y%m%d%H%M%S}"
        await safe_request(client.download_media, message, file=dst)
        return dst
    return ""

async def scrape_channel(channel_username):
    print(f"▶️  scrape_channel @{channel_username}, limit={MAX_POSTS}")
    logger.info(f"🔍 Сбор последних {MAX_POSTS} постов из @{channel_username}")

    posts = []
    async for message in client.iter_messages(channel_username, limit=MAX_POSTS):
        await asyncio.sleep(REQUEST_DELAY + random.uniform(0,1))
        try:
            text = message.text or message.message or ""
            media_path = await download_media(message)

            # Собираем данные для CSV
            post = {
                "ID":      message.id,
                "Channel": CHANNEL,
                "Date":    message.date.strftime("%Y-%m-%d %H:%M:%S"),
                "Text":    text,
                "Views":   getattr(message, "views", 0)
            }
            posts.append(post)

            print(f"📥 Пост #{message.id} (Total={len(posts)})")
            logger.info(f"📥 Пост #{message.id} (Total={len(posts)})")

            # Формируем поля для Airtable (без полей Media и Status)
            airtable_fields = {
                "Telegram ID": str(post["ID"]),
                "Channel": post["Channel"],
                "Date": post["Date"],
                "Text": post["Text"],
                "Views": post["Views"]
            }

            airtable_url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
            headers = {
                "Authorization": f"Bearer {AIRTABLE_API_KEY}",
                "Content-Type": "application/json"
            }
            resp = requests.post(airtable_url, json={"fields": airtable_fields}, headers=headers, timeout=10)
            print(f"➡️ Airtable POST {resp.status_code}")
            logger.info(f"➡️ Airtable POST {resp.status_code}: {resp.text}")
            resp.raise_for_status()

        except errors.FloodWaitError as e:
            wait = e.seconds + 5
            print(f"⚠️ FloodWaitError #{message.id}, ждём {wait}s")
            logger.warning(f"⚠️ FloodWaitError #{message.id}, ждём {wait}s")
            await asyncio.sleep(wait)
        except Exception as e:
            print(f"❌ Ошибка при посте #{message.id}: {e}")
            logger.error(f"❌ Ошибка при посте #{message.id}: {e}")

        print(f"🏁 Завершено, собрано {len(posts)} постов")
        logger.info(f"🏁 Завершено, собрано {len(posts)} постов")

        # Запись в CSV
        csv_file = "data/posts.csv"
        exists = os.path.exists(csv_file)
        with open(csv_file, "a" if exists else "w", newline="", encoding="utf-8") as f:
            fieldnames = ["ID", "Channel", "Date", "Text", "Views"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                writer.writeheader()
            writer.writerows(posts)

        print(f"✅ posts.csv обновлён: +{len(posts)} строк")
        logger.info(f"✅ posts.csv обновлён: +{len(posts)} строк")

    async def main():
        print("▶️  Запуск Telethon-клиента")
        await client.start(phone=PHONE)
        print("✅ Клиент подключён")
        try:
            await scrape_channel(CHANNEL)
        finally:
            print("▶️  Отключаем клиента")
            await client.disconnect()
            print("❎ Клиент отключён. Выход.")

    if __name__ == "__main__":
        asyncio.run(main())








