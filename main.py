# main.py
import argparse
import asyncio
import json
import os
from telethon import TelegramClient, events, functions
from telethon.sessions import StringSession

from config import API_ID, API_HASH, SESSION_NAME, SESSION_STRING, FETCH_LIMIT
from utils.logger import logger
from utils.safety import safe_request


def serialize_message(msg, channel) -> dict:
    likes = 0
    if msg.reactions:
        likes = sum(
            r.count for r in msg.reactions.results
            if getattr(r.reaction, 'emoticon', '') in ('❤️','👍')
        )
    return {
        'channel': channel,
        'id':     msg.id,
        'date':   msg.date.isoformat(),
        'text':   msg.text or msg.message or '',
        'views':  getattr(msg, 'views', 0),
        'likes':  likes,
        'link':   f"https://t.me/{channel.strip('@')}/{msg.id}"
    }

async def handle_message(data: dict):
    logger.info(f"[{data['channel']}] Пост {data['id']}")
    print(json.dumps(data, ensure_ascii=False))

async def scrape_history(client: TelegramClient, channel: str):
    logger.info(f"[{channel}] Загружаем последние {FETCH_LIMIT} сообщений")
    async for msg in client.iter_messages(channel, limit=FETCH_LIMIT):
        data = serialize_message(msg, channel)
        await handle_message(data)
    logger.info(f"[{channel}] История загружена")


def register_live_handler(client: TelegramClient, channel: str):
    @client.on(events.NewMessage(chats=channel))
    async def on_new(event):
        data = serialize_message(event.message, channel)
        await handle_message(data)
        logger.info(f"[{channel}] Новый пост {data['id']}")

async def main(channels: list[str], history: bool, listen: bool):
    # выбираем сессию: строку или файл
    if SESSION_STRING:
        session = StringSession(SESSION_STRING)
    else:
        session = SESSION_NAME
    client = TelegramClient(session, API_ID, API_HASH)
    # первый запуск может запросить код, при последующих запусках интерактива не будет
    await client.start()
    logger.info("Telegram-клиент подключён")

    if history:
        for ch in channels:
            await scrape_history(client, ch)

    if listen:
        for ch in channels:
            register_live_handler(client, ch)
        logger.info("Входим в режим прослушивания новых сообщений")
        await client.run_until_disconnected()
    else:
        await client.disconnect()
        logger.info("Завершено")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Telegram Scraper")
    parser.add_argument('--channels', nargs='+', required=True,
                        help='Список каналов: @chan1 @chan2 или ID')
    parser.add_argument('--history', action='store_true', help='Собрать историю')
    parser.add_argument('--listen',  action='store_true', help='Слушать новые сообщения')
    args = parser.parse_args()

    asyncio.run(main(
        channels=args.channels,
        history=args.history,
        listen=args.listen
    ))












