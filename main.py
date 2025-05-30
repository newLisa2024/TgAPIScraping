"""
Telegram‑скрапер: посты → Airtable + ежедневная статистика подписчиков.

Запуск:
    python main.py
"""
import asyncio
from datetime import datetime, date

from telethon import TelegramClient, events, functions
from pyairtable import Table
from pyairtable.formulas import match

from config import (
    API_ID, API_HASH, PHONE,
    CHANNEL, FETCH_LIMIT,
    AIRTABLE_API_KEY, AIRTABLE_BASE_ID,
    AIRTABLE_TABLE_NAME, AIRTABLE_STATS_TABLE
)
from utils.logger import setup_logger

logger = setup_logger()

# ─────────────—— Airtable ————──────────────────────────────────────
posts_table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
stats_table = Table(AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_STATS_TABLE) \
    if AIRTABLE_STATS_TABLE else None

# ─────────────—— Telethon ————──────────────────────────────────────
client = TelegramClient("sessions/main_session", API_ID, API_HASH)

# ════════════════ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ══════════════════════════
def get_likes(message) -> int:
    """Сумма лайков ❤️/👍; если реакций нет — 0."""
    if not message.reactions:
        return 0
    return sum(
        r.count
        for r in message.reactions.results
        if getattr(r.reaction, "emoticon", "") in ("❤️", "👍")
    )

async def get_subscriber_count() -> int:
    """Количество подписчиков канала."""
    full = await client(functions.channels.GetFullChannelRequest(CHANNEL))
    return full.full_chat.participants_count

async def upsert_post(message):
    """Создать или обновить запись поста в Airtable."""
    fields = {
        "Telegram ID": str(message.id),
        "Channel": CHANNEL,
        "Date": message.date.strftime("%Y-%m-%d %H:%M:%S"),
        "Text": (message.text or message.message or "")[:10000],
        "Views": getattr(message, "views", 0),
        "Likes": get_likes(message),
        "Link": f"https://t.me/{CHANNEL}/{message.id}",
    }

    record = next(iter(
        posts_table.all(formula=match({"Telegram ID": str(message.id)}), max_records=1)
    ), None)

    if record:
        posts_table.update(record["id"], fields, typecast=True)
        logger.info(f"🔄 Обновлён пост {message.id}")
    else:
        posts_table.create(fields, typecast=True)
        logger.info(f"➕ Добавлен пост {message.id}")

async def scrape_history():
    logger.info(f"📥 Загружаю последние {FETCH_LIMIT} сообщений @{CHANNEL}")
    async for msg in client.iter_messages(CHANNEL, limit=FETCH_LIMIT):
        await upsert_post(msg)
    logger.info("✅ История загружена")

async def save_daily_stats():
    """Записывает число подписчиков (одна запись в день)."""
    if not stats_table:
        return
    today_iso = date.today().isoformat()
    # если уже есть запись за сегодня — пропускаем
    existing = stats_table.all(
        formula=match({"Channel": CHANNEL, "Date": today_iso}), max_records=1
    )
    if existing:
        return
    subs = await get_subscriber_count()
    stats_table.create(
        {"Channel": CHANNEL, "Date": today_iso, "Subscribers": subs},
        typecast=True
    )
    logger.info(f"👥 Подписчиков: {subs} (сохранено в статистику)")

@client.on(events.NewMessage(chats=CHANNEL))
async def new_message_handler(event):
    await upsert_post(event.message)

# ════════════════ Точка входа ══════════════════════════════════════
async def main():
    await client.start(phone=PHONE)
    logger.info("▶️ Клиент подключён")

    await save_daily_stats()
    await scrape_history()

    logger.info("▶️ Переход в live‑stream")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("⏹ Остановлено пользователем")









