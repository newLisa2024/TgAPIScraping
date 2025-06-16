from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession

from main import scrape_history, register_live_handler, serialize_message, handle_message
from config import API_ID, API_HASH, SESSION_STRING

class ScrapeRequest(BaseModel):
    channels: list[str]
    history: bool = True
    listen: bool = False

app = FastAPI()

# Инициализируем клиент единожды
client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

@app.on_event("startup")
async def startup():
    # Запускаем без input(), используя строку сессии
    await client.start()
    # Не стоит вызывать client.run_until_disconnected() здесь!
    print("▶️ Telegram-клиент готов")

@app.post("/scrape")
async def scrape(req: ScrapeRequest):
    if not req.channels:
        raise HTTPException(400, "Пустой список channels")
    if req.history:
        for ch in req.channels:
            await scrape_history(client, ch)
    if req.listen:
        for ch in req.channels:
            register_live_handler(client, ch)
        return {"status": "listening", "channels": req.channels}
    return {"status": "done", "channels": req.channels}

