# api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from main import scrape_history, serialize_message, handle_message, register_live_handler
from telethon import TelegramClient
from config import API_ID, API_HASH, SESSION_STRING, SESSION_NAME

class ScrapeRequest(BaseModel):
    channels: list[str]
    history: bool = True
    listen: bool = False

app = FastAPI()

# Инициализируем TelegramClient один раз
session = SESSION_STRING or SESSION_NAME
client = TelegramClient(session, API_ID, API_HASH)

@app.on_event("startup")
async def startup():
    await client.start()  # авторизация один раз

@app.post("/scrape")
async def scrape(req: ScrapeRequest):
    if not req.channels:
        raise HTTPException(400, "Пустой список channels")
    # Запускаем историю
    if req.history:
        for ch in req.channels:
            await scrape_history(client, ch)
    # Если включен listen — регистрируем хэндлер и не завершаем
    if req.listen:
        for ch in req.channels:
            register_live_handler(client, ch)
        return {"status": "listening", "channels": req.channels}
    # Иначе отключаемся и возвращаем завершение
    await client.disconnect()
    return {"status": "done", "channels": req.channels}
