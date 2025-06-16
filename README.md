# README

## Описание
Универсальный Telegram-скрапер на основе Telethon. Позволяет собирать историю и слушать новые сообщения из любых публичных каналов по списку, заданному на запуске или через HTTP‑API.

## Структура проекта
```
TgAPIScraping/
├── config.py           # Чтение переменных окружения (.env)
├── main.py             # Точка входа: сбор истории и live‑режим
├── api.py              # HTTP‑API (FastAPI) для вызова из Make/n8n
├── utils/
│   ├── logger.py       # Логирование в файл и консоль
│   └── safety.py       # Обработка FloodWaitError
├── sessions/           # (опционально) файлы сессий Telethon
├── logs/               # Логи работы скрапера
├── .env                # Переменные окружения
└── README.md           # Текущий файл
```

## Установка
1. Клонируйте репозиторий и перейдите в папку:
   ```bash
   git clone <repo_url>
   cd TgAPIScraping
   ```
2. Создайте виртуальное окружение и активируйте:
   ```bash
   python -m venv .venv
   # Windows PowerShell:
   .\.venv\Scripts\Activate.ps1
   # Linux/macOS:
   source .venv/bin/activate
   ```
3. Установите зависимости:
   ```bash
   pip install telethon python-dotenv fastapi uvicorn pydantic
   ```

## Настройка учётных данных
В файле `.env` задайте:
```dotenv
API_ID=ваш_api_id
API_HASH=ваш_api_hash
# Для авторизации без ввода телефона:
SESSION_STRING=<ваша_строка_сессии>
# или при первом запуске скрипт спросит телефон и код, затем сохранит файл sessions/<SESSION_NAME>.session
SESSION_NAME=main_session
# Лимит сообщений для истории
FETCH_LIMIT=50
# Папка для логов
LOG_DIR=logs
```  
- `API_ID` и `API_HASH`: получите на https://my.telegram.org.  
- `SESSION_STRING`: генерируется разовым скриптом из Telethon (см. ниже).  

### Генерация `SESSION_STRING`
```python
from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = YOUR_API_ID
api_hash = 'YOUR_API_HASH'
with TelegramClient(StringSession(), api_id, api_hash) as client:
    print('SESSION_STRING=' + client.session.save())
```
Скопируйте вывод и вставьте в `.env`.

## Запуск скрапера (CLI)
Запустить сбор и/или прослушивание каналов через аргументы:
```bash
python main.py --channels @chan1 @chan2 --history --listen
```
- `--channels`: список каналов (юзернейм или числовой ID).  
- `--history`: загрузить последние `FETCH_LIMIT` сообщений из каждого канала.  
- `--listen`: слушать новые сообщения в реальном времени.

## Запуск HTTP‑API (для Make / n8n)
### 1. Добавьте файл `api.py` (рядом с `main.py`):
```python
# api.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
from main import scrape_history, register_live_handler
from telethon import TelegramClient
from telethon.sessions import StringSession
from config import API_ID, API_HASH, SESSION_STRING, SESSION_NAME

class ScrapeRequest(BaseModel):
    channels: list[str]
    history: bool = True
    listen: bool = False

app = FastAPI()

# Инициализация клиента (при старте API)
session = StringSession(SESSION_STRING) if SESSION_STRING else SESSION_NAME
client = TelegramClient(session, API_ID, API_HASH)

@app.on_event("startup")
async def startup_event():
    await client.start()

@app.post("/scrape")
async def scrape(req: ScrapeRequest):
    if not req.channels:
        raise HTTPException(status_code=400, detail="channels пуст")
    if req.history:
        for ch in req.channels:
            await scrape_history(client, ch)
    if req.listen:
        for ch in req.channels:
            register_live_handler(client, ch)
        return {"status": "listening", "channels": req.channels}
    await client.disconnect()
    return {"status": "done", "channels": req.channels}
```

### 2. Запустите API-сервис:
```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

### 3. Доступ к API
- В браузере или из Make/n8n делайте POST на `http://<ваш_сервер>:8000/scrape`.  
- **Если видите Privoxy** (ошибка 500), выключите прокси или используйте `127.0.0.1:8000` и/или установите:
  ```bash
  set NO_PROXY=localhost,127.0.0.1
  ```

### Пример вызова из Make / n8n
- **URL**: `http://<сервер>:8000/scrape`  
- **Method**: POST  
- **Body (JSON)**:
  ```json
  {
    "channels": ["@chat_gpt_expert","@neuraldeep"],
    "history": true,
    "listen": false
  }
  ```
- В ответ получите:
  ```json
  {"status":"done","channels":["@chat_gpt_expert","@neuraldeep"]}
  ```

