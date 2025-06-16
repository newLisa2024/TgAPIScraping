import os
from dotenv import load_dotenv

load_dotenv()  # читает .env

def _get_env_int(key: str, default: int | None = None) -> int:
    val = os.getenv(key)
    if val is None:
        if default is None:
            raise RuntimeError(f"Не задана обязательная переменная {key}")
        return default
    try:
        return int(val)
    except ValueError:
        raise RuntimeError(f"Переменная {key} должна быть числом, получили '{val}'")

def _get_env_str(key: str, required: bool = False, default: str = "") -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(f"Не задана обязательная переменная {key}")
    return val

# Основные настройки Telegram API
API_ID          = _get_env_int("API_ID")
API_HASH        = _get_env_str("API_HASH", required=True)
SESSION_STRING  = _get_env_str("SESSION_STRING", required=True)  # строка сессии Telethon
FETCH_LIMIT     = _get_env_int("FETCH_LIMIT", default=100)
LOG_DIR         = _get_env_str("LOG_DIR", default="logs")
