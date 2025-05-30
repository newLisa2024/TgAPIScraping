"""
Считывает .env и экспортирует константы.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE = os.getenv("PHONE", "")
CHANNEL = os.getenv("CHANNEL", "")
FETCH_LIMIT = int(os.getenv("FETCH_LIMIT", "50"))

# Airtable
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY", "")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID", "")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME", "")
AIRTABLE_STATS_TABLE = os.getenv("AIRTABLE_STATS_TABLE", "")  # для ежедневных подписчиков

