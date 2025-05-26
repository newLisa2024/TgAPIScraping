import logging
from datetime import datetime

def setup_logger():
    """Настройка логов."""
    logging.basicConfig(
        filename=f"logs/scrape_{datetime.now().strftime('%Y-%m-%d')}.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger()