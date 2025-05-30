import logging
import os
from datetime import datetime

def setup_logger() -> logging.Logger:
    """
    Создаёт файл logs/scrape_YYYY-MM-DD.log и возвращает настроенный logger.
    """
    os.makedirs("logs", exist_ok=True)
    file = f"logs/scrape_{datetime.now():%Y-%m-%d}.log"

    logging.basicConfig(
        filename=file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logging.getLogger().addHandler(console)

    return logging.getLogger()
