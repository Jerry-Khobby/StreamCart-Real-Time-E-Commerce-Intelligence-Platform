import logging 
import os 
import sys 
from logging.handlers import RotatingFileHandler 
from producer.config import LOG_DIR,LOG_LEVEL



def setup_logging(service_name: str):
    os.makedirs(LOG_DIR, exist_ok=True)

    log_path = os.path.join(LOG_DIR, f"{service_name}.log")

    logger = logging.getLogger(service_name)
    logger.setLevel(LOG_LEVEL)

    # Prevent duplicate handlers if reloaded
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # File
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
    )
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger