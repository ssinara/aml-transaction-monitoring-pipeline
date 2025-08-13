import logging
from logging.handlers import RotatingFileHandler
import os

def get_logger(script_name):
    log_dir = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f"{script_name}.log")
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s %(message)s')
    handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(handler)

    return logger