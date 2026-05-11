import logging
import os
from datetime import datetime

def get_logger(name):
    """
    Creates and returns a logger that writes to
    both the terminal AND a log file simultaneously.
    Every pipeline run gets its own timestamped log file.
    """

    # Create logs folder if it doesn't exist
    os.makedirs("logs", exist_ok=True)

    # Timestamped log file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"logs/pipeline_{timestamp}.log"

    # Create logger with the given name
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Format — what each log line looks like
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Handler 1 — write logs to file
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)

    # Handler 2 — also show logs in terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Attach both handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger