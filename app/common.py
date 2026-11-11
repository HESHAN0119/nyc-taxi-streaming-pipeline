"""Shared helpers: settings from .env and JSON logging."""

import json
import logging
import os
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

load_dotenv(PROJECT_ROOT / ".env")


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing setting {name}: add it to .env (see .env.example)")
    return value


# fields every log record has; anything else came from extra={...}
_STANDARD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    # one JSON object per line

    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "service": self.service,
            "message": record.getMessage(),
        }
        entry.update({k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS})
        if record.exc_info:
            entry["error"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def setup_logging(service: str) -> logging.Logger:
    # log to console and logs/<service>.log
    log_dir = PROJECT_ROOT / env("LOG_DIR", "logs")
    log_dir.mkdir(exist_ok=True)
    console = logging.StreamHandler()
    file = RotatingFileHandler(log_dir / f"{service}.log", maxBytes=10_000_000, backupCount=3, encoding="utf-8")
    for handler in (console, file):
        handler.setFormatter(JsonFormatter(service))
    logging.basicConfig(level=env("LOG_LEVEL", "INFO"), handlers=[console, file], force=True)
    logging.getLogger("kafka").setLevel(logging.WARNING)  # kafka-python is too noisy
    return logging.getLogger(service)
