"""JSON-line logging tuned for a dense terminal + container logs."""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone

from .config import get_settings


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        return "\t".join(
            [
                ts,
                record.levelname[0],
                record.name,
                record.getMessage(),
            ]
        )


def setup_logging() -> None:
    level = getattr(logging, get_settings().log_level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for noisy in ("httpx", "httpcore", "asyncio", "websockets", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
