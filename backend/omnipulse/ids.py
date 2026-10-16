"""ULID-based identifiers: lexicographically sortable by creation time."""

from __future__ import annotations

import os
import time

_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def ulid(ts_ms: int | None = None) -> str:
    ts = int(time.time() * 1000) if ts_ms is None else ts_ms
    value = (ts << 80) | int.from_bytes(os.urandom(10), "big")
    chars = []
    for _ in range(26):
        chars.append(_B32[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def new_event_id() -> str:
    return "evt_" + ulid()


def new_cluster_id() -> str:
    return "cls_" + ulid()


def new_webhook_id() -> str:
    return "whk_" + ulid()
