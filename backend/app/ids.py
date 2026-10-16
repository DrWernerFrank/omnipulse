"""ULID generation: 128-bit ids that sort lexicographically by creation time.

Format: 48-bit millisecond timestamp + 80-bit randomness, Crockford base32.
Event ids (evt_...) therefore double as cursors for keyset pagination.
"""
from __future__ import annotations

import os
import threading
import time

_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_lock = threading.Lock()
_last_ms: int = -1
_last_rand: int = 0


def ulid() -> str:
    global _last_ms, _last_rand
    with _lock:
        ms = int(time.time() * 1000) & ((1 << 48) - 1)
        if ms <= _last_ms:
            # Same millisecond (or clock regression): increment for monotonicity.
            _last_rand += 1
            ms = _last_ms
        else:
            _last_rand = int.from_bytes(os.urandom(10), "big")
            _last_ms = ms
        value = (ms << 80) | (_last_rand & ((1 << 80) - 1))
    out = []
    for i in range(26):
        out.append(_ALPHABET[(value >> (5 * (25 - i))) & 0x1F])
    return "".join(out)


def new_id(prefix: str) -> str:
    assert prefix in ("evt", "cls", "src", "dlq"), "unknown id namespace"
    return f"{prefix}_{ulid()}"
