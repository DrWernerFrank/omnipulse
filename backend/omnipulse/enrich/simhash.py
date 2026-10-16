"""64-bit SimHash fingerprints with a banded index (pigeonhole: 4 bands of 16 bits find all
matches within Hamming distance <= 3 using exact band lookups)."""

from __future__ import annotations

import hashlib
import re
from collections import deque
from dataclasses import dataclass

_TOKEN = re.compile(r"[a-z0-9]+")
_MASK = (1 << 64) - 1


def tokens(text: str) -> list[str]:
    return _TOKEN.findall(text.lower())


def _h64(s: str) -> int:
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), "big")


def simhash(text: str, shingle: int = 3) -> int:
    toks = tokens(text)
    if not toks:
        return 0
    feats = [" ".join(toks[i:i + shingle]) for i in range(max(1, len(toks) - shingle + 1))]
    v = [0] * 64
    for f in feats:
        h = _h64(f)
        for i in range(64):
            v[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(64):
        if v[i] > 0:
            out |= 1 << i
    return out & _MASK


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


@dataclass
class _Entry:
    key: str
    h: int
    ts: float
    payload: dict


class SimHashIndex:
    BANDS = 4
    WIDTH = 16

    def __init__(self, max_distance: int = 3) -> None:
        self.max_distance = max_distance
        self._bands: list[dict[int, set[str]]] = [dict() for _ in range(self.BANDS)]
        self._entries: dict[str, _Entry] = {}
        self._order: deque[str] = deque()

    def _band_vals(self, h: int) -> list[int]:
        m = (1 << self.WIDTH) - 1
        return [(h >> (i * self.WIDTH)) & m for i in range(self.BANDS)]

    def add(self, key: str, h: int, ts: float, payload: dict) -> None:
        if not h or key in self._entries:
            return
        self._entries[key] = _Entry(key, h, ts, payload)
        self._order.append(key)
        for i, b in enumerate(self._band_vals(h)):
            self._bands[i].setdefault(b, set()).add(key)

    def find(self, h: int) -> tuple[int, dict] | None:
        if not h:
            return None
        best: tuple[int, dict] | None = None
        seen: set[str] = set()
        for i, b in enumerate(self._band_vals(h)):
            for key in self._bands[i].get(b, ()):
                if key in seen:
                    continue
                seen.add(key)
                e = self._entries[key]
                d = hamming(h, e.h)
                if d <= self.max_distance and (best is None or d < best[0]):
                    best = (d, e.payload)
        return best

    def prune(self, cutoff_ts: float) -> None:
        while self._order and self._entries[self._order[0]].ts < cutoff_ts:
            key = self._order.popleft()
            e = self._entries.pop(key)
            for i, b in enumerate(self._band_vals(e.h)):
                bucket = self._bands[i].get(b)
                if bucket:
                    bucket.discard(key)
                    if not bucket:
                        del self._bands[i][b]

    def __len__(self) -> int:
        return len(self._entries)
