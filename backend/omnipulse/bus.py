"""Message bus: Redis Streams in full mode, asyncio fan-out queues in lite mode.

Streams
-------
omni:raw      connector output (RawItem), consumed by the enricher group
omni:events   StreamEnvelope firehose, tailed by every API replica, consumed by webhooks group
omni:control  source registry changes (API -> engine)
omni:dlq      dead-letter records for failing sources / poison items
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from typing import Any, AsyncIterator, Protocol

import orjson

log = logging.getLogger(__name__)

RAW = "omni:raw"
EVENTS = "omni:events"
CONTROL = "omni:control"
DLQ = "omni:dlq"

MAXLEN = {RAW: 200_000, EVENTS: 100_000, CONTROL: 10_000, DLQ: 20_000}


class Bus(Protocol):
    async def start(self) -> None: ...
    async def close(self) -> None: ...
    async def publish(self, stream: str, payload: dict[str, Any]) -> str: ...
    def consume(self, stream: str, group: str, consumer: str) -> AsyncIterator[tuple[str, dict[str, Any]]]: ...
    async def ack(self, stream: str, group: str, msg_id: str) -> None: ...
    def tail(self, stream: str) -> AsyncIterator[tuple[str, dict[str, Any]]]: ...
    async def recent(self, stream: str, count: int) -> list[tuple[str, dict[str, Any]]]: ...
    async def claim_once(self, key: str, ttl_seconds: int) -> bool: ...


# --------------------------------------------------------------------------------------
# Redis Streams
# --------------------------------------------------------------------------------------


class RedisBus:
    def __init__(self, url: str) -> None:
        import redis.asyncio as redis

        self._redis = redis.from_url(url, decode_responses=False, health_check_interval=30)
        self.block_ms: int | None = 2000  # None = non-blocking polling (for Redis test doubles)

    async def start(self) -> None:
        for attempt in range(30):
            try:
                await self._redis.ping()
                return
            except Exception as exc:  # pragma: no cover - startup race with compose
                log.warning("redis not ready (%s), retry %d", exc, attempt)
                await asyncio.sleep(1 + attempt * 0.5)
        raise RuntimeError("redis unavailable")

    async def close(self) -> None:
        await self._redis.aclose()

    async def publish(self, stream: str, payload: dict[str, Any]) -> str:
        msg_id = await self._redis.xadd(
            stream, {b"d": orjson.dumps(payload)}, maxlen=MAXLEN.get(stream, 100_000), approximate=True
        )
        return msg_id.decode()

    async def _ensure_group(self, stream: str, group: str) -> None:
        with contextlib.suppress(Exception):
            await self._redis.xgroup_create(stream, group, id="$", mkstream=True)

    async def consume(self, stream: str, group: str, consumer: str) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        await self._ensure_group(stream, group)
        # Re-deliver anything this consumer had pending before a restart, then go live.
        cursor = b"0"
        while True:
            try:
                resp = await self._redis.xreadgroup(group, consumer, {stream: cursor}, count=200, block=self.block_ms)
            except Exception as exc:
                if "NOGROUP" in str(exc):
                    await self._ensure_group(stream, group)
                    continue
                log.warning("xreadgroup failed: %s", exc)
                await asyncio.sleep(1)
                continue
            entries = resp[0][1] if resp else []
            if cursor == b"0" and not entries:
                cursor = b">"
                continue
            if not entries and self.block_ms is None:
                await asyncio.sleep(0.01)
            for msg_id, fields in entries:
                raw = fields.get(b"d")
                if raw is None:
                    continue
                yield msg_id.decode(), orjson.loads(raw)

    async def ack(self, stream: str, group: str, msg_id: str) -> None:
        await self._redis.xack(stream, group, msg_id)

    async def tail(self, stream: str) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        # Resolve "$" to a concrete id once: XREAD re-evaluates "$" on every call, so anything
        # published between two blocking reads would otherwise be skipped.
        try:
            newest = await self._redis.xrevrange(stream, count=1)
            last = newest[0][0] if newest else b"0-0"
        except Exception:
            last = b"0-0"
        while True:
            try:
                resp = await self._redis.xread({stream: last}, count=500, block=self.block_ms)
                if not resp and self.block_ms is None:
                    await asyncio.sleep(0.01)
            except Exception as exc:
                log.warning("xread failed: %s", exc)
                await asyncio.sleep(1)
                continue
            for _, entries in resp or []:
                for msg_id, fields in entries:
                    last = msg_id
                    raw = fields.get(b"d")
                    if raw is not None:
                        yield msg_id.decode(), orjson.loads(raw)

    async def recent(self, stream: str, count: int) -> list[tuple[str, dict[str, Any]]]:
        rows = await self._redis.xrevrange(stream, count=count)
        return [(i.decode(), orjson.loads(f[b"d"])) for i, f in rows if b"d" in f]

    async def claim_once(self, key: str, ttl_seconds: int) -> bool:
        return bool(await self._redis.set(key, b"1", nx=True, ex=ttl_seconds))


# --------------------------------------------------------------------------------------
# In-memory (lite mode / tests)
# --------------------------------------------------------------------------------------


class MemoryBus:
    def __init__(self, history: int = 5000) -> None:
        self._seq = 0
        self._history: dict[str, deque[tuple[str, dict[str, Any]]]] = {}
        self._groups: dict[tuple[str, str], asyncio.Queue] = {}
        self._tails: dict[str, set[asyncio.Queue]] = {}
        self._claims: dict[str, float] = {}
        self._history_len = history

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def publish(self, stream: str, payload: dict[str, Any]) -> str:
        self._seq += 1
        msg_id = f"{self._seq}-0"
        item = (msg_id, payload)
        self._history.setdefault(stream, deque(maxlen=self._history_len)).append(item)
        for (s, _), q in self._groups.items():
            if s == stream:
                q.put_nowait(item)
        for q in self._tails.get(stream, ()):
            if q.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()
            q.put_nowait(item)
        return msg_id

    async def consume(self, stream: str, group: str, consumer: str) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        q = self._groups.setdefault((stream, group), asyncio.Queue())
        while True:
            yield await q.get()

    async def ack(self, stream: str, group: str, msg_id: str) -> None:
        return None

    async def tail(self, stream: str) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        q: asyncio.Queue = asyncio.Queue(maxsize=20000)
        self._tails.setdefault(stream, set()).add(q)
        try:
            while True:
                yield await q.get()
        finally:
            self._tails[stream].discard(q)

    async def recent(self, stream: str, count: int) -> list[tuple[str, dict[str, Any]]]:
        return list(reversed(list(self._history.get(stream, ()))[-count:]))

    async def claim_once(self, key: str, ttl_seconds: int) -> bool:
        loop_now = asyncio.get_running_loop().time()
        exp = self._claims.get(key)
        if exp is not None and exp > loop_now:
            return False
        self._claims[key] = loop_now + ttl_seconds
        if len(self._claims) > 500_000:
            self._claims = {k: v for k, v in self._claims.items() if v > loop_now}
        return True


def make_bus(mode: str, redis_url: str) -> Bus:
    return MemoryBus() if mode == "lite" else RedisBus(redis_url)
