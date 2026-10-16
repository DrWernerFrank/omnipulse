"""Event bus.

Primary mode: Redis. Events are XADD-ed to a bounded stream (durable replay for
agents) and PUBLISH-ed to a pub/sub channel (sub-millisecond live fan-out to
WebSocket/SSE subscribers and future agent webhooks).

Fallback mode (no Redis): direct in-process fan-out to subscriber queues.

Subscribers register a FilterSpec; matching happens server-side so clients
only receive what they asked for.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import orjson
import redis.asyncio as aioredis

from .config import get_settings
from .schemas import FilterSpec, NewsEvent

log = logging.getLogger("omnipulse.bus")

EVENT_STREAM = "omnipulse:events"
LIVE_CHANNEL = "omnipulse:live"
CLUSTER_CHANNEL = "omnipulse:clusters"
STATS_CHANNEL = "omnipulse:stats"
SOURCE_CHANNEL = "omnipulse:source_state"


class Subscriber:
    def __init__(self, filters: FilterSpec | None):
        # NOTE: filters is mutated in place on resubscribe (see Bus.resubscribe)
        self.filters = filters
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=4096)
        self.closed = False

    def deliver(self, frame: dict[str, Any]) -> None:
        if self.closed:
            return
        if self.queue.full():  # backpressure: drop oldest, keep newest
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        self.queue.put_nowait(frame)

    def close(self) -> None:
        self.closed = True


class Bus:
    def __init__(self) -> None:
        self.redis: aioredis.Redis | None = None
        self.subscribers: set[Subscriber] = set()
        self._tasks: list[asyncio.Task] = []
        self.mode = "local"

    async def start(self) -> None:
        settings = get_settings()
        if settings.redis_url:
            try:
                self.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
                await self.redis.ping()
                pubsub = self.redis.pubsub()
                await pubsub.subscribe(LIVE_CHANNEL, CLUSTER_CHANNEL, STATS_CHANNEL, SOURCE_CHANNEL)
                self._tasks.append(asyncio.create_task(self._redis_loop(pubsub)))
                self.mode = "redis"
                log.info("bus: redis streams+pubsub at %s", settings.redis_url)
            except Exception as e:
                log.warning("bus: redis unavailable (%s); using in-process fan-out", e)
                self.redis = None
        else:
            log.info("bus: no REDIS_URL; using in-process fan-out")

    async def close(self) -> None:
        for t in self._tasks:
            t.cancel()
        for sub in list(self.subscribers):
            sub.close()
        if self.redis:
            await self.redis.aclose()

    # ---- publishing ----
    async def publish_event(self, ev: NewsEvent) -> None:
        payload = ev.model_dump_json()  # canonical JSON string
        if self.redis:
            try:
                pipe = self.redis.pipeline()
                pipe.xadd(EVENT_STREAM, {"d": payload}, maxlen=get_settings().stream_maxlen, approximate=True)
                pipe.publish(LIVE_CHANNEL, payload)
                await pipe.execute()
                return
            except Exception as e:
                log.warning("bus publish degraded to local: %s", e)
        self._fanout_local(LIVE_CHANNEL, orjson.loads(payload))

    async def publish_cluster(self, cs_payload: dict[str, Any]) -> None:
        await self._publish_generic(CLUSTER_CHANNEL, cs_payload)

    async def publish_stats(self, payload: dict[str, Any]) -> None:
        await self._publish_generic(STATS_CHANNEL, payload)

    async def publish_source_state(self, payload: dict[str, Any]) -> None:
        await self._publish_generic(SOURCE_CHANNEL, payload)

    async def _publish_generic(self, channel: str, payload: dict[str, Any]) -> None:
        raw = orjson.dumps(payload).decode()
        if self.redis:
            try:
                await self.redis.publish(channel, raw)
                return
            except Exception as e:
                log.warning("bus publish degraded to local: %s", e)
        self._fanout_local(channel, payload)

    # ---- subscription ----
    def subscribe(self, filters: FilterSpec | None) -> Subscriber:
        sub = Subscriber(filters)
        self.subscribers.add(sub)
        return sub

    def resubscribe(self, sub: Subscriber, filters: FilterSpec | None) -> None:
        """Swap a live subscriber's filter in place; the queue is untouched so
        the consumer never changes which queue it reads from."""
        if sub in self.subscribers:
            sub.filters = filters

    def unsubscribe(self, sub: Subscriber) -> None:
        sub.close()
        self.subscribers.discard(sub)

    # ---- replay (durable stream reads for agents) ----
    async def replay(self, last_event_id: str | None = None, count: int = 100) -> list[dict[str, Any]]:
        if not self.redis:
            return []
        try:
            start = last_event_id or "-"
            results = await self.redis.xrange(EVENT_STREAM, min=f"({start}" if last_event_id else "-", max="+", count=count)
            return [orjson.loads(r[1]["d"]) for r in results]
        except Exception as e:
            log.warning("replay failed: %s", e)
            return []

    # ---- internals ----
    async def _redis_loop(self, pubsub) -> None:
        try:
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                try:
                    payload = orjson.loads(msg["data"])
                except Exception:
                    continue
                self._fanout_local(msg["channel"], payload)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("redis listener died: %s", e)

    def _fanout_local(self, channel: str, payload: dict[str, Any]) -> None:
        if channel == LIVE_CHANNEL:
            for sub in self.subscribers:
                if sub.filters is None or sub.filters.match_dict(payload):
                    sub.deliver({"t": "event", "d": payload})
        else:
            t = {"cluster": "cluster", "stats": "stats", "source_state": "source_state"}.get(
                channel.rsplit(":", 1)[-1], "info"
            )
            frame = {"t": t, "d": payload}
            for sub in self.subscribers:
                sub.deliver(frame)
