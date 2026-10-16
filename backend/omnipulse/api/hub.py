"""In-process fan-out: one bus tail per API replica, filtered into per-subscriber queues."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import deque
from typing import Any

from .. import bus as B
from ..filters import EventFilter

log = logging.getLogger(__name__)

ALL_TYPES = frozenset({"EVENT_CREATED", "EVENT_UPDATED", "CLUSTER_UPDATED", "SOURCE_HEALTH", "HEARTBEAT"})
DEFAULT_TYPES = frozenset({"EVENT_CREATED", "EVENT_UPDATED", "CLUSTER_UPDATED"})


class Subscriber:
    def __init__(self, f: EventFilter, fmt: str, types: frozenset[str], maxsize: int) -> None:
        self.filter = f
        self.format = fmt
        self.types = types
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=maxsize)
        self.dropped = 0

    def offer(self, env: dict[str, Any]) -> None:
        if env.get("type") not in self.types or not self.filter.matches_envelope(env):
            return
        if self.queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                self.queue.get_nowait()
                self.dropped += 1
        self.queue.put_nowait(env)

    def drain(self, limit: int = 500) -> list[dict[str, Any]]:
        out = []
        while len(out) < limit:
            try:
                out.append(self.queue.get_nowait())
            except asyncio.QueueEmpty:
                break
        return out


class Hub:
    def __init__(self, bus, queue_size: int = 5000) -> None:
        self.bus = bus
        self.queue_size = queue_size
        self.subscribers: set[Subscriber] = set()
        self.recent: deque[dict[str, Any]] = deque(maxlen=2000)
        self.last_heartbeat: dict[str, Any] = {}
        self.last_health: list[dict[str, Any]] = []
        self.delivered = 0
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="hub")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        async for _, env in self.bus.tail(B.EVENTS):
            t = env.get("type")
            if t == "HEARTBEAT":
                self.last_heartbeat = env.get("data", {})
            elif t == "SOURCE_HEALTH":
                self.last_health = env.get("data", {}).get("sources", [])
            elif t in ("EVENT_CREATED", "EVENT_UPDATED"):
                self.recent.append(env)
            for sub in list(self.subscribers):
                sub.offer(env)
            self.delivered += 1

    def subscribe(self, f: EventFilter, fmt: str = "full", types: frozenset[str] = DEFAULT_TYPES) -> Subscriber:
        sub = Subscriber(f, fmt, types, self.queue_size)
        self.subscribers.add(sub)
        return sub

    def unsubscribe(self, sub: Subscriber) -> None:
        self.subscribers.discard(sub)
