"""Ingestion engine: one supervised task per source with adaptive polling, health, and DLQ."""

from __future__ import annotations

import asyncio
import logging
import random
import time
import zlib
from datetime import timedelta
from typing import Any

from .. import bus as B
from ..schema import RawItem, SourceConfig, SourceHealth, iso, utcnow
from .apis import (CisaKevConnector, GdeltConnector, GithubTrendingConnector, HackerNewsConnector,
                   KalshiConnector, NvdConnector, PolymarketConnector)
from .base import Connector, ConnectorContext, SeenSet
from .http import FetchError, Fetcher, backoff_delay
from .rss import RSSConnector, SecEdgarConnector
from .social import (BlueskyJetstreamConnector, NewsApiConnector, TelegramChannelConnector,
                     WhaleAlertConnector, XListConnector)

log = logging.getLogger(__name__)

CONNECTORS: dict[str, type[Connector]] = {
    c.kind: c
    for c in (
        RSSConnector, SecEdgarConnector, HackerNewsConnector, GdeltConnector, PolymarketConnector,
        KalshiConnector, NvdConnector, CisaKevConnector, GithubTrendingConnector,
        BlueskyJetstreamConnector, TelegramChannelConnector, NewsApiConnector, XListConnector,
        WhaleAlertConnector,
    )
}


def shard_of(source_id: str, shards: int) -> int:
    return zlib.crc32(source_id.encode()) % shards


class SourceRunner:
    def __init__(self, cfg: SourceConfig, engine: "IngestEngine", state: dict[str, Any]) -> None:
        self.cfg = cfg
        self.engine = engine
        self.state = state
        self.seen = SeenSet(state.get("seen", []))
        self.health = engine.health.setdefault(cfg.id, SourceHealth(id=cfg.id))
        self.health.status = "starting"
        self.connector: Connector | None = None
        self.task: asyncio.Task | None = None

    # ------------------------------------------------------------------------------
    def export_state(self) -> dict[str, Any]:
        st = dict(self.connector.state if self.connector else self.state)
        st["seen"] = self.seen.export(3000)
        return st

    def start(self) -> None:
        self.task = asyncio.create_task(self._main(), name=f"source:{self.cfg.id}")

    async def stop(self) -> None:
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except (asyncio.CancelledError, Exception):
                pass
        if self.connector:
            await self.connector.close()

    async def _main(self) -> None:
        cfg, s = self.cfg, self.engine.settings
        if cfg.type == "webhook":
            self.health.status = "push"
            return
        if not cfg.enabled:
            self.health.status = "paused"
            return
        if cfg.requires_key and not s.credential(cfg.requires_key):
            self.health.status = "disabled"
            self.health.last_error = f"set OMNI_{cfg.requires_key.upper()} to enable"
            return
        cls = CONNECTORS.get(cfg.type)
        if cls is None:
            self.health.status = "dead"
            self.health.last_error = f"no connector for type {cfg.type}"
            return
        self.connector = cls(cfg, self.engine.ctx, self.state)
        try:
            if cls.streaming:
                await self._run_stream()
            else:
                await self._run_poll()
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - last-resort guard
            log.exception("runner crashed: %s", cfg.id)
            self.health.status = "dead"

    # ------------------------------------------------------------------------------
    async def _emit(self, item: RawItem, first_poll: bool = False) -> bool:
        if item.guid in self.seen:
            return False
        self.seen.add(item.guid)
        now = utcnow()
        if item.published_at is None or item.published_at > now + timedelta(minutes=5):
            item.published_at = now
        # Archive-style feeds (hundreds of historical entries) must never surface stale items,
        # even after a restart where the seen-set was only partially persisted.
        max_age = self.engine.settings.max_item_age_hours * (1 if first_poll else 2)
        if now - item.published_at > timedelta(hours=max_age):
            return False
        await self.engine.sink(item)
        self.health.items_total += 1
        self.health.last_item_at = now
        return True

    async def _run_poll(self) -> None:
        cfg, s, h = self.cfg, self.engine.settings, self.health
        assert self.connector is not None
        interval = float(self.state.get("interval") or cfg.poll_interval)
        first = not self.state.get("primed")
        while True:
            h.last_poll = utcnow()
            h.polls_total += 1
            t0 = time.perf_counter()
            try:
                items = await asyncio.wait_for(self.connector.poll(), timeout=max(60, s.http_timeout * 4))
                cap = s.max_backfill_per_source if first else int(cfg.params.get("max_items_per_poll", 0))
                if cap:
                    # keep the newest N unseen items; mark the rest seen so batch feeds can't flood
                    items = sorted((i for i in items if i.guid not in self.seen),
                                   key=lambda i: i.published_at or utcnow(), reverse=True)
                    for it in items[cap:]:
                        self.seen.add(it.guid)
                    items = items[:cap]
                new = 0
                for it in items:
                    new += await self._emit(it, first_poll=first)
                elapsed = (time.perf_counter() - t0) * 1000
                h.avg_latency_ms = elapsed if not h.avg_latency_ms else 0.8 * h.avg_latency_ms + 0.2 * elapsed
                if not items and getattr(self.connector, "state", {}).get("etag"):
                    h.not_modified_total += 1
                h.last_success, h.last_error, h.consecutive_failures = utcnow(), None, 0
                h.status = "healthy"
                interval = self._adapt(interval, new)
                first = False
                self.state["primed"] = True
                delay = interval
                if getattr(self.connector, "push_active", False):
                    delay = max(delay, 300.0)
                    h.status = "push"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                h.consecutive_failures += 1
                h.last_error = f"{type(exc).__name__}: {exc}"[:300]
                dead = h.consecutive_failures >= s.dead_failure_threshold
                h.status = "dead" if dead else "degraded"
                delay = backoff_delay(h.consecutive_failures, max(cfg.min_interval, 5.0),
                                      cfg.max_interval * (4 if dead else 1))
                if isinstance(exc, FetchError) and exc.retry_after:
                    delay = max(delay, exc.retry_after)
                if h.consecutive_failures == s.dlq_failure_threshold or (dead and h.consecutive_failures % 20 == 0):
                    await self.engine.dead_letter(cfg, h.last_error, h.consecutive_failures)
                log.info("source %s failed (%d): %s", cfg.id, h.consecutive_failures, h.last_error)
            h.interval = round(delay, 1)
            self.state["interval"] = interval
            await asyncio.sleep(delay * random.uniform(0.9, 1.1))

    def _adapt(self, interval: float, n_new: int) -> float:
        """Adaptive cadence: aim for ~1 new item per poll, bounded by [min, max]."""
        lo, hi = self.cfg.min_interval, self.cfg.max_interval
        rate = float(self.state.get("rate", 0.0))
        if n_new > 0:
            rate = 0.3 * (n_new / max(interval, 1.0)) + 0.7 * rate
            target = 1.0 / rate if rate > 0 else hi
            interval = min(interval * 0.6, target)
        else:
            rate *= 0.9
            interval = interval * 1.2 + 0.5
        self.state["rate"] = rate
        return max(lo, min(hi, interval))

    async def _run_stream(self) -> None:
        h = self.health
        assert self.connector is not None
        while True:
            h.last_poll = utcnow()
            try:
                h.status = "healthy"
                await self.connector.stream(lambda it: self._emit(it))
                h.consecutive_failures = 0
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                h.consecutive_failures += 1
                h.last_error = f"{type(exc).__name__}: {exc}"[:300]
                h.status = "dead" if h.consecutive_failures >= self.engine.settings.dead_failure_threshold else "degraded"
                if h.consecutive_failures == self.engine.settings.dlq_failure_threshold:
                    await self.engine.dead_letter(self.cfg, h.last_error, h.consecutive_failures)
                await asyncio.sleep(backoff_delay(h.consecutive_failures, 2.0, 300.0))
            else:
                h.last_success = utcnow()


class IngestEngine:
    def __init__(self, settings, bus, store, fetcher: Fetcher | None = None,
                 shard: int = 0, shards: int = 1) -> None:
        self.settings = settings
        self.bus = bus
        self.store = store
        self.fetcher = fetcher or Fetcher(settings.user_agent, settings.http_timeout)
        self.ctx = ConnectorContext(self.fetcher, settings)
        self.runners: dict[str, SourceRunner] = {}
        self.health: dict[str, SourceHealth] = {}
        self.states: dict[str, dict[str, Any]] = {}
        self.shard, self.shards = shard, shards
        self._tasks: list[asyncio.Task] = []
        self.items_emitted = 0

    def owns(self, source_id: str) -> bool:
        return self.shards <= 1 or shard_of(source_id, self.shards) == self.shard

    async def sink(self, item: RawItem) -> None:
        self.items_emitted += 1
        await self.bus.publish(B.RAW, item.model_dump(mode="json"))

    async def dead_letter(self, cfg: SourceConfig, error: str | None, failures: int) -> None:
        await self.bus.publish(B.DLQ, {"kind": "source_failure", "source_id": cfg.id, "url": cfg.url,
                                       "error": error, "failures": failures, "ts": iso(utcnow())})

    # ------------------------------------------------------------------------------
    async def start(self) -> None:
        self.states = await self.store.load_source_states()
        for cfg in await self.store.list_sources():
            if self.owns(cfg.id):
                self._start(cfg)
        log.info("ingest engine started with %d sources (shard %d/%d)", len(self.runners), self.shard, self.shards)
        self._tasks = [
            asyncio.create_task(self._control_loop(), name="ingest:control"),
            asyncio.create_task(self._persist_loop(), name="ingest:persist"),
            asyncio.create_task(self._reconcile_loop(), name="ingest:reconcile"),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*(r.stop() for r in self.runners.values()), return_exceptions=True)
        await self._persist()
        await self.fetcher.close()

    def _start(self, cfg: SourceConfig) -> None:
        runner = SourceRunner(cfg, self, self.states.setdefault(cfg.id, {}))
        self.runners[cfg.id] = runner
        runner.start()

    async def reload(self, source_id: str) -> None:
        old = self.runners.pop(source_id, None)
        if old:
            self.states[source_id] = old.export_state()
            await old.stop()
        cfg = await self.store.get_source(source_id)
        if cfg is None:
            self.health.pop(source_id, None)
            return
        if self.owns(cfg.id):
            self._start(cfg)

    async def _control_loop(self) -> None:
        async for _, msg in self.bus.tail(B.CONTROL):
            sid = msg.get("source_id")
            if sid:
                log.info("control %s %s", msg.get("op"), sid)
                await self.reload(sid)

    async def _reconcile_loop(self) -> None:
        """Safety net for missed control messages: converge runners to the stored registry."""
        while True:
            await asyncio.sleep(60)
            try:
                stored = {c.id: c for c in await self.store.list_sources() if self.owns(c.id)}
                for sid in set(self.runners) - set(stored):
                    await self.reload(sid)
                for sid, cfg in stored.items():
                    r = self.runners.get(sid)
                    if r is None or r.cfg != cfg:
                        await self.reload(sid)
            except Exception as exc:
                log.warning("reconcile failed: %s", exc)

    async def _persist(self) -> None:
        try:
            await self.store.save_health(list(self.health.values()))
            await self.store.save_source_state({sid: r.export_state() for sid, r in self.runners.items()})
        except Exception as exc:
            log.warning("persist failed: %s", exc)

    async def _persist_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            await self._persist()
            await self.bus.publish(B.EVENTS, {
                "type": "SOURCE_HEALTH",
                "ts": iso(utcnow()),
                "data": {"sources": [h.model_dump(mode="json") for h in self.health.values()]},
            })
