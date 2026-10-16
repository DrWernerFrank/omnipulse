"""Ingestion engine: runs the whole source registry.

Responsibilities:
- Load config/default_sources.json into the store at boot (additive merge:
  user-added runtime sources are preserved; defaults can be disabled).
- Adaptive per-source scheduling: intervals scale between min_poll_sec and
  max_poll_sec based on observed item velocity (busy feed => poll faster).
- Health monitoring: consecutive failures => exponential backoff; beyond
  settings.auto_pause_failures the source is marked dead, parked, and probed
  again every resume_probe_sec.
- DLQ: every terminal fetch error is recorded for the /api/dlq inspector.
- Stream connectors (bluesky/telegram) run as long-lived tasks.
- Publishes source-state frames and stats to the bus for the UI.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
import time
from datetime import timedelta
from typing import Any

import orjson

from .config import get_settings
from .connectors import get_connector
from .db import Store
from .bus import Bus
from .http import HttpPool
from .ids import new_id
from .pipeline import Pipeline
from .schemas import RawItem, SourceConfig, SourceStateMessage, now_utc
from .streams import bluesky_stream, telegram_stream
from .websub import new_secret, parse_push, push_active, subscribe

log = logging.getLogger("omnipulse.engine")


class Engine:
    def __init__(self, store: Store, bus: Bus, pipeline: Pipeline):
        self.store = store
        self.bus = bus
        self.pipeline = pipeline
        self.sources: dict[str, SourceConfig] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._next_due: dict[str, float] = {}          # source_id -> monotonic ts
        self._fail_count: dict[str, int] = {}
        self._last_success: dict[str, float] = {}
        self._last_items: dict[str, int] = {}
        self._seen_external: dict[str, dict[str, float]] = {}  # source -> ext_id -> mono
        self._stop = asyncio.Event()
        self._stats_task: asyncio.Task | None = None
        self._main_task: asyncio.Task | None = None
        self._websub_task: asyncio.Task | None = None
        self._streams: dict[str, asyncio.Task] = {}

    # ------------------------------------------------------------------ #
    # lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        await self._load_defaults()
        self._main_task = asyncio.create_task(self._run(), name="engine-loop")
        self._stats_task = asyncio.create_task(self._stats_loop(), name="stats-loop")
        if push_active():
            self._websub_task = asyncio.create_task(self._websub_loop(), name="websub-loop")
            log.info("websub push mode active: hubs will deliver items instantly")
        for cfg in self.sources.values():
            if not cfg.enabled:
                continue
            if cfg.type in ("bluesky", "telegram"):
                self._spawn_stream(cfg)
        log.info(
            "engine started: %d sources (%d polling, %d streaming)",
            len(self.sources),
            sum(1 for c in self.sources.values() if c.enabled and c.type not in ("bluesky", "telegram")),
            sum(1 for c in self.sources.values() if c.enabled and c.type in ("bluesky", "telegram")),
        )

    async def stop(self) -> None:
        self._stop.set()
        for t in list(self._tasks.values()) + list(self._streams.values()):
            t.cancel()
        if self._main_task:
            self._main_task.cancel()
        if self._stats_task:
            self._stats_task.cancel()
        if self._websub_task:
            self._websub_task.cancel()

    # ------------------------------------------------------------------ #
    # registry management (API + UI surface)
    # ------------------------------------------------------------------ #
    async def _load_defaults(self) -> None:
        import os

        path = get_settings().sources_file
        if not os.path.exists(path):
            # try path relative to the repo root when running from source
            path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), path)
        try:
            with open(path, "rb") as f:
                data = orjson.loads(f.read())
        except Exception as e:
            log.error("cannot load sources file %s: %s", path, e)
            data = []
        defaults = [SourceConfig.model_validate(s) for s in data]
        existing = {s.id: s for s in await self.store.list_sources()}
        fresh = 0
        for cfg in defaults:
            if cfg.id in existing:
                # keep user runtime overrides; default entries stay as-shipped
                # unless the user explicitly changed them (state marker)
                if existing[cfg.id].state.get("user_modified"):
                    self.sources[cfg.id] = existing[cfg.id]
                else:
                    self.sources[cfg.id] = cfg
                    await self.store.upsert_source(cfg)
            else:
                self.sources[cfg.id] = cfg
                await self.store.upsert_source(cfg)
                fresh += 1
        for sid, cfg in existing.items():
            if sid not in self.sources:
                self.sources[sid] = cfg   # user-added runtime source
        log.info("registry: %d sources (%d seeded from defaults file)", len(self.sources), fresh)

    async def add_source(self, cfg: SourceConfig) -> None:
        cfg.state["user_modified"] = True
        self.sources[cfg.id] = cfg
        await self.store.upsert_source(cfg)
        self._next_due[cfg.id] = time.monotonic()
        if cfg.type in ("bluesky", "telegram") and cfg.enabled:
            self._spawn_stream(cfg)
        await self._broadcast_source_state(cfg.id, "ok" if cfg.enabled else "paused", "added")

    async def update_source(self, cfg: SourceConfig) -> None:
        cfg.state["user_modified"] = True
        self.sources[cfg.id] = cfg
        await self.store.upsert_source(cfg)
        if cfg.type in ("bluesky", "telegram"):
            old = self._streams.pop(cfg.id, None)
            if old:
                old.cancel()
            if cfg.enabled:
                self._spawn_stream(cfg)
        elif not cfg.enabled:
            # allow immediate re-evaluation
            self._next_due[cfg.id] = time.monotonic() + 10
        await self._broadcast_source_state(cfg.id, "ok" if cfg.enabled else "paused", "updated")

    async def remove_source(self, source_id: str) -> bool:
        cfg = self.sources.pop(source_id, None)
        if cfg is None:
            return False
        task = self._streams.pop(source_id, None)
        if task:
            task.cancel()
        await self.store.delete_source(source_id)
        await self._broadcast_source_state(source_id, "paused", "removed")
        return True

    async def set_enabled(self, source_id: str, enabled: bool) -> SourceConfig | None:
        cfg = self.sources.get(source_id)
        if cfg is None:
            return None
        cfg.enabled = enabled
        await self.update_source(cfg)
        return cfg

    # ------------------------------------------------------------------ #
    # main scheduling loop
    # ------------------------------------------------------------------ #
    async def _run(self) -> None:
        import os

        try:
            while not self._stop.is_set():
                now = time.monotonic()
                for sid, cfg in list(self.sources.items()):
                    if cfg.requires_key and not os.environ.get(cfg.requires_key):
                        if cfg.state.get("health") != "waiting_key":
                            cfg.state["health"] = "waiting_key"
                            await self._broadcast_source_state(
                                sid, "waiting_key", f"set {cfg.requires_key} to activate"
                            )
                        if sid in self._tasks:
                            self._tasks.pop(sid, None)
                        continue
                    if cfg.requires_key and os.environ.get(cfg.requires_key) and cfg.state.get("health") == "waiting_key":
                        cfg.state["health"] = "ok"
                        self._next_due[sid] = time.monotonic()

                ready = [
                    (sid, cfg) for sid, cfg in self.sources.items()
                    if cfg.enabled
                    and cfg.type not in ("bluesky", "telegram")
                    and (cfg.requires_key is None or os.environ.get(cfg.requires_key))
                    and self._next_due.get(sid, 0) <= now
                    and sid not in self._tasks
                ]
                for sid, cfg in ready:
                    self._tasks[sid] = asyncio.create_task(
                        self._poll_source(sid), name=f"poll-{sid}"
                    )
                await asyncio.sleep(get_settings().poll_tick_sec)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.exception("engine loop crashed: %s", e)

    async def _poll_source(self, sid: str) -> None:
        cfg = self.sources.get(sid)
        task = self._tasks.get(sid)
        try:
            if cfg is None:
                return
            connector = get_connector(cfg)
            if connector is None:
                raise ValueError(f"no connector for type {cfg.type}")
            items = await connector(cfg)

            # per-source external-id dedup (upstreams re-serve entries)
            seen = self._seen_external.setdefault(sid, {})
            fresh: list[RawItem] = []
            now = time.monotonic()
            for it in items:
                key = it.external_id or it.url or it.title
                if key in seen:
                    continue
                seen[key] = now
                fresh.append(it)
            if len(seen) > 4000:
                cutoff = now - get_settings().cluster_window_hours * 3600
                self._seen_external[sid] = {k: v for k, v in seen.items() if v >= cutoff}

            accepted = 0
            for it in fresh:
                try:
                    ev = await self.pipeline.process(it, cfg)
                    if ev is not None:
                        accepted += 1
                except Exception as e:
                    log.debug("pipeline error (%s): %s", sid, e)

            # ---- success bookkeeping + adaptive interval ----
            self._fail_count[sid] = 0
            self._last_success[sid] = now
            self._last_items[sid] = len(items)
            cfg.state["health"] = "ok"
            cfg.state["last_items"] = len(items)
            interval = self._adaptive_interval(cfg, len(fresh))
            cfg.state["interval_sec"] = interval
            if accepted > 0 and cfg.state.get("health") == "degraded":
                await self._broadcast_source_state(sid, "ok", f"recovered; {accepted} new items")

            # ---- websub: subscribe on first hub discovery (push beats polling) ----
            hub = cfg.state.get("websub_hub_found")
            if hub and push_active() and not cfg.state.get("websub_secret"):
                secret = new_secret()
                s = get_settings()
                callback = f"{s.public_url.rstrip('/')}/websub/callback/{cfg.id}?token={secret}"
                cfg.state["websub_secret"] = secret
                cfg.state["websub_hub"] = hub
                if await subscribe(hub, cfg.url or "", callback, secret):
                    log.info("websub subscribed: %s via %s (push mode)", cfg.id, hub)
        except asyncio.CancelledError:
            return
        except Exception as e:
            # ---- failure path: backoff, DLQ, auto-pause ----
            fails = self._fail_count.get(sid, 0) + 1
            self._fail_count[sid] = fails
            s = get_settings()
            base = cfg.effective_poll()
            backoff = min(base * (2 ** min(fails, 6)), s.max_poll_sec_cap)
            jitter = random.uniform(0.8, 1.2)
            cfg.state["interval_sec"] = int(backoff * jitter)
            try:
                await self.store.add_dlq(sid, f"{type(e).__name__}: {e}"[:300])
            except Exception:
                pass
            if fails >= s.auto_pause_failures:
                cfg.state["health"] = "dead"
                cfg.state["paused_until"] = time.monotonic() + s.resume_probe_sec
                await self._broadcast_source_state(sid, "dead", f"auto-paused after {fails} failures")
                log.warning("source %s marked DEAD after %d failures: %s", sid, fails, e)
            else:
                cfg.state["health"] = "degraded"
                if fails in (1, 4):
                    await self._broadcast_source_state(sid, "degraded", str(e)[:120])
        finally:
            self._tasks.pop(sid, None)
            self._next_due[sid] = time.monotonic() + float(
                cfg.state.get("interval_sec", cfg.effective_poll()) if cfg else 60
            )
            # dead sources: probe again later
            if cfg is not None and cfg.state.get("health") == "dead":
                paused_until = cfg.state.get("paused_until", 0)
                self._next_due[sid] = max(paused_until, time.monotonic() + get_settings().resume_probe_sec)
                if time.monotonic() >= paused_until:
                    cfg.state["health"] = "degraded"
                    self._fail_count[sid] = max(0, get_settings().auto_pause_failures - 2)

    def _adaptive_interval(self, cfg: SourceConfig, fresh: int) -> int:
        """Velocity-driven interval: hot feeds drop straight to the floor,
        warm feeds ease toward it, quiet feeds drift up (bounded by max_poll_sec)."""
        interval = cfg.state.get("interval_sec", cfg.effective_poll())
        if fresh >= 3:
            interval = cfg.min_poll_sec
        elif fresh > 0:
            target = cfg.min_poll_sec + (cfg.effective_poll() - cfg.min_poll_sec) / max(1, fresh)
            interval = min(interval, max(target, cfg.min_poll_sec)) * 0.6 + target * 0.4
        else:
            interval = min(interval * 1.2, cfg.max_poll_sec)
        return int(max(cfg.min_poll_sec, min(cfg.max_poll_sec, math.ceil(interval))))

    def _spawn_stream(self, cfg: SourceConfig) -> None:
        old = self._streams.get(cfg.id)
        if old:
            old.cancel()
        if cfg.type == "bluesky":
            runner = bluesky_stream
        elif cfg.type == "telegram":
            runner = telegram_stream
        else:
            return
        stop = asyncio.Event()
        task = asyncio.create_task(runner(cfg, self._stream_sink, stop), name=f"stream-{cfg.id}")
        self._streams[cfg.id] = task
        cfg.state["health"] = "ok"

    async def _stream_sink(self, item: RawItem, cfg: SourceConfig) -> None:
        try:
            await self.pipeline.process(item, cfg)
        except Exception as e:
            log.debug("stream pipeline error (%s): %s", cfg.id, e)

    # ------------------------------------------------------------------ #
    # websub push delivery (hub POSTs arrive at /websub/callback/{id})
    # ------------------------------------------------------------------ #
    async def ingest_pushed(self, source_id: str, body: bytes) -> int:
        """Process a pushed feed payload; returns the number of new events."""
        cfg = self.sources.get(source_id)
        if cfg is None:
            return -1
        items = parse_push(body, source_id, cfg.url or "")
        seen = self._seen_external.setdefault(source_id, {})
        accepted = 0
        now = time.monotonic()
        for it in items:
            key = it.external_id or it.url or it.title
            if key in seen:
                continue
            seen[key] = now
            try:
                if await self.pipeline.process(it, cfg) is not None:
                    accepted += 1
            except Exception as e:
                log.debug("push pipeline error (%s): %s", source_id, e)
        if accepted:
            self._last_success[source_id] = now
            cfg.state["health"] = "ok"
        return accepted

    async def _websub_loop(self) -> None:
        """Renew push leases well before expiry so delivery never lapses."""
        s = get_settings()
        try:
            while not self._stop.is_set():
                await asyncio.sleep(s.websub_renew_sec)
                for sid, cfg in list(self.sources.items()):
                    hub = cfg.state.get("websub_hub")
                    secret = cfg.state.get("websub_secret")
                    if not hub or not secret:
                        continue
                    callback = f"{s.public_url.rstrip('/')}/websub/callback/{sid}?token={secret}"
                    if await subscribe(hub, cfg.url or "", callback, secret):
                        log.debug("websub lease renewed: %s", sid)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.debug("websub renewal loop stopped: %s", e)

    # ------------------------------------------------------------------ #
    # state broadcasting & stats
    # ------------------------------------------------------------------ #
    async def _broadcast_source_state(self, sid: str, health: str, detail: str) -> None:
        cfg = self.sources.get(sid)
        if cfg is not None:
            cfg.state["health"] = health
        await self.bus.publish_source_state(
            {
                "source_id": sid,
                "name": cfg.name if cfg else sid,
                "health": health,
                "enabled": bool(cfg.enabled) if cfg else False,
                "detail": detail,
            }
        )

    def health(self, sid: str) -> str:
        cfg = self.sources.get(sid)
        if cfg is None:
            return "unknown"
        if not cfg.enabled:
            return "paused"
        if cfg.requires_key and not __import__("os").environ.get(cfg.requires_key):
            return "waiting_key"
        h = cfg.state.get("health", "ok")
        if h != "ok":
            return h
        last = self._last_success.get(sid, 0.0)
        if last and time.monotonic() - last > 1800:
            return "stale"
        return "ok"

    async def _stats_loop(self) -> None:
        try:
            while not self._stop.is_set():
                await asyncio.sleep(get_settings().stats_interval_sec)
                try:
                    stats = await self.compute_stats()
                    await self.bus.publish_stats(stats)
                except Exception as e:
                    log.debug("stats loop error: %s", e)
        except asyncio.CancelledError:
            pass

    async def compute_stats(self) -> dict[str, Any]:
        s = get_settings()
        store_stats = await self.store.stats()
        p50, p95 = self.pipeline.lag_percentiles()
        now = now_utc()
        events_1h = store_stats.get("events_1h", 0)
        sources_online = sum(
            1 for sid in self.sources
            if self.sources[sid].enabled and self.health(sid) in ("ok", "degraded")
        )
        return {
            "ts": now.isoformat(),
            "events_total": store_stats.get("events_total", 0),
            "events_1h": events_1h,
            "events_15m": store_stats.get("events_15m", 0),
            "events_per_min": round(events_1h / 60.0, 2),
            "lag_p50_ms": p50,
            "lag_p95_ms": p95,
            "clusters_24h": store_stats.get("clusters_24h", 0),
            "sources_online": sources_online,
            "sources_total": len(self.sources),
            "sources_paused": sum(1 for c in self.sources.values() if not c.enabled),
            "dlq_size": len(await self.store.list_dlq(1)),
            "bus_mode": self.bus.mode,
            "store_mode": "postgres" if getattr(self.store, "pool", None) else "memory",
            "top_tickers": [list(t) for t in store_stats.get("top_tickers", [])][:10],
            "pipeline": {
                "processed": self.pipeline.stats_processed,
                "duplicates": self.pipeline.stats_duplicates,
                "cluster_joins": self.pipeline.stats_cluster_joins,
                "new_clusters": self.pipeline.stats_new_clusters,
            },
        }

    def source_states(self) -> list[SourceStateMessage]:
        out = []
        for sid, cfg in sorted(self.sources.items()):
            out.append(
                SourceStateMessage(
                    source_id=sid,
                    name=cfg.name,
                    health=self.health(sid),
                    enabled=cfg.enabled,
                    detail=cfg.state.get("health_detail"),
                    last_success=(
                        now_utc() - timedelta(seconds=time.monotonic() - self._last_success[sid])
                        if sid in self._last_success else None
                    ),
                    interval_sec=cfg.state.get("interval_sec", cfg.effective_poll()),
                )
            )
        return out
