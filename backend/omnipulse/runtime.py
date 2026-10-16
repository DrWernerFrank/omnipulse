"""Process-wide runtime container: wires bus, store, embedder, engine and enricher per role."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .bus import Bus, make_bus
from .enrich.embed import Embedder, make_embedder
from .ingest.http import Fetcher
from .registry import seed_defaults
from .settings import Settings
from .store import Store, make_store

log = logging.getLogger(__name__)


@dataclass
class Runtime:
    settings: Settings
    bus: Bus
    store: Store
    embedder: Embedder
    fetcher: Fetcher
    role: str
    engine: Any = None
    enricher: Any = None
    hub: Any = None
    webhooks: Any = None
    started_at: float = field(default_factory=time.time)

    @classmethod
    async def create(cls, settings: Settings, role: str) -> "Runtime":
        if settings.mode == "lite" and role != "all":
            raise SystemExit("lite mode keeps state in memory; run role 'all' (python -m omnipulse all)")
        bus = make_bus(settings.mode, settings.redis_url)
        await bus.start()
        store = make_store(settings)
        await store.init()
        await seed_defaults(store, settings)
        embedder = await make_embedder(settings)
        fetcher = Fetcher(settings.user_agent, settings.http_timeout)
        rt = cls(settings, bus, store, embedder, fetcher, role)
        if role in ("all", "engine"):
            await rt._start_engine()
        return rt

    async def _start_engine(self) -> None:
        from .enrich.pipeline import Enricher
        from .ingest.engine import IngestEngine

        idx, _, n = self.settings.shard.partition("/")
        shard, shards = int(idx or 0), int(n or 1)
        if shard == 0:
            self.enricher = Enricher(self.settings, self.bus, self.store, self.embedder, self.fetcher)
            await self.enricher.start()
        self.engine = IngestEngine(self.settings, self.bus, self.store, shard=shard, shards=shards)
        await self.engine.start()

    async def close(self) -> None:
        for comp in (self.engine, self.enricher, self.webhooks, self.hub):
            if comp is not None:
                try:
                    await comp.stop()
                except Exception as exc:  # pragma: no cover
                    log.warning("shutdown error: %s", exc)
        await self.fetcher.close()
        await self.store.close()
        await self.bus.close()


_RUNTIME: Runtime | None = None


def set_runtime(rt: Runtime | None) -> None:
    global _RUNTIME
    _RUNTIME = rt


def get_runtime() -> Runtime:
    if _RUNTIME is None:
        raise RuntimeError("runtime not initialised")
    return _RUNTIME
