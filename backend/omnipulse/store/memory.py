"""In-memory store for lite mode and tests. Bounded; nothing survives a restart."""

from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from typing import Any, Sequence

import numpy as np

from ..filters import EventFilter
from ..schema import SourceConfig, SourceHealth, WebhookConfig


def _ts(ev: dict[str, Any]) -> str:
    return ev["published_at"]


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    from ..schema import iso

    return iso(dt)


class MemoryStore:
    def __init__(self, max_events: int = 50000, dim: int = 384) -> None:
        self.max_events = max_events
        self.dim = dim
        self.events: OrderedDict[str, dict[str, Any]] = OrderedDict()  # insertion order
        self.meta: dict[str, dict[str, Any]] = {}
        self.vectors: dict[str, np.ndarray] = {}
        self.clusters: dict[str, dict[str, Any]] = {}
        self.sources: dict[str, SourceConfig] = {}
        self.health: dict[str, SourceHealth] = {}
        self.states: dict[str, dict[str, Any]] = {}
        self.webhooks: dict[str, WebhookConfig] = {}

    async def init(self) -> None:
        return None

    async def close(self) -> None:
        return None

    # events -------------------------------------------------------------------------
    async def save_event(self, event: dict[str, Any], embedding: Sequence[float], simhash: int, canonical_url: str) -> None:
        eid = event["event_id"]
        self.events[eid] = event
        self.meta[eid] = {"simhash": simhash, "canonical_url": canonical_url}
        self.vectors[eid] = np.asarray(embedding, dtype=np.float32)
        while len(self.events) > self.max_events:
            old, _ = self.events.popitem(last=False)
            self.meta.pop(old, None)
            self.vectors.pop(old, None)

    async def update_event(self, event: dict[str, Any]) -> None:
        if event["event_id"] in self.events:
            self.events[event["event_id"]] = event

    async def get_event(self, event_id: str) -> dict[str, Any] | None:
        return self.events.get(event_id)

    def _sorted(self) -> list[dict[str, Any]]:
        return sorted(self.events.values(), key=_ts, reverse=True)

    async def query_events(self, f: EventFilter, limit: int = 100, before=None, since=None) -> list[dict[str, Any]]:
        b, s = _iso(before), _iso(since)
        out = []
        for ev in self._sorted():
            ts = _ts(ev)
            if b and ts >= b:
                continue
            if s and ts < s:
                break
            if f.matches(ev):
                out.append(ev)
                if len(out) >= limit:
                    break
        return out

    async def semantic_search(self, vector, limit, since, f) -> list[tuple[float, dict[str, Any]]]:
        if not self.vectors:
            return []
        s = _iso(since)
        ids = [i for i, ev in self.events.items() if (not s or _ts(ev) >= s) and f.matches(ev)]
        if not ids:
            return []
        mat = np.stack([self.vectors[i] for i in ids])
        q = np.asarray(vector, dtype=np.float32)
        sims = mat @ q
        order = np.argsort(-sims)[:limit]
        return [(float(sims[i]), self.events[ids[i]]) for i in order]

    async def window_rows(self, since: datetime) -> list[dict[str, Any]]:
        s = _iso(since)
        rows = []
        for eid, ev in self.events.items():
            if _ts(ev) < s:
                continue
            rows.append(
                {
                    "event_id": eid,
                    "cluster_id": ev["cluster_id"],
                    "published_at": ev["published_at"],
                    "source_id": ev["source"]["id"],
                    "tier": ev["source"]["credibility_tier"],
                    "simhash": self.meta[eid]["simhash"],
                    "canonical_url": self.meta[eid]["canonical_url"],
                    "embedding": self.vectors[eid],
                    "event": ev,
                }
            )
        return rows

    # clusters -----------------------------------------------------------------------
    async def upsert_cluster(self, cluster: dict[str, Any]) -> None:
        self.clusters[cluster["cluster_id"]] = cluster
        if len(self.clusters) > self.max_events:
            for cid in sorted(self.clusters, key=lambda c: self.clusters[c]["last_updated"])[: len(self.clusters) // 10]:
                del self.clusters[cid]

    async def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        return self.clusters.get(cluster_id)

    async def cluster_events(self, cluster_id: str, limit: int = 100) -> list[dict[str, Any]]:
        evs = [e for e in self.events.values() if e["cluster_id"] == cluster_id]
        return sorted(evs, key=_ts)[:limit]

    async def list_clusters(self, since, limit, min_sources=1, category=None) -> list[dict[str, Any]]:
        s = _iso(since)
        out = [
            c
            for c in self.clusters.values()
            if (not s or c["last_updated"] >= s)
            and c["source_count"] >= min_sources
            and (not category or category in c["categories"])
        ]
        out.sort(key=lambda c: c["last_updated"], reverse=True)
        return out[:limit]

    # sources ------------------------------------------------------------------------
    async def seed_sources(self, sources: list[SourceConfig]) -> int:
        n = 0
        for s in sources:
            if s.id not in self.sources:
                self.sources[s.id] = s
                n += 1
        return n

    async def list_sources(self, include_deleted: bool = False) -> list[SourceConfig]:
        return [s for s in self.sources.values() if include_deleted or not s.deleted]

    async def get_source(self, source_id: str) -> SourceConfig | None:
        s = self.sources.get(source_id)
        return None if s is None or s.deleted else s

    async def save_source(self, cfg: SourceConfig) -> None:
        self.sources[cfg.id] = cfg

    async def save_health(self, health: list[SourceHealth]) -> None:
        for h in health:
            self.health[h.id] = h

    async def list_health(self) -> list[SourceHealth]:
        return list(self.health.values())

    async def save_source_state(self, states: dict[str, dict[str, Any]]) -> None:
        self.states.update(states)

    async def load_source_states(self) -> dict[str, dict[str, Any]]:
        return dict(self.states)

    # webhooks -----------------------------------------------------------------------
    async def list_webhooks(self) -> list[WebhookConfig]:
        return list(self.webhooks.values())

    async def save_webhook(self, hook: WebhookConfig) -> None:
        self.webhooks[hook.id] = hook

    async def delete_webhook(self, hook_id: str) -> bool:
        return self.webhooks.pop(hook_id, None) is not None

    async def stats(self) -> dict[str, Any]:
        return {
            "events_stored": len(self.events),
            "clusters_stored": len(self.clusters),
            "sources": len([s for s in self.sources.values() if not s.deleted]),
            "backend": "memory",
        }
