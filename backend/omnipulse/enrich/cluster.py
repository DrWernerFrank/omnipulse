"""Stage-2 story clustering over a rolling time window.

``VectorWindow`` is a fixed-capacity ring buffer of normalised embeddings searched with one
BLAS mat-vec (~2-4 ms for 50k x 384). ``ClusterBook`` holds StoryCluster state: primary
(first-to-publish) source, follow-ups, syndications, aggregate tags and verification.
"""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import numpy as np

from ..schema import iso, utcnow


class VectorWindow:
    def __init__(self, dim: int, capacity: int, window_seconds: float) -> None:
        self.dim = dim
        self.capacity = capacity
        self.window = window_seconds
        self.vecs = np.zeros((capacity, dim), dtype=np.float32)
        self.ts = np.full(capacity, -np.inf, dtype=np.float64)
        self.cluster: list[str | None] = [None] * capacity
        self.event: list[str | None] = [None] * capacity
        self.idents: list[frozenset[str]] = [frozenset()] * capacity
        self.pos = 0
        self.count = 0

    def add(self, vec: np.ndarray, cluster_id: str, event_id: str, ts: float | None = None,
            idents: frozenset[str] = frozenset()) -> None:
        i = self.pos
        self.vecs[i] = vec
        self.ts[i] = time.time() if ts is None else ts
        self.cluster[i] = cluster_id
        self.event[i] = event_id
        self.idents[i] = idents
        self.pos = (i + 1) % self.capacity
        self.count = min(self.count + 1, self.capacity)

    def nearest(self, vec: np.ndarray, now: float | None = None) -> tuple[float, int]:
        best = self.candidates(vec, -1.0, k=1, now=now)
        return best[0] if best else (0.0, -1)

    def candidates(self, vec: np.ndarray, min_sim: float, k: int = 5, now: float | None = None) -> list[tuple[float, int]]:
        """Top-k in-window neighbours with similarity >= min_sim, best first."""
        if not self.count:
            return []
        n = self.count
        sims = self.vecs[:n] @ vec
        cutoff = (time.time() if now is None else now) - self.window
        sims = np.where(self.ts[:n] >= cutoff, sims, -1.0)
        k = min(k, n)
        top = np.argpartition(-sims, k - 1)[:k]
        top = top[np.argsort(-sims[top])]
        return [(float(sims[i]), int(i)) for i in top if sims[i] >= min_sim]


def identifiers(enrichment: dict) -> frozenset[str]:
    """Distinguishing identities of an item: tickers, CVE ids, organisations."""
    ids = {f"$" + t for t in enrichment.get("tickers", ())}
    ids |= {e["name"].lower() for e in enrichment.get("entities", ()) if e["type"] in ("ORG", "CVE")}
    return frozenset(ids)


@dataclass
class Member:
    event_id: str
    source_id: str
    source_name: str
    tier: int
    published_at: str
    headline: str
    publisher: str = ""


@dataclass
class StoryCluster:
    cluster_id: str
    first_seen: str
    last_updated: str
    members: list[Member] = field(default_factory=list)
    syndications: list[dict[str, Any]] = field(default_factory=list)
    categories: Counter = field(default_factory=Counter)
    tickers: Counter = field(default_factory=Counter)
    locations: Counter = field(default_factory=Counter)
    tokens: set[str] = field(default_factory=set)
    max_urgency: int = 1
    sentiment_sum: float = 0.0
    touched: float = field(default_factory=time.time)

    @property
    def primary(self) -> Member:
        return min(self.members, key=lambda m: (m.published_at, m.tier))

    @property
    def sources(self) -> list[str]:
        seen: dict[str, None] = {}
        for m in self.members:
            seen[m.source_id] = None
        for s in self.syndications:
            seen[s["source_id"]] = None
        return list(seen)

    @property
    def publishers(self) -> set[str]:
        return {m.publisher or m.source_id for m in self.members} | {
            s.get("publisher") or s["source_id"] for s in self.syndications
        }

    def verification(self) -> str:
        independent = {m.publisher or m.source_id for m in self.members if m.tier <= 2} | {
            s.get("publisher") or s["source_id"] for s in self.syndications if s.get("tier", 3) <= 2
        }
        if any(m.tier == 1 for m in self.members) or len(independent) >= 2:
            return "CONFIRMED"
        if independent:
            return "DEVELOPING"
        return "UNVERIFIED"

    def to_wire(self) -> dict[str, Any]:
        p = self.primary
        n = len(self.members)
        return {
            "cluster_id": self.cluster_id,
            "first_seen": self.first_seen,
            "last_updated": self.last_updated,
            "headline": p.headline,
            "primary_event_id": p.event_id,
            "primary_source": {"id": p.source_id, "name": p.source_name, "credibility_tier": p.tier,
                               "published_at": p.published_at},
            "size": n + len(self.syndications),
            "event_count": n,
            "syndication_count": len(self.syndications),
            "sources": self.sources,
            "source_count": len(self.publishers),  # independent publishers
            "categories": [c for c, _ in self.categories.most_common(3)],
            "tickers": [t for t, _ in self.tickers.most_common(6)],
            "locations": [loc for loc, _ in self.locations.most_common(5)],
            "max_urgency": self.max_urgency,
            "avg_sentiment": round(self.sentiment_sum / n, 3) if n else 0.0,
            "verification_status": self.verification(),
            "members": [
                {"event_id": m.event_id, "source_id": m.source_id, "source_name": m.source_name,
                 "tier": m.tier, "published_at": m.published_at, "headline": m.headline}
                for m in sorted(self.members, key=lambda m: m.published_at)[-50:]
            ],
            "syndications": self.syndications[-50:],
        }


class ClusterBook:
    def __init__(self, window_seconds: float, max_clusters: int = 100_000) -> None:
        self.window = window_seconds
        self.max_clusters = max_clusters
        self.clusters: dict[str, StoryCluster] = {}

    def get(self, cluster_id: str) -> StoryCluster | None:
        return self.clusters.get(cluster_id)

    def add_member(self, cluster_id: str, ev: dict[str, Any], tokens: set[str], publisher: str = "") -> StoryCluster:
        now = iso(utcnow())
        c = self.clusters.get(cluster_id)
        if c is None:
            c = self.clusters[cluster_id] = StoryCluster(cluster_id, first_seen=now, last_updated=now)
        enr, src = ev["enrichment"], ev["source"]
        c.members.append(Member(ev["event_id"], src["id"], src["name"], src["credibility_tier"],
                                ev["published_at"], ev["content"]["flash_headline"], publisher or src["id"]))
        c.categories.update(x for x in enr["categories"] if x != "Breaking")
        c.tickers.update(enr["tickers"])
        c.locations.update(enr["locations"])
        c.tokens |= tokens
        if len(c.tokens) > 4000:
            c.tokens = set(list(c.tokens)[-3000:])
        c.max_urgency = max(c.max_urgency, enr["urgency"])
        c.sentiment_sum += enr["sentiment"]
        c.last_updated = now
        c.touched = time.time()
        self._prune()
        return c

    def add_syndication(self, cluster_id: str, source: dict[str, Any], url: str, published_at: str,
                        publisher: str = "") -> StoryCluster | None:
        c = self.clusters.get(cluster_id)
        if c is None:
            return None
        c.syndications.append({"source_id": source["id"], "source_name": source["name"], "publisher": publisher or source["id"],
                               "tier": source["credibility_tier"], "url": url, "published_at": published_at})
        c.last_updated = iso(utcnow())
        c.touched = time.time()
        return c

    def restore(self, ev: dict[str, Any], tokens: set[str], publisher: str = "") -> None:
        c = self.add_member(ev["cluster_id"], ev, tokens, publisher)
        c.first_seen = min(c.first_seen, ev["ingested_at"])
        c.last_updated = max(m.published_at for m in c.members)

    def novelty(self, cluster_id: str | None, tokens: set[str], similarity: float) -> float:
        if cluster_id is None or cluster_id not in self.clusters or not tokens:
            return 1.0
        known = self.clusters[cluster_id].tokens
        new_ratio = len(tokens - known) / len(tokens)
        return round(max(0.0, min(1.0, 0.7 * new_ratio + 0.3 * (1.0 - max(0.0, similarity)))), 3)

    def _prune(self) -> None:
        if len(self.clusters) <= self.max_clusters:
            return
        cutoff = time.time() - self.window
        stale = [cid for cid, c in self.clusters.items() if c.touched < cutoff]
        for cid in stale or sorted(self.clusters, key=lambda k: self.clusters[k].touched)[: self.max_clusters // 10]:
            del self.clusters[cid]


def parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
