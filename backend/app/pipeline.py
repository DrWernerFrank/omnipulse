"""Ingestion pipeline: normalize -> dedup (SimHash + URL) -> semantic cluster
-> enrich -> persist -> fan out.

Latency budget per item: fingerprint+embed < 1ms, store+bus writes async.

Deduplication stages:
  0. Canonical URL seen set (24h TTL).
  1. SimHash64 over title+body; hamming <= 3 within a 24h window => duplicate.
  2. Vector cosine vs rolling cluster centroids; > 0.88 joins the cluster as a
     syndication/follow-up (emits CLUSTER_UPDATED), else creates a new cluster.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from datetime import timedelta
from typing import Any

import orjson

from .config import get_settings
from .db import Store
from .bus import Bus
from .embeddings import cosine, embed
from .enrich import EnrichmentService
from .ids import new_id
from .schemas import (
    ClusterSummary, Content, Enrichment, NewsEvent, RawItem, SourceConfig, SourceRef,
    now_utc,
)
from .text import canonical_url, hamming, html_to_markdown, jaccard, shingles, simhash64, strip_html

try:  # numpy accelerates cluster matching ~100x under load; pure-python fallback keeps it optional
    import numpy as _np
except ImportError:  # pragma: no cover
    _np = None

log = logging.getLogger("omnipulse.pipeline")


class Pipeline:
    def __init__(self, store: Store, bus: Bus, enricher: EnrichmentService):
        self.store = store
        self.bus = bus
        self.enricher = enricher
        s = get_settings()
        self.similarity = s.cluster_similarity
        self.window = timedelta(hours=s.cluster_window_hours)
        self.dedup_hamming = s.dedup_hamming
        self.max_age = timedelta(hours=s.max_item_age_hours)

        # ---- rolling indexes (pruned by age) ----
        self._seen_urls: dict[str, float] = {}                 # canonical url -> mono ts
        self._simhash_list: deque[tuple[float, int, str]] = deque()  # (ts, simhash, cluster_id)
        self._clusters: dict[str, dict[str, Any]] = {}          # cluster_id -> state
        self._cluster_events: dict[str, deque[tuple[str, float]]] = {}  # (event_id, mono ts)
        self._last_prune = 0.0
        self._lock = asyncio.Lock()

        # ---- cluster candidate index ----
        # clusters compare in last-touch order (deque tail = hottest); the index
        # caps candidates so per-event cost stays constant as the 24h window
        # fills with thousands of clusters. numpy => one matvec, else tight loop.
        self._touch: deque[str] = deque()
        self._cand_ids: list[str] = []
        self._cand_mat = None            # numpy (C, dim) float32
        self._cand_norms = None          # numpy (C,)
        self._cand_py: list[tuple[str, list[float], float]] = []  # fallback path
        self._index_dirty = True

        self._ingest_lags: deque[float] = deque(maxlen=4000)
        self.stats_processed = 0
        self.stats_duplicates = 0
        self.stats_cluster_joins = 0
        self.stats_new_clusters = 0

    # ------------------------------------------------------------------ #
    async def process(self, item: RawItem, source_cfg: SourceConfig) -> NewsEvent | None:
        """Full pipeline for one raw item. Returns None when deduplicated."""
        mono = time.monotonic()
        await self._prune_if_needed(mono)
        s = get_settings()

        # ---- stage 0: canonical URL dedup (check+register atomically) ----
        canon = canonical_url(item.url)
        async with self._lock:
            if canon and canon in self._seen_urls:
                self.stats_duplicates += 1
                return None
            if canon:
                self._seen_urls[canon] = mono

        # ---- content normalization ----
        body_md = ""
        if item.full_html:
            body_md = html_to_markdown(item.full_html)
        elif item.body_html:
            body_md = html_to_markdown(item.body_html)
        elif item.body_text:
            body_md = item.body_text
        if source_cfg.fulltext and item.url and len(body_md) < 400 and source_cfg.type in ("rss", "atom"):
            html = await self._fetch_fulltext(item.url, source_cfg)
            if html:
                body_md = html_to_markdown(html) or body_md

        # ---- stage 1: SimHash lexical dedup (check+append atomically) ----
        fp_text = f"{item.title} {strip_html(body_md)[:600]}"
        simh = simhash64(fp_text)
        async with self._lock:
            for _, existing, _cluster in self._simhash_list:
                if hamming(existing, simh) <= self.dedup_hamming:
                    self.stats_duplicates += 1
                    return None
            self._simhash_list.append((mono, simh, ""))

        # ---- stage 2: semantic clustering (capped candidate index) ----
        emb = embed(f"{item.title} {body_md[:1500]}")
        best_id: str | None = None
        best_sim = 0.0
        if self._index_dirty:
            self._rebuild_index()
        if _np is not None and self._cand_mat is not None and len(self._cand_ids):
            scores = (self._cand_mat @ _np.asarray(emb, dtype=_np.float32)) / self._cand_norms
            idx = int(scores.argmax())
            if scores[idx] > 0.0:
                best_id, best_sim = self._cand_ids[idx], float(scores[idx])
        else:
            for cid, cent, norm in self._cand_py:
                dot = 0.0
                for a, b in zip(emb, cent):
                    dot += a * b
                sim = dot / norm
                if sim > best_sim:
                    best_id, best_sim = cid, sim

        cluster_hint: dict[str, Any] | None = None
        if best_id is not None and best_sim >= self.similarity:
            cluster_id = best_id
            cluster_hint = self._clusters[cluster_id]
            is_new_cluster = False
        else:
            cluster_id = new_id("cls")
            is_new_cluster = True

        # ---- enrichment ----
        hint_payload = None
        if cluster_hint is not None:
            hint_payload = {
                "event_count": cluster_hint["count"],
                "sources": list(cluster_hint["sources"])[:5],
                "title": cluster_hint["title"],
            }
        enriched = await self.enricher.enrich(item.title, body_md, source_cfg, hint_payload)

        # ---- novelty vs existing cluster ----
        if cluster_hint is not None:
            shingle_sim = jaccard(shingles(item.title), shingles(cluster_hint["title"] or ""))
            vec_sim = cosine(emb, cluster_hint["centroid"])
            novelty = round(max(0.0, min(1.0, 1.0 - 0.55 * shingle_sim - 0.45 * vec_sim)), 3)
        else:
            novelty = 1.0

        # ---- assemble event ----
        published = item.published_at or now_utc()
        ingested = now_utc()
        if (ingested - published) > self.max_age:
            return None  # stale beyond retention window
        tickers = list(dict.fromkeys(
            [t for t in item.extra.get("tickers", []) if isinstance(t, str) and t]
            + enriched["tickers"]
        ))
        ev = NewsEvent(
            event_id=new_id("evt"),
            cluster_id=cluster_id,
            published_at=published,
            ingested_at=ingested,
            source=SourceRef(
                id=source_cfg.id, name=source_cfg.name, category=source_cfg.category,
                credibility_tier=source_cfg.credibility_tier, url=item.url or None,
            ),
            content=Content(
                raw_title=item.title or "",
                flash_headline=enriched["flash_headline"],
                executive_summary=enriched["executive_summary"],
                clean_markdown=body_md,
            ),
            enrichment=Enrichment(
                categories=enriched["categories"],
                tickers=tickers[:10],
                entities=[e for e in enriched["entities"] if isinstance(e, dict)],
                locations=enriched["locations"],
                sentiment=enriched["sentiment"],
                urgency=enriched["urgency"],
                novelty=novelty,
                verification_status=enriched["verification_status"],
            ),
        )

        # ---- cluster bookkeeping ----
        self._update_cluster(cluster_id, ev, emb, is_new_cluster, mono)

        # ---- persist + fan out ----
        await self.store.add_event(ev, simh, emb)
        await self.bus.publish_event(ev)

        cs = self._clusters[cluster_id]["summary"]
        await self.store.upsert_cluster(cs, self._clusters[cluster_id]["centroid"])
        await self.bus.publish_cluster(orjson.loads(cs.model_dump_json()))

        self._ingest_lags.append(max(0.0, (ingested - published).total_seconds() * 1000))
        self.stats_processed += 1
        if is_new_cluster:
            self.stats_new_clusters += 1
        else:
            self.stats_cluster_joins += 1
        return ev

    # ------------------------------------------------------------------ #
    def _rebuild_index(self) -> None:
        """Refresh the capped candidate set from the touch-order deque (tail = hottest)."""
        s = get_settings()
        cutoff = now_utc() - timedelta(hours=s.cluster_recency_hours)
        cap = s.cluster_candidate_cap if _np is not None else s.cluster_fallback_cap
        cands: list[tuple[str, dict[str, Any]]] = []
        picked: set[str] = set()
        for cid in reversed(self._touch):
            if len(cands) >= cap:
                break
            if cid in picked:
                continue
            st = self._clusters.get(cid)
            if st is None:
                continue
            latest = st.get("latest")
            if latest is None or latest < cutoff:
                break  # touch order == latest order; older entries follow
            picked.add(cid)
            cands.append((cid, st))
        self._cand_ids = [cid for cid, _ in cands]
        if _np is not None and cands:
            dim = len(cands[0][1]["centroid"])
            mat = _np.zeros((len(cands), dim), dtype=_np.float32)
            for i, (_, st) in enumerate(cands):
                mat[i, :] = st["centroid"]
            norms = _np.linalg.norm(mat, axis=1)
            norms[norms == 0.0] = 1.0
            self._cand_mat, self._cand_norms = mat, norms
            self._cand_py = []
        else:
            self._cand_mat = self._cand_norms = None
            self._cand_py = [
                (cid, st["centroid"], math.sqrt(sum(x * x for x in st["centroid"])) or 1.0)
                for cid, st in cands
            ]
        self._index_dirty = False

    def _update_cluster(self, cluster_id: str, ev: NewsEvent, emb: list[float],
                        new: bool, mono: float) -> None:
        st = self._clusters.get(cluster_id)
        if new or st is None:
            st = {
                "centroid": emb,
                "count": 1,
                "sources": [ev.source.id],
                "title": ev.content.flash_headline or ev.content.raw_title,
                "first_seen": ev.ingested_at,
                "latest": ev.ingested_at,
                "categories": list(ev.enrichment.categories),
                "tickers": list(ev.enrichment.tickers),
                "breaking_source": ev.source.id if ev.source.credibility_tier == 1 else None,
            }
            self._clusters[cluster_id] = st
        else:
            n = st["count"]
            st["centroid"] = [(a * n + b) / (n + 1) for a, b in zip(st["centroid"], emb)]
            st["count"] = n + 1
            if ev.source.id not in st["sources"]:
                st["sources"].append(ev.source.id)
            if not st["title"]:
                st["title"] = ev.content.flash_headline or ev.content.raw_title
            for c in ev.enrichment.categories:
                if c not in st["categories"]:
                    st["categories"].append(c)
            for t in ev.enrichment.tickers:
                if t not in st["tickers"]:
                    st["tickers"].append(t)
            if st.get("breaking_source") is None and ev.source.credibility_tier == 1:
                st["breaking_source"] = ev.source.id
            st["latest"] = ev.ingested_at

        recent = self._cluster_events.setdefault(cluster_id, deque(maxlen=400))
        recent.append((ev.event_id, mono))
        cutoff = mono - 900  # 15 minutes
        velocity = 0
        for _eid, ts in reversed(recent):
            if ts >= cutoff:
                velocity += 1
            else:
                break

        st["summary"] = ClusterSummary(
            cluster_id=cluster_id,
            title=st["title"],
            event_count=st["count"],
            sources=st["sources"][:25],
            categories=st["categories"][:5],
            tickers=st["tickers"][:10],
            breaking_source=st.get("breaking_source"),
            first_seen=st.get("first_seen"),
            latest_at=st.get("latest"),
            velocity_15m=velocity,
        )
        self._touch.append(cluster_id)
        self._index_dirty = True

    async def _fetch_fulltext(self, url: str, source_cfg: SourceConfig) -> str:
        try:
            from .http import fetch_text  # late import to avoid a cycle

            return await fetch_text(url, source_cfg) or ""
        except Exception as e:
            log.debug("fulltext fetch failed for %s: %s", url, e)
            return ""

    async def _prune_if_needed(self, mono: float) -> None:
        if mono - self._last_prune < 30:
            return
        self._last_prune = mono
        cutoff = mono - self.window.total_seconds()
        async with self._lock:
            for url, ts in list(self._seen_urls.items()):
                if ts < cutoff:
                    del self._seen_urls[url]
            while self._simhash_list and self._simhash_list[0][0] < cutoff:
                self._simhash_list.popleft()
        dt_cutoff = now_utc() - self.window
        for cid, st in list(self._clusters.items()):
            if st.get("latest") and st["latest"] < dt_cutoff:
                del self._clusters[cid]
                self._cluster_events.pop(cid, None)
        if self._touch:
            # compact to one entry per surviving cluster, preserving last-touch order
            seen: set[str] = set()
            ordered: list[str] = []
            for cid in reversed(self._touch):
                if cid in self._clusters and cid not in seen:
                    seen.add(cid)
                    ordered.append(cid)
            self._touch = deque(reversed(ordered))
        self._index_dirty = True

    # ---- metrics ----
    def lag_percentiles(self) -> tuple[float, float]:
        if not self._ingest_lags:
            return 0.0, 0.0
        vals = sorted(self._ingest_lags)
        n = len(vals)
        return round(vals[int(n * 0.50)], 1), round(vals[min(n - 1, int(n * 0.95))], 1)
