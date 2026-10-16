"""The enrichment pipeline: RawItem -> NewsEvent in a few milliseconds.

    canonical URL claim ──► SimHash (syndication) ──► tag + NER + scores ──► embed
        ──► rolling-window cluster match ──► novelty/verification ──► summary
        ──► publish EVENT_CREATED + CLUSTER_UPDATED ──► persist
        └─(async)─► full-article Markdown + optional LLM ──► EVENT_UPDATED
"""

from __future__ import annotations

import asyncio
import copy
import html
import logging
import os
import socket
import time
from collections import Counter, deque
from dataclasses import dataclass
from typing import Any

import numpy as np

from .. import bus as B
from ..ids import new_cluster_id, new_event_id
from ..ingest.extract import fetch_article_markdown, fetchable, html_to_markdown, markdown_to_plain
from ..schema import NewsEvent, RawItem, SourceConfig, iso, utcnow
from .canonical import canonicalize, url_key
from .cluster import ClusterBook, VectorWindow, identifiers, parse_iso
from .embed import content_tokens
from .llm import LLMEnricher
from .simhash import SimHashIndex, simhash, tokens
from .summarize import executive_summary, flash_headline
from .tagger import tag

log = logging.getLogger(__name__)

_RANK = {"UNVERIFIED": 0, "DEVELOPING": 1, "CONFIRMED": 2}
URL_TTL = 72 * 3600


@dataclass
class _Prepared:
    raw: RawItem
    src: SourceConfig
    canon: str
    title: str
    body_md: str
    plain: str
    headline: str
    published: Any
    source_ref: dict[str, Any]
    fp: int
    lexical_ok: bool
    t0: float
    tags: Any = None
    embed_text: str = ""
    work_ms: float = 0.0  # CPU time attributable to this item (excludes queueing)


def _fallback_source(source_id: str) -> SourceConfig:
    return SourceConfig(id=source_id, name=source_id, type="webhook", credibility_tier=3)


class Enricher:
    def __init__(self, settings, bus, store, embedder, fetcher) -> None:
        self.settings = settings
        self.bus = bus
        self.store = store
        self.embedder = embedder
        self.fetcher = fetcher
        window = settings.cluster_window_hours * 3600
        self.simidx = SimHashIndex(settings.simhash_max_distance)
        self.window = VectorWindow(settings.embed_dim, settings.cluster_index_capacity, window)
        self.book = ClusterBook(window)
        self.threshold = settings.cluster_threshold or embedder.default_threshold
        self.llm = LLMEnricher(settings)
        self.sources: dict[str, SourceConfig] = {}
        self.counters: Counter = Counter()
        # recent window, so steady-state latency isn't masked by the boot backlog
        self.latency_ms: deque[float] = deque(maxlen=300)
        self.pipeline_ms: deque[float] = deque(maxlen=300)
        self._upgrade_sem = asyncio.Semaphore(settings.full_article_concurrency)
        self._upgrades: set[asyncio.Task] = set()
        self._tasks: list[asyncio.Task] = []
        self.consumer = f"{socket.gethostname()}-{os.getpid()}"

    # ------------------------------------------------------------------------------
    async def start(self) -> None:
        await self.refresh_sources()
        await self._warm()
        log.info("enricher ready: embedder=%s threshold=%.2f window=%d vectors llm=%s",
                 self.embedder.name, self.threshold, self.window.count,
                 self.llm.provider if self.llm.enabled else "off")
        self._tasks = [
            asyncio.create_task(self._consume(), name="enrich:consume"),
            asyncio.create_task(self._maintenance(), name="enrich:maintenance"),
            asyncio.create_task(self._control(), name="enrich:control"),
        ]

    async def stop(self) -> None:
        for t in self._tasks + list(self._upgrades):
            t.cancel()

    async def refresh_sources(self) -> None:
        self.sources = {s.id: s for s in await self.store.list_sources()}

    async def _warm(self) -> None:
        """Rebuild the rolling window from the archive so clustering survives restarts."""
        since = utcnow().timestamp() - self.settings.cluster_window_hours * 3600
        from datetime import datetime, timezone

        rows = await self.store.window_rows(datetime.fromtimestamp(since, tz=timezone.utc))
        for r in rows:
            emb = r.get("embedding")
            if emb is None:
                continue
            vec = np.asarray(emb, dtype=np.float32)
            if vec.shape[0] != self.settings.embed_dim:
                continue
            ev = r["event"]
            ts = parse_iso(ev["ingested_at"]).timestamp()  # live adds use ingest time too
            self.window.add(vec, r["cluster_id"], r["event_id"], ts, identifiers(ev["enrichment"]))
            self.simidx.add(r["event_id"], r["simhash"], ts, {"cluster_id": r["cluster_id"], "source_id": r["source_id"]})
            cfg = self.sources.get(r["source_id"])
            self.book.restore(ev, set(content_tokens(ev["content"]["flash_headline"])),
                              cfg.publisher_key if cfg else r["source_id"])
        if rows:
            log.info("warmed cluster window with %d events", len(rows))

    async def _consume(self) -> None:
        """Micro-batching: drain whatever is queued (<= 64 items), embed them in one model call,
        then cluster sequentially so results stay deterministic. Under bursts this turns
        N x per-item embedding latency into one batched call."""
        queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=2000)

        async def feeder() -> None:
            async for msg_id, payload in self.bus.consume(B.RAW, "enricher", self.consumer):
                await queue.put((msg_id, payload))

        feed = asyncio.create_task(feeder(), name="enrich:feeder")
        try:
            while True:
                batch = [await queue.get()]
                while len(batch) < 64 and not queue.empty():
                    batch.append(queue.get_nowait())
                await self.process_batch(batch)
        finally:
            feed.cancel()

    async def _dead_letter(self, payload: dict[str, Any], exc: Exception) -> None:
        self.counters["errors"] += 1
        log.error("enrichment failed: %s: %s", type(exc).__name__, exc)
        await self.bus.publish(B.DLQ, {"kind": "enrichment_failure", "error": f"{type(exc).__name__}: {exc}"[:400],
                                       "item": payload, "ts": iso(utcnow())})

    async def process_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        ready: list[tuple[str, dict[str, Any], _Prepared]] = []
        for msg_id, payload in batch:
            try:
                prep = await self._prepare(RawItem.model_validate(payload))
            except Exception as exc:
                await self._dead_letter(payload, exc)
                prep = None
            if prep is None:
                await self.bus.ack(B.RAW, "enricher", msg_id)
            else:
                ready.append((msg_id, payload, prep))
        if not ready:
            return
        te = time.perf_counter()
        try:
            vecs = await self.embedder.embed([p.embed_text for _, _, p in ready])
        except Exception as exc:
            for msg_id, payload, _ in ready:
                await self._dead_letter(payload, exc)
                await self.bus.ack(B.RAW, "enricher", msg_id)
            return
        self.counters["batches"] += 1
        share = (time.perf_counter() - te) * 1000 / len(ready)
        for _, _, prep in ready:
            prep.work_ms += share
        for (msg_id, payload, prep), vec in zip(ready, vecs):
            try:
                await self._finish(prep, vec)
            except Exception as exc:
                await self._dead_letter(payload, exc)
            finally:
                await self.bus.ack(B.RAW, "enricher", msg_id)

    async def _control(self) -> None:
        async for _ in self.bus.tail(B.CONTROL):
            await self.refresh_sources()

    async def _maintenance(self) -> None:
        while True:
            await asyncio.sleep(15)
            self.simidx.prune(time.time() - self.settings.cluster_window_hours * 3600)
            await self.bus.publish(B.EVENTS, {"type": "HEARTBEAT", "ts": iso(utcnow()), "data": self.metrics()})
            if self.counters["processed"] % 20 == 0:
                try:
                    await self.refresh_sources()
                except Exception as exc:
                    log.warning("source refresh failed: %s", exc)

    def metrics(self) -> dict[str, Any]:
        def pct(d: deque[float], q: float) -> float:
            return round(float(np.percentile(list(d), q)), 1) if d else 0.0

        return {
            "counters": dict(self.counters),
            "ingest_latency_ms": {"p50": pct(self.latency_ms, 50), "p95": pct(self.latency_ms, 95)},
            "pipeline_ms": {"p50": pct(self.pipeline_ms, 50), "p95": pct(self.pipeline_ms, 95)},
            "window_vectors": self.window.count,
            "active_clusters": len(self.book.clusters),
            "embedder": self.embedder.name,
            "cluster_threshold": self.threshold,
            "llm": self.llm.provider if self.llm.enabled else "off",
        }

    # ------------------------------------------------------------------------------
    async def process(self, raw: RawItem) -> dict[str, Any] | None:
        """Single-item path (tests, inbound pushes): prepare -> embed -> finish."""
        prep = await self._prepare(raw)
        if prep is None:
            return None
        te = time.perf_counter()
        vec = (await self.embedder.embed([prep.embed_text]))[0]
        prep.work_ms += (time.perf_counter() - te) * 1000
        return await self._finish(prep, vec)

    async def _syndicated(self, p: "_Prepared") -> bool:
        """Stage 1b — lexical near-duplicate (syndicated copy) via SimHash."""
        if not p.lexical_ok:
            return False
        hit = self.simidx.find(p.fp)
        if not hit:
            return False
        self.counters["dup_simhash"] += 1
        cluster = self.book.add_syndication(hit[1]["cluster_id"], p.source_ref, p.raw.url, iso(p.published),
                                            p.src.publisher_key)
        if cluster:
            wire = cluster.to_wire()
            await self.bus.publish(B.EVENTS, {"type": "CLUSTER_UPDATED", "ts": iso(utcnow()), "data": wire})
            await self.store.upsert_cluster(wire)
        return True

    async def _prepare(self, raw: RawItem) -> "_Prepared | None":
        t0 = time.perf_counter()
        self.counters["received"] += 1
        src = self.sources.get(raw.source_id) or _fallback_source(raw.source_id)

        # Stage 1a — exact duplicate by canonical URL (shared across replicas via Redis SET NX)
        canon = canonicalize(raw.url)
        if not await self.bus.claim_once(f"omni:seen:{url_key(canon)}", URL_TTL):
            self.counters["dup_url"] += 1
            return None

        title = html.unescape(raw.title).strip()
        body_md = html_to_markdown(raw.content_html or raw.summary)
        plain = markdown_to_plain(body_md)
        headline = flash_headline(title)
        published = raw.published_at or raw.fetched_at
        source_ref = self._source_ref(src, raw, canon)

        lex = f"{headline} {plain[:400]}"
        prep = _Prepared(raw=raw, src=src, canon=canon, title=title, body_md=body_md, plain=plain,
                         headline=headline, published=published, source_ref=source_ref, fp=simhash(lex),
                         lexical_ok=len(tokens(lex)) >= 8, t0=t0)
        if await self._syndicated(prep):
            return None

        # Stage 1c — the same publisher re-posting one headline across several of its feeds
        norm = " ".join(tokens(headline))
        if norm and not await self.bus.claim_once(f"omni:title:{src.publisher_key}:{url_key('t:' + norm)}", 24 * 3600):
            self.counters["dup_same_publisher"] += 1
            return None

        prep.tags = tag(title, plain, src, raw.hints)
        prep.embed_text = f"{headline}. {plain[:280]}"  # headline-weighted; ~2.5x fewer tokens
        prep.work_ms = (time.perf_counter() - t0) * 1000
        return prep

    async def _finish(self, p: "_Prepared", vec: np.ndarray) -> dict[str, Any] | None:
        # An earlier item in the same micro-batch may be the original of this copy.
        if await self._syndicated(p):
            return None
        raw, src, canon, tags = p.raw, p.src, p.canon, p.tags
        title, body_md, plain, headline = p.title, p.body_md, p.plain, p.headline
        published, source_ref, fp = p.published, p.source_ref, p.fp
        t_finish = time.perf_counter()
        pub = src.publisher_key

        # Stage 2 — semantic clustering
        idents = identifiers({"tickers": tags.tickers, "entities": tags.entities})
        existing, sim = None, 0.0
        for cand_sim, idx in self.window.candidates(vec, self.threshold, k=5):
            other = self.window.idents[idx]
            if idents and other and idents.isdisjoint(other):
                continue  # look-alike template, different subject (e.g. two companies' 8-Ks)
            existing, sim = self.window.cluster[idx], cand_sim
            break
        cluster_id = existing or new_cluster_id()
        toks = set(content_tokens(f"{headline} {plain[:1500]}"))
        novelty = self.book.novelty(existing, toks, sim if existing else 0.0)

        urgency = tags.urgency
        if existing and novelty < 0.2:
            urgency = max(1, urgency - 1)  # pure follow-up

        verification = raw.hints.get("verification") or {1: "CONFIRMED", 2: "DEVELOPING"}.get(src.credibility_tier, "UNVERIFIED")
        prior = self.book.get(existing) if existing else None
        if prior and src.credibility_tier <= 2:
            prior_v = prior.verification()
            if prior_v == "CONFIRMED" or (prior_v == "DEVELOPING" and pub not in prior.publishers):
                verification = "CONFIRMED"  # independently corroborated
        elif prior and _RANK[prior.verification()] > _RANK[verification]:
            verification = prior.verification()

        fallback = self._fallback_bullets(src, source_ref, tags, prior, iso(published))
        event_id = new_event_id()
        event = NewsEvent.model_validate({
            "event_id": event_id,
            "cluster_id": cluster_id,
            "published_at": published,
            "ingested_at": raw.fetched_at,
            "source": source_ref,
            "content": {
                "raw_title": title,
                "flash_headline": headline,
                "executive_summary": executive_summary(headline, plain, fallback),
                "clean_markdown": body_md,
            },
            "enrichment": {
                "categories": tags.categories,
                "tickers": tags.tickers,
                "entities": tags.entities,
                "locations": tags.locations,
                "sentiment": tags.sentiment,
                "urgency": urgency,
                "novelty": novelty,
                "verification_status": verification,
            },
        }).to_wire()

        # Publish first (latency), then update indexes and persist.
        await self.bus.publish(B.EVENTS, {"type": "EVENT_CREATED", "ts": iso(utcnow()), "data": event})
        cluster = self.book.add_member(cluster_id, event, toks, pub)
        self.window.add(vec, cluster_id, event_id, idents=identifiers(event["enrichment"]))
        self.simidx.add(event_id, fp, time.time(), {"cluster_id": cluster_id, "source_id": src.id})
        cwire = cluster.to_wire()
        await self.bus.publish(B.EVENTS, {"type": "CLUSTER_UPDATED", "ts": iso(utcnow()), "data": cwire})
        await self.store.save_event(event, vec, fp, canon)
        await self.store.upsert_cluster(cwire)

        self.counters["processed"] += 1
        self.counters["clustered" if existing else "new_clusters"] += 1
        self.pipeline_ms.append(p.work_ms + (time.perf_counter() - t_finish) * 1000)
        self.latency_ms.append((utcnow() - raw.fetched_at).total_seconds() * 1000)
        self._schedule_upgrade(event, src, len(plain))
        return event

    def _source_ref(self, src: SourceConfig, raw: RawItem, canon: str) -> dict[str, Any]:
        display = raw.hints.get("display_source")
        name = src.name
        if display and display.lower() not in src.name.lower():
            name = f"{display} via {src.name}" if src.params.get("aggregator") or src.type == "gdelt" else f"{src.name} · {display}"
        return {"id": src.id, "name": name, "category": src.category, "credibility_tier": src.credibility_tier,
                "url": raw.url or canon}

    @staticmethod
    def _fallback_bullets(src: SourceConfig, ref: dict[str, Any], tags, prior, published_iso: str) -> list[str]:
        out = []
        if prior:
            p = prior.primary
            n = len(prior.publishers)
            if p.published_at <= published_iso:
                out.append(f"Follow-up to a story first reported by {p.source_name} at {p.published_at[11:16]} UTC "
                           f"({n} independent sources so far).")
            else:  # arrived late but published first — this is the earlier report
                out.append(f"Earliest known report of a story also covered by {p.source_name} "
                           f"({n + 1} independent sources so far).")
        named = [e["name"] for e in tags.entities if e["type"] in ("ORG", "PERSON", "CRYPTO", "CVE")][:4]
        if named or tags.tickers:
            bits = []
            if named:
                bits.append("Mentions " + ", ".join(named))
            if tags.tickers:
                bits.append("tickers " + " ".join("$" + t for t in tags.tickers[:5]))
            out.append("; ".join(bits) + ".")
        cats = ", ".join(c for c in tags.categories if c != "Breaking") or "General"
        out.append(f"Reported by {ref['name']} (credibility tier {src.credibility_tier}); category: {cats}.")
        return out

    # ------------------------------------------------------------------------------
    def _schedule_upgrade(self, event: dict[str, Any], src: SourceConfig, plain_len: int) -> None:
        s = self.settings
        urgency = event["enrichment"]["urgency"]
        want_article = (s.fetch_full_article and plain_len < 700 and urgency >= s.full_article_min_urgency
                        and src.params.get("full_article", True) and fetchable(event["source"]["url"]))
        want_llm = self.llm.enabled and (urgency >= s.llm_min_urgency or src.credibility_tier == 1)
        if not (want_article or want_llm) or len(self._upgrades) > 500:
            return
        task = asyncio.create_task(self._upgrade(event, want_article, want_llm))
        self._upgrades.add(task)
        task.add_done_callback(self._upgrades.discard)

    async def _upgrade(self, event: dict[str, Any], want_article: bool, want_llm: bool) -> None:
        event = copy.deepcopy(event)  # the original is already on the bus / in the store
        content, enr = event["content"], event["enrichment"]
        changed = False
        try:
            if want_article:
                async with self._upgrade_sem:
                    md = await fetch_article_markdown(self.fetcher, event["source"]["url"])
                if len(md) > len(content["clean_markdown"]) + 200:
                    content["clean_markdown"] = md
                    content["executive_summary"] = executive_summary(
                        content["flash_headline"], markdown_to_plain(md), content["executive_summary"]
                    )
                    self.counters["articles_fetched"] += 1
                    changed = True
            if want_llm:
                text = markdown_to_plain(content["clean_markdown"]) or content["raw_title"]
                data = await self.llm.enrich(content["raw_title"], text, event["source"]["name"])
                if data:
                    for k in ("flash_headline", "executive_summary"):
                        if k in data:
                            content[k] = data[k]
                    for k in ("categories", "tickers", "entities", "locations", "sentiment"):
                        if k in data and data[k] not in (None, []):
                            enr[k] = data[k]
                    if "urgency" in data:
                        enr["urgency"] = int(round((enr["urgency"] + data["urgency"]) / 2))
                    if "Breaking" not in enr["categories"] and enr["urgency"] >= 9:
                        enr["categories"] = enr["categories"] + ["Breaking"]
                    self.counters["llm_enriched"] += 1
                    changed = True
            if changed:
                event = NewsEvent.model_validate(event).to_wire()
                await self.bus.publish(B.EVENTS, {"type": "EVENT_UPDATED", "ts": iso(utcnow()), "data": event})
                await self.store.update_event(event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.info("upgrade failed for %s: %s", event["event_id"], exc)
