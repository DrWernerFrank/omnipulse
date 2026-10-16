"""Storage layer.

- PgStore: PostgreSQL + TimescaleDB hypertable + pgvector clusters. Durable,
  agent-friendly (SQL/RAG queries), and the mode used by docker-compose.
- MemoryStore: bounded in-process store. Used when DATABASE_URL is unset or
  unreachable so the terminal still boots and streams with zero infrastructure.

Both implement the same interface, chosen at startup by `create_store`.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import timedelta
from typing import Any, Protocol

import asyncpg

from .config import Settings, get_settings
from .schemas import (
    ClusterSummary,
    FilterSpec,
    NewsEvent,
    SourceConfig,
    now_utc,
)

log = logging.getLogger("omnipulse.db")

_DDL = """
CREATE TABLE IF NOT EXISTS sources (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  type TEXT NOT NULL DEFAULT 'rss',
  url TEXT,
  category TEXT NOT NULL DEFAULT 'wire',
  tier TEXT NOT NULL DEFAULT 'C',
  credibility_tier INT NOT NULL DEFAULT 3,
  poll_interval_sec INT NOT NULL DEFAULT 60,
  min_poll_sec INT NOT NULL DEFAULT 10,
  max_poll_sec INT NOT NULL DEFAULT 900,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  fulltext BOOLEAN NOT NULL DEFAULT FALSE,
  headers JSONB NOT NULL DEFAULT '{}',
  params JSONB NOT NULL DEFAULT '{}',
  tags TEXT[] NOT NULL DEFAULT '{}',
  requires_key TEXT,
  state JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS clusters (
  id TEXT PRIMARY KEY,
  title TEXT,
  breaking_source TEXT,
  event_count INT NOT NULL DEFAULT 0,
  sources TEXT[] NOT NULL DEFAULT '{}',
  categories TEXT[] NOT NULL DEFAULT '{}',
  tickers TEXT[] NOT NULL DEFAULT '{}',
  first_seen TIMESTAMPTZ,
  latest_at TIMESTAMPTZ,
  embedding @EMB@
);

CREATE TABLE IF NOT EXISTS events (
  event_id TEXT NOT NULL,
  cluster_id TEXT,
  published_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL,
  source_id TEXT,
  source_name TEXT,
  source_category TEXT,
  source_credibility_tier INT,
  source_url TEXT,
  raw_title TEXT,
  flash_headline TEXT,
  executive_summary TEXT[],
  clean_markdown TEXT,
  categories TEXT[] NOT NULL DEFAULT '{}',
  tickers TEXT[] NOT NULL DEFAULT '{}',
  entities JSONB NOT NULL DEFAULT '[]',
  locations TEXT[] NOT NULL DEFAULT '{}',
  sentiment REAL,
  urgency INT,
  novelty REAL,
  verification_status TEXT,
  simhash BIGINT,
  payload JSONB NOT NULL,
  PRIMARY KEY (event_id, ingested_at)
);

CREATE TABLE IF NOT EXISTS dlq (
  id BIGSERIAL PRIMARY KEY,
  source_id TEXT,
  error TEXT,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  detail JSONB
);
"""

_POST_DDL = """
CREATE INDEX IF NOT EXISTS events_recent ON events (ingested_at DESC);
CREATE INDEX IF NOT EXISTS events_categories ON events USING gin (categories);
CREATE INDEX IF NOT EXISTS events_tickers ON events USING gin (tickers);
CREATE INDEX IF NOT EXISTS events_cluster ON events (cluster_id, ingested_at DESC);
"""

_TRGM_DDL = """
CREATE INDEX IF NOT EXISTS events_title_trgm ON events USING gin (lower(raw_title) gin_trgm_ops);
"""


def _vec_literal(emb: list[float]) -> str:
    return "[" + ",".join(f"{x:.5g}" for x in emb) + "]"


class Store(Protocol):
    has_vector: bool

    async def add_event(self, ev: NewsEvent, simhash: int, embedding: list[float]) -> None: ...
    async def upsert_cluster(self, cs: ClusterSummary, embedding: list[float]) -> None: ...
    async def get_event(self, event_id: str) -> NewsEvent | None: ...
    async def query_events(self, f: FilterSpec) -> list[NewsEvent]: ...
    async def cluster_events(self, cluster_id: str, limit: int = 50) -> list[NewsEvent]: ...
    async def list_clusters(self, limit: int = 100) -> list[ClusterSummary]: ...
    async def get_cluster(self, cluster_id: str) -> ClusterSummary | None: ...
    async def similar_clusters(self, embedding: list[float], k: int = 10, days: int = 7) -> list[tuple[str, float, ClusterSummary]]: ...
    async def list_sources(self) -> list[SourceConfig]: ...
    async def get_source(self, source_id: str) -> SourceConfig | None: ...
    async def upsert_source(self, cfg: SourceConfig) -> None: ...
    async def delete_source(self, source_id: str) -> bool: ...
    async def add_dlq(self, source_id: str, error: str, detail: dict | None = None) -> None: ...
    async def list_dlq(self, limit: int = 50) -> list[dict]: ...
    async def stats(self) -> dict[str, Any]: ...


# --------------------------------------------------------------------------- #
# PostgreSQL / TimescaleDB
# --------------------------------------------------------------------------- #
class PgStore:
    def __init__(self, pool: asyncpg.Pool, settings: Settings, has_vector: bool):
        self.pool = pool
        self.settings = settings
        self.has_vector = has_vector

    @classmethod
    async def create(cls, settings: Settings) -> "PgStore | None":
        pool: asyncpg.Pool | None = None
        for attempt in range(40):
            try:
                pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=8)
                break
            except Exception as e:  # startup race with container init
                if attempt == 0:
                    log.warning("postgres not ready (%s); retrying", e)
                await asyncio.sleep(1.5)
        if pool is None:
            return None
        try:
            async with pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
                has_vector = True
                emb = "vector(256)"
                try:
                    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                except Exception:
                    has_vector = False
                    emb = "float8[]"
                    log.warning("pgvector unavailable; clusters stored as float8[] (semantic search degraded)")
                await conn.execute(_DDL.replace("@EMB@", emb))
                try:
                    await conn.execute("SELECT create_hypertable('events','ingested_at', if_not_exists => TRUE, migrate_data => TRUE);")
                except Exception as e:
                    log.warning("hypertable creation skipped: %s", e)
                await conn.execute(_POST_DDL)
                try:
                    await conn.execute(_TRGM_DDL)
                except Exception:
                    pass
                if has_vector:
                    try:
                        await conn.execute(
                            "CREATE INDEX IF NOT EXISTS clusters_emb_ivf ON clusters "
                            "USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);"
                        )
                    except Exception:
                        pass
            log.info("postgres store ready (vector=%s)", has_vector)
            return cls(pool, settings, has_vector)
        except Exception as e:
            log.error("schema bootstrap failed: %s", e)
            await pool.close()
            return None

    async def close(self) -> None:
        await self.pool.close()

    # ---- events ----
    async def add_event(self, ev: NewsEvent, simhash: int, embedding: list[float]) -> None:
        async with self.pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO events (event_id, cluster_id, published_at, ingested_at, source_id,
                  source_name, source_category, source_credibility_tier, source_url, raw_title,
                  flash_headline, executive_summary, clean_markdown, categories, tickers, entities,
                  locations, sentiment, urgency, novelty, verification_status, simhash, payload)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
                ON CONFLICT DO NOTHING
                """,
                ev.event_id, ev.cluster_id, ev.published_at, ev.ingested_at,
                ev.source.id, ev.source.name, ev.source.category, ev.source.credibility_tier,
                ev.source.url, ev.content.raw_title, ev.content.flash_headline,
                ev.content.executive_summary, ev.content.clean_markdown,
                ev.enrichment.categories, ev.enrichment.tickers,
                json.dumps([e.model_dump() for e in ev.enrichment.entities]),
                ev.enrichment.locations, ev.enrichment.sentiment, ev.enrichment.urgency,
                ev.enrichment.novelty, ev.enrichment.verification_status, simhash,
                ev.model_dump_json(),
            )

    async def upsert_cluster(self, cs: ClusterSummary, embedding: list[float]) -> None:
        async with self.pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO clusters (id, title, breaking_source, event_count, sources, categories,
                                      tickers, first_seen, latest_at, embedding)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::vector)
                ON CONFLICT (id) DO UPDATE SET
                  event_count = EXCLUDED.event_count, sources = EXCLUDED.sources,
                  categories = EXCLUDED.categories, tickers = EXCLUDED.tickers,
                  latest_at = EXCLUDED.latest_at, title = EXCLUDED.title, embedding = EXCLUDED.embedding
                """,
                cs.cluster_id, cs.title, cs.breaking_source, cs.event_count, cs.sources,
                cs.categories, cs.tickers, cs.first_seen, cs.latest_at,
                _vec_literal(embedding),
            )

    async def _row_to_event(self, row: asyncpg.Record) -> NewsEvent:
        return NewsEvent.model_validate_json(row["payload"])

    async def get_event(self, event_id: str) -> NewsEvent | None:
        async with self.pool.acquire() as c:
            row = await c.fetchrow(
                "SELECT payload FROM events WHERE event_id = $1 ORDER BY ingested_at DESC LIMIT 1",
                event_id,
            )
            return await self._row_to_event(row) if row else None

    async def query_events(self, f: FilterSpec) -> list[NewsEvent]:
        where = []
        args: list[Any] = []
        if f.cursor:
            where.append("event_id < $%d")
            args.append(f.cursor)
        if f.since:
            where.append("published_at >= $%d")
            args.append(f.since)
        if f.categories:
            where.append("categories && $%d")
            args.append(f.categories)
        if f.tickers:
            where.append("tickers && $%d")
            args.append(f.tickers)
        if f.sources:
            where.append("source_id = ANY($%d)")
            args.append(f.sources)
        if f.min_urgency is not None:
            where.append("urgency >= $%d")
            args.append(f.min_urgency)
        if f.q:
            where.append("(lower(raw_title) LIKE $%d OR lower(flash_headline) LIKE $%d)")
            args.extend([f"%{f.q.lower()}%", f"%{f.q.lower()}%"])
        # renumber placeholders
        clauses = []
        idx = 0
        for w in where:
            n = w.count("$%d")
            parts = w.split("$%d")
            rebuilt = ""
            for i, part in enumerate(parts):
                rebuilt += part
                if i < len(parts) - 1:
                    idx += 1
                    rebuilt += f"${idx}"
            clauses.append(rebuilt)
        limit = min(f.limit, 1000)
        sql = "SELECT payload FROM events"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += f" ORDER BY event_id DESC LIMIT {limit}"
        async with self.pool.acquire() as c:
            rows = await c.fetch(sql, *args)
            return [await self._row_to_event(r) for r in rows]

    async def cluster_events(self, cluster_id: str, limit: int = 50) -> list[NewsEvent]:
        async with self.pool.acquire() as c:
            rows = await c.fetch(
                "SELECT payload FROM events WHERE cluster_id = $1 ORDER BY event_id DESC LIMIT $2",
                cluster_id, limit,
            )
            return [await self._row_to_event(r) for r in rows]

    async def list_clusters(self, limit: int = 100) -> list[ClusterSummary]:
        async with self.pool.acquire() as c:
            rows = await c.fetch(
                """
                SELECT c.id, c.title, c.breaking_source, c.event_count, c.sources, c.categories,
                       c.tickers, c.first_seen, c.latest_at,
                       (SELECT count(*) FROM events e
                          WHERE e.cluster_id = c.id AND e.ingested_at > now() - interval '15 minutes') AS velocity
                FROM clusters c ORDER BY c.latest_at DESC NULLS LAST LIMIT $1
                """,
                limit,
            )
            return [self._row_to_cluster(r) for r in rows]

    async def get_cluster(self, cluster_id: str) -> ClusterSummary | None:
        async with self.pool.acquire() as c:
            row = await c.fetchrow(
                """
                SELECT c.id, c.title, c.breaking_source, c.event_count, c.sources, c.categories,
                       c.tickers, c.first_seen, c.latest_at,
                       (SELECT count(*) FROM events e
                          WHERE e.cluster_id = c.id AND e.ingested_at > now() - interval '15 minutes') AS velocity
                FROM clusters c WHERE c.id = $1
                """,
                cluster_id,
            )
            return self._row_to_cluster(row) if row else None

    @staticmethod
    def _row_to_cluster(r: asyncpg.Record) -> ClusterSummary:
        return ClusterSummary(
            cluster_id=r["id"], title=r["title"] or "", event_count=r["event_count"],
            sources=list(r["sources"] or []), categories=list(r["categories"] or []),
            tickers=list(r["tickers"] or []), breaking_source=r["breaking_source"],
            first_seen=r["first_seen"], latest_at=r["latest_at"], velocity_15m=r["velocity"],
        )

    async def similar_clusters(self, embedding: list[float], k: int = 10, days: int = 7) -> list[tuple[str, float, ClusterSummary]]:
        async with self.pool.acquire() as c:
            if self.has_vector:
                rows = await c.fetch(
                    """
                    SELECT id, title, breaking_source, event_count, sources, categories, tickers,
                           first_seen, latest_at, 1 - (embedding <=> $1::vector) AS sim
                    FROM clusters WHERE latest_at > now() - ($3 || ' days')::interval
                    ORDER BY embedding <=> $1::vector LIMIT $2
                    """,
                    _vec_literal(embedding), k, str(days),
                )
            else:
                rows = await c.fetch(
                    "SELECT id, title, breaking_source, event_count, sources, categories, tickers, "
                    "first_seen, latest_at, 0.0 AS sim FROM clusters ORDER BY latest_at DESC LIMIT $2",
                    _vec_literal(embedding), k,
                )
            return [
                (r["id"], float(r["sim"]), PgStore._row_to_cluster(r)) for r in rows
            ]

    # ---- sources ----
    async def list_sources(self) -> list[SourceConfig]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT * FROM sources ORDER BY tier, id")
            return [self._row_to_source(r) for r in rows]

    async def get_source(self, source_id: str) -> SourceConfig | None:
        async with self.pool.acquire() as c:
            row = await c.fetchrow("SELECT * FROM sources WHERE id = $1", source_id)
            return self._row_to_source(row) if row else None

    @staticmethod
    def _row_to_source(r: asyncpg.Record) -> SourceConfig:
        return SourceConfig(
            id=r["id"], name=r["name"], type=r["type"], url=r["url"], category=r["category"],
            tier=r["tier"], credibility_tier=r["credibility_tier"],
            poll_interval_sec=r["poll_interval_sec"], min_poll_sec=r["min_poll_sec"],
            max_poll_sec=r["max_poll_sec"], enabled=r["enabled"], fulltext=r["fulltext"],
            headers=json.loads(r["headers"]) if isinstance(r["headers"], str) else dict(r["headers"]),
            params=json.loads(r["params"]) if isinstance(r["params"], str) else dict(r["params"]),
            tags=list(r["tags"] or []), requires_key=r["requires_key"],
            state=json.loads(r["state"]) if isinstance(r["state"], str) else dict(r["state"]),
        )

    async def upsert_source(self, cfg: SourceConfig) -> None:
        async with self.pool.acquire() as c:
            await c.execute(
                """
                INSERT INTO sources (id, name, type, url, category, tier, credibility_tier,
                  poll_interval_sec, min_poll_sec, max_poll_sec, enabled, fulltext, headers,
                  params, tags, requires_key, state, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17, now())
                ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, type=EXCLUDED.type,
                  url=EXCLUDED.url, category=EXCLUDED.category, tier=EXCLUDED.tier,
                  credibility_tier=EXCLUDED.credibility_tier,
                  poll_interval_sec=EXCLUDED.poll_interval_sec, min_poll_sec=EXCLUDED.min_poll_sec,
                  max_poll_sec=EXCLUDED.max_poll_sec, enabled=EXCLUDED.enabled,
                  fulltext=EXCLUDED.fulltext, headers=EXCLUDED.headers, params=EXCLUDED.params,
                  tags=EXCLUDED.tags, requires_key=EXCLUDED.requires_key, state=EXCLUDED.state,
                  updated_at=now()
                """,
                cfg.id, cfg.name, cfg.type, cfg.url, cfg.category, cfg.tier,
                cfg.credibility_tier, cfg.poll_interval_sec, cfg.min_poll_sec,
                cfg.max_poll_sec, cfg.enabled, cfg.fulltext, json.dumps(cfg.headers),
                json.dumps(cfg.params), cfg.tags, cfg.requires_key, json.dumps(cfg.state),
            )

    async def delete_source(self, source_id: str) -> bool:
        async with self.pool.acquire() as c:
            return (await c.execute("DELETE FROM sources WHERE id = $1", source_id))[-1] == "1"

    # ---- dlq ----
    async def add_dlq(self, source_id: str, error: str, detail: dict | None = None) -> None:
        async with self.pool.acquire() as c:
            await c.execute(
                "INSERT INTO dlq (source_id, error, detail) VALUES ($1, $2, $3)",
                source_id, error[:500], json.dumps(detail or {}),
            )

    async def list_dlq(self, limit: int = 50) -> list[dict]:
        async with self.pool.acquire() as c:
            rows = await c.fetch(
                "SELECT id, source_id, error, ts, detail FROM dlq ORDER BY id DESC LIMIT $1", limit
            )
            return [
                {
                    "id": r["id"], "source_id": r["source_id"], "error": r["error"],
                    "ts": r["ts"].isoformat(),
                    "detail": json.loads(r["detail"]) if isinstance(r["detail"], str) else dict(r["detail"] or {}),
                }
                for r in rows
            ]

    # ---- stats ----
    async def stats(self) -> dict[str, Any]:
        async with self.pool.acquire() as c:
            total = await c.fetchval("SELECT count(*) FROM events")
            last_1h = await c.fetchval("SELECT count(*) FROM events WHERE ingested_at > now() - interval '1 hour'")
            last_15m = await c.fetchval("SELECT count(*) FROM events WHERE ingested_at > now() - interval '15 minutes'")
            clusters_24h = await c.fetchval(
                "SELECT count(*) FROM clusters WHERE latest_at > now() - interval '24 hours'"
            )
            cats = await c.fetch(
                "SELECT category, count(*) AS n FROM events "
                "WHERE ingested_at > now() - interval '1 hour' "
                "GROUP BY category ORDER BY n DESC"
            )
            ticks = await c.fetch(
                """
                SELECT t, count(*) AS n FROM (
                  SELECT unnest(tickers) AS t FROM events
                  WHERE ingested_at > now() - interval '1 hour'
                ) s GROUP BY t ORDER BY n DESC LIMIT 10
                """,
            )
            return {
                "events_total": total,
                "events_1h": last_1h,
                "events_15m": last_15m,
                "clusters_24h": clusters_24h,
                "categories_1h": {r["category"]: r["n"] for r in cats},
                "top_tickers": [(r["t"], r["n"]) for r in ticks],
            }


# --------------------------------------------------------------------------- #
# In-process fallback store (bounded ring buffers)
# --------------------------------------------------------------------------- #
class MemoryStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.has_vector = False
        self._events: deque[NewsEvent] = deque(maxlen=settings.memory_buffer)
        self._by_id: dict[str, NewsEvent] = {}
        self._clusters: dict[str, tuple[ClusterSummary, list[float]]] = {}
        self._cluster_order: deque[str] = deque()  # ids in latest_at order (approximate)
        self._sources: dict[str, SourceConfig] = {}
        self._dlq: deque[dict] = deque(maxlen=500)

    async def close(self) -> None: ...

    async def add_event(self, ev: NewsEvent, simhash: int, embedding: list[float]) -> None:
        if ev.event_id not in self._by_id:
            self._events.append(ev)
            self._by_id[ev.event_id] = ev
            if len(self._by_id) > self.settings.memory_buffer:
                dropped = self._events.popleft()
                self._by_id.pop(dropped.event_id, None)

    async def upsert_cluster(self, cs: ClusterSummary, embedding: list[float]) -> None:
        if cs.cluster_id in self._clusters:
            self._clusters[cs.cluster_id] = (cs, embedding)
        else:
            self._clusters[cs.cluster_id] = (cs, embedding)
            self._cluster_order.append(cs.cluster_id)
        while len(self._cluster_order) > 5000:
            old = self._cluster_order.popleft()
            self._clusters.pop(old, None)

    async def get_event(self, event_id: str) -> NewsEvent | None:
        return self._by_id.get(event_id)

    async def query_events(self, f: FilterSpec) -> list[NewsEvent]:
        out: list[NewsEvent] = []
        started = f.cursor is None
        for ev in reversed(self._events):
            if not started:
                if ev.event_id < f.cursor:
                    started = True
                else:
                    continue
            if f.match(ev):
                out.append(ev)
                if len(out) >= min(f.limit, 1000):
                    break
        return out

    async def cluster_events(self, cluster_id: str, limit: int = 50) -> list[NewsEvent]:
        return [e for e in reversed(self._events) if e.cluster_id == cluster_id][:limit]

    async def list_clusters(self, limit: int = 100) -> list[ClusterSummary]:
        # single reversed pass over the last 15m of events for velocity,
        # then newest-first clusters (sort by latest_at, not insertion order)
        now = now_utc()
        vel: dict[str, int] = {}
        cutoff = now - timedelta(minutes=15)
        for e in reversed(self._events):
            if e.ingested_at < cutoff:
                break
            vel[e.cluster_id] = vel.get(e.cluster_id, 0) + 1
        items: list[ClusterSummary] = []
        for cid, got in self._clusters.items():
            cs = got[0]
            cs.velocity_15m = vel.get(cid, 0)
            items.append(cs)
        items.sort(key=lambda c: c.latest_at or c.first_seen or now, reverse=True)
        return items[:limit]

    async def get_cluster(self, cluster_id: str) -> ClusterSummary | None:
        got = self._clusters.get(cluster_id)
        return got[0] if got else None

    async def similar_clusters(self, embedding: list[float], k: int = 10, days: int = 7) -> list[tuple[str, float, ClusterSummary]]:
        import math

        norm = math.sqrt(sum(x * x for x in embedding)) or 1.0
        cutoff = now_utc() - timedelta(days=days)
        scored: list[tuple[float, str, ClusterSummary]] = []
        for cid, (cs, emb) in self._clusters.items():
            if cs.latest_at and cs.latest_at < cutoff:
                continue
            dot = sum(a * b for a, b in zip(embedding, emb))
            denom = norm * (math.sqrt(sum(x * x for x in emb)) or 1.0)
            scored.append((dot / denom if denom else 0.0, cid, cs))
        scored.sort(key=lambda t: -t[0])
        return [(cid, sim, cs) for sim, cid, cs in scored[:k]]

    async def list_sources(self) -> list[SourceConfig]:
        return list(self._sources.values())

    async def get_source(self, source_id: str) -> SourceConfig | None:
        return self._sources.get(source_id)

    async def upsert_source(self, cfg: SourceConfig) -> None:
        self._sources[cfg.id] = cfg

    async def delete_source(self, source_id: str) -> bool:
        return self._sources.pop(source_id, None) is not None

    async def add_dlq(self, source_id: str, error: str, detail: dict | None = None) -> None:
        self._dlq.appendleft(
            {"source_id": source_id, "error": error[:500], "ts": now_utc().isoformat(), "detail": detail or {}}
        )

    async def list_dlq(self, limit: int = 50) -> list[dict]:
        return list(self._dlq)[:limit]

    async def stats(self) -> dict[str, Any]:
        # single reversed pass with early exit — O(recent) instead of O(buffer)
        now = now_utc()
        last_15m = 0
        last_1h = 0
        cats: dict[str, int] = {}
        ticks: dict[str, int] = {}
        cutoff_15m = now - timedelta(minutes=15)
        cutoff_1h = now - timedelta(hours=1)
        for e in reversed(self._events):
            if e.ingested_at < cutoff_1h:
                break
            last_1h += 1
            if e.ingested_at >= cutoff_15m:
                last_15m += 1
            for c in e.enrichment.categories:
                cats[c] = cats.get(c, 0) + 1
            for t in e.enrichment.tickers:
                ticks[t] = ticks.get(t, 0) + 1
        cutoff_24h = now - timedelta(hours=24)
        clusters_24h = sum(
            1 for cs, _ in self._clusters.values()
            if cs.latest_at and cs.latest_at >= cutoff_24h
        )
        return {
            "events_total": len(self._events),
            "events_1h": last_1h,
            "events_15m": last_15m,
            "clusters_24h": clusters_24h,
            "categories_1h": cats,
            "top_tickers": sorted(ticks.items(), key=lambda t: -t[1])[:10],
        }


async def create_store(settings: Settings) -> Store:
    if settings.database_url:
        pg = await PgStore.create(settings)
        if pg is not None:
            return pg
        log.warning("postgres unavailable; falling back to in-memory store")
    return MemoryStore(settings)
