"""PostgreSQL + TimescaleDB (hypertable archive) + pgvector (HNSW semantic index)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Sequence

import orjson

from ..filters import EventFilter
from ..schema import SourceConfig, SourceHealth, WebhookConfig

log = logging.getLogger(__name__)

MIGRATION_LOCK = 0x0_0A1_5E  # advisory lock id shared by every replica

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS sources (
    id          text PRIMARY KEY,
    config      jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_health (
    id          text PRIMARY KEY,
    health      jsonb NOT NULL,
    state       jsonb NOT NULL DEFAULT '{{}}'::jsonb,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
    event_id        text        NOT NULL,
    cluster_id      text        NOT NULL,
    published_at    timestamptz NOT NULL,
    ingested_at     timestamptz NOT NULL,
    source_id       text        NOT NULL,
    source_name     text        NOT NULL,
    source_category text        NOT NULL,
    tier            smallint    NOT NULL,
    canonical_url   text,
    simhash         bigint,
    urgency         smallint    NOT NULL,
    sentiment       real        NOT NULL,
    novelty         real        NOT NULL,
    verification    text        NOT NULL,
    categories      text[]      NOT NULL DEFAULT '{{}}',
    tickers         text[]      NOT NULL DEFAULT '{{}}',
    locations       text[]      NOT NULL DEFAULT '{{}}',
    headline        text        NOT NULL,
    entities_text   text        NOT NULL DEFAULT '',
    title_tsv       tsvector,
    embedding       vector({dim}),
    doc             jsonb       NOT NULL,
    PRIMARY KEY (event_id, published_at)
);

SELECT create_hypertable('events', 'published_at', if_not_exists => TRUE,
                         chunk_time_interval => INTERVAL '1 day');

CREATE INDEX IF NOT EXISTS events_event_id   ON events (event_id);
CREATE INDEX IF NOT EXISTS events_cluster    ON events (cluster_id, published_at);
CREATE INDEX IF NOT EXISTS events_source     ON events (source_id, published_at DESC);
CREATE INDEX IF NOT EXISTS events_urgency    ON events (urgency, published_at DESC);
CREATE INDEX IF NOT EXISTS events_categories ON events USING gin (categories);
CREATE INDEX IF NOT EXISTS events_tickers    ON events USING gin (tickers);
CREATE INDEX IF NOT EXISTS events_locations  ON events USING gin (locations);
CREATE INDEX IF NOT EXISTS events_tsv        ON events USING gin (title_tsv);
CREATE INDEX IF NOT EXISTS events_embedding  ON events USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS clusters (
    cluster_id    text PRIMARY KEY,
    first_seen    timestamptz NOT NULL,
    last_updated  timestamptz NOT NULL,
    size          integer     NOT NULL,
    source_count  integer     NOT NULL,
    max_urgency   smallint    NOT NULL,
    categories    text[]      NOT NULL DEFAULT '{{}}',
    doc           jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS clusters_updated ON clusters (last_updated DESC);

CREATE TABLE IF NOT EXISTS webhooks (
    id          text PRIMARY KEY,
    config      jsonb NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
"""


def _vec(v: Sequence[float]) -> str:
    return "[" + ",".join(f"{float(x):.6f}" for x in v) + "]"


def _parse_vec(s: str | None) -> list[float] | None:
    if not s:
        return None
    return [float(x) for x in s.strip("[]").split(",") if x]


def _signed64(v: int) -> int:
    return v - (1 << 64) if v >= (1 << 63) else v


def _unsigned64(v: int | None) -> int:
    if v is None:
        return 0
    return v + (1 << 64) if v < 0 else v


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class PostgresStore:
    def __init__(self, dsn: str, dim: int = 384, retention_days: int = 90) -> None:
        self.dsn = dsn
        self.dim = dim
        self.retention_days = retention_days
        self.pool = None

    async def _init_conn(self, conn) -> None:
        await conn.set_type_codec(
            "jsonb", encoder=lambda v: orjson.dumps(v).decode(), decoder=orjson.loads, schema="pg_catalog"
        )

    async def init(self) -> None:
        import asyncpg

        for attempt in range(40):
            try:
                self.pool = await asyncpg.create_pool(
                    self.dsn, min_size=1, max_size=12, init=self._init_conn, command_timeout=30
                )
                break
            except Exception as exc:  # pragma: no cover - compose startup race
                log.warning("postgres not ready (%s), retry %d", exc, attempt)
                await asyncio.sleep(1 + attempt * 0.5)
        else:
            raise RuntimeError("postgres unavailable")
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK)
            try:
                await conn.execute(SCHEMA.format(dim=self.dim))
                if self.retention_days > 0:
                    await conn.execute(
                        f"SELECT add_retention_policy('events', INTERVAL '{int(self.retention_days)} days', if_not_exists => TRUE)"
                    )
            finally:
                await conn.execute("SELECT pg_advisory_unlock($1)", MIGRATION_LOCK)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    # events -------------------------------------------------------------------------
    async def save_event(self, event: dict[str, Any], embedding, simhash: int, canonical_url: str) -> None:
        enr, src, content = event["enrichment"], event["source"], event["content"]
        await self.pool.execute(
            """
            INSERT INTO events (event_id, cluster_id, published_at, ingested_at, source_id, source_name,
                source_category, tier, canonical_url, simhash, urgency, sentiment, novelty, verification,
                categories, tickers, locations, headline, entities_text, title_tsv, embedding, doc)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,
                    to_tsvector('english', $18::text || ' ' || $20::text), $21::text::vector, $22)
            ON CONFLICT DO NOTHING
            """,
            event["event_id"], event["cluster_id"], _dt(event["published_at"]), _dt(event["ingested_at"]),
            src["id"], src["name"], src["category"], src["credibility_tier"], canonical_url, _signed64(simhash),
            enr["urgency"], enr["sentiment"], enr["novelty"], enr["verification_status"],
            enr["categories"], enr["tickers"], enr["locations"], content["flash_headline"],
            " | ".join(e["name"].lower() for e in enr["entities"]), content["raw_title"],
            _vec(embedding), event,
        )

    async def update_event(self, event: dict[str, Any]) -> None:
        enr = event["enrichment"]
        await self.pool.execute(
            """UPDATE events SET doc=$2, headline=$3, categories=$4, tickers=$5, locations=$6, urgency=$7,
               sentiment=$8, entities_text=$9 WHERE event_id=$1 AND published_at=$10""",
            event["event_id"], event, event["content"]["flash_headline"], enr["categories"], enr["tickers"],
            enr["locations"], enr["urgency"], enr["sentiment"],
            " | ".join(e["name"].lower() for e in enr["entities"]), _dt(event["published_at"]),
        )

    async def get_event(self, event_id: str) -> dict[str, Any] | None:
        row = await self.pool.fetchrow("SELECT doc FROM events WHERE event_id=$1 LIMIT 1", event_id)
        return row["doc"] if row else None

    @staticmethod
    def _where(f: EventFilter, args: list[Any]) -> list[str]:
        def p(v: Any) -> str:
            args.append(v)
            return f"${len(args)}"

        w = [f"urgency >= {p(f.min_urgency)}", f"urgency <= {p(f.max_urgency)}",
             f"tier >= {p(f.min_tier)}", f"tier <= {p(f.max_tier)}"]
        if f.min_sentiment is not None:
            w.append(f"sentiment >= {p(f.min_sentiment)}")
        if f.max_sentiment is not None:
            w.append(f"sentiment <= {p(f.max_sentiment)}")
        if f.min_novelty:
            w.append(f"novelty >= {p(f.min_novelty)}")
        if f.categories:
            w.append(f"categories && {p(f.categories)}::text[]")
        if f.tickers:
            w.append(f"tickers && {p(f.tickers)}::text[]")
        if f.locations:
            w.append(f"locations && {p(f.locations)}::text[]")
        if f.verification:
            w.append(f"verification = ANY({p(f.verification)}::text[])")
        if f.clusters:
            w.append(f"cluster_id = ANY({p(f.clusters)}::text[])")
        if f.source_categories:
            w.append(f"lower(source_category) = ANY({p(f.source_categories)}::text[])")
        if f.sources:
            pats = [f"%{s}%" for s in f.sources]
            ph = p(pats)
            w.append(f"(source_id ILIKE ANY({ph}::text[]) OR source_name ILIKE ANY({ph}::text[]))")
        if f.entities:
            w.append(f"entities_text ILIKE ANY({p([f'%{e}%' for e in f.entities])}::text[])")
        for t in f.text:
            w.append(f"headline ILIKE {p(f'%{t}%')}")
        for x in f.exclude:
            w.append(f"headline NOT ILIKE {p(f'%{x}%')}")
        return w

    async def query_events(self, f: EventFilter, limit: int = 100, before=None, since=None) -> list[dict[str, Any]]:
        args: list[Any] = []
        w = self._where(f, args)
        if before:
            args.append(before)
            w.append(f"published_at < ${len(args)}")
        if since:
            args.append(since)
            w.append(f"published_at >= ${len(args)}")
        args.append(limit)
        sql = f"SELECT doc FROM events WHERE {' AND '.join(w)} ORDER BY published_at DESC LIMIT ${len(args)}"
        return [r["doc"] for r in await self.pool.fetch(sql, *args)]

    async def semantic_search(self, vector, limit, since, f) -> list[tuple[float, dict[str, Any]]]:
        args: list[Any] = [_vec(vector)]  # sent as text; cast server-side (no client codec needed)
        w = self._where(f, args)
        if since:
            args.append(since)
            w.append(f"published_at >= ${len(args)}")
        args.append(limit)
        sql = (
            f"SELECT doc, 1 - (embedding <=> $1::text::vector) AS score FROM events WHERE {' AND '.join(w)} "
            f"ORDER BY embedding <=> $1::text::vector LIMIT ${len(args)}"
        )
        return [(float(r["score"]), r["doc"]) for r in await self.pool.fetch(sql, *args)]

    async def window_rows(self, since: datetime) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """SELECT event_id, cluster_id, published_at, source_id, tier, simhash, canonical_url,
                      embedding::text AS emb, doc
               FROM events WHERE published_at >= $1 ORDER BY published_at""",
            since,
        )
        return [
            {
                "event_id": r["event_id"], "cluster_id": r["cluster_id"], "published_at": r["doc"]["published_at"],
                "source_id": r["source_id"], "tier": r["tier"], "simhash": _unsigned64(r["simhash"]),
                "canonical_url": r["canonical_url"], "embedding": _parse_vec(r["emb"]), "event": r["doc"],
            }
            for r in rows
        ]

    # clusters -----------------------------------------------------------------------
    async def upsert_cluster(self, c: dict[str, Any]) -> None:
        await self.pool.execute(
            """INSERT INTO clusters (cluster_id, first_seen, last_updated, size, source_count, max_urgency, categories, doc)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
               ON CONFLICT (cluster_id) DO UPDATE SET last_updated=EXCLUDED.last_updated, size=EXCLUDED.size,
                 source_count=EXCLUDED.source_count, max_urgency=EXCLUDED.max_urgency,
                 categories=EXCLUDED.categories, doc=EXCLUDED.doc""",
            c["cluster_id"], _dt(c["first_seen"]), _dt(c["last_updated"]), c["size"], c["source_count"],
            c["max_urgency"], c["categories"], c,
        )

    async def get_cluster(self, cluster_id: str) -> dict[str, Any] | None:
        row = await self.pool.fetchrow("SELECT doc FROM clusters WHERE cluster_id=$1", cluster_id)
        return row["doc"] if row else None

    async def cluster_events(self, cluster_id: str, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            "SELECT doc FROM events WHERE cluster_id=$1 ORDER BY published_at LIMIT $2", cluster_id, limit
        )
        return [r["doc"] for r in rows]

    async def list_clusters(self, since, limit, min_sources=1, category=None) -> list[dict[str, Any]]:
        args: list[Any] = [min_sources]
        w = ["source_count >= $1"]
        if since:
            args.append(since)
            w.append(f"last_updated >= ${len(args)}")
        if category:
            args.append(category)
            w.append(f"${len(args)} = ANY(categories)")
        args.append(limit)
        rows = await self.pool.fetch(
            f"SELECT doc FROM clusters WHERE {' AND '.join(w)} ORDER BY last_updated DESC LIMIT ${len(args)}", *args
        )
        return [r["doc"] for r in rows]

    # sources ------------------------------------------------------------------------
    async def seed_sources(self, sources: list[SourceConfig]) -> int:
        n = 0
        async with self.pool.acquire() as conn:
            for s in sources:
                res = await conn.execute(
                    "INSERT INTO sources (id, config) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
                    s.id, s.model_dump(mode="json"),
                )
                n += res.endswith(" 1")
        return n

    async def list_sources(self, include_deleted: bool = False) -> list[SourceConfig]:
        rows = await self.pool.fetch("SELECT config FROM sources ORDER BY id")
        out = []
        for r in rows:
            try:
                cfg = SourceConfig.model_validate(r["config"])
            except Exception as exc:
                log.warning("invalid stored source config: %s", exc)
                continue
            if include_deleted or not cfg.deleted:
                out.append(cfg)
        return out

    async def get_source(self, source_id: str) -> SourceConfig | None:
        row = await self.pool.fetchrow("SELECT config FROM sources WHERE id=$1", source_id)
        if not row:
            return None
        cfg = SourceConfig.model_validate(row["config"])
        return None if cfg.deleted else cfg

    async def save_source(self, cfg: SourceConfig) -> None:
        await self.pool.execute(
            """INSERT INTO sources (id, config) VALUES ($1,$2)
               ON CONFLICT (id) DO UPDATE SET config=EXCLUDED.config, updated_at=now()""",
            cfg.id, cfg.model_dump(mode="json"),
        )

    async def save_health(self, health: list[SourceHealth]) -> None:
        if not health:
            return
        await self.pool.executemany(
            """INSERT INTO source_health (id, health) VALUES ($1,$2)
               ON CONFLICT (id) DO UPDATE SET health=EXCLUDED.health, updated_at=now()""",
            [(h.id, h.model_dump(mode="json")) for h in health],
        )

    async def list_health(self) -> list[SourceHealth]:
        rows = await self.pool.fetch("SELECT health FROM source_health")
        return [SourceHealth.model_validate(r["health"]) for r in rows]

    async def save_source_state(self, states: dict[str, dict[str, Any]]) -> None:
        if not states:
            return
        await self.pool.executemany(
            """INSERT INTO source_health (id, health, state) VALUES ($1, jsonb_build_object('id', $1::text), $2)
               ON CONFLICT (id) DO UPDATE SET state=EXCLUDED.state""",
            list(states.items()),
        )

    async def load_source_states(self) -> dict[str, dict[str, Any]]:
        rows = await self.pool.fetch("SELECT id, state FROM source_health")
        return {r["id"]: r["state"] for r in rows}

    # webhooks -----------------------------------------------------------------------
    async def list_webhooks(self) -> list[WebhookConfig]:
        rows = await self.pool.fetch("SELECT config FROM webhooks ORDER BY created_at")
        return [WebhookConfig.model_validate(r["config"]) for r in rows]

    async def save_webhook(self, hook: WebhookConfig) -> None:
        await self.pool.execute(
            "INSERT INTO webhooks (id, config) VALUES ($1,$2) ON CONFLICT (id) DO UPDATE SET config=EXCLUDED.config",
            hook.id, hook.model_dump(mode="json"),
        )

    async def delete_webhook(self, hook_id: str) -> bool:
        res = await self.pool.execute("DELETE FROM webhooks WHERE id=$1", hook_id)
        return res.endswith(" 1")

    async def stats(self) -> dict[str, Any]:
        row = await self.pool.fetchrow(
            """SELECT (SELECT count(*) FROM events WHERE published_at > now() - interval '24 hours') AS ev24,
                      (SELECT count(*) FROM clusters WHERE last_updated > now() - interval '24 hours') AS cl24,
                      (SELECT count(*) FROM sources WHERE NOT coalesce((config->>'deleted')::bool, false)) AS src"""
        )
        return {"events_24h": row["ev24"], "clusters_24h": row["cl24"], "sources": row["src"], "backend": "postgres"}
