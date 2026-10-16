"""Read APIs: events, story clusters, semantic search, taxonomy, stats, dead-letter queue."""

from __future__ import annotations

import time
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from .. import bus as B
from ..filters import compact, headline_line
from ..schema import TAXONOMY, NewsEvent
from .deps import build_filter, parse_time, require_read, rt

router = APIRouter(prefix="/v1", tags=["data"], dependencies=[Depends(require_read)])

Fmt = Literal["full", "compact", "headline"]
DSL_HELP = ("Filter DSL, e.g. `u>=7 cat:crypto,macro $NVDA src:reuters loc:US tier<=2 sent>0.3 "
            "nov>=0.5 vs:confirmed \"rate cut\" -opinion`")


def fmt_events(events: list[dict[str, Any]], fmt: str) -> list[Any]:
    if fmt == "compact":
        return [compact(e) for e in events]
    if fmt == "headline":
        return [headline_line(e) for e in events]
    return events


@router.get("/events", summary="Query the event archive (newest first)")
async def list_events(
    q: str | None = Query(None, description=DSL_HELP),
    categories: str | None = Query(None, description="Comma-separated taxonomy categories"),
    tickers: str | None = Query(None, description="Comma-separated tickers, e.g. NVDA,BTC"),
    sources: str | None = Query(None, description="Comma-separated source id/name substrings"),
    min_urgency: int | None = Query(None, ge=1, le=10),
    since: str | None = Query(None, description="ISO-8601 or relative: 30m, 6h, 2d"),
    before: str | None = Query(None, description="Cursor: ISO timestamp; returns events published before it"),
    limit: int = Query(100, ge=1, le=5000),
    format: Fmt = "full",
):
    f = build_filter(q, categories, tickers, sources, min_urgency)
    events = await rt().store.query_events(f, limit=limit, before=parse_time(before), since=parse_time(since))
    return {
        "count": len(events),
        "next_before": events[-1]["published_at"] if len(events) == limit else None,
        "items": fmt_events(events, format),
    }


@router.get("/events/{event_id}", response_model=NewsEvent, summary="Fetch one event with full Markdown")
async def get_event(event_id: str):
    ev = await rt().store.get_event(event_id)
    if not ev:
        raise HTTPException(404, "event not found")
    return ev


@router.get("/clusters", summary="Developing stories (StoryClusters), most recently updated first")
async def list_clusters(
    hours: float = Query(6, gt=0, le=168),
    min_sources: int = Query(1, ge=1),
    category: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    slim: bool = Query(False, description="Omit member/syndication lists"),
):
    clusters = await rt().store.list_clusters(parse_time(f"{hours}h"), limit, min_sources, category)
    if slim:
        clusters = [{k: v for k, v in c.items() if k not in ("members", "syndications")} for c in clusters]
    return {"count": len(clusters), "items": clusters}


@router.get("/clusters/{cluster_id}", summary="A StoryCluster with its member events")
async def get_cluster(cluster_id: str, format: Fmt = "compact", limit: int = Query(100, ge=1, le=500)):
    c = await rt().store.get_cluster(cluster_id)
    if not c:
        raise HTTPException(404, "cluster not found")
    events = await rt().store.cluster_events(cluster_id, limit)
    return {"cluster": c, "events": fmt_events(events, format)}


@router.get("/search", summary="Semantic search (vector similarity) over recent events")
async def search(
    query: str = Query(..., min_length=2, description="Natural-language query"),
    q: str | None = Query(None, description=DSL_HELP),
    hours: float = Query(24, gt=0, le=24 * 90),
    limit: int = Query(20, ge=1, le=200),
    format: Fmt = "compact",
):
    r = rt()
    vec = (await r.embedder.embed([query]))[0]
    hits = await r.store.semantic_search(vec, limit, parse_time(f"{hours}h"), build_filter(q))
    items = []
    for score, ev in hits:
        body = fmt_events([ev], format)[0]
        items.append({"score": round(score, 4), "event": body})
    return {"count": len(items), "embedder": r.embedder.name, "items": items}


@router.get("/taxonomy", summary="Categories, verification states, and the filter DSL")
async def taxonomy():
    return {
        "categories": list(TAXONOMY),
        "verification_status": ["CONFIRMED", "DEVELOPING", "UNVERIFIED"],
        "credibility_tiers": {"1": "wire / official primary source", "2": "established outlet or aggregator",
                              "3": "social / community / unverified"},
        "urgency": "1 (opinion/recap) .. 10 (market-moving flash)",
        "dsl": DSL_HELP,
        "stream_types": ["EVENT_CREATED", "EVENT_UPDATED", "CLUSTER_UPDATED", "SOURCE_HEALTH", "HEARTBEAT"],
    }


@router.get("/stats", summary="Pipeline metrics, latency percentiles, subscriber counts")
async def stats():
    r = rt()
    out: dict[str, Any] = {
        "mode": r.settings.mode,
        "role": r.role,
        "uptime_s": round(time.time() - r.started_at),
        "store": await r.store.stats(),
        "pipeline": r.enricher.metrics() if r.enricher else r.hub.last_heartbeat,
        "stream_subscribers": len(r.hub.subscribers),
        "envelopes_delivered": r.hub.delivered,
    }
    if r.engine:
        out["ingest"] = {"sources_running": len(r.engine.runners), "items_emitted": r.engine.items_emitted}
    if r.webhooks:
        out["webhooks"] = {"active": len(r.webhooks.hooks), "delivered": r.webhooks.delivered,
                           "failed": r.webhooks.failed}
    return out


@router.get("/dlq", summary="Dead-letter queue: failing sources and poison items")
async def dlq(limit: int = Query(50, ge=1, le=500)):
    rows = await rt().bus.recent(B.DLQ, limit)
    return {"count": len(rows), "items": [{"id": i, **d} for i, d in rows]}
