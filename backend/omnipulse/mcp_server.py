"""Model Context Protocol server (official ``mcp`` SDK, FastMCP) — streamable HTTP at /mcp.

Connect from any MCP client, e.g. ``claude mcp add --transport http omnipulse http://localhost:8000/mcp``;
stdio-only clients can bridge with ``npx mcp-remote http://localhost:8000/mcp``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Literal

from mcp.server.fastmcp import FastMCP

from . import bus as B
from .filters import EventFilter, compact, headline_line
from .registry import slugify
from .runtime import get_runtime
from .schema import TAXONOMY, NewsEvent, SourceConfig

INSTRUCTIONS = """OmniPulse is a real-time global news intelligence hub. Every item is a NewsEvent with
flash_headline, a 3-bullet executive_summary, categories, tickers, entities, ISO locations,
sentiment (-1..1), urgency (1-10), novelty (0-1) and verification_status. Related reports are
grouped into StoryClusters (primary source + follow-ups + syndications).

Recommended loop: list_story_clusters for what's developing -> get_latest_events with a filter
-> get_event for full Markdown -> wait_for_events to block until something new matches.
Filters use a compact DSL: `u>=7 cat:crypto,macro $NVDA src:reuters loc:US tier<=2 sent<-0.3
vs:confirmed "rate cut" -opinion`. Use format="compact" (default) to save tokens."""

# host="0.0.0.0" disables the SDK's localhost-only Host-header check; the API sits behind
# its own CORS/API-key controls and is reached via Docker service names and reverse proxies.
mcp = FastMCP("OmniPulse", instructions=INSTRUCTIONS, stateless_http=True, json_response=True, host="0.0.0.0")

Fmt = Literal["compact", "full", "headline"]


def _fmt(events: list[dict[str, Any]], fmt: str) -> list[Any]:
    if fmt == "full":
        return events
    if fmt == "headline":
        return [headline_line(e) for e in events]
    return [compact(e) for e in events]


def _since(hours: float | None):
    if not hours:
        return None
    from datetime import datetime, timedelta, timezone

    return datetime.now(timezone.utc) - timedelta(hours=hours)


@mcp.tool()
async def get_latest_events(filter: str = "", limit: int = 25, hours: float = 0, format: Fmt = "compact") -> dict:
    """Newest events matching an optional filter DSL (e.g. "u>=7 cat:macro"), newest first.

    hours limits the look-back window (0 = no limit). limit max 200."""
    rt = get_runtime()
    events = await rt.store.query_events(EventFilter.parse(filter), limit=max(1, min(limit, 200)), since=_since(hours))
    return {"count": len(events), "events": _fmt(events, format)}


@mcp.tool()
async def search_events(query: str, filter: str = "", hours: float = 24, limit: int = 10) -> dict:
    """Semantic (embedding) search over recent events, e.g. "chip export controls to China".
    Combine with a filter DSL to narrow (e.g. "tier<=2 u>=5")."""
    rt = get_runtime()
    vec = (await rt.embedder.embed([query]))[0]
    hits = await rt.store.semantic_search(vec, max(1, min(limit, 50)), _since(hours), EventFilter.parse(filter))
    return {"count": len(hits), "results": [{"score": round(s, 3), **compact(e)} for s, e in hits]}


@mcp.tool()
async def get_event(event_id: str, include_markdown: bool = True) -> dict:
    """Full NewsEvent by id (evt_...), including clean article Markdown when include_markdown."""
    ev = await get_runtime().store.get_event(event_id)
    if not ev:
        return {"error": f"event {event_id} not found"}
    if not include_markdown:
        ev = {**ev, "content": {**ev["content"], "clean_markdown": ""}}
    return ev


@mcp.tool()
async def list_story_clusters(hours: float = 6, min_sources: int = 2, category: str = "", limit: int = 20) -> dict:
    """Developing stories, most recently updated first. min_sources=2+ returns corroborated stories.
    category is one of the taxonomy names (Macro, Equities, Crypto, AI/Tech, Geopolitics,
    Cybersecurity, Science, Regulatory, Breaking)."""
    clusters = await get_runtime().store.list_clusters(_since(hours), max(1, min(limit, 100)), max(1, min_sources),
                                                      category or None)
    slim = [{k: c[k] for k in ("cluster_id", "headline", "primary_source", "source_count", "size", "max_urgency",
                               "avg_sentiment", "categories", "tickers", "locations", "verification_status",
                               "first_seen", "last_updated")} for c in clusters]
    return {"count": len(slim), "clusters": slim}


@mcp.tool()
async def get_story_cluster(cluster_id: str, format: Fmt = "compact") -> dict:
    """A StoryCluster (cls_...) with every member event in publication order and its syndications."""
    rt = get_runtime()
    c = await rt.store.get_cluster(cluster_id)
    if not c:
        return {"error": f"cluster {cluster_id} not found"}
    events = await rt.store.cluster_events(cluster_id, 100)
    return {"cluster": {k: v for k, v in c.items() if k != "members"}, "events": _fmt(events, format)}


@mcp.tool()
async def wait_for_events(filter: str = "", timeout_seconds: float = 30, max_events: int = 20) -> dict:
    """Block until new events matching the filter arrive (or timeout). Use for event-driven agents:
    e.g. filter="u>=8" to wake on market-moving headlines. timeout max 300 s."""
    rt = get_runtime()
    sub = rt.hub.subscribe(EventFilter.parse(filter), "compact", frozenset({"EVENT_CREATED"}))
    got: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(1.0, min(timeout_seconds, 300.0))
    try:
        while len(got) < max_events:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                env = await asyncio.wait_for(sub.queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            got.append(compact(env["data"]))
            # collect a short burst before returning
            if len(got) == 1:
                deadline = min(deadline, time.monotonic() + 1.5)
    finally:
        rt.hub.unsubscribe(sub)
    return {"count": len(got), "timed_out": not got, "events": got}


@mcp.tool()
async def list_sources(category: str = "", status: str = "") -> dict:
    """Configured sources with health (status: healthy|degraded|dead|paused|disabled|push)."""
    rt = get_runtime()
    if rt.engine:
        health = {k: v.model_dump(mode="json") for k, v in rt.engine.health.items()}
    else:
        health = {h["id"]: h for h in rt.hub.last_health}
    out = []
    for s in await rt.store.list_sources():
        h = health.get(s.id, {})
        if (category and s.category != category) or (status and h.get("status") != status):
            continue
        out.append({"id": s.id, "name": s.name, "type": s.type, "category": s.category, "tier": s.credibility_tier,
                    "enabled": s.enabled, "status": h.get("status"), "items": h.get("items_total"),
                    "last_error": h.get("last_error")})
    return {"count": len(out), "sources": out}


@mcp.tool()
async def add_source(name: str, url: str, type: str = "rss", category: str = "general", credibility_tier: int = 2,
                     poll_interval: float = 60, default_categories: list[str] | None = None) -> dict:
    """Add a feed at runtime (type rss for RSS/Atom; see the REST API for other types).
    default_categories biases tagging, e.g. ["Crypto"]."""
    rt = get_runtime()
    try:
        cfg = SourceConfig(id=slugify(name), name=name, url=url, type=type, category=category,
                           credibility_tier=credibility_tier, poll_interval=poll_interval,
                           min_interval=min(poll_interval, 30), default_categories=default_categories or [])
    except Exception as exc:
        return {"error": str(exc)}
    if await rt.store.get_source(cfg.id):
        return {"error": f"source {cfg.id} already exists"}
    await rt.store.save_source(cfg)
    await rt.bus.publish(B.CONTROL, {"op": "upsert", "source_id": cfg.id})
    return {"added": cfg.id}


@mcp.tool()
async def set_source_enabled(source_id: str, enabled: bool) -> dict:
    """Pause (enabled=false) or resume (enabled=true) a source."""
    rt = get_runtime()
    s = await rt.store.get_source(source_id)
    if not s:
        return {"error": "source not found"}
    s.enabled = enabled
    await rt.store.save_source(s)
    await rt.bus.publish(B.CONTROL, {"op": "resume" if enabled else "pause", "source_id": source_id})
    return {"id": source_id, "enabled": enabled}


@mcp.tool()
async def remove_source(source_id: str) -> dict:
    """Remove a source from the registry."""
    rt = get_runtime()
    s = await rt.store.get_source(source_id)
    if not s:
        return {"error": "source not found"}
    s.deleted, s.enabled = True, False
    await rt.store.save_source(s)
    await rt.bus.publish(B.CONTROL, {"op": "delete", "source_id": source_id})
    return {"id": source_id, "deleted": True}


@mcp.tool()
async def pipeline_stats() -> dict:
    """Ingestion/enrichment throughput, latency percentiles, dedup counters, active clusters."""
    rt = get_runtime()
    return {"store": await rt.store.stats(),
            "pipeline": rt.enricher.metrics() if rt.enricher else rt.hub.last_heartbeat}


@mcp.resource("omnipulse://taxonomy", mime_type="application/json")
def taxonomy() -> dict:
    """Categories, verification states and the filter DSL."""
    return {"categories": list(TAXONOMY), "verification_status": ["CONFIRMED", "DEVELOPING", "UNVERIFIED"],
            "dsl_examples": ["u>=8", "cat:crypto $BTC", "cat:geo loc:UA,RU tier<=2", "src:sec u>=6",
                             "cat:cyber \"actively exploited\""]}


@mcp.resource("omnipulse://schema/news-event", mime_type="application/json")
def news_event_schema() -> dict:
    """JSON Schema of the canonical NewsEvent."""
    return NewsEvent.model_json_schema()


@mcp.prompt()
def market_brief(focus: str = "global markets", hours: int = 4) -> str:
    """Prompt template: produce a desk briefing from the live feed."""
    return (
        f"Using the OmniPulse tools, brief me on {focus} over the last {hours} hours. "
        f"1) list_story_clusters(hours={hours}, min_sources=2). 2) For the top clusters by max_urgency, "
        "call get_story_cluster. 3) get_latest_events(filter='u>=7', hours="
        f"{hours}). Report: top stories with primary source and verification status, tickers affected, "
        "net sentiment, and what is still unconfirmed. Cite event_ids."
    )
