"""OmniPulse MCP server (stdio transport).

Exposes the live news intelligence hub to AI agents over the Model Context
Protocol. Runs standalone:  python -m app.mcp_server   (env OMNIPULSE_API_URL
points at the OmniPulse REST API; defaults to http://localhost:8000).

Tools (agent-callable):
  omnipulse_hello                      — handshake + capability probe
  get_latest_events(q, categories, tickers, min_urgency, limit, since, compact)
  get_event(event_id)
  search_events(q, since, limit)       — deep query w/ pagination
  get_story_clusters(limit)
  get_cluster(cluster_id)
  get_cluster_events(cluster_id, limit)
  get_breaking(min_urgency, max_age_sec, limit)   — urgent, fresh events only
  get_top_tickers(hours, limit)
  watch(filter)                        — subscribe to live events matching a filter
  unwatch(subscription_id)
  poll_watch(subscription_id, limit)    — drain a watch queue (pull model)
  list_sources()
  add_source(config) / pause_source(id) / resume_source(id) / remove_source(id)
  get_stats()
  get_dlq()

Resources:
  omnipulse://events/latest    — newest 50 events (auto-refreshed)
  omnipulse://clusters/active   — active story clusters
  omnipulse://stats             — live terminal stats
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

import httpx
import orjson
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Resource, TextContent, Tool

API = os.environ.get("OMNIPULSE_API_URL", "http://localhost:8000")
NAME = "omnipulse"

# --------------------------------------------------------------------------- #
# thin REST client
# --------------------------------------------------------------------------- #
_client: httpx.AsyncClient | None = None


async def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0, base_url=API)
    r = await _client.get(path, params=params)
    r.raise_for_status()
    return orjson.loads(r.content)


async def api_post(path: str, json_body: dict[str, Any] | None = None) -> Any:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0, base_url=API)
    r = await _client.post(path, json=json_body)
    r.raise_for_status()
    text = r.content
    return orjson.loads(text) if text else {}


async def api_patch(path: str, json_body: dict[str, Any] | None = None) -> Any:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0, base_url=API)
    r = await _client.patch(path, json=json_body)
    r.raise_for_status()
    return orjson.loads(r.content) if r.content else {}


async def api_delete(path: str) -> Any:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0, base_url=API)
    r = await _client.delete(path)
    r.raise_for_status()
    return orjson.loads(r.content) if r.content else {}


# --------------------------------------------------------------------------- #
# watch registry (SSE-backed pull queues)
# --------------------------------------------------------------------------- #
class Watch:
    def __init__(self, wid: str, params: dict[str, Any]):
        self.id = wid
        self.params = params
        self.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=1000)
        self.created = time.monotonic()

WATCHES: dict[str, Watch] = {}


async def _sse_watcher(watch: Watch) -> None:
    """Background SSE consumer filling the watch queue."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30.0, base_url=API)
    try:
        async with _client.stream("GET", "/stream/events", params=watch.params) as resp:
            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                try:
                    payload = orjson.loads(line[5:].strip())
                except Exception:
                    continue
                if isinstance(payload, dict) and payload.get("t") == "event":
                    if watch.queue.full():
                        try:
                            watch.queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                    watch.queue.put_nowait(payload["d"])
    except Exception:
        pass  # stopped / disconnected


# --------------------------------------------------------------------------- #
# tool definitions
# --------------------------------------------------------------------------- #
TOOLS = [
    Tool(
        name="omnipulse_hello",
        description="Handshake with the OmniPulse hub. Returns version, latency and available capabilities.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_latest_events",
        description=(
            "Fetch the most recent events (newest first). Filter by free-text query, "
            "categories, tickers, minimum urgency (1-10) and recency. Use compact=true "
            "for a token-optimized format (~85% fewer tokens) when scanning many events."
        ),
        inputSchema={
            "type": "object",
            "properties": {
                "q": {"type": "string", "description": "substring match on headline/body"},
                "categories": {"type": "array", "items": {"type": "string"},
                    "enum": ["Macro", "Equities", "Crypto", "AI/Tech", "Geopolitics",
                             "Cybersecurity", "Science", "Regulatory", "Breaking"]},
                "tickers": {"type": "array", "items": {"type": "string"}, "description": "e.g. NVDA, BTC"},
                "min_urgency": {"type": "integer", "minimum": 1, "maximum": 10},
                "max_age_sec": {"type": "integer", "description": "only events fresher than this"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 25},
                "compact": {"type": "boolean", "default": True},
            },
        },
    ),
    Tool(
        name="get_event",
        description="Fetch a single full NewsEvent document by event_id (complete clean_markdown body).",
        inputSchema={"type": "object", "required": ["event_id"],
            "properties": {"event_id": {"type": "string"}}},
    ),
    Tool(
        name="search_events",
        description="Deep search across the 24h event store with pagination. Returns compact events + next cursor.",
        inputSchema={"type": "object",
            "properties": {
                "q": {"type": "string"},
                "since": {"type": "string", "description": "ISO-8601 datetime"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
                "cursor": {"type": "string", "description": "from previous page"},
            }},
    ),
    Tool(
        name="get_story_clusters",
        description="List active story clusters (deduplicated stories). velocity_15m shows which stories are accelerating.",
        inputSchema={"type": "object", "properties": {"limit": {"type": "integer", "default": 25, "maximum": 100}}},
    ),
    Tool(
        name="get_cluster",
        description="Get one story cluster metadata (breaking source, total events, sources covering it).",
        inputSchema={"type": "object", "required": ["cluster_id"],
            "properties": {"cluster_id": {"type": "string"}}},
    ),
    Tool(
        name="get_cluster_events",
        description="All events inside a story cluster, newest first (how a story developed).",
        inputSchema={"type": "object", "required": ["cluster_id"],
            "properties": {"cluster_id": {"type": "string"}, "limit": {"type": "integer", "default": 30}}},
    ),
    Tool(
        name="get_breaking",
        description="Flash events only: urgency >= 8 and fresher than max_age_sec. The 'tape' for time-critical agents.",
        inputSchema={"type": "object",
            "properties": {
                "min_urgency": {"type": "integer", "default": 8, "minimum": 5, "maximum": 10},
                "max_age_sec": {"type": "integer", "default": 3600},
                "limit": {"type": "integer", "default": 20, "maximum": 100},
            }},
    ),
    Tool(
        name="get_top_tickers",
        description="Most-mentioned tickers in the last N hours with event counts.",
        inputSchema={"type": "object",
            "properties": {"hours": {"type": "integer", "default": 1, "maximum": 24},
                          "limit": {"type": "integer", "default": 10, "maximum": 25}}},
    ),
    Tool(
        name="watch",
        description=(
            "Subscribe to live events matching a filter. Returns a subscription_id; "
            "then poll_watch() to drain queued events. Multiple watches are allowed."
        ),
        inputSchema={"type": "object",
            "properties": {
                "q": {"type": "string"},
                "categories": {"type": "array", "items": {"type": "string"}},
                "tickers": {"type": "array", "items": {"type": "string"}},
                "min_urgency": {"type": "integer"},
            }},
    ),
    Tool(
        name="unwatch",
        description="Cancel a watch subscription.",
        inputSchema={"type": "object", "required": ["subscription_id"],
            "properties": {"subscription_id": {"type": "string"}}},
    ),
    Tool(
        name="poll_watch",
        description="Drain a watch queue: returns all events buffered since the last poll (then empties it).",
        inputSchema={"type": "object", "required": ["subscription_id"],
            "properties": {"subscription_id": {"type": "string"}, "limit": {"type": "integer", "default": 100}}},
    ),
    Tool(
        name="list_sources",
        description="List the ingestion registry: every source, its health, poll interval and enabled state.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="add_source",
        description=(
            "Register a new ingestion source at runtime (takes effect immediately). "
            "types: rss|atom|json|hn_frontpage|hn_show|arxiv|gdelt|sec_edgar|polymarket|"
            "kalshi|github_releases|github_trending|nvd|reddit|cloud_status|html|bluesky|telegram."
        ),
        inputSchema={"type": "object", "required": ["id", "name", "type", "url"],
            "properties": {
                "id": {"type": "string"}, "name": {"type": "string"},
                "type": {"type": "string"},
                "url": {"type": "string"},
                "category": {"type": "string"},
                "credibility_tier": {"type": "integer", "minimum": 1, "maximum": 3},
                "poll_interval_sec": {"type": "integer"},
                "fulltext": {"type": "boolean"},
            }},
    ),
    Tool(
        name="pause_source",
        description="Pause ingestion of a source (e.g. too noisy).",
        inputSchema={"type": "object", "required": ["source_id"],
            "properties": {"source_id": {"type": "string"}}},
    ),
    Tool(
        name="resume_source",
        description="Resume a paused source.",
        inputSchema={"type": "object", "required": ["source_id"],
            "properties": {"source_id": {"type": "string"}}},
    ),
    Tool(
        name="remove_source",
        description="Remove a source from the registry permanently.",
        inputSchema={"type": "object", "required": ["source_id"],
            "properties": {"source_id": {"type": "string"}}},
    ),
    Tool(
        name="get_stats",
        description="Hub stats: events/min, ingest lag percentiles, online sources, top tickers.",
        inputSchema={"type": "object", "properties": {}},
    ),
    Tool(
        name="get_dlq",
        description="Dead-letter queue: fetch failures per source (for self-diagnosing broken scrapers).",
        inputSchema={"type": "object", "properties": {"limit": {"type": "integer", "default": 20}}},
    ),
]


def _j(data: Any) -> str:
    return orjson.dumps(data, option=orjson.OPT_INDENT_2).decode()


# --------------------------------------------------------------------------- #
# server
# --------------------------------------------------------------------------- #
server = Server(f"omnipulse-mcp")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return TOOLS


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    try:
        if name == "omnipulse_hello":
            t0 = time.monotonic()
            stats = await api_get("/api/health")
            return [TextContent(type="text", text=_j({
                "service": "OmniPulse MCP",
                "api": API,
                "roundtrip_ms": round((time.monotonic() - t0) * 1000, 1),
                "backend": stats,
                "tools": [t.name for t in TOOLS],
                "resources": ["omnipulse://events/latest", "omnipulse://clusters/active", "omnipulse://stats"],
            }))]

        if name == "get_latest_events":
            params: dict[str, Any] = {"compact": "true"}
            for k in ("q", "min_urgency", "max_age_sec", "limit"):
                if arguments.get(k) is not None:
                    params[k] = arguments[k]
            for k in ("categories", "tickers"):
                if arguments.get(k):
                    params[k] = ",".join(arguments[k])
            return [TextContent(type="text", text=_j(await api_get("/api/events", params)))]

        if name == "get_event":
            ev = await api_get(f"/api/events/{arguments['event_id']}")
            return [TextContent(type="text", text=_j(ev))]

        if name == "search_events":
            params = {"compact": "true", "limit": arguments.get("limit", 50)}
            if arguments.get("q"):
                params["q"] = arguments["q"]
            if arguments.get("since"):
                params["since"] = arguments["since"]
            if arguments.get("cursor"):
                params["cursor"] = arguments["cursor"]
            return [TextContent(type="text", text=_j(await api_get("/api/events", params)))]

        if name == "get_story_clusters":
            return [TextContent(type="text", text=_j(await api_get("/api/clusters", {"limit": arguments.get("limit", 25)})))]

        if name == "get_cluster":
            return [TextContent(type="text", text=_j(await api_get(f"/api/clusters/{arguments['cluster_id']}")))]

        if name == "get_cluster_events":
            data = await api_get(f"/api/clusters/{arguments['cluster_id']}/events",
                                 {"limit": arguments.get("limit", 30)})
            return [TextContent(type="text", text=_j(data))]

        if name == "get_breaking":
            params = {
                "compact": "true",
                "min_urgency": arguments.get("min_urgency", 8),
                "max_age_sec": arguments.get("max_age_sec", 3600),
                "limit": arguments.get("limit", 20),
            }
            return [TextContent(type="text", text=_j(await api_get("/api/events", params)))]

        if name == "get_top_tickers":
            stats = await api_get("/api/stats")
            return [TextContent(type="text", text=_j(stats.get("top_tickers", [])))]

        if name == "watch":
            params: dict[str, Any] = {}
            if arguments.get("q"):
                params["q"] = arguments["q"]
            if arguments.get("categories"):
                params["categories"] = ",".join(arguments["categories"])
            if arguments.get("tickers"):
                params["tickers"] = ",".join(arguments["tickers"])
            if arguments.get("min_urgency"):
                params["min_urgency"] = arguments["min_urgency"]
            wid = f"watch_{uuid.uuid4().hex[:12]}"
            w = Watch(wid, params)
            WATCHES[wid] = w
            asyncio.create_task(_sse_watcher(w))
            return [TextContent(type="text", text=_j({"subscription_id": wid, "filter": params}))]

        if name == "unwatch":
            WATCHES.pop(arguments["subscription_id"], None)
            return [TextContent(type="text", text=_j({"unsubscribed": arguments["subscription_id"]}))]

        if name == "poll_watch":
            w = WATCHES.get(arguments["subscription_id"])
            if w is None:
                raise ValueError("unknown subscription_id")
            out = []
            while not w.queue.empty() and len(out) < arguments.get("limit", 100):
                out.append(w.queue.get_nowait())
            return [TextContent(type="text", text=_j(out))]

        if name == "list_sources":
            return [TextContent(type="text", text=_j(await api_get("/api/sources")))]

        if name == "add_source":
            cfg = {k: arguments[k] for k in ("id", "name", "type", "url", "category",
                                             "credibility_tier", "poll_interval_sec", "fulltext")
                   if arguments.get(k) is not None}
            created = await api_post("/api/sources", cfg)
            return [TextContent(type="text", text=_j(created))]

        if name == "pause_source":
            return [TextContent(type="text", text=_j(await api_post(f"/api/sources/{arguments['source_id']}/pause")))]

        if name == "resume_source":
            return [TextContent(type="text", text=_j(await api_post(f"/api/sources/{arguments['source_id']}/resume")))]

        if name == "remove_source":
            return [TextContent(type="text", text=_j(await api_delete(f"/api/sources/{arguments['source_id']}")))]

        if name == "get_stats":
            return [TextContent(type="text", text=_j(await api_get("/api/stats")))]

        if name == "get_dlq":
            return [TextContent(type="text", text=_j(await api_get("/api/dlq", {"limit": arguments.get("limit", 20)})))]

        raise ValueError(f"unknown tool {name}")
    except httpx.HTTPStatusError as e:
        return [TextContent(type="text", text=_j({"error": f"API {e.response.status_code}: {e.response.text[:300]}"}))]
    except Exception as e:
        return [TextContent(type="text", text=_j({"error": f"{type(e).__name__}: {e}"}))]


@server.list_resources()
async def list_resources() -> list[Resource]:
    return [
        Resource(
            uri="omnipulse://events/latest",
            name="Latest events",
            description="The 50 newest events (compact form), refreshed on read",
            mimeType="application/json",
        ),
        Resource(
            uri="omnipulse://clusters/active",
            name="Active story clusters",
            description="Currently developing story clusters",
            mimeType="application/json",
        ),
        Resource(
            uri="omnipulse://stats",
            name="Hub stats",
            description="Live ingestion statistics",
            mimeType="application/json",
        ),
    ]


@server.read_resource()
async def read_resource(uri: str) -> list[TextContent]:
    if uri == "omnipulse://events/latest":
        data = await api_get("/api/events", {"limit": 50, "compact": "true"})
    elif uri == "omnipulse://clusters/active":
        data = await api_get("/api/clusters", {"limit": 50})
    elif uri == "omnipulse://stats":
        data = await api_get("/api/stats")
    else:
        raise ValueError(f"unknown resource {uri}")
    return [TextContent(type="text", text=_j(data))]


async def amain() -> None:
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> None:
    asyncio.run(amain())


if __name__ == "__main__":
    main()
