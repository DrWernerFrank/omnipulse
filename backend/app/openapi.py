"""Hand-written OpenAPI 3.1 spec — the stable agent contract.

Kept static (not auto-generated) so AI agents can rely on it across releases.
Describes every /api endpoint, filter parameters, the canonical NewsEvent
schema, and the streaming channels.
"""
from __future__ import annotations

import orjson

from . import __version__
from .schemas import CATEGORIES, NewsEvent

_EVENT_SCHEMA = orjson.loads(orjson.dumps(NewsEvent.model_json_schema()))

# common filter params reused across endpoints
_FILTER_PARAMS = [
    {"name": "q", "in": "query", "schema": {"type": "string"},
     "description": "Case-insensitive substring match on title/flash headline/body"},
    {"name": "categories", "in": "query", "schema": {"type": "string"},
     "description": f"Comma-separated. One of: {', '.join(CATEGORIES)}"},
    {"name": "tickers", "in": "query", "schema": {"type": "string"},
     "description": "Comma-separated tickers, e.g. NVDA,BTC"},
    {"name": "sources", "in": "query", "schema": {"type": "string"},
     "description": "Comma-separated source ids, e.g. reuters_wire,sec_edgar"},
    {"name": "min_urgency", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": 10},
     "description": "Only events with urgency >= this value"},
    {"name": "since", "in": "query", "schema": {"type": "string"},
     "description": "ISO-8601 datetime or unix (milli)seconds"},
    {"name": "max_age_sec", "in": "query", "schema": {"type": "integer"},
     "description": "Drop events older than this many seconds"},
    {"name": "compact", "in": "query", "schema": {"type": "boolean"},
     "description": "Token-optimized compact wire format (see /api/schema legend)"},
]

OPENAPI_SPEC = {
    "openapi": "3.1.0",
    "info": {
        "title": "OmniPulse — Real-Time News & Agent Intelligence API",
        "version": __version__,
        "description": (
            "Ultra-low-latency global event firehose for humans (terminal UI) and "
            "autonomous AI agents (REST, WebSocket, SSE, MCP). All events conform to "
            "the canonical NewsEvent schema. Filter server-side; stream what matches. "
            "POST/PATCH/DELETE /api/sources manages the live ingestion registry at runtime."
        ),
    },
    "servers": [{"url": "/"}],
    "tags": [
        {"name": "events"}, {"name": "clusters"}, {"name": "sources"},
        {"name": "streams"}, {"name": "ops"},
    ],
    "paths": {
        "/api/events": {
            "get": {
                "tags": ["events"], "summary": "Query events (keyset paginated)",
                "parameters": _FILTER_PARAMS + [
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100, "maximum": 1000}},
                    {"name": "cursor", "in": "query", "schema": {"type": "string"},
                     "description": "Pass X-Next-Cursor from the previous page"},
                ],
                "responses": {
                    "200": {
                        "description": "Array of NewsEvent (compact if compact=true)",
                        "headers": {
                            "X-Next-Cursor": {"schema": {"type": "string"}},
                            "X-Result-Count": {"schema": {"type": "integer"}},
                        },
                        "content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/NewsEvent"}}}},
                    },
                    "401": {"description": "API key required (if configured)"},
                },
            },
        },
        "/api/events.jsonl": {
            "get": {
                "tags": ["events"], "summary": "Snapshot as newline-delimited JSON (batch-friendly)",
                "parameters": _FILTER_PARAMS,
                "responses": {"200": {"description": "NDJSON; one NewsEvent per line",
                    "content": {"application/x-ndjson": {}}}},
            },
        },
        "/api/events/{event_id}": {
            "get": {
                "tags": ["events"], "summary": "Fetch a single event",
                "parameters": [
                    {"name": "event_id", "in": "path", "required": True, "schema": {"type": "string"}},
                    {"name": "compact", "in": "query", "schema": {"type": "boolean"}},
                ],
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/NewsEvent"}}}},
                              "404": {"description": "not found"}},
            },
        },
        "/api/clusters": {
            "get": {
                "tags": ["clusters"], "summary": "Active story clusters (24h window)",
                "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer", "default": 100}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/StoryCluster"}}}}}},
            },
        },
        "/api/clusters/{cluster_id}": {
            "get": {
                "tags": ["clusters"], "summary": "One story cluster",
                "parameters": [{"name": "cluster_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/StoryCluster"}}}},
                              "404": {"description": "not found"}},
            },
        },
        "/api/clusters/{cluster_id}/events": {
            "get": {
                "tags": ["clusters"], "summary": "All events in a story cluster",
                "parameters": [
                    {"name": "cluster_id", "in": "path", "required": True, "schema": {"type": "string"}},
                    {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}},
                ],
                "responses": {"200": {"content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/NewsEvent"}}}}}},
            },
        },
        "/api/sources": {
            "get": {
                "tags": ["sources"], "summary": "Registry state (health, intervals)",
                "responses": {"200": {"content": {"application/json": {"schema": {"type": "array", "items": {"$ref": "#/components/schemas/SourceState"}}}}}},
            },
            "post": {
                "tags": ["sources"], "summary": "Register a new source at runtime",
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/SourceConfig"}}}},
                "responses": {"201": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/SourceConfig"}}}},
                              "409": {"description": "id already exists"}},
            },
        },
        "/api/sources/{source_id}": {
            "get": {
                "tags": ["sources"], "summary": "One source config",
                "parameters": [{"name": "source_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/SourceConfig"}}}}, "404": {"description": "not found"}},
            },
            "patch": {
                "tags": ["sources"], "summary": "Update a source (interval, url, enabled...)",
                "parameters": [{"name": "source_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "requestBody": {"required": True, "content": {"application/json": {"schema": {"$ref": "#/components/schemas/SourceConfig"}}}},
                "responses": {"200": {"description": "updated"}, "404": {"description": "not found"}},
            },
            "delete": {
                "tags": ["sources"], "summary": "Remove a source",
                "parameters": [{"name": "source_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "deleted"}, "404": {"description": "not found"}},
            },
        },
        "/api/sources/{source_id}/pause": {
            "post": {"tags": ["sources"], "summary": "Pause ingestion for a source",
                "parameters": [{"name": "source_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "paused"}}},
        },
        "/api/sources/{source_id}/resume": {
            "post": {"tags": ["sources"], "summary": "Resume ingestion for a source",
                "parameters": [{"name": "source_id", "in": "path", "required": True, "schema": {"type": "string"}}],
                "responses": {"200": {"description": "resumed"}}},
        },
        "/api/stats": {
            "get": {"tags": ["ops"], "summary": "Live terminal stats (events/min, lag percentiles, sources)",
                "responses": {"200": {"content": {"application/json": {"schema": {"$ref": "#/components/schemas/Stats"}}}}}},
        },
        "/api/dlq": {
            "get": {"tags": ["ops"], "summary": "Dead-letter queue (broken fetches)",
                "parameters": [{"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50}}],
                "responses": {"200": {"description": "recent DLQ entries"}}},
        },
        "/api/health": {
            "get": {"tags": ["ops"], "summary": "Liveness probe",
                "responses": {"200": {"description": "ok"}}},
        },
        "/api/schema": {
            "get": {"tags": ["ops"], "summary": "Canonical NewsEvent JSON schema + compact-key legend",
                "responses": {"200": {"description": "JSON Schema (draft 2020-12)"}}},
        },
        "/ws": {
            "get": {"tags": ["streams"], "summary": "WebSocket firehose (server-filtered)",
                "description": (
                    "Connect with query params (q, categories, tickers, sources, min_urgency, compact) "
                    "or ?subscribe=<urlencoded FilterSpec JSON>. Frames: "
                    '{"t":"event"|"cluster"|"stats"|"source_state"|"ping","d":{...}}. '
                    "Events are only delivered when they match your filter (server-side)."
                ),
                "responses": {"101": {"description": "switching protocols"}}},
        },
        "/stream/events": {
            "get": {"tags": ["streams"], "summary": "SSE firehose (same filters as /api/events)",
                "responses": {"200": {"description": "text/event-stream; named events: event",
                    "content": {"text/event-stream": {}}}}},
        },
        "/stream/stats": {
            "get": {"tags": ["streams"], "summary": "SSE stats channel (2s cadence)",
                "responses": {"200": {"description": "text/event-stream", "content": {"text/event-stream": {}}}}},
        },
        "/stream/sources": {
            "get": {"tags": ["streams"], "summary": "SSE source-health channel",
                "responses": {"200": {"description": "text/event-stream", "content": {"text/event-stream": {}}}}},
        },
    },
    "components": {
        "securitySchemes": {
            "apiKey": {
                "type": "apiKey",
                "in": "header",
                "name": "X-API-Key",
                "description": "Only required when OMNIPULSE_API_KEY is set on the server.",
            }
        },
        "schemas": {
            "NewsEvent": _EVENT_SCHEMA,
            "StoryCluster": {
                "type": "object",
                "properties": {
                    "cluster_id": {"type": "string"},
                    "title": {"type": "string"},
                    "event_count": {"type": "integer"},
                    "sources": {"type": "array", "items": {"type": "string"}},
                    "categories": {"type": "array", "items": {"type": "string"}},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "breaking_source": {"type": ["string", "null"]},
                    "first_seen": {"type": "string", "format": "date-time"},
                    "latest_at": {"type": "string", "format": "date-time"},
                    "velocity_15m": {"type": "integer", "description": "events in the last 15 minutes"},
                },
            },
            "SourceConfig": {
                "type": "object",
                "required": ["id", "name", "url"],
                "properties": {
                    "id": {"type": "string", "description": "unique id, e.g. my_source"},
                    "name": {"type": "string"},
                    "type": {"type": "string", "enum": ["rss", "atom", "json", "hn_frontpage", "hn_show",
                           "arxiv", "gdelt", "sec_edgar", "polymarket", "kalshi", "github_releases",
                           "github_trending", "nvd", "reddit", "cloud_status", "html", "bluesky", "telegram"]},
                    "url": {"type": "string"},
                    "category": {"type": "string"},
                    "credibility_tier": {"type": "integer", "minimum": 1, "maximum": 3},
                    "poll_interval_sec": {"type": "integer", "default": 60},
                    "enabled": {"type": "boolean", "default": True},
                    "fulltext": {"type": "boolean", "description": "fetch article page for clean markdown"},
                    "params": {"type": "object", "description": "connector-specific options"},
                },
            },
            "SourceState": {
                "type": "object",
                "properties": {
                    "source_id": {"type": "string"},
                    "name": {"type": "string"},
                    "health": {"type": "string", "enum": ["ok", "stale", "degraded", "dead", "paused", "waiting_key"]},
                    "enabled": {"type": "boolean"},
                    "last_success": {"type": ["string", "null"], "format": "date-time"},
                    "interval_sec": {"type": ["integer", "null"]},
                },
            },
            "Stats": {
                "type": "object",
                "properties": {
                    "ts": {"type": "string", "format": "date-time"},
                    "events_total": {"type": "integer"},
                    "events_1h": {"type": "integer"},
                    "events_15m": {"type": "integer"},
                    "events_per_min": {"type": "number"},
                    "lag_p50_ms": {"type": "number"},
                    "lag_p95_ms": {"type": "number"},
                    "clusters_24h": {"type": "integer"},
                    "sources_online": {"type": "integer"},
                    "sources_total": {"type": "integer"},
                    "sources_paused": {"type": "integer"},
                    "dlq_size": {"type": "integer"},
                    "bus_mode": {"type": "string"},
                    "store_mode": {"type": "string"},
                    "top_tickers": {"type": "array", "items": {"type": "array", "items": {}}},
                },
            },
        },
    },
    "security": [{"apiKey": []}],
}
