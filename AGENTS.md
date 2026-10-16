# Attaching AI agents to OmniPulse

## 1. MCP (preferred — richest interface)

Any MCP client (Claude Desktop, Claude Code, Cursor, custom agents):

```json
{
  "mcpServers": {
    "omnipulse": {
      "command": "python",
      "args": ["-m", "app.mcp_server"],
      "cwd": "/path/to/omnipulse/backend",
      "env": { "OMNIPULSE_API_URL": "http://localhost:8000" }
    }
  }
}
```

Or via docker: `docker compose --profile agents up -d && docker attach omnipulse-mcp`

### Tools

| Tool | Purpose |
|---|---|
| `omnipulse_hello` | handshake, latency probe, capability list |
| `get_latest_events` | newest events; filters + compact mode |
| `get_event` | full document incl. clean_markdown body |
| `search_events` | 24h deep search, cursor pagination |
| `get_story_clusters` | active stories ranked by velocity |
| `get_cluster` / `get_cluster_events` | story metadata / how it developed |
| `get_breaking` | urgency ≥ N, fresher than max_age_sec |
| `get_top_tickers` | most-mentioned tickers |
| `watch` / `poll_watch` / `unwatch` | live push subscriptions, pull model |
| `list_sources` / `add_source` / `pause_source` / `resume_source` / `remove_source` | registry control |
| `get_stats` / `get_dlq` | hub health & broken-source diagnostics |

### Live-watch pattern

```
→ watch({"categories": ["Breaking", "Geopolitics"], "min_urgency": 7})
← {"subscription_id": "watch_a1b2c3...", ...}
... during each reasoning cycle:
→ poll_watch({"subscription_id": "watch_a1b2c3..."})
← [event, event, event]
→ unwatch({"subscription_id": "watch_a1b2c3..."})
```

## 2. REST

Base: `http://localhost:8000`. Spec: `/openapi.json` (OpenAPI 3.1, hand-stable).

The one URL that matters: `GET /api/events?compact=true&limit=50`

Filters (all optional, combinable): `q`, `dsl`, `categories`, `tickers`,
`sources`, `entities`, `min_urgency`, `since` (ISO-8601 or epoch),
`max_age_sec`, `cursor` (from `X-Next-Cursor`), `limit` ≤ 1000, `compact`,
`format=headline` (one-line wire output).

**Query DSL** (`dsl`, merged from the v1 engine — same grammar everywhere):

```
u>=7 cat:crypto,macro $NVDA tk:BTC src:reuters scat:wire loc:US tier<=2
sent>0.3 nov>=0.5 vs:confirmed ent:openai cluster:cls_... "rate cut" -opinion
```

Repeated values OR, different keys AND, bare words must appear in the
headline, `-word` excludes.

**Outbound webhooks** (merged from v1): register a receiver and get
HMAC-signed POSTs of matching events:

```
curl -X POST localhost:8000/api/webhooks \
  -d '{"url":"https://your.service/news","secret":"s3cret","dsl":"u>=8 cat:macro"}'
```

Envelope `{"type":"EVENT_CREATED","data":{…}}`, headers `X-OmniPulse-Event`,
`X-OmniPulse-Webhook`, `X-OmniPulse-Signature: sha256=<hmac-sha256 body>`.

`GET /api/events.jsonl?compact=true&limit=500` — NDJSON batch snapshot.

## 2b. v1 profile (merged original engine)

`OMNIPULSE_PROFILE=v1` serves the Redis-Stream architecture at `/v1/*`
(same NewsEvent schema): REST + WS/SSE streams with `?filter=` DSL +
`format=full|compact|headline`, source CRUD, `wait_for_events` MCP tool via
streamable HTTP at `/mcp` (`claude mcp add --transport http omnipulse
http://localhost:8000/mcp`). See `omnipulse/backend/omnipulse/` for its
module docs; config via `OMNI_*` env vars.

## 3. WebSocket

```
ws://host/ws?tickers=NVDA,BTC&min_urgency=7&compact=true
ws://host/ws?dsl=u%3E%3D7%20cat%3Acrypto
```

Server filters; you only receive matches. Re-filter live:

```json
{"type": "resubscribe", "filters": {"q": "fed", "categories": ["Macro"], "min_urgency": 6, "dsl": "u>=6"}}
```

## 4. SSE

```
curl -N "http://localhost:8000/stream/events?categories=Breaking&compact=true"
```

## Auth

Set `OMNIPULSE_API_KEY` on the server → send `X-API-Key` header (or `?api_key=`
on the WS). Unset = open (local mode).

## Compact legend

```
i event_id      c cluster_id    p published_at   s source.id
n source.name   t flash_headline  u urgency     v sentiment
w novelty       z verification  k categories    x tickers    l article url
```
