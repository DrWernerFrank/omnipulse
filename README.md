# OmniPulse ⌁

**Ultra-low-latency real-time news terminal & AI-agent intelligence hub.**

Two audiences, one engine:

1. **Human operators** — a keyboard-first, high-density, Bloomberg-style streaming
   web terminal (virtualized 10,000-row tape, story clusters, source registry,
   command palette).
2. **Autonomous AI agents** — a native **MCP server**, a stable **OpenAPI 3.1**
   REST surface, and token-optimized **WebSocket / SSE firehoses** with
   server-side filtering. Zero HTML overhead, ~85% token reduction in compact mode.

**Zero-key boot:** `docker compose up` starts streaming live global news
immediately. 71 pre-configured sources — wires, SEC EDGAR, central banks,
prediction markets, ArXiv, Hacker News, CVE feeds, cloud status pages, Reddit,
Bluesky firehose — all no-API-key. Optional OpenAI / Anthropic / Ollama keys add
LLM headline refinement on top of the always-on local heuristic NLP pipeline.

```
┌──────────────────────────────────────────────────────────────────────┐
│  sources (71)          ingestion engine          consumers           │
│                                                                        │
│  RSS/Atom ─┐                                                      ┌─▶ WS /ws (filtered)      │
│  JSON APIs ─┼─▶ HttpPool ─▶ connectors ─▶ pipeline ─▶ bus ─▶ SSE /stream/*   │
│  WebScrape ─┘   (rate-limit,    (SimHash dedup,  (Redis        ├─▶ REST /api/*            │
│  Bluesky WS ─▶  conditional,     embed + cluster,  Streams +     │    (+ keyset pagination) │
│  Telegram  ─▶  backoff+jitter)  enrich, novelty)   pub/sub)      ├─▶ MCP server (stdio)     │
│                                                             └─▶ Terminal UI (React)     │
│                                             │                           │
│                                             └─▶ PostgreSQL + TimescaleDB + pgvector       │
│                                                 (hypertable events · vector clusters ·     │
│                                                  RAG-ready)  ── fallback: in-process store  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Quick start

### Docker (recommended)

```bash
cp .env.example .env
docker compose up -d --build
```

| Service | URL | Notes |
|---|---|---|
| Terminal UI | http://localhost:3000 | keyboard-first UI |
| REST API | http://localhost:8000/api/stats | agent surface |
| OpenAPI 3.1 | http://localhost:8000/openapi.json | stable agent contract |
| WS firehose | ws://localhost:8000/ws?categories=Breaking | server-side filtered |
| SSE firehose | http://localhost:8000/stream/events | `text/event-stream` |

The first events appear within seconds (Tier-A wires poll every 15–30s).
TimescaleDB + pgvector + Redis boot alongside; if any of them is unreachable the
backend degrades gracefully to in-process equivalents and keeps streaming.

### Bare metal

```bash
cd backend
python -m venv .venv && . .venv/Scripts/activate   # or source .venv/bin/activate
pip install -r requirements.txt
# no DATABASE_URL / REDIS_URL => in-process fallbacks, still fully functional
set OMNIPULSE_SOURCES_FILE=../config/default_sources.json
uvicorn app.main:app --port 8000

cd ../frontend && npm install && npm run dev   # terminal at :5173 (proxied)
```

### Agent stack (MCP)

```bash
docker compose --profile agents up -d
docker attach omnipulse-mcp        # stdio MCP server
```

Or run natively: `OMNIPULSE_API_URL=http://localhost:8000 python -m app.mcp_server`

Attach it to any MCP client (Claude Desktop, Claude Code, Cursor, …):

```json
{
  "mcpServers": {
    "omnipulse": {
      "command": "python",
      "args": ["-m", "app.mcp_server"],
      "env": { "OMNIPULSE_API_URL": "http://localhost:8000" }
    }
  }
}
```

---

## The canonical NewsEvent schema

Every surface — DB rows, REST, WS, SSE, MCP, UI — speaks this exact document:

```json
{
  "event_id": "evt_01J9Z...",
  "cluster_id": "cls_88A2B...",
  "published_at": "2026-10-16T18:05:12.410Z",
  "ingested_at": "2026-10-16T18:05:13.102Z",
  "source": {
    "id": "reuters_wire", "name": "Reuters", "category": "wire",
    "credibility_tier": 1, "url": "https://..."
  },
  "content": {
    "raw_title": "...", "flash_headline": "...",
    "executive_summary": ["...", "...", "..."], "clean_markdown": "..."
  },
  "enrichment": {
    "categories": ["AI/Tech", "Equities"],
    "tickers": ["NVDA", "MSFT"],
    "entities": [{"name": "OpenAI", "type": "ORG"}],
    "locations": ["US", "EU"],
    "sentiment": 0.74, "urgency": 9, "novelty": 0.95,
    "verification_status": "CONFIRMED"
  }
}
```

**Compact wire form** (`?compact=true`, ~85% fewer tokens — the legend is at
`/api/schema` and in the `X-Compact-Legend` header):

```json
{"i":"evt_..","c":"cls_..","p":"2026-10-16T18:05:12Z","s":"reuters_wire",
 "n":"Reuters","t":"NVIDIA beats on record data-center revenue","u":9,
 "v":0.74,"w":0.95,"z":"CONFIRMED","k":["AI/Tech"],"x":["NVDA"],"l":"https://..."}
```

---

## Agent quick reference

### REST (filter server-side, keyset pagination)

```bash
curl "localhost:8000/api/events?compact=true&limit=50"
curl "localhost:8000/api/events?categories=Breaking&min_urgency=8&max_age_sec=600"
curl "localhost:8000/api/events?q=nvidia&tickers=NVDA,BTC&since=2026-10-16T00:00:00Z"
curl "localhost:8000/api/events.jsonl?compact=true&limit=500"   # NDJSON batch
curl "localhost:8000/api/clusters"                             # story clusters
curl "localhost:8000/api/clusters/{id}/events"                  # story timeline
curl "localhost:8000/api/sources"                              # registry health
curl -X POST localhost:8000/api/sources -H 'content-type: application/json' \
  -d '{"id":"my_feed","name":"My Feed","type":"rss","url":"https://x.com/rss"}'
curl -X POST localhost:8000/api/sources/my_feed/pause
curl "localhost:8000/api/stats"        # events/min, lag p50/p95, dedup counters
curl "localhost:8000/api/dlq"          # broken-source inspector
```

### WebSocket (live, server-filtered)

```
ws://localhost:8000/ws?categories=Breaking&min_urgency=8&compact=true
```

Frames: `{"t":"event"|"cluster"|"stats"|"source_state"|"ping","d":{...}}`.
Resubscribe live: send `{"type":"resubscribe","filters":{"q":"fed","min_urgency":7}}`.
Full filter spec via `?subscribe=<urlencoded FilterSpec JSON>`.

### SSE

```
GET /stream/events?tickers=NVDA&compact=true   # named event: event
GET /stream/stats                              # 2s cadence chrome
GET /stream/sources                            # source health changes
```

### MCP tools (19)

`omnipulse_hello`, `get_latest_events`, `get_event`, `search_events`,
`get_story_clusters`, `get_cluster`, `get_cluster_events`, `get_breaking`,
`get_top_tickers`, `watch`, `poll_watch`, `unwatch`, `list_sources`,
`add_source`, `pause_source`, `resume_source`, `remove_source`, `get_stats`,
`get_dlq` — plus resources `omnipulse://events/latest`,
`omnipulse://clusters/active`, `omnipulse://stats`.

Agent pattern for live monitoring:
```
watch({"categories":["Breaking"],"min_urgency":8})  → subscription_id
poll_watch(subscription_id)                          # each reasoning cycle
unwatch(subscription_id)
```

---

## Terminal keyboard map

| Key | Action |
|---|---|
| `j` `k` / `↑` `↓` | move tape cursor |
| `Enter` | open event dossier |
| `Space` | freeze / resume tape |
| `/` | focus search |
| `1`–`9` | toggle category filters |
| `u` | cycle urgency ladder (all → ≥5 → ≥7 → ≥8 → ≥9 → 10) |
| `f` / `s` | cycle left pane (filters / sources / clusters) |
| `d` | toggle detail drawer |
| `Ctrl+K` | command palette |
| `g` / `G` | jump to top / bottom |
| `Esc` | close drawer / palette |

Left pane tabs: **filters** (search, categories, urgency slider, tickers),
**sources** (live registry health with pause/resume), **clusters** (stories by
velocity — click for the full timeline of how a story developed). Right pane:
full event dossier (flash summary, 3-bullet brief, entities, tickers, locations,
cluster link, clean body, origin link).

---

## Architecture

```
omnipulse/
├── docker-compose.yml            # postgres(timescale+pgvector) · redis · backend · frontend · mcp
├── .env.example                  # every knob, all optional
├── config/
│   └── default_sources.json      # 71 verified zero-key sources (tiers A-F)
├── backend/app/
│   ├── main.py                   # FastAPI assembly + lifespan
│   ├── api.py                    # REST + WS + SSE surface
│   ├── openapi.py                # hand-written OpenAPI 3.1 (agent-stable)
│   ├── mcp_server.py             # stdio MCP server (19 tools, 3 resources)
│   ├── engine.py                 # adaptive scheduler, health, DLQ, streams
│   ├── pipeline.py               # dedup → cluster → enrich → persist → fanout
│   ├── enrich.py                 # heuristic NLP + optional LLM refinement
│   ├── connectors.py             # 16 connector types
│   ├── streams.py                # Bluesky Jetstream + Telegram listeners
│   ├── http.py                   # token buckets, conditional GET, backoff
│   ├── bus.py                    # Redis Streams+pub/sub (in-process fallback)
│   ├── db.py                     # PgStore / MemoryStore
│   ├── embeddings.py             # 256-dim hashed embeddings (~µs, zero deps)
│   ├── text.py                   # SimHash, MinHash, boilerplate→markdown
│   ├── gazetteer.py              # orgs/people/tickers/geo/sentiment data
│   ├── schemas.py                # canonical NewsEvent + FilterSpec
│   ├── ids.py                    # monotonic ULIDs
│   └── config.py                 # OMNIPULSE_* settings
├── backend/tests/                # 41 tests: ids, text, embeddings, schemas,
│   │                             #   enrichment heuristics, pipeline e2e
└── frontend/src/
    ├── App.tsx                   # layout + global hotkeys
    ├── store.ts                  # Zustand store (10k-row ring buffer)
    ├── api.ts                    # WS client w/ auto-reconnect + resubscribe
    └── components/               # TopBar, EventTape (TanStack Virtual),
                                  #   LeftPanel, DetailDrawer, CommandPalette, StatusBar
```

### Ingestion & resilience

- **Adaptive polling**: intervals scale between `min_poll_sec` and `max_poll_sec`
  by observed item velocity — busy wires speed up, quiet blogs drift down.
- **Per-host token buckets** (`per_host_rate` sustained, `per_host_burst` cap) +
  **exponential backoff with full jitter** on 429/5xx.
- **Conditional GETs**: ETag / If-Modified-Since cached per URL; 304s cost nothing.
- **DLQ + health**: every terminal fetch failure lands in the dead-letter queue
  (`/api/dlq`); 8 consecutive failures auto-pause a source, probed again every
  15 minutes; recovery broadcasts to the UI.
- **Keyed sources stay parked** until their env var exists (`waiting_key` state).

### Enrichment (local-first, ~µs)

1. **SimHash64 + canonical-URL dedup** — exact syndication duplicates dropped.
2. **256-dim hashed embeddings + rolling 24h centroids** — cosine > 0.88 joins a
   StoryCluster (tracks the original breaker vs follow-ups, emits CLUSTER_UPDATED).
3. **Taxonomy** (9 categories), **NER** (orgs, people, products, CVEs),
   **tickers** (`$NVDA`/`NVDA`/`bitcoin` → NVDA/BTC), **ISO country codes**,
   **sentiment** (-1..+1 finance lexicon), **urgency** (1–10 flash ladder with
   opinion/recap dampeners), **novelty** (headline-shingle + vector overlap vs
   cluster), **verification** (CONFIRMED when corroborated/official).
4. **flash_headline + 3-bullet executive summary** tuned for scan-reading and
   low-token agent consumption.
5. Optional **LLM refinement** (OpenAI / Anthropic / Ollama) — only for
   urgency ≥ 4 items, strict-JSON prompts, silent heuristic fallback on any
   failure. Without keys the system is fully functional.

### Storage

- **PostgreSQL + TimescaleDB**: `events` hypertable partitioned by
  `ingested_at`, GIN indexes on categories/tickers, trigram title search.
- **pgvector**: `clusters.embedding vector(256)` with ivfflat cosine index —
  agents get true semantic RAG queries. Without pgvector the column degrades
  to `float8[]` (exact scan still works).
- **No infra? No problem.** `MemoryStore` keeps identical semantics in-process
  (bounded ring buffers); the terminal and API run unchanged.

### Bus

- **Redis Streams** (`omnipulse:events`, 200k bound, XRANGE replay for agents) +
  **pub/sub** channels (`live`, `clusters`, `stats`, `source_state`) for
  sub-millisecond fan-out to every WS/SSE subscriber.
- Filters are evaluated **server-side** at fan-out; subscribers only receive
  what they asked for.
- No Redis? Direct in-process delivery (identical frame protocol).

---

## Runtime source management

Sources are rows, not files — manage them live:

```bash
# add any RSS/JSON/HTML/ws source at runtime
curl -X POST localhost:8000/api/sources -d '{
  "id":"acme_pr", "name":"Acme PR", "type":"rss",
  "url":"https://acme.com/news.xml", "category":"wire",
  "credibility_tier": 2, "poll_interval_sec": 60 }'

curl -X PATCH localhost:8000/api/sources/acme_pr -d '{"poll_interval_sec": 30}'
curl -X POST localhost:8000/api/sources/acme_pr/pause
curl -X POST localhost:8000/api/sources/acme_pr/resume
curl -X DELETE localhost:8000/api/sources/acme_pr
```

The default registry re-seeds missing entries on each boot but never overwrites
user-modified sources.

## Configuration

Everything is an env var (prefix `OMNIPULSE_`), all optional. See
[`.env.example`](.env.example). Highlights:

| Var | Default | Meaning |
|---|---|---|
| `OMNIPULSE_API_KEY` | unset | optional guard on /api and streams |
| `OMNIPULSE_DATABASE_URL` | unset | Postgres DSN (unset → memory store) |
| `OMNIPULSE_REDIS_URL` | unset | Redis URL (unset → local bus) |
| `OMNIPULSE_LLM_PROVIDER` | `auto` | `auto\|none\|openai\|anthropic\|ollama` |
| `OMNIPULSE_CLUSTER_SIMILARITY` | `0.88` | StoryCluster join threshold |
| `OMNIPULSE_PER_HOST_RATE` | `2.0` | req/sec per host (politeness) |
| `OMNIPULSE_SOURCES_FILE` | `config/default_sources.json` | seed registry |
| `OMNIPULSE_HTTP2` | `true` | HTTP/2 for feed fetches (ALPN-negotiated) |
| `OMNIPULSE_KEEPALIVE_EXPIRY_SEC` | `300` | warm-connection lifetime; amortizes TLS handshakes across poll cycles |
| `OMNIPULSE_POLL_TICK_SEC` | `0.1` | scheduler wake-up granularity |
| `OMNIPULSE_CLUSTER_CANDIDATE_CAP` | `600` | clusters compared per event (numpy matvec) |
| `OMNIPULSE_WEBSUB_ENABLED` | `false` | WebSub push delivery (needs a public `OMNIPULSE_PUBLIC_URL`) |

## Latency budget

Where the milliseconds go, and what keeps each stage fast:

| Stage | Cost | Mechanism |
|---|---|---|
| Feed fetch | network-bound | HTTP/2 multiplexing, 300s keepalive (no TLS re-handshake), conditional GET (304 = zero body), per-host token buckets |
| Scheduling | ≤ 100ms | 0.1s engine tick; adaptive intervals — ≥3 fresh items snaps a source to its `min_poll_sec` floor (wires 8s, SEC 8-K 5s) |
| Pipeline | ~0.6ms p50 / event | numpy cluster matvec (capped candidate index), SimHash dedup in one locked pass, substring-prefiltered gazetteer NER |
| Enrichment | ~0.9ms / event | pure-local lexicons; LLM refinement only for urgency ≥ 4 when keys are set |
| Delivery | < 5ms in-process | bus fan-out to WS/SSE/MCP queues; UI tape seeds instantly via REST snapshot on load |

Push mode (WebSub) eliminates poll latency entirely for hubs that advertise
`<link rel="hub">` — enable with `OMNIPULSE_WEBSUB_ENABLED=true` and a
publicly reachable `OMNIPULSE_PUBLIC_URL`. Polling continues as fallback;
pushed items skip the queue entirely.

## Verification

```bash
cd backend && python -m pytest tests -q   # 53 unit/integration tests
cd backend && python -m compileall -q app # syntax gate
cd frontend && npm run build              # tsc + vite production build
```
