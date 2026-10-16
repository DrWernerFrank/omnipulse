"""FastAPI application factory: REST + streams + webhooks + MCP (+ optional static terminal)."""

from __future__ import annotations

import contextlib
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from .. import __version__
from ..runtime import Runtime, get_runtime, set_runtime
from ..settings import Settings
from .deps import _presented_key
from .hub import Hub
from .routes_data import router as data_router
from .routes_hooks import router as hooks_router
from .routes_sources import router as sources_router
from .routes_stream import router as stream_router
from .webhooks import WebhookDispatcher

log = logging.getLogger(__name__)

LLMS_TXT = """# OmniPulse

> Real-time global news terminal and agent intelligence hub. Every item is a normalised NewsEvent
> (flash_headline, 3-bullet executive_summary, categories, tickers, entities, ISO locations,
> sentiment -1..1, urgency 1-10, novelty 0-1, verification_status) grouped into StoryClusters.

## Agent interfaces
- MCP (streamable HTTP): /mcp — tools: get_latest_events, search_events, get_event, list_story_clusters,
  get_story_cluster, wait_for_events, list_sources, add_source, set_source_enabled, remove_source, pipeline_stats
- OpenAPI 3.1: /openapi.json (docs at /docs)
- REST: GET /v1/events?q=<dsl>&format=compact|full|headline, /v1/events/{id}, /v1/clusters, /v1/clusters/{id},
  /v1/search?query=..., /v1/sources (POST/PATCH/DELETE to manage), /v1/stats, /v1/taxonomy
- Streams: WS /v1/stream, SSE /v1/stream/sse, NDJSON /v1/stream/jsonl  (params: q, format, types, backfill)
- Webhooks: POST /v1/webhooks {url, q, event_types, format} — signed X-OmniPulse-Signature: sha256=HMAC(secret, body)

## Filter DSL
u>=7  cat:crypto,macro  $NVDA  tk:BTC  src:reuters  scat:wire  loc:US  tier<=2  sent>0.3  nov>=0.5
vs:confirmed  ent:openai  cluster:cls_...  "exact phrase"  -excludeword

## Token tips
format=compact uses short keys (id, cid, ts, src, tier, h, u, sent, nov, vs, url, cat, tk, loc, sum);
format=headline returns one line per event.
"""


class MCPAuth:
    """Require an API key on /mcp when OMNI_API_KEYS is configured (MCP tools can mutate sources)."""

    def __init__(self, app) -> None:
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith("/mcp"):
            keys = get_runtime().settings.api_key_set
            if keys:
                headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
                from urllib.parse import parse_qs

                query = {k: v[0] for k, v in parse_qs(scope.get("query_string", b"").decode()).items()}
                if _presented_key(headers, query) not in keys:
                    resp = JSONResponse({"error": "missing or invalid API key"}, status_code=401)
                    await resp(scope, receive, send)
                    return
        await self.app(scope, receive, send)


def create_app(settings: Settings, role: str = "api") -> FastAPI:
    from ..mcp_server import mcp

    mcp_app = mcp.streamable_http_app()  # creates mcp.session_manager

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        rt = await Runtime.create(settings, role)
        rt.hub = Hub(rt.bus, settings.subscriber_queue_size)
        rt.hub.start()
        rt.webhooks = WebhookDispatcher(rt.bus, rt.store, settings.user_agent)
        rt.webhooks.start()
        set_runtime(rt)
        log.info("OmniPulse %s up — mode=%s role=%s", __version__, settings.mode, role)
        try:
            async with mcp.session_manager.run():
                yield
        finally:
            await rt.close()
            set_runtime(None)

    app = FastAPI(
        title="OmniPulse",
        version=__version__,
        summary="Ultra-low-latency news terminal & AI-agent intelligence hub",
        description=LLMS_TXT,
        lifespan=lifespan,
    )
    origins = [o.strip() for o in settings.cors_origins.split(",") if o.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=origins or ["*"], allow_methods=["*"], allow_headers=["*"])
    app.add_middleware(GZipMiddleware, minimum_size=2048)
    app.add_middleware(MCPAuth)

    app.include_router(data_router)
    app.include_router(sources_router)
    app.include_router(stream_router)
    app.include_router(hooks_router)

    @app.get("/healthz", include_in_schema=False)
    @app.get("/health", include_in_schema=False)
    async def healthz():
        return {"ok": True}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        rt = get_runtime()
        return {"ok": True, "mode": rt.settings.mode, "role": rt.role, "embedder": rt.embedder.name}

    @app.get("/llms.txt", include_in_schema=False, response_class=PlainTextResponse)
    async def llms_txt():
        return LLMS_TXT

    @app.exception_handler(ValueError)
    async def value_error(_: Request, exc: ValueError):
        return JSONResponse({"detail": str(exc)}, status_code=422)

    # MCP streamable-HTTP endpoint lives at /mcp
    app.router.routes.extend(mcp_app.routes)

    static = Path(settings.static_dir) if settings.static_dir else None
    if static is None:
        # merged repo: the v1 tiling terminal lives in frontend_v1/; fall back to frontend/
        for cand in (Path(__file__).resolve().parents[3] / "frontend_v1" / "dist",
                     Path(__file__).resolve().parents[3] / "frontend" / "dist",
                     Path("/app/static")):
            if cand.is_dir():
                static = cand
                break
    if static and static.is_dir() and (static / "index.html").exists():
        app.mount("/", StaticFiles(directory=static, html=True), name="terminal")
        log.info("serving terminal UI from %s", static)
    return app
