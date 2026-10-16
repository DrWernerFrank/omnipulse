"""FastAPI application assembly + lifespan (store, bus, pipeline, engine).

The merged repo ships two selectable engine profiles (OMNIPULSE_PROFILE):

- ``v2`` (default): the fast in-process engine — /api/*, /ws, /stream/*,
  stdio MCP, zero-infra fallbacks, numpy clustering.
- ``v1``: the merged-in original architecture — Redis-stream pipeline,
  Timescale/pgvector store, sharded engine, tiling terminal UI — served
  under /v1/* with streamable-HTTP MCP at /mcp. Configured via OMNI_* env.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from . import __version__
from .api import router
from .bus import Bus
from .config import get_settings
from .db import create_store
from .engine import Engine
from .enrich import EnrichmentService
from .http import HttpPool
from .logging import setup_logging
from .openapi import OPENAPI_SPEC
from .pipeline import Pipeline
from .webhooks import WebhookDispatcher

log = logging.getLogger("omnipulse.app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings = get_settings()
    store = await create_store(settings)
    bus = Bus()
    await bus.start()
    enricher = EnrichmentService()
    enricher.start()
    pipeline = Pipeline(store, bus, enricher)
    engine = Engine(store, bus, pipeline)
    pool = HttpPool.get()
    await pool.start()

    app.state.store = store
    app.state.bus = bus
    app.state.pipeline = pipeline
    app.state.engine = engine

    hooks = WebhookDispatcher(store, bus)
    app.state.webhooks = hooks
    await hooks.start()

    await engine.start()
    log.info("OmniPulse v%s up — API on %s", __version__, settings.public_url)
    try:
        yield
    finally:
        await hooks.stop()
        await engine.stop()
        await bus.close()
        await pool.close()
        if hasattr(store, "close"):
            await store.close()
        log.info("OmniPulse shut down cleanly")


def _create_v2_app() -> FastAPI:
    app = FastAPI(
        title="OmniPulse",
        version=__version__,
        lifespan=lifespan,
        # we serve our own hand-written OpenAPI 3.1 doc (below) — disable the
        # auto-generated one to keep /openapi.json agent-stable.
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=get_settings().cors_list,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi():
        return JSONResponse(OPENAPI_SPEC)

    @app.get("/", include_in_schema=False)
    async def root():
        # agents hit /api/*, humans use the terminal (served by the frontend container
        # or any static host); give agents a friendly JSON signpost at the root.
        return JSONResponse(
            {
                "service": "OmniPulse",
                "version": __version__,
                "profile": "v2",
                "terminal": "/index.html (frontend container, port 3000)",
                "api": "/api/events?compact=true",
                "openapi": "/openapi.json",
                "ws": "/ws?categories=Breaking&compact=true",
                "sse": ["/stream/events", "/stream/stats", "/stream/sources"],
                "mcp": "python -m app.mcp_server (stdio)",
            }
        )

    return app


def _create_v1_app() -> FastAPI:
    """Boot the v1 profile: the merged-in original engine (OMNI_* env namespace).

    Role "all" runs engine + API in one process (OMNI_MODE=lite keeps it
    zero-infrastructure; OMNI_MODE=full uses Redis + Timescale).
    """
    from omnipulse.api.app import create_app as create_v1_app
    from omnipulse.settings import get_settings as get_v1_settings

    return create_v1_app(get_v1_settings(), "all")


def create_app() -> FastAPI:
    """Select which engine profile this process serves (OMNIPULSE_PROFILE)."""
    settings = get_settings()
    if settings.profile == "v1":
        log.info("OMNIPULSE_PROFILE=v1 — serving the merged original architecture (/v1/*)")
        return _create_v1_app()
    return _create_v2_app()


app = create_app()
