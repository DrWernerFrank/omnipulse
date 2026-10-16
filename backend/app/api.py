"""HTTP API surface.

Human endpoints: /  (terminal UI redirect)
Agent endpoints (all JSON, all filterable):
  GET  /api/events            (limit, cursor, q, categories, tickers, sources,
                               min_urgency, since, max_age_sec, compact=true)
  GET  /api/events/{id}
  GET  /api/events.jsonl      (batch snapshot as newline-delimited JSON)
  GET  /api/clusters          (+ /api/clusters/{id}, /api/clusters/{id}/events)
  GET  /api/sources           (+ POST/PATCH/DELETE /api/sources, /api/sources/{id}/pause|resume)
  GET  /api/webhooks          (+ POST/PATCH/DELETE — outbound delivery, merged from v1)
  GET  /api/stats, /api/dlq, /api/health
  GET  /api/schema            (the canonical NewsEvent JSON schema doc)
  GET  /openapi.json          (hand-written OpenAPI 3.1)
Streaming:
  WS   /ws            ?subscribe=<json FilterSpec>&compact=true
  SSE  /stream/events (?q=&categories=&...)    text/event-stream
  SSE  /stream/stats  /stream/sources         (terminal chrome)
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any

import orjson
from fastapi import APIRouter, Body, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, PlainTextResponse, RedirectResponse, Response, StreamingResponse

from .bus import Bus
from .config import get_settings
from .db import Store
from .engine import Engine
from .ids import new_id
from .pipeline import Pipeline
from .schemas import (
    CATEGORIES, COMPACT_KEY_LEGEND, VERIFICATION_STATUSES, FilterSpec, NewsEvent,
    SourceConfig,
)
from .websub import verify_signature
from .webhooks import WebhookDispatcher

router = APIRouter()

# injected at startup by app.state; helpers take the FastAPI app itself
# (a Request exposes .app; a WebSocket exposes .app too)
def _store(app) -> Store:
    return app.state.store


def _bus(app) -> Bus:
    return app.state.bus


def _engine(app) -> Engine:
    return app.state.engine


def _pipeline(app) -> Pipeline:
    return app.state.pipeline


def _check_key(request: Request) -> None:
    key = get_settings().api_key
    if key:
        provided = request.headers.get("x-api-key") or (
            request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        )
        if provided != key:
            raise HTTPException(status_code=401, detail="invalid or missing API key")


# --------------------------------------------------------------------------- #
# events
# --------------------------------------------------------------------------- #
def _filter_from_request(request: Request, limit_default: int = 100) -> FilterSpec:
    q = request.query_params
    return FilterSpec(
        q=q.get("q"),
        dsl=q.get("dsl"),
        categories=[c for c in (q.get("categories") or "").split(",") if c],
        tickers=[t.upper() for t in (q.get("tickers") or "").split(",") if t],
        sources=[s for s in (q.get("sources") or "").split(",") if s],
        entities=[e for e in (q.get("entities") or "").split(",") if e],
        min_urgency=int(q["min_urgency"]) if q.get("min_urgency") else None,
        max_age_sec=int(q["max_age_sec"]) if q.get("max_age_sec") else None,
        since=_parse_since(q.get("since")),
        limit=min(int(q.get("limit", limit_default)), 1000),
        cursor=q.get("cursor"),
        compact=q.get("compact") in ("1", "true", "yes"),
    )


def _parse_since(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        if v.isdigit():
            return datetime.fromtimestamp(int(v) / (1000 if len(v) > 10 else 1), tz=timezone.utc)
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None


def _event_response(ev: NewsEvent, compact: bool) -> dict[str, Any]:
    d = orjson.loads(ev.model_dump_json())
    return ev.compact() if compact else d


@router.get("/api/events")
async def list_events(request: Request):
    _check_key(request)
    f = _filter_from_request(request)
    events = await _store(request.app).query_events(f)
    if request.query_params.get("format") == "headline":
        # one-line wire format (merged from v1): HH:MM:SSz U7 +0.12 [Reuters|T1] ... $NVDA (Macro)
        from .dsl import headline_line

        text = "\n".join(headline_line(e.model_dump(mode="json")) for e in events)
        return PlainTextResponse(
            text + ("\n" if text else ""),
            headers={"X-Result-Count": str(len(events))},
        )
    payload = [_event_response(e, f.compact) for e in events]
    resp = Response(orjson.dumps(payload), media_type="application/json")
    next_cursor = events[-1].event_id if len(events) >= f.limit else None
    resp.headers["X-Next-Cursor"] = next_cursor or ""
    resp.headers["X-Result-Count"] = str(len(events))
    if f.compact:
        resp.headers["X-Compact-Legend"] = json.dumps(COMPACT_KEY_LEGEND)
    return resp


@router.get("/api/events.jsonl")
async def list_events_jsonl(request: Request):
    _check_key(request)
    f = _filter_from_request(request, limit_default=500)
    events = await _store(request.app).query_events(f)
    lines = b"\n".join(orjson.dumps(_event_response(e, f.compact)) for e in events)
    return StreamingResponse(
        iter([lines]),
        media_type="application/x-ndjson",
        headers={"X-Result-Count": str(len(events))},
    )


@router.get("/api/events/{event_id}")
async def get_event(event_id: str, request: Request, compact: bool = False):
    _check_key(request)
    ev = await _store(request.app).get_event(event_id)
    if ev is None:
        raise HTTPException(404, "event not found")
    return _event_response(ev, compact)


# --------------------------------------------------------------------------- #
# clusters
# --------------------------------------------------------------------------- #
@router.get("/api/clusters")
async def list_clusters(request: Request, limit: int = Query(default=100, le=500)):
    _check_key(request)
    clusters = await _store(request.app).list_clusters(limit)
    return [orjson.loads(c.model_dump_json()) for c in clusters]


@router.get("/api/clusters/{cluster_id}")
async def get_cluster(cluster_id: str, request: Request):
    _check_key(request)
    cs = await _store(request.app).get_cluster(cluster_id)
    if cs is None:
        raise HTTPException(404, "cluster not found")
    return orjson.loads(cs.model_dump_json())


@router.get("/api/clusters/{cluster_id}/events")
async def cluster_events(cluster_id: str, request: Request, limit: int = Query(default=50, le=200)):
    _check_key(request)
    events = await _store(request.app).cluster_events(cluster_id, limit)
    f = _filter_from_request(request, limit_default=limit)
    return [_event_response(e, f.compact) for e in events]


# --------------------------------------------------------------------------- #
# sources (runtime registry management â€” used by UI settings pane & agents)
# --------------------------------------------------------------------------- #
@router.get("/api/sources")
async def list_sources(request: Request):
    _check_key(request)
    return [
        {
            "id": m.source_id, "name": m.name, "health": m.health, "enabled": m.enabled,
            "detail": m.detail, "last_success": m.last_success.isoformat() if m.last_success else None,
            "interval_sec": m.interval_sec,
        }
        for m in _engine(request.app).source_states()
    ]


@router.get("/api/sources/{source_id}")
async def get_source(source_id: str, request: Request):
    _check_key(request)
    cfg = _engine(request.app).sources.get(source_id)
    if cfg is None:
        raise HTTPException(404, "source not found")
    return orjson.loads(cfg.model_dump_json())


@router.post("/api/sources", status_code=201)
async def add_source(request: Request, cfg: SourceConfig = Body(...)):
    _check_key(request)
    if cfg.id in _engine(request.app).sources:
        raise HTTPException(409, f"source {cfg.id} already exists")
    await _engine(request.app).add_source(cfg)
    return orjson.loads(cfg.model_dump_json())


@router.patch("/api/sources/{source_id}")
async def update_source(source_id: str, request: Request, cfg: SourceConfig = Body(...)):
    _check_key(request)
    if source_id not in _engine(request.app).sources:
        raise HTTPException(404, "source not found")
    cfg.id = source_id
    await _engine(request.app).update_source(cfg)
    return orjson.loads(cfg.model_dump_json())


@router.post("/api/sources/{source_id}/pause")
async def pause_source(source_id: str, request: Request):
    _check_key(request)
    cfg = await _engine(request.app).set_enabled(source_id, False)
    if cfg is None:
        raise HTTPException(404, "source not found")
    return {"id": source_id, "enabled": False}


@router.post("/api/sources/{source_id}/resume")
async def resume_source(source_id: str, request: Request):
    _check_key(request)
    cfg = await _engine(request.app).set_enabled(source_id, True)
    if cfg is None:
        raise HTTPException(404, "source not found")
    return {"id": source_id, "enabled": True}


@router.delete("/api/sources/{source_id}")
async def delete_source(source_id: str, request: Request):
    _check_key(request)
    if not await _engine(request.app).remove_source(source_id):
        raise HTTPException(404, "source not found")
    return {"deleted": source_id}


# --------------------------------------------------------------------------- #
# websub push callbacks (hubs call these when a subscribed feed updates)
# --------------------------------------------------------------------------- #
@router.get("/websub/callback/{source_id}")
async def websub_verify(source_id: str, request: Request):
    """Synchronous hub verification: echo hub.challenge when the token matches."""
    cfg = _engine(request.app).sources.get(source_id)
    if cfg is None:
        return Response(status_code=404)
    q = request.query_params
    secret = cfg.state.get("websub_secret", "")
    if not secret or q.get("token") != secret:
        return Response(status_code=403)
    mode = q.get("hub.mode", "")
    if mode in ("subscribe", "unsubscribe") and q.get("hub.topic"):
        if mode == "subscribe":
            cfg.state["websub_confirmed"] = True
        return Response(content=q.get("hub.challenge", ""), media_type="text/plain")
    return Response(status_code=404)


@router.post("/websub/callback/{source_id}")
async def websub_push(source_id: str, request: Request):
    """Pushed feed payload: verify HMAC, parse, ingest instantly (no poll)."""
    cfg = _engine(request.app).sources.get(source_id)
    if cfg is None:
        return Response(status_code=404)
    body = await request.body()
    secret = cfg.state.get("websub_secret", "")
    if not secret or not verify_signature(secret, body, request.headers.get("x-hub-signature")):
        return Response(status_code=403)
    accepted = await _engine(request.app).ingest_pushed(source_id, body)
    return Response(status_code=200 if accepted >= 0 else 404)


# --------------------------------------------------------------------------- #
# ops
# --------------------------------------------------------------------------- #
@router.get("/api/stats")
async def stats(request: Request):
    _check_key(request)
    return await _engine(request.app).compute_stats()


@router.get("/api/dlq")
async def dlq(request: Request, limit: int = Query(default=50, le=200)):
    _check_key(request)
    return await _store(request.app).list_dlq(limit)


@router.get("/api/health")
async def health(request: Request):
    eng = _engine(request.app)
    return {
        "ok": True,
        "ts": datetime.now(timezone.utc).isoformat(),
        "profile": get_settings().profile,
        "sources_online": sum(1 for m in eng.source_states() if m.health in ("ok", "degraded")),
        "sources_total": len(eng.sources),
    }


# --------------------------------------------------------------------------- #
# outbound webhooks (merged from v1)
# --------------------------------------------------------------------------- #
def _webhooks(app) -> "WebhookDispatcher":
    hooks = getattr(app.state, "webhooks", None)
    if hooks is None:
        raise HTTPException(503, "webhook dispatcher not running")
    return hooks


@router.get("/api/webhooks")
async def list_webhooks(request: Request):
    _check_key(request)
    return [_webhooks(request.app).public_view(h) for h in _webhooks(request.app).hooks.values()]


@router.post("/api/webhooks", status_code=201)
async def register_webhook(request: Request):
    _check_key(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    url = (body.get("url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise HTTPException(422, "url must be an http(s) endpoint")
    hook = await _webhooks(request.app).register(
        url,
        secret=body.get("secret") or "",
        dsl=body.get("dsl") or None,
    )
    return _webhooks(request.app).public_view(hook)


@router.patch("/api/webhooks/{hook_id}")
async def update_webhook(hook_id: str, request: Request):
    _check_key(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "invalid JSON body")
    if "active" not in body:
        raise HTTPException(422, "body must include 'active' (bool)")
    hook = await _webhooks(request.app).set_active(hook_id, bool(body["active"]))
    if hook is None:
        raise HTTPException(404, "webhook not found")
    return _webhooks(request.app).public_view(hook)


@router.delete("/api/webhooks/{hook_id}")
async def delete_webhook(hook_id: str, request: Request):
    _check_key(request)
    ok = await _webhooks(request.app).remove(hook_id)
    if not ok:
        raise HTTPException(404, "webhook not found")
    return {"deleted": hook_id}


@router.get("/api/schema")
async def schema(request: Request):
    _check_key(request)
    return {
        "NewsEvent": NewsEvent.model_json_schema(),
        "categories": CATEGORIES,
        "verification_statuses": VERIFICATION_STATUSES,
        "compact_key_legend": COMPACT_KEY_LEGEND,
    }


# --------------------------------------------------------------------------- #
# WebSocket firehose
# --------------------------------------------------------------------------- #
@router.websocket("/ws")
async def ws_firehose(ws: WebSocket):
    settings = get_settings()
    if settings.api_key:
        provided = ws.query_params.get("api_key") or ws.headers.get("x-api-key", "")
        if provided != settings.api_key:
            await ws.close(code=4401)
            return
    if ws.query_params.get("subscribe"):
        try:
            raw = orjson.loads(ws.query_params["subscribe"])
            filters = FilterSpec.model_validate(raw)
        except Exception:
            await ws.close(code=4400)
            return
    else:
        filters = FilterSpec(
            q=ws.query_params.get("q"),
            dsl=ws.query_params.get("dsl"),
            categories=[c for c in (ws.query_params.get("categories") or "").split(",") if c],
            tickers=[t.upper() for t in (ws.query_params.get("tickers") or "").split(",") if t],
            sources=[s for s in (ws.query_params.get("sources") or "").split(",") if s],
            min_urgency=int(ws.query_params["min_urgency"]) if ws.query_params.get("min_urgency") else None,
            compact=ws.query_params.get("compact") in ("1", "true", "yes"),
        )
    await ws.accept()
    bus = _bus(ws.app)
    sub = bus.subscribe(filters)

    async def sender() -> None:
        while True:
            frame = await sub.queue.get()
            if filters.compact and frame.get("t") == "event":
                frame = {"t": "event", "d": _compact_from_full(frame["d"])}
            await ws.send_text(orjson.dumps(frame).decode())

    async def receiver() -> None:
        # protocol: {"type": "resubscribe", "filters": {...}} swaps the live filter
        while True:
            msg = await ws.receive_text()
            try:
                data = orjson.loads(msg)
            except Exception:
                continue
            if isinstance(data, dict) and data.get("type") == "resubscribe":
                try:
                    new_filters = FilterSpec.model_validate(data.get("filters") or {})
                except Exception:
                    continue
                # mutate the live filter in place; sender's queue is untouched
                filters.q = new_filters.q
                filters.dsl = new_filters.dsl
                filters.categories = new_filters.categories
                filters.tickers = new_filters.tickers
                filters.sources = new_filters.sources
                filters.min_urgency = new_filters.min_urgency
                await ws.send_text(orjson.dumps({
                    "t": "hello",
                    "d": {"resubscribed": True},
                }).decode())

    send_task = asyncio.create_task(sender())
    recv_task = asyncio.create_task(receiver())
    try:
        await ws.send_text(orjson.dumps({
            "t": "hello",
            "d": {"subscriptions": filters.model_dump(exclude_none=True)},
        }).decode())
        done, pending = await asyncio.wait(
            {send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for t in pending:
            t.cancel()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        send_task.cancel()
        recv_task.cancel()
        bus.unsubscribe(sub)
        try:
            await ws.close()
        except Exception:
            pass


def _compact_from_full(d: dict[str, Any]) -> dict[str, Any]:
    from .schemas import compact_dict

    return compact_dict(d)


# --------------------------------------------------------------------------- #
# SSE firehoses
# --------------------------------------------------------------------------- #
async def _sse_response(request: Request, channel: str, filters: FilterSpec | None):
    bus = _bus(request.app)
    sub = bus.subscribe(filters)

    async def gen():
        yield ": ok\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    frame = await asyncio.wait_for(sub.queue.get(), timeout=15.0)
                    data = orjson.dumps(frame["d"]).decode()
                    if filters and filters.compact and frame.get("t") == "event":
                        data = orjson.dumps(_compact_from_full(frame["d"])).decode()
                    yield f"event: {frame['t']}\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            bus.unsubscribe(sub)

    return StreamingResponse(
        gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@router.get("/stream/events")
async def sse_events(request: Request):
    _check_key(request)
    filters = _filter_from_request(request, limit_default=1)
    filters.limit = 1  # limit is meaningless for a live stream
    return await _sse_response(request, "events", filters)


@router.get("/stream/stats")
async def sse_stats(request: Request):
    _check_key(request)
    return await _sse_response(request, "stats", None)


@router.get("/stream/sources")
async def sse_sources(request: Request):
    _check_key(request)
    return await _sse_response(request, "sources", None)

