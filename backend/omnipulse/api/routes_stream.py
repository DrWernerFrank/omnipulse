"""Live firehoses: WebSocket, Server-Sent Events, and JSONL (NDJSON) chunked streaming.

All three share one filter DSL (``q``), output ``format`` (full | compact | headline) and
envelope ``types`` (EVENT_CREATED, EVENT_UPDATED, CLUSTER_UPDATED, SOURCE_HEALTH, HEARTBEAT).
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, AsyncIterator

import orjson
from fastapi import APIRouter, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

from ..filters import EventFilter, render
from ..schema import iso, utcnow
from .deps import require_read, rt, ws_authorized
from .hub import ALL_TYPES, DEFAULT_TYPES, Subscriber

router = APIRouter(prefix="/v1/stream", tags=["stream"])


def _types(raw: str | None) -> frozenset[str]:
    if not raw:
        return DEFAULT_TYPES
    t = frozenset(x.strip().upper() for x in raw.split(",") if x.strip())
    return (t & ALL_TYPES) or DEFAULT_TYPES


async def _backfill(f: EventFilter, n: int) -> list[dict[str, Any]]:
    if n <= 0:
        return []
    events = await rt().store.query_events(f, limit=min(n, 2000))
    return [{"type": "EVENT_CREATED", "ts": iso(utcnow()), "data": e, "backfill": True} for e in reversed(events)]


async def _envelopes(sub: Subscriber, heartbeat: float = 15.0) -> AsyncIterator[dict[str, Any] | None]:
    """Yields envelopes; ``None`` means 'send a keep-alive'."""
    while True:
        try:
            env = await asyncio.wait_for(sub.queue.get(), timeout=heartbeat)
        except asyncio.TimeoutError:
            yield None
            continue
        yield env


@router.websocket("")
async def ws_stream(
    ws: WebSocket,
    q: str | None = None,
    format: str = "full",
    types: str | None = None,
    backfill: int = 0,
):
    """Bidirectional stream. Client ops (JSON text frames):

    * ``{"op":"subscribe","q":"u>=7 cat:crypto","format":"compact","types":["EVENT_CREATED"]}``
    * ``{"op":"backfill","limit":200}`` — replay recent matching events from the archive
    * ``{"op":"ping"}``

    Server frames are single envelopes, or ``{"type":"BATCH","data":[...]}`` under bursts.
    """
    if not ws_authorized(ws):
        await ws.close(code=4401)
        return
    await ws.accept()
    hub = rt().hub
    state = {"filter": EventFilter.parse(q), "format": format if format in ("full", "compact", "headline") else "full"}
    sub = hub.subscribe(state["filter"], state["format"], _types(types))

    async def send(envs: list[dict[str, Any]]) -> None:
        rendered = []
        for e in envs:
            r = render(e, state["format"])
            rendered.append(r if isinstance(r, dict) else {"type": e["type"], "line": r})
        frame = rendered[0] if len(rendered) == 1 else {"type": "BATCH", "data": rendered}
        await ws.send_text(orjson.dumps(frame).decode())

    async def reader() -> None:
        while True:
            msg = orjson.loads(await ws.receive_text())
            op = msg.get("op")
            if op == "subscribe":
                state["filter"] = EventFilter.parse(msg.get("q") or msg.get("dsl"))
                if msg.get("format") in ("full", "compact", "headline"):
                    state["format"] = msg["format"]
                sub.filter = state["filter"]
                if msg.get("types"):
                    sub.types = _types(",".join(msg["types"]))
                await ws.send_text(orjson.dumps({"type": "SUBSCRIBED", "q": msg.get("q", ""), "format": state["format"]}).decode())
            elif op == "backfill":
                envs = await _backfill(state["filter"], int(msg.get("limit", 200)))
                for i in range(0, len(envs), 200):
                    await send(envs[i:i + 200])
            elif op == "ping":
                await ws.send_text(orjson.dumps({"type": "PONG", "ts": iso(utcnow())}).decode())

    async def writer() -> None:
        async for env in _envelopes(sub):
            if env is None:
                await ws.send_text(orjson.dumps({"type": "HEARTBEAT", "ts": iso(utcnow()), "data": {}}).decode())
                continue
            await send([env, *sub.drain(499)])

    await ws.send_text(orjson.dumps({"type": "WELCOME", "ts": iso(utcnow()),
                                     "data": {"format": state["format"], "types": sorted(sub.types)}}).decode())
    if backfill:
        envs = await _backfill(state["filter"], backfill)
        for i in range(0, len(envs), 200):
            await send(envs[i:i + 200])
    tasks = [asyncio.create_task(reader()), asyncio.create_task(writer())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    except WebSocketDisconnect:
        pass
    finally:
        for t in tasks:
            t.cancel()
        hub.unsubscribe(sub)
        with contextlib.suppress(Exception):
            await ws.close()


@router.get("/sse", dependencies=[Depends(require_read)], summary="Server-Sent Events firehose")
async def sse_stream(
    q: str | None = Query(None, description="Filter DSL"),
    format: str = Query("compact", pattern="^(full|compact|headline)$"),
    types: str | None = Query(None, description="Comma-separated envelope types"),
    backfill: int = Query(0, ge=0, le=2000),
):
    f = EventFilter.parse(q)
    sub = rt().hub.subscribe(f, format, _types(types))

    async def gen() -> AsyncIterator[bytes]:
        try:
            yield b"retry: 2000\n\n"
            for env in await _backfill(f, backfill):
                yield _sse(env, format)
            async for env in _envelopes(sub):
                yield b": keep-alive\n\n" if env is None else _sse(env, format)
        finally:
            rt().hub.unsubscribe(sub)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _sse(env: dict[str, Any], fmt: str) -> bytes:
    payload = render(env, fmt)
    data = payload if isinstance(payload, str) else orjson.dumps(payload).decode()
    eid = env.get("data", {}).get("event_id", "")
    return f"event: {env['type']}\n{f'id: {eid}' + chr(10) if eid else ''}data: {data}\n\n".encode()


@router.get("/jsonl", dependencies=[Depends(require_read)], summary="Newline-delimited JSON firehose")
async def jsonl_stream(
    q: str | None = Query(None, description="Filter DSL"),
    format: str = Query("compact", pattern="^(full|compact|headline)$"),
    types: str | None = Query("EVENT_CREATED", description="Comma-separated envelope types"),
    backfill: int = Query(0, ge=0, le=2000),
    max_events: int = Query(0, ge=0, description="Close the stream after N envelopes (0 = never)"),
):
    f = EventFilter.parse(q)
    sub = rt().hub.subscribe(f, format, _types(types))

    async def gen() -> AsyncIterator[bytes]:
        sent = 0
        try:
            for env in await _backfill(f, backfill):
                yield _line(env, format)
                sent += 1
                if max_events and sent >= max_events:
                    return
            async for env in _envelopes(sub):
                if env is None:
                    yield b"\n"
                    continue
                yield _line(env, format)
                sent += 1
                if max_events and sent >= max_events:
                    return
        finally:
            rt().hub.unsubscribe(sub)

    return StreamingResponse(gen(), media_type="application/x-ndjson",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _line(env: dict[str, Any], fmt: str) -> bytes:
    payload = render(env, fmt)
    return (payload if isinstance(payload, str) else orjson.dumps(payload).decode()).encode() + b"\n"
