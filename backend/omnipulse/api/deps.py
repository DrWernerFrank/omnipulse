"""Shared FastAPI dependencies: runtime access, API-key auth, query helpers."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException, Request, WebSocket

from ..filters import EventFilter
from ..runtime import Runtime, get_runtime


def rt() -> Runtime:
    return get_runtime()


def _presented_key(headers, query) -> str | None:
    key = headers.get("x-api-key")
    auth = headers.get("authorization", "")
    if not key and auth.lower().startswith("bearer "):
        key = auth[7:].strip()
    return key or query.get("api_key")


def check_key(headers, query, write: bool) -> None:
    s = get_runtime().settings
    keys = s.api_key_set
    if not keys or (not write and not s.protect_reads):
        return
    if _presented_key(headers, query) not in keys:
        raise HTTPException(401, "missing or invalid API key (X-API-Key header, Bearer token, or ?api_key=)")


def require_write(request: Request) -> None:
    check_key(request.headers, request.query_params, write=True)


def require_read(request: Request) -> None:
    check_key(request.headers, request.query_params, write=False)


def ws_authorized(ws: WebSocket) -> bool:
    try:
        check_key(ws.headers, ws.query_params, write=False)
        return True
    except HTTPException:
        return False


_REL = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd])$")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    m = _REL.match(value.strip())
    if m:
        n, unit = float(m.group(1)), m.group(2)
        delta = {"s": timedelta(seconds=n), "m": timedelta(minutes=n), "h": timedelta(hours=n), "d": timedelta(days=n)}[unit]
        return datetime.now(timezone.utc) - delta
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(422, f"invalid time {value!r}: use ISO-8601 or a relative value like 30m / 6h / 2d") from exc
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def build_filter(
    q: str | None = None,
    categories: str | None = None,
    tickers: str | None = None,
    sources: str | None = None,
    min_urgency: int | None = None,
) -> EventFilter:
    f = EventFilter.parse(q)
    if categories:
        f._apply("cat:" + categories)
    if tickers:
        f._apply("tk:" + tickers)
    if sources:
        f._apply("src:" + sources)
    if min_urgency:
        f.min_urgency = max(f.min_urgency, min_urgency)
    return f
