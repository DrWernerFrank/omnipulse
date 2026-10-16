"""Runtime source registry management (add / update / pause / resume / remove / validate)."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from .. import bus as B
from ..ingest.base import ConnectorContext
from ..ingest.engine import CONNECTORS
from ..registry import slugify
from ..schema import SOURCE_TYPES, SourceConfig, SourceUpdate
from .deps import require_read, require_write, rt

router = APIRouter(prefix="/v1/sources", tags=["sources"])


class SourceCreate(BaseModel):
    id: str | None = Field(None, description="Slug; derived from name when omitted")
    name: str
    type: str = Field("rss", description=f"One of {list(SOURCE_TYPES)}")
    url: str = ""
    category: str = "general"
    credibility_tier: int = Field(2, ge=1, le=3)
    poll_interval: float = 60.0
    min_interval: float = 5.0
    max_interval: float = 900.0
    enabled: bool = True
    default_categories: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)


async def _health_map() -> dict[str, dict[str, Any]]:
    r = rt()
    if r.engine:
        return {k: v.model_dump(mode="json") for k, v in r.engine.health.items()}
    if r.hub.last_health:
        return {h["id"]: h for h in r.hub.last_health}
    return {h.id: h.model_dump(mode="json") for h in await r.store.list_health()}


async def _notify(op: str, source_id: str) -> None:
    await rt().bus.publish(B.CONTROL, {"op": op, "source_id": source_id})


async def _validate(cfg: SourceConfig) -> dict[str, Any]:
    """Run one poll without emitting, so callers learn immediately whether a source works."""
    cls = CONNECTORS.get(cfg.type)
    if cls is None or cls.streaming:
        return {"ok": True, "note": "streaming/push source; validated on connect"}
    r = rt()
    conn = cls(cfg, ConnectorContext(r.fetcher, r.settings), {})
    try:
        items = await asyncio.wait_for(conn.poll(), timeout=30)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"[:300]}
    return {"ok": True, "items": len(items), "sample": [i.title for i in items[:3]]}


@router.get("", dependencies=[Depends(require_read)], summary="List sources with live health")
async def list_sources(group: str | None = None, category: str | None = None):
    health = await _health_map()
    out = []
    for s in await rt().store.list_sources():
        if (group and s.group != group) or (category and s.category != category):
            continue
        out.append({**s.model_dump(mode="json"), "health": health.get(s.id)})
    return {"count": len(out), "items": out}


@router.get("/health", dependencies=[Depends(require_read)], summary="Health summary for every source")
async def sources_health():
    health = await _health_map()
    counts: dict[str, int] = {}
    for h in health.values():
        counts[h.get("status", "unknown")] = counts.get(h.get("status", "unknown"), 0) + 1
    return {"summary": counts, "items": sorted(health.values(), key=lambda h: h["id"])}


@router.get("/{source_id}", dependencies=[Depends(require_read)])
async def get_source(source_id: str):
    s = await rt().store.get_source(source_id)
    if not s:
        raise HTTPException(404, "source not found")
    return {**s.model_dump(mode="json"), "health": (await _health_map()).get(source_id)}


@router.post("", status_code=201, dependencies=[Depends(require_write)], summary="Add a source at runtime")
async def create_source(body: SourceCreate, validate: bool = Query(True, description="Test-poll before saving")):
    data = body.model_dump()
    data["id"] = data.get("id") or slugify(body.name)
    try:
        cfg = SourceConfig.model_validate(data)
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc
    existing = await rt().store.get_source(cfg.id)
    if existing:
        raise HTTPException(409, f"source {cfg.id} already exists")
    check = await _validate(cfg) if validate else None
    if check and not check["ok"]:
        raise HTTPException(422, {"message": "source validation failed", **check})
    await rt().store.save_source(cfg)
    await _notify("upsert", cfg.id)
    return {"source": cfg.model_dump(mode="json"), "validation": check}


@router.patch("/{source_id}", dependencies=[Depends(require_write)])
async def update_source(source_id: str, body: SourceUpdate):
    s = await rt().store.get_source(source_id)
    if not s:
        raise HTTPException(404, "source not found")
    try:
        cfg = SourceConfig.model_validate(s.model_dump() | body.model_dump(exclude_none=True))
    except Exception as exc:
        raise HTTPException(422, str(exc)) from exc
    await rt().store.save_source(cfg)
    await _notify("upsert", cfg.id)
    return cfg.model_dump(mode="json")


async def _set_enabled(source_id: str, enabled: bool) -> dict[str, Any]:
    s = await rt().store.get_source(source_id)
    if not s:
        raise HTTPException(404, "source not found")
    s.enabled = enabled
    await rt().store.save_source(s)
    await _notify("resume" if enabled else "pause", source_id)
    return {"id": source_id, "enabled": enabled}


@router.post("/{source_id}/pause", dependencies=[Depends(require_write)])
async def pause_source(source_id: str):
    return await _set_enabled(source_id, False)


@router.post("/{source_id}/resume", dependencies=[Depends(require_write)])
async def resume_source(source_id: str):
    return await _set_enabled(source_id, True)


@router.delete("/{source_id}", dependencies=[Depends(require_write)], summary="Remove a source (soft delete)")
async def delete_source(source_id: str):
    s = await rt().store.get_source(source_id)
    if not s:
        raise HTTPException(404, "source not found")
    s.deleted = True
    s.enabled = False
    await rt().store.save_source(s)
    await _notify("delete", source_id)
    return {"id": source_id, "deleted": True}
