"""Webhooks out (agent subscriptions) and in (Discord-style / generic push, WebSub callbacks)."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import secrets
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field, HttpUrl

from .. import bus as B
from ..filters import EventFilter
from ..ids import new_webhook_id
from ..ingest.base import parse_dt
from ..ingest.rss import entries_to_items, parse_feed, RSSConnector
from ..schema import RawItem, WebhookConfig
from .deps import check_key, require_write, rt

router = APIRouter(tags=["webhooks"])


class WebhookCreate(BaseModel):
    url: HttpUrl
    q: str = Field("", description="Filter DSL; empty = everything")
    event_types: list[Literal["EVENT_CREATED", "EVENT_UPDATED", "CLUSTER_UPDATED"]] = ["EVENT_CREATED"]
    format: Literal["full", "compact", "headline"] = "compact"
    secret: str | None = Field(None, description="HMAC-SHA256 secret; generated when omitted")


@router.get("/v1/webhooks", dependencies=[Depends(require_write)])
async def list_webhooks():
    hooks = await rt().store.list_webhooks()
    return {"items": [h.model_dump(mode="json", exclude={"secret"}) for h in hooks]}


@router.post("/v1/webhooks", status_code=201, dependencies=[Depends(require_write)],
             summary="Subscribe a URL to filtered events (signed with X-OmniPulse-Signature)")
async def create_webhook(body: WebhookCreate):
    EventFilter.parse(body.q)  # validate early
    hook = WebhookConfig(id=new_webhook_id(), url=str(body.url), dsl=body.q, event_types=list(body.event_types),
                         format=body.format, secret=body.secret or secrets.token_urlsafe(24))
    await rt().store.save_webhook(hook)
    if rt().webhooks:
        await rt().webhooks.reload()
    return hook.model_dump(mode="json")


@router.delete("/v1/webhooks/{hook_id}", dependencies=[Depends(require_write)])
async def delete_webhook(hook_id: str):
    if not await rt().store.delete_webhook(hook_id):
        raise HTTPException(404, "webhook not found")
    if rt().webhooks:
        await rt().webhooks.reload()
    return {"id": hook_id, "deleted": True}


# --------------------------------------------------------------------------------------
# Inbound push: Discord webhook payloads or a generic item / list of items
# --------------------------------------------------------------------------------------


def _stable(text: str) -> str:
    return hashlib.blake2b(text.encode(), digest_size=8).hexdigest()


def _items_from_payload(source_id: str, payload: Any) -> list[RawItem]:
    rows = payload if isinstance(payload, list) else [payload]
    out: list[RawItem] = []
    for p in rows:
        if not isinstance(p, dict):
            continue
        if "embeds" in p or "content" in p and "title" not in p:  # Discord webhook shape
            for e in p.get("embeds") or [{}]:
                title = e.get("title") or (p.get("content") or "").split("\n", 1)[0]
                if not title:
                    continue
                url = e.get("url") or ""
                out.append(RawItem(
                    source_id=source_id, guid=url or f"discord:{_stable(title + (p.get('content') or ''))}",
                    url=url or f"urn:discord:{source_id}:{_stable(title)}", title=title[:300],
                    summary=e.get("description") or p.get("content") or "", author=p.get("username", ""),
                    published_at=parse_dt(e.get("timestamp")), hints={"display_source": p.get("username", "")},
                ))
            continue
        if not p.get("title"):
            continue
        url = p.get("url") or f"urn:{source_id}:{_stable(str(p['title']))}"
        out.append(RawItem(
            source_id=source_id, guid=p.get("guid") or url, url=url, title=str(p["title"])[:300],
            summary=p.get("summary") or p.get("body") or "", content_html=p.get("content_html", ""),
            author=p.get("author", ""), published_at=parse_dt(p.get("published_at")),
            hints={k: v for k, v in (p.get("hints") or {}).items()
                   if k in ("categories", "tickers", "urgency_floor", "urgency_bonus", "verification", "display_source")},
        ))
    return out


@router.post("/v1/ingest/{source_id}", status_code=202, tags=["ingest"],
             summary="Push items into the pipeline (Discord webhook format or {title,url,summary,...})")
async def ingest_push(source_id: str, request: Request):
    cfg = await rt().store.get_source(source_id)
    if not cfg:
        raise HTTPException(404, "unknown source; create it first with type 'webhook'")
    secret = cfg.params.get("secret")
    if secret:
        given = request.headers.get("x-omni-secret") or request.query_params.get("secret", "")
        if not hmac.compare_digest(str(secret), given):
            raise HTTPException(401, "bad source secret")
    else:
        check_key(request.headers, request.query_params, write=True)
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(400, "body must be JSON") from exc
    items = _items_from_payload(source_id, payload)
    for it in items:
        await rt().bus.publish(B.RAW, it.model_dump(mode="json"))
    return {"accepted": len(items)}


# --------------------------------------------------------------------------------------
# WebSub (PubSubHubbub) subscriber callback
# --------------------------------------------------------------------------------------


@router.get("/websub/callback/{source_id}", include_in_schema=False)
async def websub_verify(source_id: str, request: Request):
    qp = request.query_params
    cfg = await rt().store.get_source(source_id)
    if not cfg:
        raise HTTPException(404)
    mode = qp.get("hub.mode")
    if mode in ("subscribe", "unsubscribe") and qp.get("hub.challenge"):
        return PlainTextResponse(qp["hub.challenge"])
    return PlainTextResponse("ok")


@router.post("/websub/callback/{source_id}", include_in_schema=False)
async def websub_push(source_id: str, request: Request):
    cfg = await rt().store.get_source(source_id)
    if not cfg:
        raise HTTPException(404)
    body = await request.body()
    secret = cfg.params.get("websub_secret")
    if secret:
        sig = request.headers.get("x-hub-signature", "")
        algo, _, digest = sig.partition("=")
        if algo not in ("sha1", "sha256") or not hmac.compare_digest(
            hmac.new(secret.encode(), body, getattr(hashlib, algo)).hexdigest(), digest
        ):
            return PlainTextResponse("bad signature", status_code=202)  # spec: ack, but ignore
    parsed = await asyncio.to_thread(parse_feed, body)
    conn = RSSConnector(cfg, None, {})  # type: ignore[arg-type]
    items = conn.post_process(entries_to_items(conn, parsed))
    for it in items:
        await rt().bus.publish(B.RAW, it.model_dump(mode="json"))
    return PlainTextResponse("ok", status_code=202)
