"""Outbound webhook delivery — merged from v1, rebuilt on the v2 bus.

Any registered webhook receives a JSON envelope per matching event:

    {"type": "EVENT_CREATED", "data": {...NewsEvent...}}

Headers (wire-compatible with the v1 profile's dispatcher):
    X-OmniPulse-Event: EVENT_CREATED
    X-OmniPulse-Webhook: <hook id>
    X-OmniPulse-Signature: sha256=<hex hmac-sha256 of the raw body>   (if secret set)

Delivery: 3 attempts with 1s/5s/25s backoff; 5 consecutive exhausted hooks
are auto-deactivated (re-enable via PATCH). Filters use the terminal DSL
(``u>=7 cat:crypto $NVDA``); an unset ``dsl`` receives everything.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
from typing import Any

import httpx
import orjson

from .bus import Bus
from .config import get_settings
from .dsl import parse_dsl
from .schemas import WebhookConfig, now_utc

log = logging.getLogger("omnipulse.webhooks")

RETRY_DELAYS = (1.0, 5.0, 25.0)
MAX_CONSECUTIVE_FAILURES = 5
DELIVER_TIMEOUT = 10.0


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookDispatcher:
    def __init__(self, store, bus: Bus) -> None:
        self.store = store
        self.bus = bus
        self.hooks: dict[str, WebhookConfig] = {}
        self._sub = None
        self._task: asyncio.Task | None = None
        self._inflight: set[asyncio.Task] = set()
        self._sem = asyncio.Semaphore(8)
        self._client: httpx.AsyncClient | None = None
        self._stopping = False

    def _make_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=DELIVER_TIMEOUT,
            headers={"User-Agent": get_settings().user_agent},
            trust_env=False,
        )

    async def start(self) -> None:
        try:
            self.hooks = {h.id: h for h in await self.store.list_webhooks() if h.active}
        except Exception as e:
            log.warning("webhook load failed: %s", e)
            self.hooks = {}
        if not self.hooks:
            return
        self._client = self._make_client()
        # unfiltered subscription: each hook applies its own DSL here, so
        # runtime registration/removal never touches the bus subscription
        self._sub = self.bus.subscribe(None)
        self._task = asyncio.create_task(self._consume(), name="webhooks")
        log.info("webhook dispatcher started (%d active)", len(self.hooks))

    async def stop(self) -> None:
        self._stopping = True
        if self._sub is not None:
            self.bus.unsubscribe(self._sub)
            self._sub = None
        for t in self._inflight:
            t.cancel()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # ---- runtime management (REST /api/webhooks) ----
    async def register(self, url: str, secret: str = "", dsl: str | None = None) -> WebhookConfig:
        import secrets as _secrets

        hook = WebhookConfig(
            id="hook_" + _secrets.token_hex(8),
            url=url,
            secret=secret,
            dsl=dsl or None,
        )
        await self.store.save_webhook(hook)
        self.hooks[hook.id] = hook
        if self._task is None:
            await self.start()
        return hook

    async def remove(self, hook_id: str) -> bool:
        hook = self.hooks.pop(hook_id, None)
        removed = await self.store.delete_webhook(hook_id)
        if not self.hooks and self._task is not None:
            await self.stop()
        return removed or hook is not None

    async def set_active(self, hook_id: str, active: bool) -> WebhookConfig | None:
        hook = self.hooks.get(hook_id)
        if hook is None:
            stored = [h for h in await self.store.list_webhooks() if h.id == hook_id]
            if not stored:
                return None
            hook = stored[0]
        hook.active = active
        if active:
            hook.failures = 0
            self.hooks[hook.id] = hook
        else:
            self.hooks.pop(hook.id, None)
        await self.store.save_webhook(hook)
        return hook

    # ---- delivery ----
    async def _consume(self) -> None:
        sub = self._sub
        try:
            while not self._stopping:
                try:
                    frame = await asyncio.wait_for(sub.queue.get(), timeout=5.0)
                except (asyncio.TimeoutError, TimeoutError):
                    continue
                if frame.get("t") != "event":
                    continue
                ev = frame.get("d") or {}
                for hook in list(self.hooks.values()):
                    if hook.dsl and not parse_dsl(hook.dsl).matches(ev):
                        continue
                    t = asyncio.create_task(self._deliver(hook, ev))
                    self._inflight.add(t)
                    t.add_done_callback(self._inflight.discard)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error("webhook consumer died: %s", e)

    async def _deliver(self, hook: WebhookConfig, ev: dict[str, Any]) -> None:
        if self._client is None:
            return
        body = orjson.dumps({"type": "EVENT_CREATED", "data": ev})
        headers = {
            "Content-Type": "application/json",
            "X-OmniPulse-Event": "EVENT_CREATED",
            "X-OmniPulse-Webhook": hook.id,
        }
        if hook.secret:
            headers["X-OmniPulse-Signature"] = sign(hook.secret, body)
        async with self._sem:
            for attempt, delay in enumerate((0.0,) + RETRY_DELAYS):
                if delay:
                    await asyncio.sleep(delay)
                if self._stopping:
                    return
                try:
                    r = await self._client.post(hook.url, content=body, headers=headers)
                    if r.status_code < 300:
                        hook.delivered += 1
                        hook.failures = 0
                        hook.last_status = r.status_code
                        hook.last_delivery_at = now_utc()
                        hook.last_error = None
                        await self._persist(hook)
                        return
                    hook.last_status = r.status_code
                    hook.last_error = f"HTTP {r.status_code}"
                except Exception as e:
                    hook.last_error = str(e)[:300]
        # all attempts exhausted
        hook.failures += 1
        hook.last_delivery_at = now_utc()
        await self._persist(hook)
        if hook.failures >= MAX_CONSECUTIVE_FAILURES:
            log.warning("webhook %s auto-deactivated after %d consecutive failures", hook.id, hook.failures)
            await self.set_active(hook.id, False)

    async def _persist(self, hook: WebhookConfig) -> None:
        try:
            await self.store.save_webhook(hook)
        except Exception as e:
            log.warning("webhook persist failed: %s", e)

    def public_view(self, hook: WebhookConfig) -> dict[str, Any]:
        out = hook.model_dump(mode="json")
        out["secret"] = "***" if hook.secret else ""
        return out
