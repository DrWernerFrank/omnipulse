"""Outbound agent webhooks: filtered, HMAC-signed, retried, auto-disabled when failing.

Uses a Redis consumer group on omni:events so each envelope is delivered exactly once
across any number of API replicas.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import socket
import time

import httpx
import orjson

from .. import bus as B
from ..filters import EventFilter, render
from ..schema import WebhookConfig

log = logging.getLogger(__name__)
MAX_FAILURES = 25


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookDispatcher:
    def __init__(self, bus, store, user_agent: str) -> None:
        self.bus = bus
        self.store = store
        self.client = httpx.AsyncClient(timeout=10, headers={"User-Agent": user_agent})
        self.hooks: list[tuple[WebhookConfig, EventFilter]] = []
        self.sem = asyncio.Semaphore(32)
        self.delivered = 0
        self.failed = 0
        self._tasks: list[asyncio.Task] = []
        self._loaded = 0.0

    async def reload(self) -> None:
        self.hooks = [(h, EventFilter.parse(h.dsl)) for h in await self.store.list_webhooks() if h.active]
        self._loaded = time.monotonic()

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._run(), name="webhooks")]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await self.client.aclose()

    async def _run(self) -> None:
        await self.reload()
        consumer = f"{socket.gethostname()}-{os.getpid()}"
        async for msg_id, env in self.bus.consume(B.EVENTS, "webhooks", consumer):
            try:
                if time.monotonic() - self._loaded > 30:
                    await self.reload()
                for hook, f in self.hooks:
                    if env.get("type") in hook.event_types and f.matches_envelope(env):
                        asyncio.create_task(self._deliver(hook, env))
            finally:
                await self.bus.ack(B.EVENTS, "webhooks", msg_id)

    async def _deliver(self, hook: WebhookConfig, env: dict) -> None:
        payload = render(env, hook.format)
        body = orjson.dumps(payload if isinstance(payload, dict) else {"type": env["type"], "line": payload})
        headers = {"Content-Type": "application/json", "X-OmniPulse-Event": env.get("type", ""),
                   "X-OmniPulse-Webhook": hook.id}
        if hook.secret:
            headers["X-OmniPulse-Signature"] = sign(hook.secret, body)
        async with self.sem:
            for attempt in range(3):
                try:
                    r = await self.client.post(hook.url, content=body, headers=headers)
                    if r.status_code < 300:
                        self.delivered += 1
                        if hook.failures:
                            hook.failures = 0
                            await self.store.save_webhook(hook)
                        return
                    if 400 <= r.status_code < 500 and r.status_code != 429:
                        break
                except httpx.HTTPError:
                    pass
                await asyncio.sleep((1, 4, 10)[attempt])
        self.failed += 1
        hook.failures += 1
        if hook.failures >= MAX_FAILURES:
            hook.active = False
            log.warning("webhook %s disabled after %d failures", hook.id, hook.failures)
            await self.reload_after_save(hook)
        elif hook.failures % 5 == 0:
            await self.store.save_webhook(hook)

    async def reload_after_save(self, hook: WebhookConfig) -> None:
        await self.store.save_webhook(hook)
        await self.reload()
