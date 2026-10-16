"""Shared async HTTP client for all connectors.

Politeness & resilience:
- Per-host token-bucket rate limiting (settings.per_host_rate / burst).
- Conditional requests: ETag / If-Modified-Since stored per URL, 304 => zero body.
- Exponential backoff with full jitter on 429/5xx/network errors (max 4 tries).
- Global concurrency cap; one connection pool reused by everything.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Any

import httpx

from .config import get_settings

log = logging.getLogger("omnipulse.http")


class TokenBucket:
    __slots__ = ("rate", "capacity", "tokens", "updated")

    def __init__(self, rate: float, capacity: int):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(capacity)
        self.updated = time.monotonic()

    async def take(self) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
            self.updated = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            await asyncio.sleep((1.0 - self.tokens) / self.rate)


class HttpPool:
    _instance: "HttpPool | None" = None

    def __init__(self) -> None:
        s = get_settings()
        self.client: httpx.AsyncClient | None = None
        self._buckets: dict[str, TokenBucket] = defaultdict(
            lambda: TokenBucket(s.per_host_rate, s.per_host_burst)
        )
        self._global = asyncio.Semaphore(s.ingest_concurrency)
        self._conditionals: dict[str, tuple[str | None, str | None]] = {}  # url -> (etag, last_modified)
        self._not_modified: dict[str, float] = {}
        self.user_agent = s.user_agent

    @classmethod
    def get(cls) -> "HttpPool":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def start(self) -> None:
        s = get_settings()
        if self.client is None:
            self.client = httpx.AsyncClient(
                timeout=httpx.Timeout(s.fetch_timeout_sec, connect=10.0),
                limits=httpx.Limits(
                    max_connections=256, max_keepalive_connections=128,
                    keepalive_expiry=s.keepalive_expiry_sec,
                ),
                follow_redirects=True,
                http2=s.http2,
            )

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    # ------------------------------------------------------------------ #
    async def fetch(
        self,
        url: str,
        *,
        source_headers: dict[str, str] | None = None,
        source_id: str = "",
        conditional: bool = True,
    ) -> httpx.Response | None:
        """Fetch with politeness + retries. Returns None on final failure or
        304 Not Modified (caller distinguishes via was_not_modified())."""
        s = get_settings()
        host = httpx.URL(url).host
        headers = {"User-Agent": self.user_agent}
        if source_headers:
            headers.update(source_headers)
        etag, last_mod = self._conditionals.get(url, (None, None))
        if conditional and etag:
            headers["If-None-Match"] = etag
        elif conditional and last_mod:
            headers["If-Modified-Since"] = last_mod

        last_err: Exception | None = None
        for attempt in range(4):
            await self._buckets[host].take()
            if self.client is None:
                await self.start()
            try:
                async with self._global:
                    resp = await self.client.get(url, headers=headers)
                if resp.status_code == 304:
                    self._not_modified[url] = time.monotonic()
                    return None
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise httpx.HTTPStatusError(
                        f"retryable {resp.status_code}", request=resp.request, response=resp
                    )
                resp.raise_for_status()
                if conditional:
                    new_etag = resp.headers.get("etag")
                    new_lm = resp.headers.get("last-modified")
                    if new_etag or new_lm:
                        self._conditionals[url] = (new_etag, new_lm)
                return resp
            except Exception as e:
                last_err = e
                if attempt == 3:
                    break
                base = min(2 ** attempt, 8)
                await asyncio.sleep(random.uniform(0, base) + 0.25)
        log.debug("fetch failed after retries: %s %s (%s)", source_id or host, url, last_err)
        return None

    # Convenience wrappers -------------------------------------------------
    async def fetch_text(self, url: str, source_headers: dict[str, str] | None = None,
                         source_id: str = "", conditional: bool = True) -> str | None:
        resp = await self.fetch(
            url, source_headers=source_headers, source_id=source_id, conditional=conditional
        )
        if resp is None:
            return None
        ct = resp.headers.get("content-type", "")
        try:
            text = resp.content.decode("utf-8", errors="replace")
        except Exception:
            return None
        if "charset" in ct and "utf-8" not in ct.lower():
            try:
                text = resp.content.decode(ct.split("charset=")[-1].split(";")[0].strip(), errors="replace")
            except Exception:
                pass
        return text

    async def fetch_json(self, url: str, source_headers: dict[str, str] | None = None,
                         source_id: str = "") -> Any | None:
        import orjson

        text = await self.fetch_text(url, source_headers=source_headers, source_id=source_id)
        if text is None:
            return None
        try:
            return orjson.loads(text)
        except Exception:
            return None

    def was_not_modified(self, url: str, within: float = 5.0) -> bool:
        ts = self._not_modified.get(url)
        return ts is not None and (time.monotonic() - ts) < within


# Module-level helpers used by the pipeline / connectors --------------------
async def fetch_text(url: str, source_cfg) -> str | None:
    pool = HttpPool.get()
    await pool.start()
    return await pool.fetch_text(url, source_headers=source_cfg.headers, source_id=source_cfg.id)
