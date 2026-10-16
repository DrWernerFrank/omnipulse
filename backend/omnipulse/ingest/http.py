"""Polite HTTP: shared connection pool, per-domain token buckets, conditional GETs, retry hints."""

from __future__ import annotations

import asyncio
import email.utils
import random
import time
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx

# requests/second, burst — tuned to each provider's published or observed limits
DOMAIN_LIMITS: dict[str, tuple[float, float]] = {
    "sec.gov": (8.0, 10.0),  # SEC fair-access: <= 10 req/s
    "news.google.com": (0.5, 3.0),
    "reddit.com": (0.15, 2.0),  # anonymous RSS tolerates ~10 req/min
    "services.nvd.nist.gov": (5 / 30, 2.0),  # 5 req / 30 s without an API key
    "api.gdeltproject.org": (0.2, 1.0),  # 1 req / 5 s
    "hacker-news.firebaseio.com": (25.0, 50.0),
    "github.com": (0.5, 3.0),
    "t.me": (0.5, 2.0),
    "gamma-api.polymarket.com": (2.0, 4.0),
    "api.elections.kalshi.com": (2.0, 4.0),
    "public.api.bsky.app": (5.0, 10.0),
}
DEFAULT_LIMIT = (2.0, 5.0)


def registrable_domain(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    for d in DOMAIN_LIMITS:
        if host == d or host.endswith("." + d):
            return d
    return host


class FetchError(Exception):
    def __init__(self, message: str, status: int | None = None, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after


class TokenBucket:
    def __init__(self, rate: float, capacity: float) -> None:
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()
        self.blocked_until = 0.0  # set by 429 / Retry-After

    async def acquire(self) -> None:
        async with self.lock:
            while True:
                now = time.monotonic()
                if now < self.blocked_until:
                    wait = self.blocked_until - now
                    if wait > 5:
                        # Don't park pollers on the lock for minutes; let the scheduler back off.
                        raise FetchError("rate limited (cooling down)", 429, wait)
                    await asyncio.sleep(wait)
                    continue
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)


@dataclass
class FetchResult:
    status: int
    url: str
    content: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)
    etag: str | None = None
    last_modified: str | None = None
    elapsed_ms: float = 0.0

    @property
    def not_modified(self) -> bool:
        return self.status == 304

    def text(self) -> str:
        return self.content.decode("utf-8", errors="replace")


def _retry_after(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            dt = email.utils.parsedate_to_datetime(value)
            return max(0.0, dt.timestamp() - time.time())
        except Exception:
            return None


class Fetcher:
    def __init__(self, user_agent: str, timeout: float = 15.0) -> None:
        self.user_agent = user_agent
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout, connect=min(timeout, 8.0)),
            follow_redirects=True,
            http2=True,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100, keepalive_expiry=300),
            headers={
                "User-Agent": user_agent,
                "Accept-Encoding": "gzip, deflate",
                "Accept": "application/rss+xml, application/atom+xml, application/json, text/xml, text/html;q=0.9, */*;q=0.8",
            },
        )
        self.buckets: dict[str, TokenBucket] = {}

    def bucket(self, url: str) -> TokenBucket:
        d = registrable_domain(url)
        b = self.buckets.get(d)
        if b is None:
            rate, cap = DOMAIN_LIMITS.get(d, DEFAULT_LIMIT)
            b = self.buckets[d] = TokenBucket(rate, cap)
        return b

    async def get(
        self,
        url: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        headers: dict[str, str] | None = None,
        params: dict | None = None,
        max_bytes: int = 15_000_000,
    ) -> FetchResult:
        return await self.request("GET", url, etag=etag, last_modified=last_modified, headers=headers,
                                  params=params, max_bytes=max_bytes)

    async def request(
        self,
        method: str,
        url: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        headers: dict[str, str] | None = None,
        params: dict | None = None,
        data: dict | None = None,
        max_bytes: int = 15_000_000,
    ) -> FetchResult:
        bucket = self.bucket(url)
        await bucket.acquire()
        h = dict(headers or {})
        if etag:
            h["If-None-Match"] = etag
        if last_modified:
            h["If-Modified-Since"] = last_modified
        t0 = time.perf_counter()
        for attempt in range(2):
            try:
                return await self._once(method, url, h, params, data, max_bytes, bucket, etag, last_modified, t0)
            except FetchError as exc:
                stale = isinstance(exc.__cause__, (httpx.RemoteProtocolError, httpx.ReadError, httpx.ConnectError))
                if attempt or method != "GET" or not stale:
                    raise
                await asyncio.sleep(0.2 + random.random() * 0.5)
        raise AssertionError("unreachable")

    async def _once(self, method, url, h, params, data, max_bytes, bucket, etag, last_modified, t0) -> FetchResult:
        try:
            async with self.client.stream(method, url, headers=h, params=params, data=data) as resp:
                if resp.status_code == 304:
                    return FetchResult(304, str(resp.url), headers=dict(resp.headers), etag=etag,
                                       last_modified=last_modified, elapsed_ms=(time.perf_counter() - t0) * 1000)
                if resp.status_code in (429, 503):
                    ra = _retry_after(resp.headers.get("Retry-After"))
                    pause = ra if ra is not None else 30 + random.random() * 30
                    bucket.blocked_until = time.monotonic() + min(pause, 900)
                    raise FetchError(f"HTTP {resp.status_code} (rate limited)", resp.status_code, pause)
                if resp.status_code >= 400:
                    raise FetchError(f"HTTP {resp.status_code}", resp.status_code)
                chunks, size = [], 0
                async for chunk in resp.aiter_bytes():
                    size += len(chunk)
                    if size > max_bytes:
                        raise FetchError(f"response exceeds {max_bytes} bytes")
                    chunks.append(chunk)
                return FetchResult(
                    resp.status_code,
                    str(resp.url),
                    b"".join(chunks),
                    dict(resp.headers),
                    resp.headers.get("ETag"),
                    resp.headers.get("Last-Modified"),
                    (time.perf_counter() - t0) * 1000,
                )
        except httpx.TimeoutException as exc:
            raise FetchError(f"timeout: {type(exc).__name__}") from exc
        except httpx.HTTPError as exc:
            raise FetchError(f"{type(exc).__name__}: {exc}") from exc

    async def close(self) -> None:
        await self.client.aclose()


def backoff_delay(failures: int, base: float, cap: float) -> float:
    """Exponential backoff with full jitter (AWS architecture blog variant)."""
    ceiling = min(cap, base * (2 ** min(failures, 16)))
    return random.uniform(base, max(base, ceiling))
