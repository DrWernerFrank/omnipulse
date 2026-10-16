"""WebSub (PubSubHubbub) push subscriptions: hubs deliver new entries the
moment they exist, eliminating poll latency entirely for feeds that advertise
a hub link.

Activation requires:
  OMNIPULSE_WEBSUB_ENABLED=true
  OMNIPULSE_PUBLIC_URL=https://your.host   (must be reachable from the hub)

Without both, every function here is a no-op and the system polls normally.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
from typing import Any

import feedparser

from .config import get_settings
from .schemas import RawItem

log = logging.getLogger("omnipulse.websub")


def push_active() -> bool:
    """True when push mode is configured (enabled + non-local public URL)."""
    s = get_settings()
    if not s.websub_enabled:
        return False
    url = (s.public_url or "").lower()
    return url.startswith("http") and "localhost" not in url and "127.0.0.1" not in url


def discover_hub(parsed: Any) -> str | None:
    """Pull <link rel="hub"> out of a parsed feed (None => feed has no hub)."""
    try:
        links = parsed.feed.get("links") or []
    except Exception:
        return None
    for link in links:
        if not isinstance(link, dict):
            continue
        href = link.get("href") or ""
        if link.get("rel") == "hub" and href.startswith("http"):
            return href
    return None


def new_secret() -> str:
    return secrets.token_hex(16)


async def subscribe(hub: str, topic: str, callback: str, secret: str) -> bool:
    """Request a push subscription (async verification). Non-fatal on failure."""
    import httpx

    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as c:
            r = await c.post(
                hub,
                data={
                    "hub.mode": "subscribe",
                    "hub.topic": topic,
                    "hub.callback": callback,
                    "hub.verify": "async",
                    "hub.lease_seconds": str(get_settings().websub_lease_sec),
                    "hub.secret": secret,
                },
            )
            ok = r.status_code in (200, 202, 204)
            if not ok:
                log.debug("websub hub %s rejected subscribe: %d %s", hub, r.status_code, r.text[:200])
            return ok
    except Exception as e:
        log.debug("websub subscribe failed (%s): %s", hub, e)
        return False


def verify_signature(secret: str, body: bytes, header: str | None) -> bool:
    """X-Hub-Signature: sha1=<hmac-sha1(secret, body)> (RFC 8030 style)."""
    if not header or "=" not in header:
        return False
    algo, _, digest = header.partition("=")
    algo = algo.strip().lower()
    if algo not in ("sha1", "sha256"):
        return False
    expected = hmac.new(
        secret.encode(), body, hashlib.sha1 if algo == "sha1" else hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, digest.strip().lower())


def parse_push(body: bytes, source_id: str, base_url: str = "") -> list[RawItem]:
    """Parse a pushed atom/rss payload into RawItems (same shape as polling)."""
    parsed = feedparser.parse(body)
    if parsed.bozo and not parsed.entries:
        return []
    from .connectors import _clean_feed_title, _feed_date

    is_gnews = "news.google.com" in base_url
    items: list[RawItem] = []
    for e in parsed.entries[:60]:
        url = ""
        for key in ("link", "guid", "id"):
            v = e.get(key)
            if isinstance(v, str) and v.startswith("http"):
                url = v
                break
        title = _clean_feed_title(e.get("title", "") or "", is_gnews)
        if not title or len(title) < 10:
            continue
        items.append(
            RawItem(
                source_id=source_id,
                external_id=str(e.get("id") or e.get("guid") or url or title)[:300],
                title=title,
                url=url,
                published_at=_feed_date(e),
                body_html=e.get("summary") or e.get("description") or "",
            )
        )
    return items
