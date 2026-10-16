"""Streaming (push) connectors.

- Bluesky Jetstream: wss firehose with server-side subscribeRepos filtering.
  Filtered for journalist/OSINT-ish patterns and breaking keywords; unverified
  accounts still pass but land with credibility_tier 3 (UNVERIFIED).
- Telegram: optional MTProto listener via Telethon when api id/hash configured.

Each stream connector runs one long-lived task per source and pushes RawItems
into the same pipeline as polled sources.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Awaitable, Callable

import orjson
import websockets

from .config import get_settings
from .schemas import RawItem, SourceConfig, now_utc

log = logging.getLogger("omnipulse.stream")

ItemSink = Callable[[RawItem, SourceConfig], Awaitable[None]]

# breaking-signal keywords for social firehose gating
BREAK_RE = re.compile(
    r"(breaking|just in|blast|explosion|missile|airstrike|shelling|killed|dead|dies|resign"
    r"|earthquake|wildfire|flood|evacuat|emergency|coup|shoot|attack|crash|plunge|surge"
    r"|halt(ed)?|down|outage|breach|hack|zero-?day|fed |fomc|cpi|rate (cut|hike)|tape|flash)",
    re.I,
)
JOURNALIST_HINT = re.compile(
    r"(journalist|reporter|news|osint|geoint|analyst|editor|bureau|correspondent|writer)",
    re.I,
)


def _bluesky_worthy(commit: dict[str, Any]) -> dict[str, Any] | None:
    """Return a post dict if it passes the newsworthiness gate."""
    try:
        if commit.get("kind") != "commit":
            return None
        op = (commit.get("commit") or {}).get("ops") or [{}]
        for o in op:
            if o.get("action") == "create" and o.get("record", {}).get("$type") == "app.bsky.feed.post":
                rec = o["record"]
                text = (rec.get("text") or "").strip()
                if not text or len(text) < 25:
                    return None
                if rec.get("reply"):  # skip replies; top-level posts only
                    return None
                return {"did": commit.get("did"), "text": text, "record": rec}
    except Exception:
        return None
    return None


async def bluesky_stream(cfg: SourceConfig, sink: ItemSink, stop: asyncio.Event) -> None:
    """One websocket to Jetstream; applies client-side filters, then pipeline."""
    url = cfg.url or "wss://jetstream2.us-east.bsky.network/subscribe"
    settings = get_settings()
    keywords = cfg.params.get("break_keywords")
    if keywords:
        pattern = re.compile("|".join(keywords), re.I)
    else:
        pattern = BREAK_RE

    backoff = 1.0
    while not stop.is_set():
        try:
            async with websockets.connect(url, max_size=2_000_000, ping_interval=20) as ws:
                log.info("bluesky jetstream connected (%s)", cfg.id)
                backoff = 1.0
                while not stop.is_set():
                    raw = await ws.recv()
                    try:
                        commit = orjson.loads(raw)
                    except Exception:
                        continue
                    post = _bluesky_worthy(commit)
                    if post is None:
                        continue
                    text = post["text"]
                    if not pattern.search(text):
                        continue
                    did = post["did"] or ""
                    rkey = (post["record"].get("rkey") if isinstance(post["record"], dict) else None) or now_utc().strftime("%Y%m%d%H%M%S%f")  # type: ignore[union-attr]
                    item = RawItem(
                        source_id=cfg.id,
                        external_id=f"bsky_{did}_{rkey}"[:300],
                        title=text[:280],
                        url=f"https://bsky.app/profile/{did}/post/{rkey}",
                        published_at=now_utc(),
                        body_text=text,
                        extra={"hint_categories": ["Breaking", "Geopolitics"]},
                    )
                    try:
                        await sink(item, cfg)
                    except Exception as e:
                        log.debug("pipeline rejected bluesky item: %s", e)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if stop.is_set():
                return
            log.warning("bluesky stream %s disconnected (%s); reconnecting in %.1fs", cfg.id, e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# --------------------------------------------------------------------------- #
# Telegram (optional; requires api id/hash in env + telethon installed)
# --------------------------------------------------------------------------- #
async def telegram_stream(cfg: SourceConfig, sink: ItemSink, stop: asyncio.Event) -> None:
    settings = get_settings()
    if not (settings.telegram_api_id and settings.telegram_api_hash):
        log.info("telegram source %s paused (no api credentials)", cfg.id)
        return
    try:
        from telethon import TelegramClient, events  # type: ignore
    except ImportError:
        log.warning("telethon not installed; telegram source %s inactive", cfg.id)
        return

    channels = cfg.params.get("channels", [])
    client = TelegramClient(
        "omnipulse", settings.telegram_api_id, settings.telegram_api_hash
    )

    @client.on(events.NewMessage(channels=channels if channels else None))
    async def _on_msg(event):  # noqa: ANN001
        try:
            text = (event.raw_text or "").strip()
            if len(text) < 25:
                return
            chat = await event.get_chat()
            channel_name = getattr(chat, "title", None) or cfg.name
            item = RawItem(
                source_id=cfg.id,
                external_id=f"tg_{event.id}",
                title=text.splitlines()[0][:280],
                url=f"https://t.me/{getattr(chat, 'username', '')}/{event.id}" if getattr(chat, "username", None) else "",
                published_at=getattr(event.message, "date", None),
                body_text=text[:4000],
                extra={"channel": channel_name, "hint_categories": ["Breaking", "Geopolitics"]},
            )
            await sink(item, cfg)
        except Exception as e:
            log.debug("telegram item skipped: %s", e)

    await client.start()  # type: ignore[func-returns-value]
    log.info("telegram listener active on %s channels (%s)", len(channels), cfg.id)
    while not stop.is_set():
        await asyncio.sleep(5)
    await client.disconnect()
