"""Social / community firehoses: Bluesky Jetstream (streaming), Telegram public channels,
plus optional key-gated connectors (NewsAPI, X lists, Whale Alert)."""

from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import orjson
from bs4 import BeautifulSoup

from ..schema import RawItem, utcnow
from .base import Connector, Emit, parse_dt

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# Bluesky Jetstream
# --------------------------------------------------------------------------------------

DEFAULT_BSKY_PATTERN = r"(?:^|\s)(?:BREAKING|JUST IN|FLASH|URGENT|DEVELOPING)\b[:\s—-]"


class BlueskyJetstreamConnector(Connector):
    """Consumes the Jetstream JSON firehose and keeps only breaking-news posts.

    Two filters (OR-ed): ``params.dids`` — always keep posts from these accounts (journalists,
    OSINT); ``params.pattern`` — regex the post text must match (default: BREAKING/JUST IN
    markers). A byte-level pre-check rejects >99% of the firehose before JSON parsing.
    """

    kind = "bluesky"
    streaming = True

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        p = self.cfg.params
        self.dids: set[str] = set(p.get("dids", []))
        self.pattern = re.compile(p.get("pattern", DEFAULT_BSKY_PATTERN))
        self.langs = set(p.get("langs", ["en"]))
        self.min_len = int(p.get("min_length", 40))
        self.max_per_minute = int(p.get("max_per_minute", 30))
        self._recent: deque[float] = deque()
        self._handles: dict[str, str] = {}
        kws = p.get("prefilter", ["BREAKING", "JUST IN", "FLASH", "URGENT", "DEVELOPING"])
        self._prefilter = [k.encode() for k in kws]

    def _url(self) -> str:
        base = self.cfg.url or "wss://jetstream2.us-east.bsky.network/subscribe"
        q: list[tuple[str, str]] = [("wantedCollections", "app.bsky.feed.post")]
        if self.cfg.params.get("only_dids") and self.dids:
            q += [("wantedDids", d) for d in list(self.dids)[:10000]]
        if cursor := self.state.get("cursor"):
            # rewind 5 s to cover the reconnect gap; Jetstream cursors are unix microseconds
            q.append(("cursor", str(int(cursor) - 5_000_000)))
        return f"{base}?{urlencode(q)}"

    async def _handle(self, did: str) -> str:
        if did in self._handles:
            return self._handles[did]
        try:
            res = await self.ctx.fetcher.get(
                "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile", params={"actor": did}
            )
            handle = orjson.loads(res.content).get("handle", did)
        except Exception:
            handle = did
        if len(self._handles) > 20000:
            self._handles.clear()
        self._handles[did] = handle
        return handle

    def _accept(self, raw: bytes) -> dict[str, Any] | None:
        if self.dids:
            did_hit = any(d.encode() in raw[:120] for d in self.dids) if len(self.dids) < 50 else True
        else:
            did_hit = False
        if not did_hit and not any(k in raw for k in self._prefilter):
            return None
        msg = orjson.loads(raw)
        commit = msg.get("commit") or {}
        if msg.get("kind") != "commit" or commit.get("operation") != "create":
            return None
        rec = commit.get("record") or {}
        text = rec.get("text") or ""
        if len(text) < self.min_len or rec.get("reply"):
            return None
        langs = set(rec.get("langs") or [])
        if self.langs and langs and not langs & self.langs:
            return None
        if msg.get("did") not in self.dids and not self.pattern.search(text):
            return None
        return msg

    async def stream(self, emit: Emit) -> None:
        import websockets

        async with websockets.connect(self._url(), max_size=2**22, ping_interval=20, open_timeout=15) as ws:
            log.info("bluesky jetstream connected (%s)", self.cfg.id)
            last_cursor_save = 0.0
            async for raw in ws:
                if isinstance(raw, str):
                    raw = raw.encode()
                try:
                    msg = self._accept(raw)
                except orjson.JSONDecodeError:
                    continue
                now = time.monotonic()
                if now - last_cursor_save > 2:
                    m = re.search(rb'"time_us":(\d+)', raw)
                    if m:
                        self.state["cursor"] = int(m.group(1))
                        last_cursor_save = now
                if msg is None:
                    continue
                while self._recent and now - self._recent[0] > 60:
                    self._recent.popleft()
                if len(self._recent) >= self.max_per_minute:
                    continue
                self._recent.append(now)
                await emit(await self._to_item(msg))

    async def _to_item(self, msg: dict[str, Any]) -> RawItem:
        did, commit = msg["did"], msg["commit"]
        rec = commit["record"]
        text = rec.get("text", "")
        handle = await self._handle(did)
        first_line = text.strip().split("\n", 1)[0]
        title = first_line if len(first_line) <= 220 else first_line[:217].rsplit(" ", 1)[0] + "…"
        ext = ((rec.get("embed") or {}).get("external") or {})
        summary = f"<p>{html.escape(text)}</p>"
        if ext.get("uri"):
            summary += f'<p>Link: <a href="{html.escape(ext["uri"])}">{html.escape(ext.get("title") or ext["uri"])}</a></p>'
        return self.item(
            guid=f"at://{did}/{commit['collection']}/{commit['rkey']}",
            url=f"https://bsky.app/profile/{handle}/post/{commit['rkey']}",
            title=title,
            summary=summary,
            author=handle,
            published_at=parse_dt(rec.get("createdAt")) or utcnow(),
            hints={"display_source": f"@{handle}", "verification": "UNVERIFIED",
                   "watched_account": did in self.dids},
        )


# --------------------------------------------------------------------------------------
# Telegram public channels via the t.me/s/<channel> web preview (no MTProto credentials)
# --------------------------------------------------------------------------------------


def _parse_telegram(page: str, channel: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(page, "lxml")
    posts = []
    for msg in soup.select("div.tgme_widget_message[data-post]"):
        text_el = msg.select_one("div.tgme_widget_message_text")
        time_el = msg.select_one("a.tgme_widget_message_date time[datetime]")
        if not text_el:
            continue
        for br in text_el.find_all("br"):
            br.replace_with("\n")
        posts.append({
            "post": msg["data-post"],
            "text": text_el.get_text("", strip=True),
            "html": str(text_el),
            "time": time_el["datetime"] if time_el else None,
        })
    return posts


class TelegramChannelConnector(Connector):
    kind = "telegram"

    async def poll(self) -> list[RawItem]:
        channel = self.cfg.params["channel"]
        res = await self.ctx.fetcher.get(f"https://t.me/s/{channel}", headers={"Accept": "text/html"})
        posts = await asyncio.to_thread(_parse_telegram, res.text(), channel)
        out = []
        for p in posts:
            first = p["text"].split("\n", 1)[0].strip()
            if len(first) < 15:
                first = p["text"][:200].replace("\n", " ")
            out.append(
                self.item(
                    guid=f"tg:{p['post']}",
                    url=f"https://t.me/{p['post']}",
                    title=first[:220],
                    summary=p["html"],
                    published_at=parse_dt(p["time"]),
                    hints={"verification": "UNVERIFIED", "display_source": f"t.me/{channel}"},
                )
            )
        return out


# --------------------------------------------------------------------------------------
# Optional key-gated connectors
# --------------------------------------------------------------------------------------


class NewsApiConnector(Connector):
    kind = "newsapi"

    async def poll(self) -> list[RawItem]:
        p = dict(self.cfg.params) or {"language": "en", "category": "general"}
        p.setdefault("pageSize", 100)
        res = await self.ctx.fetcher.get(
            self.cfg.url or "https://newsapi.org/v2/top-headlines",
            params={k: str(v) for k, v in p.items()},
            headers={"X-Api-Key": self.ctx.settings.newsapi_key},
        )
        out = []
        for a in orjson.loads(res.content).get("articles", []):
            if not a.get("url") or not a.get("title") or a["title"] == "[Removed]":
                continue
            out.append(self.item(
                guid=a["url"], url=a["url"], title=a["title"], summary=a.get("description") or "",
                content_html=a.get("content") or "", author=a.get("author") or "",
                published_at=parse_dt(a.get("publishedAt")),
                hints={"display_source": (a.get("source") or {}).get("name", "")},
            ))
        return out


class XListConnector(Connector):
    kind = "x_list"

    async def poll(self) -> list[RawItem]:
        list_id = self.cfg.params["list_id"]
        res = await self.ctx.fetcher.get(
            f"https://api.x.com/2/lists/{list_id}/tweets",
            params={"tweet.fields": "created_at,author_id", "expansions": "author_id",
                    "user.fields": "username", "max_results": "100"},
            headers={"Authorization": f"Bearer {self.ctx.settings.x_bearer_token}"},
        )
        data = orjson.loads(res.content)
        users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
        out = []
        for t in data.get("data", []):
            user = users.get(t.get("author_id"), "i")
            text = t.get("text", "")
            out.append(self.item(
                guid=f"x:{t['id']}", url=f"https://x.com/{user}/status/{t['id']}",
                title=text.split("\n", 1)[0][:220], summary=f"<p>{html.escape(text)}</p>", author=user,
                published_at=parse_dt(t.get("created_at")),
                hints={"display_source": f"@{user}", "verification": "UNVERIFIED"},
            ))
        return out


class WhaleAlertConnector(Connector):
    kind = "whale_alert"

    async def poll(self) -> list[RawItem]:
        start = int(self.state.get("cursor") or time.time() - 600)
        res = await self.ctx.fetcher.get(
            "https://api.whale-alert.io/v1/transactions",
            params={"api_key": self.ctx.settings.whale_alert_key, "start": str(start),
                    "min_value": str(self.cfg.params.get("min_value", 10_000_000))},
        )
        data = orjson.loads(res.content)
        self.state["cursor"] = int(time.time()) - 30
        out = []
        for tx in data.get("transactions", []) or []:
            sym = tx.get("symbol", "").upper()
            frm = (tx.get("from") or {}).get("owner") or (tx.get("from") or {}).get("owner_type", "unknown")
            to = (tx.get("to") or {}).get("owner") or (tx.get("to") or {}).get("owner_type", "unknown")
            usd = float(tx.get("amount_usd") or 0)
            out.append(self.item(
                guid=f"whale:{tx.get('hash')}:{tx.get('id')}",
                url=f"https://whale-alert.io/transaction/{tx.get('blockchain')}/{tx.get('hash')}",
                title=f"Whale Alert: {tx.get('amount', 0):,.0f} ${sym} (${usd / 1e6:,.1f}M) moved {frm} → {to}",
                summary="", published_at=datetime.fromtimestamp(tx.get("timestamp", time.time()), tz=timezone.utc),
                hints={"categories": ["Crypto"], "tickers": [sym], "urgency_bonus": 2 if usd > 100e6 else 0,
                       "verification": "CONFIRMED"},
            ))
        return out
