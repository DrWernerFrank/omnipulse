"""RSS / Atom / RDF connector with conditional GET and WebSub (PubSubHubbub) push upgrade."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import feedparser
import orjson

from ..schema import RawItem
from .base import Connector, struct_to_dt

log = logging.getLogger(__name__)


def parse_feed(content: bytes) -> Any:
    return feedparser.parse(content, sanitize_html=False, resolve_relative_uris=False)


def entries_to_items(conn: Connector, parsed: Any) -> list[RawItem]:
    params = conn.cfg.params
    aggregator = params.get("aggregator")
    items: list[RawItem] = []
    for e in parsed.entries:
        link = e.get("link") or ""
        if not link:
            for lk in e.get("links", ()):
                if lk.get("href"):
                    link = lk["href"]
                    break
        title = (e.get("title") or "").strip()
        if not link or not title:
            continue
        guid = e.get("id") or e.get("guid") or link
        summary = e.get("summary") or e.get("description") or ""
        content_html = ""
        if e.get("content"):
            content_html = max((c.get("value", "") for c in e.content), key=len, default="")
        hints: dict[str, Any] = {}
        if aggregator == "gnews":
            publisher = (e.get("source") or {}).get("title") or params.get("publisher", "")
            if publisher and title.endswith(" - " + publisher):
                title = title[: -(len(publisher) + 3)].rstrip()
            elif " - " in title:
                title = title.rsplit(" - ", 1)[0].rstrip()
            if publisher:
                hints["display_source"] = publisher
            summary = ""  # Google News summaries are just the title + publisher link
        if aggregator == "reddit":
            m = re.search(r'<a href="([^"]+)">\[link\]</a>', content_html or summary)
            if m and "reddit.com" not in m.group(1):
                hints["external_url"] = m.group(1)
        if strip := params.get("strip_title_prefix"):
            title = re.sub(strip, "", title).strip()
        published = struct_to_dt(e.get("published_parsed") or e.get("updated_parsed"))
        items.append(
            conn.item(
                guid=str(guid),
                url=link,
                title=title,
                summary=summary,
                content_html=content_html,
                author=e.get("author", "") or "",
                published_at=published,
                hints=hints,
            )
        )
    return items


class RSSConnector(Connector):
    kind = "rss"

    async def poll(self) -> list[RawItem]:
        res = await self.conditional_get(self.cfg.url)
        if res.not_modified:
            return []
        parsed = await asyncio.to_thread(parse_feed, res.content)
        if parsed.bozo and not parsed.entries:
            raise ValueError(f"unparseable feed: {getattr(parsed, 'bozo_exception', 'unknown')}")
        await self._maybe_websub(parsed)
        return self.post_process(entries_to_items(self, parsed))

    def post_process(self, items: list[RawItem]) -> list[RawItem]:
        return items

    # -- WebSub ----------------------------------------------------------------------
    async def _maybe_websub(self, parsed: Any) -> None:
        base = self.ctx.settings.public_base_url.rstrip("/")
        if not base or not self.cfg.params.get("websub", True):
            return
        links = parsed.feed.get("links", []) if hasattr(parsed, "feed") else []
        hub = next((lk.get("href") for lk in links if lk.get("rel") == "hub"), None)
        if not hub:
            return
        topic = next((lk.get("href") for lk in links if lk.get("rel") == "self"), None) or self.cfg.url
        now = time.time()
        if self.state.get("websub_expires", 0) > now + 3600 or self.state.get("websub_requested", 0) > now - 3600:
            return
        lease = 7 * 86400
        try:
            res = await self.ctx.fetcher.request(
                "POST",
                hub,
                data={
                    "hub.mode": "subscribe",
                    "hub.topic": topic,
                    "hub.callback": f"{base}/websub/callback/{self.cfg.id}",
                    "hub.lease_seconds": str(lease),
                },
            )
            self.state.update(websub_hub=hub, websub_topic=topic, websub_requested=now)
            if res.status in (202, 204):
                # The hub verifies asynchronously; assume the lease once accepted.
                self.state["websub_expires"] = now + lease
                log.info("websub subscription requested for %s via %s", self.cfg.id, hub)
        except Exception as exc:
            log.info("websub subscribe failed for %s: %s", self.cfg.id, exc)
            self.state["websub_requested"] = now

    @property
    def push_active(self) -> bool:
        return self.state.get("websub_expires", 0) > time.time()


# --------------------------------------------------------------------------------------
# SEC EDGAR — the "getcurrent" Atom feed, enriched with CIK -> ticker mapping
# --------------------------------------------------------------------------------------

FORM_URGENCY = {
    "8-K": 6, "8-K/A": 5, "6-K": 5, "SC 13D": 6, "SC 13D/A": 5, "SC 13G": 4, "S-1": 5, "S-4": 6,
    "10-K": 5, "10-Q": 4, "4": 4, "13F-HR": 3, "DEF 14A": 3, "425": 5, "SC TO-T": 7,
}
_EDGAR_TITLE = re.compile(r"^(?P<form>[\w\-/ ]+?) - (?P<company>.+?) \((?P<cik>\d{10})\)\s*\((?P<role>[^)]+)\)")
_8K_ITEMS = {
    "1.01": "Entry into a Material Definitive Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition",
    "2.02": "Results of Operations (earnings)",
    "2.05": "Exit / Restructuring Costs",
    "2.06": "Material Impairments",
    "3.01": "Delisting Notice",
    "4.02": "Non-Reliance on Prior Financials",
    "5.01": "Change in Control",
    "5.02": "Officer / Director Departure or Appointment",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
}
_HIGH_ITEMS = {"1.03", "2.01", "2.02", "3.01", "4.02", "5.01"}


class SecEdgarConnector(RSSConnector):
    kind = "sec_edgar"
    _cik_map: dict[str, str] | None = None
    _cik_loaded = 0.0
    _cik_lock = asyncio.Lock()

    @property
    def headers(self) -> dict[str, str]:
        return {"User-Agent": self.ctx.settings.sec_user_agent, **self.cfg.headers}

    async def _load_ciks(self) -> None:
        cls = SecEdgarConnector
        async with cls._cik_lock:
            if cls._cik_map is not None and time.time() - cls._cik_loaded < 86400:
                return
            try:
                res = await self.ctx.fetcher.get(
                    "https://www.sec.gov/files/company_tickers.json", headers=self.headers
                )
                data = orjson.loads(res.content)
                cls._cik_map = {str(v["cik_str"]).zfill(10): v["ticker"] for v in data.values()}
            except Exception as exc:
                log.warning("SEC ticker map unavailable: %s", exc)
                cls._cik_map = cls._cik_map or {}
            cls._cik_loaded = time.time()

    async def poll(self) -> list[RawItem]:
        await self._load_ciks()
        return await super().poll()

    def post_process(self, items: list[RawItem]) -> list[RawItem]:
        out = []
        cik_map = SecEdgarConnector._cik_map or {}
        for it in items:
            m = _EDGAR_TITLE.match(it.title)
            if not m:
                out.append(it)
                continue
            form, company, cik = m.group("form").strip(), m.group("company").strip(), m.group("cik")
            ticker = cik_map.get(cik)
            # Insider (Form 4) feeds list both issuer and reporting person; keep issuer rows only.
            if form == "4" and m.group("role").lower().startswith("reporting"):
                continue
            items_found = re.findall(r"Item (\d\.\d\d)", it.summary)
            desc = "; ".join(_8K_ITEMS[i] for i in items_found if i in _8K_ITEMS)
            urgency = FORM_URGENCY.get(form, 3) + (2 if set(items_found) & _HIGH_ITEMS else 0)
            title = f"{company}{f' (${ticker})' if ticker else ''} files {form}"
            if desc:
                title += f": {desc}"
            it.title = title
            it.hints.update(
                categories=["Regulatory", "Equities"],
                tickers=[ticker] if ticker else [],
                entities=[{"name": company.title(), "type": "ORG"}],
                urgency_floor=min(urgency, 9),
                verification="CONFIRMED",
                form_type=form,
            )
            out.append(it)
        return out
