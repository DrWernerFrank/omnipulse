"""Source connectors: each turns a SourceConfig into RawItems.

Connector selection is by SourceConfig.type:
  rss | atom          -> feedparser over HttpPool (conditional GET)
  json               -> arbitrary REST/JSON endpoints with per-source params
  hn_frontpage       -> Hacker News Algolia front_page API
  hn_show            -> Hacker News Show HN (Algolia)
  arxiv              -> ArXiv Atom API by category
  gdelt              -> GDELT DOC 2.0 API
  sec_edgar          -> SEC EDGAR full-text/atom feeds
  polymarket         -> Polymarket Gamma API (probability shifts)
  kalshi             -> Kalshi public REST (probability shifts)
  github_releases    -> GitHub releases.atom per repo
  github_trending    -> GitHub search API (stars > N, recent push)
  nvd                -> NVD CVE JSON 2.0 API (recent CVEs)
  reddit             -> Reddit JSON listing (public .json endpoints)
  cloud_status       -> status-page RSS (typed via rss; alias kept for clarity)
  html               -> generic page scraper -> <a> link extraction (labs w/o RSS)
  cisa_kev           -> CISA Known Exploited Vulnerabilities catalog (merged from v1)
  telegram_web       -> public Telegram channels via t.me/s/ preview, no keys (merged from v1)
  bluesky            -> Bluesky Jetstream websocket firehose (stream connector)
  telegram           -> optional Telethon listener (requires api id/hash)
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Callable
from urllib.parse import quote_plus

import feedparser
import orjson
from bs4 import BeautifulSoup

from .http import HttpPool
from .schemas import RawItem, SourceConfig, now_utc

log = logging.getLogger("omnipulse.connectors")

ConnectResult = list[RawItem]
ConnectFn = Callable[[SourceConfig], Any]  # async def fetch(cfg) -> list[RawItem]

RE_ISO = re.compile(r"\d{4}-\d{2}-\d{2}")

# Google News RSS proxy titles look like "Actual headline - Publisher";
# strip the publisher suffix so dedup/enrichment see the real headline.
_GN_PUB_SUFFIX = re.compile(r"\s+-\s+[A-Za-z0-9 .&'()/-]{2,40}$")


def _clean_feed_title(title: str, is_google_news: bool) -> str:
    t = re.sub(r"\s+", " ", title or "").strip()
    if is_google_news and " - " in t:
        stripped = _GN_PUB_SUFFIX.sub("", t).strip()
        if len(stripped) >= 10:
            t = stripped
    return t


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _feed_date(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        st = entry.get(key)
        if st:
            try:
                return datetime(*st[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    return None


def _require(cfg: SourceConfig) -> bool:
    if cfg.requires_key:
        import os
        return bool(os.environ.get(cfg.requires_key))
    return True


async def _rss_family(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    text = await pool.fetch_text(cfg.url or "", source_headers=cfg.headers, source_id=cfg.id)
    if text is None:
        if pool.was_not_modified(cfg.url or ""):
            return []          # healthy, nothing new
        raise ConnectionError(f"fetch failed: {cfg.url}")
    parsed = feedparser.parse(text)
    if parsed.bozo and not parsed.entries:
        raise ValueError(f"unparseable feed: {getattr(parsed, 'bozo_exception', 'unknown')}")
    # remember any advertised WebSub hub so the engine can switch to push
    from .websub import discover_hub

    hub = discover_hub(parsed)
    if hub and cfg.state.get("websub_hub_found") != hub:
        cfg.state["websub_hub_found"] = hub
    is_gnews = "news.google.com" in (cfg.url or "")
    items: ConnectResult = []
    for e in parsed.entries[: cfg.params.get("max_items", 60)]:
        url = ""
        for key in ("link", "guid", "id"):
            v = e.get(key)
            if isinstance(v, str) and v.startswith("http"):
                url = v
                break
        title = _clean_feed_title(e.get("title", "") or "", is_gnews)
        if not title or len(title) < 10:
            continue  # junk/syndication-label-only entries
        body = e.get("summary") or e.get("description") or ""
        published = _feed_date(e)
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=str(e.get("id") or e.get("guid") or url or title)[:300],
                title=title,
                url=url,
                published_at=published,
                body_html=body if "<" in body else "",
                body_text=body if "<" not in body else "",
                extra={"hint_categories": cfg.params.get("hint_categories", [])},
            )
        )
    return items


# --------------------------------------------------------------------------- #
# JSON (generic)
# --------------------------------------------------------------------------- #
async def _json_generic(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    url = cfg.url or ""
    data = await pool.fetch_json(url, source_headers=cfg.headers, source_id=cfg.id)
    if data is None:
        raise ConnectionError(f"fetch failed: {url}")
    list_path = cfg.params.get("list_path", "")
    rows = data
    if list_path:
        for part in list_path.split("."):
            if isinstance(rows, dict):
                rows = rows.get(part, [])
            elif isinstance(rows, list) and part.isdigit():
                rows = rows[int(part)]
            else:
                rows = []
                break
    if not isinstance(rows, list):
        rows = [rows]
    f_title = cfg.params.get("title_field", "title")
    f_url = cfg.params.get("url_field", "url")
    f_date = cfg.params.get("date_field", "published_at")
    f_body = cfg.params.get("body_field", "body")
    items: ConnectResult = []
    for row in rows[: cfg.params.get("max_items", 60)]:
        if not isinstance(row, dict):
            continue
        title = str(row.get(f_title) or "").strip()
        url = str(row.get(f_url) or "").strip()
        if not title:
            continue
        published = None
        raw_date = row.get(f_date)
        if isinstance(raw_date, (int, float)):
            ts = raw_date / 1000 if raw_date > 10_000_000_000 else raw_date
            try:
                published = datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass
        elif isinstance(raw_date, str) and raw_date:
            try:
                published = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
            except Exception:
                pass
        body = row.get(f_body) or ""
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=str(row.get("id") or url or title)[:300],
                title=title,
                url=url if url.startswith("http") else "",
                published_at=published,
                body_text=str(body)[:6000] if body else "",
                extra={},
            )
        )
    return items


# --------------------------------------------------------------------------- #
# Hacker News (Algolia — no key, generous limits)
# --------------------------------------------------------------------------- #
async def _hn(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    tag = cfg.params.get("tag", "front_page")
    url = (
        f"https://hn.algolia.com/api/v1/search?tags={tag}&hitsPerPage={cfg.params.get('max_items', 50)}"
        if cfg.params.get("mode", "search") == "search"
        else f"https://hn.algolia.com/api/v1/search_by_date?tags={tag}&hitsPerPage={cfg.params.get('max_items', 50)}"
    )
    data = await pool.fetch_json(url, source_id=cfg.id)
    if not data:
        raise ConnectionError("hn algolia failed")
    items: ConnectResult = []
    for h in data.get("hits", [])[: cfg.params.get("max_items", 50)]:
        title = h.get("title") or h.get("story_title") or ""
        if not title:
            continue
        url = h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID')}"
        points = h.get("points") or 0
        comments = h.get("num_comments") or 0
        body = (
            f"{points} points, {comments} comments on HN."
            + (f" Author: {h.get('author')}." if h.get("author") else "")
            + (f" {h.get('story_text') or h.get('comment_text') or ''}"[:500])
        )
        published = None
        if h.get("created_at_i"):
            published = datetime.fromtimestamp(h["created_at_i"], tz=timezone.utc)
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=f"hn_{h.get('objectID')}",
                title=title,
                url=url,
                published_at=published,
                body_text=body,
                extra={"tickers": [], "hint_categories": ["AI/Tech"]},
            )
        )
    return items


# --------------------------------------------------------------------------- #
# ArXiv
# --------------------------------------------------------------------------- #
async def _arxiv(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    cats = cfg.params.get("categories", ["cs.AI"])
    q = " OR ".join(f"cat:{c}" for c in cats)
    url = (
        "https://export.arxiv.org/api/query?"
        f"search_query={quote_plus(q)}&sortBy=submittedDate&sortOrder=descending"
        f"&max_results={cfg.params.get('max_items', 25)}"
    )
    text = await pool.fetch_text(url, source_id=cfg.id)
    if not text:
        raise ConnectionError("arxiv fetch failed")
    parsed = feedparser.parse(text)
    items: ConnectResult = []
    for e in parsed.entries:
        title = re.sub(r"\s+", " ", e.get("title", "")).strip()
        if not title:
            continue
        url = e.get("link") or ""
        summary = re.sub(r"\s+", " ", e.get("summary", "")).strip()[:1200]
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=str(e.get("id") or url),
                title=title,
                url=url,
                published_at=_feed_date(e),
                body_text=summary,
                extra={"hint_categories": ["Science", "AI/Tech"]},
            )
        )
    return items[: cfg.params.get("max_items", 25)]


# --------------------------------------------------------------------------- #
# GDELT
# --------------------------------------------------------------------------- #
async def _gdelt(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    query = cfg.params.get("query", "sourcelang:english")
    url = (
        "https://api.gdeltproject.org/api/v2/doc/doc?"
        f"query={quote_plus(query)}&mode=ArtList&maxrecords={cfg.params.get('max_items', 75)}"
        "&sort=DateDesc&format=json"
    )
    data = await pool.fetch_json(url, source_id=cfg.id)
    if not data:
        raise ConnectionError("gdelt fetch failed")
    items: ConnectResult = []
    for a in data.get("articles", [])[: cfg.params.get("max_items", 75)]:
        title = a.get("title") or ""
        if not title:
            continue
        published = None
        try:
            published = datetime.strptime(a.get("seendate", ""), "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except Exception:
            pass
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=str(a.get("url") or title)[:300],
                title=title,
                url=a.get("url") or "",
                published_at=published,
                body_text=(a.get("context") or "")[:1000],
                extra={"hint_categories": ["Geopolitics"]},
            )
        )
    return items


# --------------------------------------------------------------------------- #
# SEC EDGAR
# --------------------------------------------------------------------------- #
async def _sec_edgar(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    forms = cfg.params.get("forms", ["8-K"])
    items: ConnectResult = []
    for form in forms:
        url = (
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type="
            f"{quote_plus(form)}&company=&dateb=&owner=include&count=25&output=atom"
        )
        text = await pool.fetch_text(
            url, source_headers={"User-Agent": cfg.headers.get("User-Agent", "")}, source_id=cfg.id
        )
        if not text:
            continue
        parsed = feedparser.parse(text)
        for e in parsed.entries:
            title = e.get("title", "")
            if not title:
                continue
            # titles look like "8-K  Apple Inc. (AAPL) (CIK 0000320193)"
            m = re.search(r"\(([A-Z]{1,5}(?:\.[A-Z]{1,2})?)\)", title)
            tickers = [m.group(1)] if m else []
            items.append(
                RawItem(
                    source_id=cfg.id,
                    external_id=str(e.get("id") or e.get("link"))[:300],
                    title=title,
                    url=e.get("link") or "",
                    published_at=_feed_date(e),
                    body_html=e.get("summary") or "",
                    extra={"tickers": tickers, "hint_categories": ["Regulatory", "Equities"]},
                )
            )
    return items[:80]


# --------------------------------------------------------------------------- #
# Prediction markets: Polymarket / Kalshi probability shifts
# --------------------------------------------------------------------------- #
async def _polymarket(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    url = (
        "https://gamma-api.polymarket.com/markets?closed=false"
        f"&order=volume24hr&ascending=false&limit={cfg.params.get('max_items', 50)}"
    )
    data = await pool.fetch_json(url, source_id=cfg.id)
    if not isinstance(data, list):
        raise ConnectionError("polymarket fetch failed")
    items: ConnectResult = []
    for m in data:
        try:
            q = m.get("closedPrice") or m.get("lastTradePrice") or 0
            prev = (m.get("oneDayPriceChange") or 0) or 0
        except Exception:
            continue
        prob = float(m.get("outcomePrices", "[0.5,0.5]").strip("[]").split(",")[0] or 0)
        shift = abs(float(prev))
        if shift < cfg.params.get("min_shift", 0.05):
            continue
        title = (m.get("question") or m.get("title") or "").strip()
        if not title:
            continue
        direction = "up" if float(prev) > 0 else "down"
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=f"poly_{m.get('id')}",
                title=f"Polymarket odds {direction} {int(shift*100)}%: {title}",
                url=f"https://polymarket.com/market/{m.get('slug', '')}",
                published_at=now_utc(),
                body_text=(
                    f"Implied probability now {int(prob*100)}% "
                    f"(24h move {direction} {int(shift*100)}%). Volume 24h: {m.get('volume24hr', 0)}."
                ),
                extra={"hint_categories": ["Geopolitics", "Macro"]},
            )
        )
    return items[:40]


async def _kalshi(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    url = (
        "https://api.elections.kalshi.com/trade-api/v2/markets"
        f"?status=open&limit={cfg.params.get('max_items', 100)}&with_nested_market_data=false"
    )
    data = await pool.fetch_json(url, source_id=cfg.id)
    if not isinstance(data, dict):
        raise ConnectionError("kalshi fetch failed")
    items: ConnectResult = []
    for m in data.get("markets", []):
        yes = m.get("yes_bid") or 0
        prev = m.get("last_price") or 0
        if abs(yes - prev) / 100.0 < cfg.params.get("min_shift", 0.05):
            continue
        title = (m.get("title") or m.get("ticker") or "").strip()
        if not title:
            continue
        direction = "up" if yes > prev else "down"
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=f"kalshi_{m.get('ticker')}",
                title=f"Kalshi odds {direction}: {title} (yes {yes}¢)",
                url="https://kalshi.com/markets",
                published_at=now_utc(),
                body_text=f"Kalshi yes bid at {yes}¢, previous {prev}¢.",
                extra={"hint_categories": ["Geopolitics", "Macro"]},
            )
        )
    return items[:40]


# --------------------------------------------------------------------------- #
# GitHub
# --------------------------------------------------------------------------- #
async def _github_releases(cfg: SourceConfig) -> ConnectResult:
    return await _rss_family(cfg)  # releases.atom is an atom feed


async def _github_trending(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    min_stars = cfg.params.get("min_stars", 500)
    days = cfg.params.get("within_days", 7)
    created = (now_utc() - timedelta(days=days)).strftime("%Y-%m-%d")
    url = (
        "https://api.github.com/search/repositories?"
        f"q=created:%3E{created}+stars:%3E{min_stars}&sort=stars&order=desc&per_page=25"
    )
    data = await pool.fetch_json(
        url,
        source_headers={"Accept": "application/vnd.github+json"},
        source_id=cfg.id,
    )
    if not isinstance(data, dict):
        raise ConnectionError("github search failed")
    items: ConnectResult = []
    for r in data.get("items", []):
        title = f"{r.get('full_name', '')} trending: {r.get('stargazers_count', 0)} stars"
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=f"gh_{r.get('id')}",
                title=title,
                url=r.get("html_url") or "",
                published_at=None,
                body_text=(r.get("description") or "")[:1000],
                extra={"hint_categories": ["AI/Tech"]},
            )
        )
    return items


# --------------------------------------------------------------------------- #
# NVD CVEs (JSON API 2.0, no key required; key raises rate limits)
# --------------------------------------------------------------------------- #
async def _nvd(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    start = now_utc() - timedelta(hours=cfg.params.get("lookback_hours", 24))
    end = start + timedelta(hours=24)
    url = (
        "https://services.nvd.nist.gov/rest/json/cves/2.0?"
        f"pubStartDate={start.strftime('%Y-%m-%dT%H:%M:%S.000')}"
        f"&pubEndDate={end.strftime('%Y-%m-%dT%H:%M:%S.000')}"
        f"&resultsPerPage={cfg.params.get('max_items', 40)}"
    )
    data = await pool.fetch_json(url, source_id=cfg.id)
    if not isinstance(data, dict):
        raise ConnectionError("nvd fetch failed")
    items: ConnectResult = []
    for v in data.get("vulnerabilities", []):
        cve = v.get("cve", {})
        cve_id = cve.get("id", "")
        if not cve_id:
            continue
        desc = ""
        for d in cve.get("descriptions", []):
            if d.get("lang") == "en":
                desc = d.get("value", "")
                break
        metrics = cve.get("metrics", {})
        cvss_score = None
        for key in ("cvssMetricV31", "cvssMetricV30", "cvssMetricV2"):
            if metrics.get(key):
                cvss_score = metrics[key][0].get("cvssData", {}).get("baseScore")
                break
        sev = (cve.get("cisaExploitAdd") and "ACTIVELY EXPLOITED") or (
            f"CVSS {cvss_score}" if cvss_score else "unrated"
        )
        title = f"{cve_id} ({sev}): {desc.split('. ')[0]}"
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=cve_id,
                title=title[:400],
                url=f"https://nvd.nist.gov/vuln/detail/{cve_id}",
                published_at=None,
                body_text=desc[:2000],
                extra={"hint_categories": ["Cybersecurity"]},
            )
        )
    return items[: cfg.params.get("max_items", 40)]


# --------------------------------------------------------------------------- #
# Reddit (public .json endpoints — no OAuth needed for read-only listing)
# --------------------------------------------------------------------------- #
async def _reddit(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    subs = cfg.params.get("subreddits", ["worldnews"])
    items: ConnectResult = []
    for sub in subs:
        url = f"https://www.reddit.com/r/{sub}/new.json?limit={cfg.params.get('max_items_per_sub', 25)}"
        data = await pool.fetch_json(
            url, source_headers={"User-Agent": pool.user_agent}, source_id=cfg.id
        )
        if not isinstance(data, dict):
            continue
        for child in data.get("data", {}).get("children", []):
            d = child.get("data", {})
            title = d.get("title") or ""
            if not title:
                continue
            created = None
            if d.get("created_utc"):
                created = datetime.fromtimestamp(d["created_utc"], tz=timezone.utc)
            items.append(
                RawItem(
                    source_id=cfg.id,
                    external_id=f"reddit_{d.get('id')}",
                    title=title,
                    url=f"https://www.reddit.com{d.get('permalink', '')}" if d.get("permalink") else "",
                    published_at=created,
                    body_text=(d.get("selftext") or "")[:3000],
                    extra={
                        "tickers": [],
                        "hint_categories": cfg.params.get("hint_categories", []),
                        "score": d.get("score", 0),
                    },
                )
            )
    return items


# --------------------------------------------------------------------------- #
# Generic HTML link scraper (for sites without RSS — AI lab blogs etc.)
# --------------------------------------------------------------------------- #
_A_TAG = re.compile(
    r"<a[^>]+href=[\"']([^\"'#]+)[\"'][^>]*>(.*?)</a>", re.S | re.I
)
_TAG = re.compile(r"<[^>]+>")


async def _html_links(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    text = await pool.fetch_text(cfg.url or "", source_headers=cfg.headers, source_id=cfg.id)
    if not text:
        raise ConnectionError(f"html fetch failed: {cfg.url}")
    base = cfg.url or ""
    seen: set[str] = set()
    items: ConnectResult = []
    min_len = cfg.params.get("min_title_len", 30)
    for href, inner in _A_TAG.findall(text):
        inner_text = _TAG.sub("", inner)
        inner_text = re.sub(r"\s+", " ", inner_text).strip()
        if len(inner_text) < min_len or href in seen:
            continue
        if href.startswith("/"):
            href = base.rstrip("/") + href
        if not href.startswith("http"):
            continue
        seen.add(href)
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=href[:300],
                title=inner_text[:300],
                url=href,
                published_at=None,
                body_text="",
                extra={"hint_categories": cfg.params.get("hint_categories", [])},
            )
        )
    return items[: cfg.params.get("max_items", 30)]


# --------------------------------------------------------------------------- #
# CISA KEV catalog (merged from v1)
# --------------------------------------------------------------------------- #
async def _cisa_kev(cfg: SourceConfig) -> ConnectResult:
    pool = HttpPool.get()
    url = cfg.url or "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
    data = await pool.fetch_json(url, source_headers=cfg.headers, source_id=cfg.id)
    if not isinstance(data, dict):
        raise ConnectionError(f"fetch failed: {url}")
    days = int(cfg.params.get("days", 3))
    horizon = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    items: ConnectResult = []
    for v in data.get("vulnerabilities", []):
        if str(v.get("dateAdded", "")) < horizon:
            continue
        cid = str(v.get("cveID", "")).strip()
        if not cid:
            continue
        ransomware = str(v.get("knownRansomwareCampaignUse", "")).lower() == "known"
        title = (
            f"CISA adds actively exploited {cid} to KEV: {v.get('vendorProject', '')} "
            f"{v.get('product', '')} — {v.get('vulnerabilityName', '')}"
        ).strip()
        summary = (
            f"{v.get('shortDescription', '')}. Required action: {v.get('requiredAction', '')} "
            f"Due date: {v.get('dueDate', '')}."
            + (" Known use in ransomware campaigns." if ransomware else "")
        )
        published = None
        try:
            published = datetime.fromisoformat(str(v.get("dateAdded", "")) + "T00:00:00+00:00")
        except ValueError:
            pass
        items.append(
            RawItem(
                source_id=cfg.id,
                external_id=f"kev:{cid}",
                title=title,
                url=f"https://nvd.nist.gov/vuln/detail/{cid}",
                published_at=published,
                body_text=summary,
                extra={"hint_categories": ["Cybersecurity"]},
            )
        )
    return items[: cfg.params.get("max_items", 25)]


# --------------------------------------------------------------------------- #
# Telegram public channels via the t.me/s/<channel> web preview — no API keys
# (merged from v1; the keyed Telethon stream stays available as type=telegram)
# --------------------------------------------------------------------------- #
def _parse_telegram_page(page: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(page, "lxml")
    posts = []
    for msg in soup.select("div.tgme_widget_message[data-post]"):
        text_el = msg.select_one("div.tgme_widget_message_text")
        if text_el is None:
            continue
        time_el = msg.select_one("a.tgme_widget_message_date time[datetime]")
        for br in text_el.find_all("br"):
            br.replace_with("\n")
        posts.append(
            {
                "post": msg.get("data-post", ""),
                "text": text_el.get_text("", strip=True),
                "html": str(text_el),
                "time": time_el.get("datetime") if time_el else None,
            }
        )
    return posts


async def _telegram_web(cfg: SourceConfig) -> ConnectResult:
    channels = [c.strip() for c in (cfg.params.get("channels") or [cfg.params.get("channel")]) if c and str(c).strip()]
    if not channels:
        raise ConnectionError("telegram_web: no channel configured (params.channels)")
    pool = HttpPool.get()
    items: ConnectResult = []
    for channel in channels[:4]:  # keep one source bounded
        html_text = await pool.fetch_text(
            f"https://t.me/s/{channel}", source_headers={"Accept": "text/html"}, source_id=cfg.id
        )
        if not html_text:
            continue
        posts = await asyncio.to_thread(_parse_telegram_page, html_text)
        for p in posts[: cfg.params.get("max_items", 20)]:
            text = str(p["text"] or "")
            first = text.split("\n", 1)[0].strip()
            if len(first) < 15:
                first = " ".join(text[:200].split())
            if len(first) < 10:
                continue
            published = None
            if p["time"]:
                try:
                    published = datetime.fromisoformat(str(p["time"]).replace("Z", "+00:00"))
                except ValueError:
                    pass
            items.append(
                RawItem(
                    source_id=cfg.id,
                    external_id=f"tg:{p['post']}",
                    title=first[:220],
                    url=f"https://t.me/{p['post']}",
                    published_at=published,
                    body_html=str(p["html"]),
                    extra={"hint_categories": cfg.params.get("hint_categories", ["Breaking", "Geopolitics"])},
                )
            )
    return items


# --------------------------------------------------------------------------- #
# registry
# --------------------------------------------------------------------------- #
CONNECTORS: dict[str, ConnectFn] = {
    "rss": _rss_family,
    "atom": _rss_family,
    "json": _json_generic,
    "hn_frontpage": _hn,
    "hn_show": _hn,
    "arxiv": _arxiv,
    "gdelt": _gdelt,
    "sec_edgar": _sec_edgar,
    "polymarket": _polymarket,
    "kalshi": _kalshi,
    "github_releases": _github_releases,
    "github_trending": _github_trending,
    "nvd": _nvd,
    "reddit": _reddit,
    "html": _html_links,
    "cloud_status": _rss_family,
    "cisa_kev": _cisa_kev,
    "telegram_web": _telegram_web,
}


def get_connector(cfg: SourceConfig) -> ConnectFn | None:
    if cfg.type in ("bluesky", "telegram"):
        return None  # stream connectors handled by the streaming module
    return CONNECTORS.get(cfg.type)
