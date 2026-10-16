"""Structured REST connectors: Hacker News, GDELT, prediction markets, NVD, CISA KEV, GitHub Trending."""

from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import orjson
from bs4 import BeautifulSoup

from ..schema import RawItem, utcnow
from .base import Connector, parse_dt
from .http import FetchError

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# Hacker News (Firebase API)
# --------------------------------------------------------------------------------------


class HackerNewsConnector(Connector):
    """Emits stories once they cross a score threshold; flags high-velocity comment threads."""

    kind = "hackernews"
    BASE = "https://hacker-news.firebaseio.com/v0"

    FALLBACK = {"topstories": "https://hnrss.org/frontpage", "showstories": "https://hnrss.org/show",
                "newstories": "https://hnrss.org/newest", "beststories": "https://hnrss.org/best"}

    async def poll(self) -> list[RawItem]:
        if self.state.get("fallback_until", 0) > time.time():
            return await self._poll_rss()
        try:
            return await self._poll_firebase()
        except FetchError as exc:
            if exc.status is not None:
                raise
            # Firebase unreachable (e.g. regional blocks on Google infrastructure): use hnrss.org.
            log.info("HN Firebase unreachable (%s); falling back to hnrss.org for 30 min", exc)
            self.state["fallback_until"] = time.time() + 1800
            return await self._poll_rss()

    async def _poll_rss(self) -> list[RawItem]:
        from .rss import entries_to_items, parse_feed

        p = self.cfg.params
        url = self.FALLBACK.get(p.get("list", "topstories"), self.FALLBACK["topstories"])
        res = await self.ctx.fetcher.get(url, params={"points": str(int(p.get("min_score", 100)))})
        items = entries_to_items(self, await asyncio.to_thread(parse_feed, res.content))
        for it in items:
            # share GUIDs with the Firebase path ("hn:<id>") so switching paths never re-emits
            m = re.search(r"item\?id=(\d+)", f"{it.guid} {it.summary}")
            if m:
                it.guid = f"hn:{m.group(1)}"
            it.hints.update(categories=["AI/Tech"])
        return items

    async def _poll_firebase(self) -> list[RawItem]:
        p = self.cfg.params
        listing = p.get("list", "topstories")
        top_n = int(p.get("top_n", 30))
        min_score = int(p.get("min_score", 100))
        res = await self.ctx.fetcher.get(f"{self.BASE}/{listing}.json")
        ids = orjson.loads(res.content)[:top_n]
        emitted: set[int] = set(self.state.get("emitted", []))
        todo = [i for i in ids if i not in emitted]
        sem = asyncio.Semaphore(10)

        async def fetch(i: int) -> dict | None:
            async with sem:
                try:
                    r = await self.ctx.fetcher.get(f"{self.BASE}/item/{i}.json")
                    return orjson.loads(r.content)
                except Exception:
                    return None

        out = []
        for story in await asyncio.gather(*(fetch(i) for i in todo)):
            if not story or story.get("type") != "story" or story.get("dead") or story.get("deleted"):
                continue
            score, comments = story.get("score", 0), story.get("descendants", 0)
            if score < min_score:
                continue
            sid = story["id"]
            emitted.add(sid)
            hn_url = f"https://news.ycombinator.com/item?id={sid}"
            age_h = max(0.25, (time.time() - story.get("time", time.time())) / 3600)
            velocity = comments / age_h
            text = story.get("text", "")
            summary = (
                f"<p>{score} points, {comments} comments on Hacker News "
                f"(~{velocity:.0f} comments/hour). Discussion: {hn_url}</p>{text}"
            )
            out.append(
                self.item(
                    guid=f"hn:{sid}",
                    url=story.get("url") or hn_url,
                    title=html.unescape(story.get("title", "")),
                    summary=summary,
                    author=story.get("by", ""),
                    published_at=datetime.fromtimestamp(story.get("time", time.time()), tz=timezone.utc),
                    hints={"categories": ["AI/Tech"], "urgency_bonus": 1 if velocity > 60 else 0,
                           "discussion_url": hn_url, "score": score},
                )
            )
        self.state["emitted"] = list(emitted)[-2000:]
        return out


# --------------------------------------------------------------------------------------
# GDELT DOC 2.0 API
# --------------------------------------------------------------------------------------


class GdeltConnector(Connector):
    kind = "gdelt"

    async def poll(self) -> list[RawItem]:
        p = self.cfg.params
        res = await self.ctx.fetcher.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={
                "query": p.get("query", "sourcelang:english"),
                "mode": "artlist",
                "format": "json",
                "maxrecords": str(p.get("maxrecords", 75)),
                "timespan": p.get("timespan", "15min"),
                "sort": "datedesc",
            },
            headers=self.headers,
        )
        text = res.text().strip()
        if not text.startswith("{"):
            raise ValueError(f"GDELT error: {text[:160]}")
        data = orjson.loads(text)
        out = []
        for a in data.get("articles", []):
            if not a.get("url") or not a.get("title"):
                continue
            out.append(
                self.item(
                    guid=a["url"],
                    url=a["url"],
                    title=a["title"],
                    summary="",
                    published_at=parse_dt(a.get("seendate")),
                    hints={"display_source": a.get("domain", ""),
                           "source_country": a.get("sourcecountry", "")},
                )
            )
        return out


# --------------------------------------------------------------------------------------
# Prediction markets: Polymarket (Gamma) and Kalshi — emit probability shifts
# --------------------------------------------------------------------------------------


class _ShiftDetector(Connector):
    """Keeps a per-market baseline and emits when |p - baseline| >= threshold."""

    def shift_items(self, markets: list[dict[str, Any]], venue: str) -> list[RawItem]:
        threshold = float(self.cfg.params.get("threshold", 0.05))
        baselines: dict[str, float] = self.state.setdefault("baselines", {})
        first = not baselines
        out = []
        now = utcnow()
        for m in markets:
            mid, price = m["id"], m["price"]
            if price is None:
                continue
            base = baselines.get(mid)
            if base is None:
                baselines[mid] = price
                continue
            delta = price - base
            if abs(delta) < threshold:
                continue
            baselines[mid] = price
            if first:
                continue
            arrow = "surges" if delta > 0 else "drops"
            title = (
                f"{venue}: odds {arrow} {abs(delta) * 100:.0f}pp to {price * 100:.0f}% — {m['question']}"
            )
            summary = (
                f"<p>Implied probability moved from {base * 100:.1f}% to {price * 100:.1f}% "
                f"({delta * 100:+.1f} pp).</p>"
                + (f"<p>24h volume: ${m['volume']:,.0f}.</p>" if m.get("volume") else "")
            )
            out.append(
                self.item(
                    guid=f"{venue}:{mid}:{int(now.timestamp())}",
                    url=m["url"],
                    title=title,
                    summary=summary,
                    published_at=now,
                    hints={"urgency_bonus": 2 if abs(delta) >= 0.15 else 1, "sentiment_hint": 0.0,
                           "verification": "CONFIRMED", "market": m["question"]},
                )
            )
        if len(baselines) > 5000:
            keep = {m["id"] for m in markets}
            self.state["baselines"] = {k: v for k, v in baselines.items() if k in keep}
        return out


class PolymarketConnector(_ShiftDetector):
    kind = "polymarket"

    async def poll(self) -> list[RawItem]:
        p = self.cfg.params
        res = await self.ctx.fetcher.get(
            "https://gamma-api.polymarket.com/markets",
            params={"active": "true", "closed": "false", "order": "volume24hr", "ascending": "false",
                    "limit": str(p.get("limit", 150))},
        )
        min_vol = float(p.get("min_volume_24h", 50000))
        markets = []
        for m in orjson.loads(res.content):
            try:
                prices = orjson.loads(m.get("outcomePrices") or "[]")
                outcomes = orjson.loads(m.get("outcomes") or "[]")
            except orjson.JSONDecodeError:
                continue
            if not prices or float(m.get("volume24hr") or 0) < min_vol:
                continue
            yes_idx = outcomes.index("Yes") if "Yes" in outcomes else 0
            ev_slug = (m.get("events") or [{}])[0].get("slug") or m.get("slug")
            markets.append({
                "id": str(m["id"]),
                "question": m.get("question", ""),
                "price": float(prices[yes_idx]),
                "volume": float(m.get("volume24hr") or 0),
                "url": f"https://polymarket.com/event/{ev_slug}",
            })
        return self.shift_items(markets, "Polymarket")


class KalshiConnector(_ShiftDetector):
    kind = "kalshi"

    async def poll(self) -> list[RawItem]:
        p = self.cfg.params
        wanted = {c.lower() for c in p.get("categories", ["Politics", "Economics", "World", "Financials", "Elections"])}
        res = await self.ctx.fetcher.get(
            "https://api.elections.kalshi.com/trade-api/v2/events",
            params={"status": "open", "with_nested_markets": "true", "limit": "200"},
        )
        markets = []
        for ev in orjson.loads(res.content).get("events", []):
            if wanted and (ev.get("category") or "").lower() not in wanted:
                continue
            for m in ev.get("markets", []) or []:
                price = None
                if m.get("last_price_dollars") not in (None, ""):
                    price = float(m["last_price_dollars"])
                elif m.get("last_price") is not None:
                    price = float(m["last_price"]) / 100
                if not price or float(m.get("volume_24h") or m.get("volume") or 0) < float(p.get("min_volume", 1000)):
                    continue
                q = m.get("title") or ev.get("title", "")
                if m.get("yes_sub_title") and m["yes_sub_title"] not in q:
                    q = f"{q} — {m['yes_sub_title']}"
                markets.append({
                    "id": m["ticker"],
                    "question": q,
                    "price": price,
                    "volume": None,
                    "url": f"https://kalshi.com/markets/{(ev.get('series_ticker') or m['ticker']).lower()}",
                })
        return self.shift_items(markets, "Kalshi")


# --------------------------------------------------------------------------------------
# Security: NIST NVD CVE API 2.0 and CISA Known Exploited Vulnerabilities
# --------------------------------------------------------------------------------------


MAJOR_VENDORS = re.compile(
    r"(Microsoft|Windows|Exchange|Azure|Apple|iOS|macOS|Google|Chrome|Android|Linux kernel|Cisco|Fortinet|"
    r"FortiOS|Palo Alto|PAN-OS|Ivanti|Citrix|NetScaler|VMware|ESXi|Oracle|SAP|Adobe|Atlassian|Confluence|"
    r"Jenkins|Apache|OpenSSL|OpenSSH|Kubernetes|Docker|AWS|Juniper|F5|BIG-IP|SonicWall|Zyxel|Mozilla|Firefox|"
    r"WordPress|Samsung|Qualcomm|Intel|AMD|NVIDIA|Siemens|Schneider|GitLab|GitHub)"
)


def _cvss(cve: dict[str, Any]) -> tuple[float | None, str]:
    metrics = cve.get("metrics", {})
    for key in ("cvssMetricV40", "cvssMetricV31", "cvssMetricV30", "cvssMetricV2"):
        for m in metrics.get(key, []):
            d = m.get("cvssData", {})
            if "baseScore" in d:
                return float(d["baseScore"]), d.get("baseSeverity") or m.get("baseSeverity", "")
    return None, ""


class NvdConnector(Connector):
    kind = "nvd"

    async def poll(self) -> list[RawItem]:
        p = self.cfg.params
        min_cvss = float(p.get("min_cvss", 7.0))
        now = datetime.now(timezone.utc)
        start = parse_dt(self.state.get("cursor")) or now - timedelta(hours=6)
        start = max(start - timedelta(minutes=10), now - timedelta(days=7))
        fmt = "%Y-%m-%dT%H:%M:%S.000"
        res = await self.ctx.fetcher.get(
            "https://services.nvd.nist.gov/rest/json/cves/2.0",
            params={"pubStartDate": start.strftime(fmt), "pubEndDate": now.strftime(fmt), "resultsPerPage": "200"},
        )
        data = orjson.loads(res.content)
        self.state["cursor"] = now.isoformat()
        out = []
        for v in data.get("vulnerabilities", []):
            cve = v.get("cve", {})
            score, severity = _cvss(cve)
            desc = next((d["value"] for d in cve.get("descriptions", []) if d.get("lang") == "en"), "")
            if score is None or score < min_cvss:
                continue
            cid = cve.get("id", "")
            out.append(
                self.item(
                    guid=cid,
                    url=f"https://nvd.nist.gov/vuln/detail/{cid}",
                    title=f"{cid} ({severity or 'CVSS'} {score:.1f}): {desc[:180]}",
                    summary=f"<p>{html.escape(desc)}</p>",
                    published_at=parse_dt(cve.get("published")),
                    hints={"categories": ["Cybersecurity"], "entities": [{"name": cid, "type": "CVE"}],
                           "urgency_floor": (5 if score >= 9.0 else 4) + (2 if MAJOR_VENDORS.search(desc) else 0),
                           "urgency_bonus": -1 if "through commit" in desc else 0,
                           "verification": "CONFIRMED"},
                )
            )
        return out


class CisaKevConnector(Connector):
    kind = "cisa_kev"

    async def poll(self) -> list[RawItem]:
        res = await self.conditional_get(
            self.cfg.url or "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
        )
        if res.not_modified:
            return []
        data = orjson.loads(res.content)
        horizon = (datetime.now(timezone.utc) - timedelta(days=int(self.cfg.params.get("days", 3)))).date().isoformat()
        out = []
        for v in data.get("vulnerabilities", []):
            if v.get("dateAdded", "") < horizon:
                continue
            cid = v.get("cveID", "")
            ransomware = v.get("knownRansomwareCampaignUse", "").lower() == "known"
            out.append(
                self.item(
                    guid=f"kev:{cid}",
                    url=f"https://nvd.nist.gov/vuln/detail/{cid}",
                    title=f"CISA adds actively exploited {cid} to KEV: {v.get('vendorProject', '')} "
                          f"{v.get('product', '')} — {v.get('vulnerabilityName', '')}",
                    summary=(
                        f"<p>{html.escape(v.get('shortDescription', ''))}</p>"
                        f"<p>Required action: {html.escape(v.get('requiredAction', ''))} "
                        f"Due date: {v.get('dueDate', '')}.</p>"
                        + ("<p>Known use in ransomware campaigns.</p>" if ransomware else "")
                    ),
                    published_at=parse_dt(v.get("dateAdded")),
                    hints={"categories": ["Cybersecurity"], "entities": [{"name": cid, "type": "CVE"}],
                           "urgency_floor": 8 if ransomware else 7, "verification": "CONFIRMED"},
                )
            )
        return out


# --------------------------------------------------------------------------------------
# GitHub Trending (HTML) — no public API exists
# --------------------------------------------------------------------------------------


def _parse_trending(html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "lxml")
    repos = []
    for row in soup.select("article.Box-row"):
        a = row.select_one("h2 a")
        if not a or not a.get("href"):
            continue
        path = a["href"].strip("/")
        desc_el = row.select_one("p")
        lang_el = row.select_one('[itemprop="programmingLanguage"]')
        today = 0
        for span in row.select("span"):
            m = re.search(r"([\d,]+)\s+stars?\s+(today|this week|this month)", span.get_text(" ", strip=True))
            if m:
                today = int(m.group(1).replace(",", ""))
        repos.append({
            "path": path,
            "description": desc_el.get_text(" ", strip=True) if desc_el else "",
            "language": lang_el.get_text(strip=True) if lang_el else "",
            "stars_today": today,
        })
    return repos


class GithubTrendingConnector(Connector):
    kind = "github_trending"

    async def poll(self) -> list[RawItem]:
        p = self.cfg.params
        url = self.cfg.url or "https://github.com/trending"
        res = await self.ctx.fetcher.get(url, params={"since": p.get("since", "daily")},
                                         headers={"Accept": "text/html"})
        repos = await asyncio.to_thread(_parse_trending, res.text())
        min_stars = int(p.get("min_stars_today", 150))
        now = utcnow()
        return [
            self.item(
                guid=f"gh:{r['path']}:{now.date().isoformat()}",
                url=f"https://github.com/{r['path']}",
                title=f"Trending on GitHub: {r['path']} (+{r['stars_today']:,} stars today) — {r['description'][:140]}",
                summary=f"<p>{html.escape(r['description'])}</p><p>Language: {r['language'] or 'n/a'}.</p>",
                published_at=now,
                hints={"categories": ["AI/Tech"], "urgency_bonus": 1 if r["stars_today"] > 1000 else 0},
            )
            for r in repos
            if r["stars_today"] >= min_stars
        ]
