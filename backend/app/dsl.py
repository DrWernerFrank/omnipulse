"""Terminal query DSL — merged in from the v1 engine.

Compiles a compact query string to an ``EventFilter`` (same grammar the v1
tiling terminal's command bar uses):

    u>=7 cat:crypto,macro $NVDA tk:BTC src:reuters scat:wire loc:US tier<=2
    sent>0.3 nov>=0.5 vs:confirmed ent:openai cluster:cls_... "rate cut" -opinion

Repeated values for the same key are OR-ed; different keys are AND-ed; bare
words must all appear in the headline/title (case-insensitive); ``-word``
excludes. Use via ``?dsl=`` on REST/WS/SSE (alongside the plain ``q``
substring filter, which keeps working).
"""
from __future__ import annotations

import re
import shlex
from typing import Any

from pydantic import BaseModel, Field

from .schemas import CATEGORIES

_CACHE_MAX = 512


def resolve_category(token: str) -> str | None:
    t = token.lower().replace("/", "").replace("-", "").strip()
    if not t:
        return None
    aliases = {
        "ai": "AI/Tech", "tech": "AI/Tech", "aitech": "AI/Tech",
        "geo": "Geopolitics", "world": "Geopolitics",
        "cyber": "Cybersecurity", "security": "Cybersecurity", "infosec": "Cybersecurity",
        "reg": "Regulatory", "regulation": "Regulatory",
        "eq": "Equities", "stocks": "Equities", "equity": "Equities",
        "macro": "Macro", "econ": "Macro", "economy": "Macro",
        "crypto": "Crypto", "web3": "Crypto",
        "sci": "Science", "science": "Science",
        "breaking": "Breaking", "brk": "Breaking", "flash": "Breaking",
    }
    if t in aliases:
        return aliases[t]
    for cat in CATEGORIES:
        if cat.lower().replace("/", "").startswith(t):
            return cat
    return None


class EventFilter(BaseModel):
    categories: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    source_categories: list[str] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    verification: list[str] = Field(default_factory=list)
    clusters: list[str] = Field(default_factory=list)
    min_urgency: int = 1
    max_urgency: int = 10
    min_tier: int = 1
    max_tier: int = 3
    min_sentiment: float | None = None
    max_sentiment: float | None = None
    min_novelty: float = 0.0
    text: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)

    # ----------------------------------------------------------------------------
    @classmethod
    def parse(cls, dsl: str | None) -> "EventFilter":
        f = cls()
        if not dsl or not dsl.strip():
            return f
        try:
            tokens = shlex.split(dsl)
        except ValueError:
            tokens = dsl.split()
        for tok in tokens:
            f._apply(tok)
        return f

    def _apply(self, tok: str) -> None:
        if tok.startswith("$") and len(tok) > 1:
            self.tickers.extend(t.upper() for t in tok[1:].split(",") if t)
            return
        m = re.match(r"^(u|urg|urgency|tier|sent|sentiment|nov|novelty)(>=|<=|>|<|=|:)(-?[\d.]+)$", tok, re.I)
        if m:
            key, op, raw = m.group(1).lower(), m.group(2), m.group(3)
            try:
                val = float(raw)
            except ValueError:
                return
            self._apply_numeric(key[0:3] if key.startswith(("sen", "nov")) else key[0], op, val)
            return
        if ":" in tok and not tok.startswith(("-", "http")):
            key, _, value = tok.partition(":")
            values = [v for v in value.split(",") if v]
            key = key.lower()
            if key in ("cat", "category", "c"):
                self.categories.extend(c for c in (resolve_category(v) for v in values) if c)
            elif key in ("tk", "ticker", "t"):
                self.tickers.extend(v.upper().lstrip("$") for v in values)
            elif key in ("src", "source", "s"):
                self.sources.extend(v.lower() for v in values)
            elif key in ("scat", "sourcecat", "type"):
                self.source_categories.extend(v.lower() for v in values)
            elif key in ("loc", "location", "country"):
                self.locations.extend(v.upper() for v in values)
            elif key in ("ent", "entity", "e"):
                self.entities.extend(v.lower() for v in values)
            elif key in ("vs", "verified", "verification"):
                self.verification.extend(v.upper() for v in values)
            elif key in ("cluster", "cls"):
                self.clusters.extend(values)
            else:
                self.text.append(tok.lower())
            return
        if tok.startswith("-") and len(tok) > 1:
            self.exclude.append(tok[1:].lower())
            return
        self.text.append(tok.lower())

    def _apply_numeric(self, key: str, op: str, val: float) -> None:
        if key == "u":
            v = int(val)
            if op in (">=", ":", "="):
                self.min_urgency = v
                if op == "=":
                    self.max_urgency = v
            elif op == ">":
                self.min_urgency = v + 1
            elif op == "<=":
                self.max_urgency = v
            elif op == "<":
                self.max_urgency = v - 1
        elif key == "t":
            v = int(val)
            if op in (":", "="):
                self.min_tier = self.max_tier = v
            elif op in ("<=",):
                self.max_tier = v
            elif op == "<":
                self.max_tier = v - 1
            elif op == ">=":
                self.min_tier = v
            elif op == ">":
                self.min_tier = v + 1
        elif key == "sen":
            if op in (">", ">=", ":", "="):
                self.min_sentiment = val
            else:
                self.max_sentiment = val
        elif key == "nov":
            self.min_novelty = val

    # ----------------------------------------------------------------------------
    @property
    def is_empty(self) -> bool:
        return self == EventFilter()

    def matches(self, ev: dict[str, Any]) -> bool:
        enr = ev.get("enrichment") or {}
        src = ev.get("source") or {}
        urgency = enr.get("urgency", 1)
        if urgency < self.min_urgency or urgency > self.max_urgency:
            return False
        tier = src.get("credibility_tier", 3)
        if tier < self.min_tier or tier > self.max_tier:
            return False
        sent = enr.get("sentiment", 0.0)
        if self.min_sentiment is not None and sent < self.min_sentiment:
            return False
        if self.max_sentiment is not None and sent > self.max_sentiment:
            return False
        if enr.get("novelty", 1.0) < self.min_novelty:
            return False
        if self.categories and not set(self.categories) & set(enr.get("categories") or ()):
            return False
        if self.tickers and not set(self.tickers) & set(enr.get("tickers") or ()):
            return False
        if self.locations and not set(self.locations) & set(enr.get("locations") or ()):
            return False
        if self.verification and enr.get("verification_status") not in self.verification:
            return False
        if self.clusters and ev.get("cluster_id") not in self.clusters:
            return False
        if self.sources:
            sid, sname = src.get("id", "").lower(), src.get("name", "").lower()
            if not any(s in sid or s in sname for s in self.sources):
                return False
        if self.source_categories and src.get("category", "").lower() not in self.source_categories:
            return False
        if self.entities:
            names = " | ".join(e.get("name", "").lower() for e in enr.get("entities") or ())
            if not any(e in names for e in self.entities):
                return False
        if self.text or self.exclude:
            content = ev.get("content") or {}
            hay = (content.get("flash_headline", "") + " " + content.get("raw_title", "")).lower()
            if any(t not in hay for t in self.text):
                return False
            if any(x in hay for x in self.exclude):
                return False
        return True

    def matches_cluster(self, c: dict[str, Any]) -> bool:
        """Coarse match for CLUSTER_UPDATED envelopes (cluster-level aggregates only)."""
        if c.get("max_urgency", 1) < self.min_urgency:
            return False
        if self.categories and not set(self.categories) & set(c.get("categories") or ()):
            return False
        if self.tickers and not set(self.tickers) & set(c.get("tickers") or ()):
            return False
        if self.locations and not set(self.locations) & set(c.get("locations") or ()):
            return False
        if self.clusters and c.get("cluster_id") not in self.clusters:
            return False
        if self.sources and not any(s in sid.lower() for s in self.sources for sid in c.get("sources") or ()):
            return False
        if self.text:
            hay = c.get("headline", "").lower()
            if any(t not in hay for t in self.text):
                return False
        return True

    def matches_envelope(self, env: dict[str, Any]) -> bool:
        t = env.get("type")
        if t in ("EVENT_CREATED", "EVENT_UPDATED"):
            return self.matches(env.get("data") or {})
        if t == "CLUSTER_UPDATED":
            return self.matches_cluster(env.get("data") or {})
        return True


_cache: dict[str, EventFilter] = {}


def parse_dsl(dsl: str | None) -> EventFilter:
    """Parse (with a small cache — bus filters run this per event)."""
    if not dsl:
        return EventFilter()
    hit = _cache.get(dsl)
    if hit is not None:
        return hit
    f = EventFilter.parse(dsl)
    if len(_cache) >= _CACHE_MAX:
        _cache.clear()
    _cache[dsl] = f
    return f


def headline_line(ev: dict[str, Any]) -> str:
    """One-line wire format: HH:MM:SSz U<urgency> <sent> [src|tier] headline $tk (cats) id."""
    enr, src, content = ev.get("enrichment") or {}, ev.get("source") or {}, ev.get("content") or {}
    tk = " ".join("$" + t for t in (enr.get("tickers") or [])[:4])
    cats = ",".join((enr.get("categories") or [])[:3])
    ts = (ev.get("published_at") or "")
    return (
        f"{ts[11:19]}Z U{enr.get('urgency', 1):<2} {enr.get('sentiment', 0.0):+.2f} "
        f"[{src.get('name', '?')}|T{src.get('credibility_tier', 3)}] {content.get('flash_headline', '')}"
        f"{' ' + tk if tk else ''} ({cats}) {ev.get('event_id', '')}"
    ).rstrip()
