"""Canonical OmniPulse schemas.

Every event across the DB, REST API, WebSocket/SSE firehoses, MCP tools and the
frontend adheres to the same NewsEvent JSON document defined here.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field

VERIFICATION_STATUSES = ("CONFIRMED", "DEVELOPING", "UNVERIFIED")

CATEGORIES = [
    "Macro",
    "Equities",
    "Crypto",
    "AI/Tech",
    "Geopolitics",
    "Cybersecurity",
    "Science",
    "Regulatory",
    "Breaking",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class Entity(BaseModel):
    name: str
    type: str = "ORG"  # ORG | PERSON | PRODUCT


class SourceRef(BaseModel):
    id: str
    name: str
    category: str
    credibility_tier: int = 3
    url: str | None = None  # link to the originating article/post


class Content(BaseModel):
    raw_title: str = ""
    flash_headline: str = ""
    executive_summary: list[str] = Field(default_factory=list)
    clean_markdown: str = ""


class Enrichment(BaseModel):
    categories: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)
    entities: list[Entity] = Field(default_factory=list)
    locations: list[str] = Field(default_factory=list)  # ISO-3166 alpha-2 codes
    sentiment: float = 0.0        # -1.0 .. +1.0
    urgency: int = 1              # 1 .. 10
    novelty: float = 1.0          # 0.0 .. 1.0
    verification_status: str = "DEVELOPING"


class NewsEvent(BaseModel):
    event_id: str
    cluster_id: str
    published_at: datetime
    ingested_at: datetime
    source: SourceRef
    content: Content
    enrichment: Enrichment

    def compact(self) -> dict[str, Any]:
        """Token-optimized wire form (roughly 85% fewer tokens than full JSON)."""
        return compact_dict(self.model_dump(mode="json"))


def compact_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Build the compact wire dict from a full event dict (model_dump(mode='json'))."""
    src = d.get("source") or {}
    enr = d.get("enrichment") or {}
    con = d.get("content") or {}
    return {
        "i": d.get("event_id"),
        "c": d.get("cluster_id"),
        "p": d.get("published_at"),
        "s": src.get("id"),
        "n": src.get("name"),
        "t": con.get("flash_headline"),
        "u": enr.get("urgency"),
        "v": _round2(enr.get("sentiment")),
        "w": _round2(enr.get("novelty")),
        "z": enr.get("verification_status"),
        "k": enr.get("categories", []),
        "x": enr.get("tickers", []),
        "l": src.get("url"),
    }


def _round2(v: Any) -> Any:
    return round(v, 3) if isinstance(v, (int, float)) else v


COMPACT_KEY_LEGEND = {
    "i": "event_id",
    "c": "cluster_id",
    "p": "published_at (ISO-8601 UTC)",
    "s": "source.id",
    "n": "source.name",
    "t": "content.flash_headline",
    "u": "enrichment.urgency (1-10)",
    "v": "enrichment.sentiment (-1..+1)",
    "w": "enrichment.novelty (0..1)",
    "z": "enrichment.verification_status",
    "k": "enrichment.categories[]",
    "x": "enrichment.tickers[]",
    "l": "source.url (article link)",
}


class RawItem(BaseModel):
    """Normalized item emitted by every connector. The pipeline turns this
    into a NewsEvent (dedup -> cluster -> enrich -> publish)."""

    source_id: str
    external_id: str = ""          # stable id from the upstream feed (guid, link, id...)
    title: str = ""
    url: str = ""
    published_at: datetime | None = None
    body_html: str = ""            # raw content snippet / summary HTML
    body_text: str = ""            # pre-cleaned text (APIs already provide this)
    full_html: str = ""            # full article HTML (when fulltext fetch succeeded)
    extra: dict[str, Any] = Field(default_factory=dict)  # tickers, prices, hint categories...

    @property
    def text_for_fingerprint(self) -> str:
        return f"{self.title} {self.body_text}"


class ClusterSummary(BaseModel):
    cluster_id: str
    title: str = ""
    event_count: int = 0
    sources: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    tickers: list[str] = Field(default_factory=list)
    breaking_source: str | None = None  # source id that broke the story first
    first_seen: datetime | None = None
    latest_at: datetime | None = None
    velocity_15m: int = 0


class SourceConfig(BaseModel):
    """Registry entry. Persisted in the DB, editable at runtime via API/UI."""

    id: str
    name: str
    type: str = "rss"
    url: str | None = None
    category: str = "wire"       # wire|finance|macro|regulatory|prediction|ai|crypto|cyber|social|community|infra|official|science|osint
    tier: str = "C"              # A..F polling cadence band
    credibility_tier: int = 3    # 1 = tier-1 wire, 2 = established media, 3 = community/social
    poll_interval_sec: int = 60
    min_poll_sec: int = 10
    max_poll_sec: int = 900
    enabled: bool = True
    fulltext: bool = False       # fetch article page and extract clean markdown
    headers: dict[str, str] = Field(default_factory=dict)
    params: dict[str, Any] = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    requires_key: str | None = None  # env var name that must be set to activate
    state: dict[str, Any] = Field(default_factory=dict)  # runtime: etag, seen ids, health...

    def effective_poll(self) -> int:
        return max(self.min_poll_sec, min(self.poll_interval_sec, self.max_poll_sec))


class SourceStateMessage(BaseModel):
    source_id: str
    name: str
    health: str  # ok|stale|degraded|dead|paused|waiting_key
    enabled: bool
    detail: str | None = None
    last_success: datetime | None = None
    interval_sec: int | None = None


class StatsPayload(BaseModel):
    ts: datetime
    events_total: int = 0
    events_1h: int = 0
    events_15m: int = 0
    events_per_min: float = 0.0
    lag_p50_ms: float = 0.0
    lag_p95_ms: float = 0.0
    clusters_24h: int = 0
    sources_online: int = 0
    sources_total: int = 0
    sources_paused: int = 0
    dlq_size: int = 0
    bus_mode: str = "local"
    store_mode: str = "memory"
    top_tickers: list[tuple[str, int]] = Field(default_factory=list)


class FilterSpec(BaseModel):
    """Unified filter model for REST, WS subscriptions and SSE channels."""

    q: str | None = None
    categories: list[str] | None = None
    tickers: list[str] | None = None
    sources: list[str] | None = None
    entities: list[str] | None = None
    min_urgency: int | None = None
    max_age_sec: int | None = None
    since: datetime | None = None
    limit: int = Field(default=100, ge=1, le=1000)
    cursor: str | None = None
    compact: bool = False

    def match(self, ev: NewsEvent) -> bool:
        return self.match_dict(ev.model_dump(mode="json"))

    def match_dict(self, d: dict[str, Any]) -> bool:
        enr = d.get("enrichment") or {}
        if self.categories and not (set(self.categories) & set(enr.get("categories") or [])):
            return False
        if self.tickers and not (set(self.tickers) & set(enr.get("tickers") or [])):
            return False
        if self.sources and (d.get("source") or {}).get("id") not in self.sources:
            return False
        if self.min_urgency is not None and (enr.get("urgency") or 0) < self.min_urgency:
            return False
        if self.since is not None:
            published = _parse_dt(d.get("published_at"))
            since = _parse_dt(self.since) or self.since
            if published is None or published < since:
                return False
        if self.max_age_sec is not None:
            published = _parse_dt(d.get("published_at"))
            if published is None:
                return False
            if (now_utc() - published).total_seconds() > self.max_age_sec:
                return False
        if self.entities:
            names = {e.get("name", "").lower() for e in (enr.get("entities") or [])}
            if not any(e.lower() in names for e in self.entities):
                return False
        if self.q:
            ql = self.q.lower()
            con = d.get("content") or {}
            hay = " ".join(
                [
                    con.get("raw_title") or "",
                    con.get("flash_headline") or "",
                    con.get("clean_markdown") or "",
                ]
            ).lower()
            if ql not in hay:
                return False
        return True


def _parse_dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, str):
        try:
            from pydantic import TypeAdapter

            return TypeAdapter(datetime).validate_python(v)
        except Exception:
            return None
    return None
