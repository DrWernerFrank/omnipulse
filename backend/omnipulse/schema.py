"""Canonical data models shared by the UI, database, and agent APIs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

TAXONOMY: tuple[str, ...] = (
    "Macro",
    "Equities",
    "Crypto",
    "AI/Tech",
    "Geopolitics",
    "Cybersecurity",
    "Science",
    "Regulatory",
    "Breaking",
)

VerificationStatus = Literal["CONFIRMED", "DEVELOPING", "UNVERIFIED"]
EntityType = Literal["PERSON", "ORG", "TICKER", "COUNTRY", "LOCATION", "CRYPTO", "PRODUCT", "CVE"]

SOURCE_TYPES = (
    "rss",
    "sec_edgar",
    "hackernews",
    "gdelt",
    "polymarket",
    "kalshi",
    "nvd",
    "cisa_kev",
    "github_trending",
    "bluesky",
    "telegram",
    "newsapi",
    "x_list",
    "whale_alert",
    "webhook",
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


# --------------------------------------------------------------------------------------
# NewsEvent — the canonical, strictly-shaped event
# --------------------------------------------------------------------------------------


class SourceRef(_Model):
    id: str
    name: str
    category: str
    credibility_tier: int = Field(ge=1, le=3)
    url: str  # canonical link to the item itself


class Content(_Model):
    raw_title: str
    flash_headline: str
    executive_summary: list[str]
    clean_markdown: str


class Entity(_Model):
    name: str
    type: EntityType


class Enrichment(_Model):
    categories: list[str]
    tickers: list[str]
    entities: list[Entity]
    locations: list[str]
    sentiment: float = Field(ge=-1.0, le=1.0)
    urgency: int = Field(ge=1, le=10)
    novelty: float = Field(ge=0.0, le=1.0)
    verification_status: VerificationStatus


class NewsEvent(_Model):
    event_id: str
    cluster_id: str
    published_at: datetime
    ingested_at: datetime
    source: SourceRef
    content: Content
    enrichment: Enrichment

    def to_wire(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["published_at"] = iso(self.published_at)
        data["ingested_at"] = iso(self.ingested_at)
        return data


# --------------------------------------------------------------------------------------
# Ingestion-side models
# --------------------------------------------------------------------------------------


class RawItem(BaseModel):
    """What a connector emits before enrichment."""

    source_id: str
    guid: str
    url: str
    title: str
    summary: str = ""  # HTML or plain text
    content_html: str = ""  # full HTML body when the feed carries it
    author: str = ""
    published_at: datetime | None = None
    fetched_at: datetime = Field(default_factory=utcnow)
    # connector-provided hints: categories, tickers, entities, urgency_floor, urgency_bonus,
    # verification, display_source (e.g. publisher behind an aggregator)
    hints: dict[str, Any] = Field(default_factory=dict)


class SourceConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str = Field(pattern=r"^[a-z0-9][a-z0-9_\-]{1,63}$")
    name: str
    type: str
    url: str = ""
    category: str = "general"
    credibility_tier: int = Field(2, ge=1, le=3)
    group: str = ""  # registry group (A-F)
    poll_interval: float = 60.0
    min_interval: float = 5.0
    max_interval: float = 900.0
    enabled: bool = True
    default_categories: list[str] = Field(default_factory=list)
    params: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)
    requires_key: str | None = None  # Settings attribute that must be non-empty
    publisher: str = ""  # editorial owner; feeds of one publisher never corroborate each other
    deleted: bool = False

    @property
    def publisher_key(self) -> str:
        return (self.publisher or self.params.get("publisher") or self.id.split("_", 1)[0]).lower()

    @field_validator("type")
    @classmethod
    def _known_type(cls, v: str) -> str:
        if v not in SOURCE_TYPES:
            raise ValueError(f"unknown source type {v!r}; expected one of {SOURCE_TYPES}")
        return v

    @field_validator("default_categories")
    @classmethod
    def _known_categories(cls, v: list[str]) -> list[str]:
        bad = [c for c in v if c not in TAXONOMY]
        if bad:
            raise ValueError(f"unknown categories {bad}; taxonomy is {TAXONOMY}")
        return v


class SourceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    url: str | None = None
    category: str | None = None
    credibility_tier: int | None = Field(None, ge=1, le=3)
    poll_interval: float | None = None
    min_interval: float | None = None
    max_interval: float | None = None
    enabled: bool | None = None
    default_categories: list[str] | None = None
    params: dict[str, Any] | None = None
    headers: dict[str, str] | None = None


class SourceHealth(BaseModel):
    id: str
    status: Literal["healthy", "degraded", "dead", "paused", "disabled", "starting", "push"] = "starting"
    interval: float = 0.0
    last_poll: datetime | None = None
    last_success: datetime | None = None
    last_item_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    items_total: int = 0
    polls_total: int = 0
    not_modified_total: int = 0
    avg_latency_ms: float = 0.0


class WebhookConfig(BaseModel):
    id: str
    url: str
    secret: str = ""
    dsl: str = ""
    event_types: list[str] = Field(default_factory=lambda: ["EVENT_CREATED"])
    format: Literal["full", "compact", "headline"] = "compact"
    active: bool = True
    failures: int = 0
    created_at: datetime = Field(default_factory=utcnow)


class StreamEnvelope(BaseModel):
    """Envelope for everything on the live firehose."""

    type: Literal["EVENT_CREATED", "EVENT_UPDATED", "CLUSTER_UPDATED", "SOURCE_HEALTH", "HEARTBEAT"]
    data: dict[str, Any]
    ts: str = Field(default_factory=lambda: iso(utcnow()))
