"""Tests for the speed pass: websub push, cluster candidate index,
adaptive scheduling, and the enforced `since` filter."""
import asyncio
import hashlib
import hmac
from datetime import datetime, timedelta, timezone

import pytest

from app.config import get_settings
from app.schemas import FilterSpec, NewsEvent, SourceConfig, now_utc
from app.websub import (
    discover_hub, new_secret, parse_push, push_active, verify_signature,
)

ATOM_HUB = b"""<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <link rel="hub" href="https://hub.example.com/"/>
  <link rel="self" href="https://blog.example.com/atom.xml"/>
  <title>Example Feed</title>
  <entry>
    <id>tag:example.com,2026:1</id>
    <title>Chaos at the exchange as spreads blow out</title>
    <link href="https://example.com/posts/1"/>
    <updated>2026-10-16T10:00:00Z</updated>
    <summary>Trading halted briefly.</summary>
  </entry>
  <entry>
    <id>tag:example.com,2026:2</id>
    <title>Markets stabilize after chaotic morning session</title>
    <link href="https://example.com/posts/2"/>
    <updated>2026-10-16T11:00:00Z</updated>
  </entry>
</feed>"""


def test_push_active_defaults_off():
    assert push_active() is False  # no public URL configured in tests


def test_push_active_requires_public_url(monkeypatch):
    s = get_settings()
    monkeypatch.setattr(type(s), "websub_enabled", True, raising=False)
    # public_url still localhost => push stays off
    assert push_active() is False


def test_discover_hub():
    import feedparser

    parsed = feedparser.parse(ATOM_HUB)
    assert discover_hub(parsed) == "https://hub.example.com/"


def test_discover_hub_absent():
    import feedparser

    parsed = feedparser.parse(b"<feed xmlns='http://www.w3.org/2005/Atom'><title>x</title></feed>")
    assert discover_hub(parsed) is None


def test_verify_signature_sha1():
    secret = new_secret()
    body = ATOM_HUB
    sig = "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()
    assert verify_signature(secret, body, sig) is True
    assert verify_signature(secret, body + b"x", sig) is False
    assert verify_signature(secret, body, None) is False
    assert verify_signature("wrong", body, sig) is False


def test_verify_signature_sha256():
    secret = new_secret()
    sig = "sha256=" + hmac.new(secret.encode(), ATOM_HUB, hashlib.sha256).hexdigest()
    assert verify_signature(secret, ATOM_HUB, sig) is True


def test_parse_push_items():
    items = parse_push(ATOM_HUB, "src1")
    assert len(items) == 2
    assert items[0].title == "Chaos at the exchange as spreads blow out"
    assert items[0].url == "https://example.com/posts/1"
    assert items[0].external_id == "tag:example.com,2026:1"
    assert items[1].published_at is not None


def test_parse_push_junk_skipped():
    items = parse_push(b"not xml at all", "src1")
    assert items == []


# --------------------------------------------------------------------------- #
# cluster candidate index
# --------------------------------------------------------------------------- #
def _mk_event(i: int, cluster: str, when: datetime | None = None) -> NewsEvent:
    from app.schemas import Content, Enrichment, SourceRef

    return NewsEvent(
        event_id=f"evt_{i:026d}",
        cluster_id=cluster,
        published_at=when or now_utc(),
        ingested_at=when or now_utc(),
        source=SourceRef(id=f"s{i}", name="s", category="wire", credibility_tier=1),
        content=Content(raw_title=f"story {i}", flash_headline=f"story {i}"),
        enrichment=Enrichment(categories=["Breaking"]),
    )


def test_cluster_index_respects_cap():
    from app.bus import Bus
    from app.db import MemoryStore
    from app.embeddings import embed
    from app.enrich import EnrichmentService
    from app.pipeline import Pipeline

    store = MemoryStore(get_settings())
    enricher = EnrichmentService()
    enricher.start()
    pl = Pipeline(store, Bus(), enricher)
    s = get_settings()

    # synthesize clusters directly (bypasses embedding similarity of process())
    n = s.cluster_candidate_cap + 80
    for i in range(n):
        ev = _mk_event(i, f"cls_{i:026d}")
        pl._update_cluster(f"cls_{i:026d}", ev, embed(f"topic {i} lexicon {i * 7919}"), True, float(i))
    assert len(pl._clusters) == n
    pl._rebuild_index()
    expected_cap = s.cluster_candidate_cap if pl._cand_mat is not None else s.cluster_fallback_cap
    assert len(pl._cand_ids) == expected_cap
    # candidates must be the most recently touched clusters
    newest = list(pl._touch)[-expected_cap:]
    assert set(pl._cand_ids) == set(newest)


def test_cluster_join_still_works_at_scale():
    from app.bus import Bus
    from app.db import MemoryStore
    from app.enrich import EnrichmentService
    from app.pipeline import Pipeline
    from app.schemas import RawItem

    store = MemoryStore(get_settings())
    enricher = EnrichmentService()
    enricher.start()
    pl = Pipeline(store, Bus(), enricher)
    cfg = SourceConfig(id="join", name="join", type="rss", url="https://y.example/f",
                       category="wire", credibility_tier=1)

    async def go():
        first = await pl.process(
            RawItem(source_id="join", external_id="a",
                    title="Rocket launch fails seconds after liftoff",
                    url="https://y.example/a", body_text="An unmanned rocket failed."),
            cfg,
        )
        # same story from another outlet -> must join the cluster, not fork a new one
        second = await pl.process(
            RawItem(source_id="join", external_id="b",
                    title="Rocket launch fails seconds after liftoff: engines shut down",
                    url="https://y.example/b", body_text="An unmanned rocket failed."),
            cfg,
        )
        return first, second

    first, second = asyncio.run(go())
    assert first and second
    assert second.cluster_id == first.cluster_id
    assert pl.stats_cluster_joins == 1


# --------------------------------------------------------------------------- #
# adaptive scheduler + since filter
# --------------------------------------------------------------------------- #
def test_adaptive_hot_feed_hits_floor():
    from app.engine import Engine

    cfg = SourceConfig(id="hot", name="hot", type="rss", url="https://z.example/f",
                       category="wire", credibility_tier=1,
                       poll_interval_sec=30, min_poll_sec=8, max_poll_sec=300)
    eng = Engine.__new__(Engine)  # no __init__ side effects; only the method under test
    assert eng._adaptive_interval(cfg, fresh=5) == 8
    assert eng._adaptive_interval(cfg, fresh=3) == 8
    quiet = eng._adaptive_interval(cfg, fresh=0)
    assert cfg.min_poll_sec <= quiet <= cfg.max_poll_sec


def test_since_filter_enforced():
    ev = _mk_event(1, "cls_1", when=now_utc() - timedelta(hours=2))
    recent = FilterSpec(since=now_utc() - timedelta(hours=1))
    assert recent.match(ev) is False
    old = FilterSpec(since=now_utc() - timedelta(hours=3))
    assert old.match(ev) is True
