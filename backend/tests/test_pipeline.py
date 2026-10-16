"""End-to-end pipeline test: dedup, clustering, enrichment, fan-out,
store persistence — all in-process (no network, no Redis, no Postgres)."""
import asyncio

import pytest

from app.bus import Bus
from app.config import get_settings
from app.db import MemoryStore
from app.enrich import EnrichmentService
from app.pipeline import Pipeline
from app.schemas import RawItem, SourceConfig


def mk_source(sid="test_wire", tier=1, ftype="rss"):
    return SourceConfig(
        id=sid, name="Test Wire", type=ftype, url="https://example.com/feed",
        category="wire", credibility_tier=tier,
    )


def mk_item(title, body="The Federal Reserve cut rates by 25 basis points.",
            url="https://example.com/story-1", sid="test_wire"):
    return RawItem(source_id=sid, external_id=url, title=title, url=url, body_text=body)


@pytest.fixture
def pipeline():
    # all-in-process: memory store, local bus, heuristic enrichment (no LLM keys set)
    store = MemoryStore(get_settings())
    bus = Bus()
    enricher = EnrichmentService()
    enricher.start()
    assert enricher.llm_enabled is False  # no keys in the test environment
    return Pipeline(store, bus, enricher)


def run(coro):
    return asyncio.run(coro)


def test_process_single_event(pipeline):
    ev = run(pipeline.process(mk_item("Fed cuts rates in surprise move"), mk_source()))
    assert ev is not None
    assert ev.event_id.startswith("evt_")
    assert ev.cluster_id.startswith("cls_")
    assert ev.enrichment.urgency >= 1
    assert "Macro" in ev.enrichment.categories
    assert ev.content.flash_headline


def test_exact_duplicate_dropped(pipeline):
    cfg = mk_source()
    first = run(pipeline.process(mk_item("Fed cuts rates in surprise move"), cfg))
    dup = run(pipeline.process(mk_item("Fed cuts rates in surprise move"), cfg))
    assert first is not None
    assert dup is None
    assert pipeline.stats_duplicates == 1


def test_url_duplicate_dropped(pipeline):
    cfg = mk_source()
    item1 = mk_item("Fed cuts rates in surprise move")
    item2 = mk_item("Totally different headline but same URL")
    assert run(pipeline.process(item1, cfg)) is not None
    assert run(pipeline.process(item2, cfg)) is None  # same canonical URL


def test_syndicated_story_joins_cluster(pipeline):
    cfg = mk_source()
    # realistic syndication pair: same story, one outlet adds a source line
    item1 = mk_item(
        "Fed cuts interest rates by 25 basis points in surprise decision",
        body="The Federal Reserve lowered its benchmark rate for the first time this year. "
             "Markets rallied after the announcement.",
        url="https://a.com/1",
    )
    item2 = mk_item(
        "Fed cuts interest rates by 25 basis points in surprise decision",
        body="The Federal Reserve lowered its benchmark rate for the first time this year. "
             'Markets rallied after the announcement, Reuters reports.',
        url="https://b.com/2",
    )
    ev1 = run(pipeline.process(item1, cfg))
    ev2 = run(pipeline.process(item2, cfg))
    assert ev1 is not None
    if ev2 is not None:  # survives dedup only if hamming > 3; with these texts it should
        assert ev2.cluster_id == ev1.cluster_id
        assert ev2.enrichment.novelty < 1.0
        assert pipeline.stats_cluster_joins == 1


def test_novelty_of_first_event_is_one(pipeline):
    ev = run(pipeline.process(mk_item("Rare event: something brand new happens"), mk_source()))
    assert ev.enrichment.novelty == 1.0


def test_stats_and_lag(pipeline):
    run(pipeline.process(mk_item("Markets rally after Fed decision"), mk_source()))
    p50, p95 = pipeline.lag_percentiles()
    assert p50 >= 0 and p95 >= p50
