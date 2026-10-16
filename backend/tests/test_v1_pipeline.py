import asyncio
from datetime import timedelta

import pytest

from omnipulse import bus as B
from omnipulse.bus import MemoryBus
from omnipulse.enrich.embed import HashingEmbedder
from omnipulse.enrich.pipeline import Enricher
from omnipulse.filters import EventFilter
from omnipulse.ingest.http import Fetcher
from omnipulse.registry import seed_defaults
from omnipulse.schema import NewsEvent, RawItem, utcnow
from omnipulse.store import MemoryStore


@pytest.fixture
async def enricher(settings):
    bus, store = MemoryBus(), MemoryStore()
    await seed_defaults(store, settings)
    e = Enricher(settings, bus, store, HashingEmbedder(settings.embed_dim), Fetcher("test"))
    await e.refresh_sources()
    return e


def raw(source, url, title, summary="", minutes_ago=0):
    return RawItem(source_id=source, guid=url, url=url, title=title, summary=summary,
                   published_at=utcnow() - timedelta(minutes=minutes_ago))


async def test_event_schema_and_publish(enricher):
    tail = enricher.bus.tail(B.EVENTS)
    pending = asyncio.ensure_future(tail.__anext__())
    await asyncio.sleep(0)  # let the tail register its queue before publishing
    ev = await enricher.process(raw("test_wire", "https://example.com/fed",
                                    "Federal Reserve cuts interest rates by 50 basis points in surprise move",
                                    "<p>The Fed cut its benchmark rate by half a point on Wednesday. Chair Jerome "
                                    "Powell cited a cooling labor market. Stocks rallied and Treasury yields fell.</p>"))
    NewsEvent.model_validate(ev)  # strict canonical schema
    assert ev["event_id"].startswith("evt_") and ev["cluster_id"].startswith("cls_")
    assert ev["source"]["credibility_tier"] == 1
    assert ev["enrichment"]["verification_status"] == "CONFIRMED"
    assert "Macro" in ev["enrichment"]["categories"]
    assert len(ev["content"]["executive_summary"]) == 3
    _, env = await asyncio.wait_for(pending, 2)
    assert env["type"] == "EVENT_CREATED" and env["data"]["event_id"] == ev["event_id"]


async def test_url_dedup(enricher):
    a = await enricher.process(raw("test_wire", "https://example.com/story?utm_source=rss", "Some headline about markets"))
    b = await enricher.process(raw("test_blog", "https://www.example.com/story", "Some headline about markets"))
    assert a and b is None
    assert enricher.counters["dup_url"] == 1


async def test_syndication_folds_into_cluster(enricher):
    body = ("Oil prices jumped more than 4% on Monday after OPEC+ agreed to deepen production cuts through the "
            "end of next year, according to a statement released after the meeting in Vienna.")
    a = await enricher.process(raw("test_wire", "https://wire.example/oil", "Oil jumps 4% as OPEC+ deepens output cuts", body))
    b = await enricher.process(raw("test_blog", "https://blog.example/oil-copy", "Oil jumps 4% as OPEC+ deepens output cuts", body))
    assert a and b is None
    assert enricher.counters["dup_simhash"] == 1
    cluster = enricher.book.get(a["cluster_id"]).to_wire()
    assert cluster["syndication_count"] == 1 and cluster["source_count"] == 2


async def test_semantic_clustering_and_verification(enricher):
    first = await enricher.process(raw("test_social", "https://social.example/1",
                                       "Explosion reported at Rotterdam port oil terminal, fire crews responding", minutes_ago=5))
    assert first["enrichment"]["verification_status"] == "UNVERIFIED"
    follow = await enricher.process(raw("test_wire", "https://wire.example/2",
                                        "Large explosion at Rotterdam port oil terminal, fire crews responding to blaze"))
    assert follow["cluster_id"] == first["cluster_id"]
    assert follow["enrichment"]["verification_status"] == "CONFIRMED"
    assert follow["enrichment"]["novelty"] < 1.0
    c = enricher.book.get(first["cluster_id"]).to_wire()
    assert c["primary_source"]["id"] == "test_social"  # first to publish
    assert c["verification_status"] == "CONFIRMED"
    other = await enricher.process(raw("test_blog", "https://blog.example/3",
                                       "Apple unveils new MacBook Pro lineup with M6 chips"))
    assert other["cluster_id"] != first["cluster_id"]
    assert other["enrichment"]["novelty"] == 1.0


async def test_store_query_and_semantic_search(enricher):
    await enricher.process(raw("test_wire", "https://a.example/1", "Bitcoin surges past $150,000 on ETF inflows"))
    await enricher.process(raw("test_wire", "https://a.example/2", "Ukraine and Russia agree to Black Sea ceasefire talks"))
    crypto = await enricher.store.query_events(EventFilter.parse("cat:crypto"))
    assert len(crypto) == 1 and "BTC" in crypto[0]["enrichment"]["tickers"]
    vec = (await enricher.embedder.embed(["bitcoin price surges"]))[0]
    hits = await enricher.store.semantic_search(vec, 2, None, EventFilter())
    assert hits[0][1]["content"]["raw_title"].startswith("Bitcoin")


async def test_same_publisher_feeds_do_not_corroborate(enricher):
    from omnipulse.schema import SourceConfig

    await enricher.store.save_source(SourceConfig(id="wire_markets", name="Test Wire Markets", type="webhook",
                                                  credibility_tier=2, publisher="wire"))
    await enricher.store.save_source(SourceConfig(id="blog_two", name="Blog Two", type="webhook",
                                                  credibility_tier=2, publisher="blog"))
    await enricher.refresh_sources()
    a = await enricher.process(raw("test_blog", "https://blog.example/x", "Chipmaker shares slide after export curbs"))
    # same publisher, same headline, different feed + URL -> dropped as a re-post
    assert await enricher.process(raw("blog_two", "https://blog.example/feed2/x", "Chipmaker shares slide after export curbs")) is None
    assert enricher.counters["dup_same_publisher"] == 1
    assert a["enrichment"]["verification_status"] == "DEVELOPING"
    c = enricher.book.get(a["cluster_id"]).to_wire()
    assert c["source_count"] == 1


async def test_templated_filings_of_different_companies_do_not_cluster(enricher):
    def filing(company, ticker, url):
        r = raw("test_wire", url, f"{company} (${ticker}) files 8-K: Regulation FD Disclosure; Other Events")
        r.hints.update(tickers=[ticker], entities=[{"name": company, "type": "ORG"}], categories=["Regulatory"])
        return r

    a = await enricher.process(filing("Copart Inc", "CPRT", "https://sec.example/1"))
    b = await enricher.process(filing("Goldman Sachs Private Credit Corp", "GSCR", "https://sec.example/2"))
    assert a["cluster_id"] != b["cluster_id"]
    assert b["enrichment"]["tickers"] == ["GSCR"]  # authoritative symbol, no "$GS" guess


async def test_micro_batch_path(enricher):
    body = ("Copper prices hit a record high on Thursday after a major Chilean mine halted output following "
            "a landslide, traders said, tightening an already stretched global supply picture.")
    items = [
        raw("test_wire", "https://w.example/cu", "Copper hits record as Chile mine halts output", body),
        raw("test_blog", "https://b.example/cu-copy", "Copper hits record as Chile mine halts output", body),  # syndicated, same batch
        raw("test_blog", "https://w.example/cu?utm_source=x", "Copper hits record (dup url)"),               # URL duplicate
        raw("test_social", "https://s.example/other", "Volcano erupts in Iceland, flights diverted"),
    ]
    await enricher.process_batch([(f"{i}-0", it.model_dump(mode="json")) for i, it in enumerate(items)])
    assert enricher.counters["processed"] == 2
    assert enricher.counters["dup_simhash"] == 1 and enricher.counters["dup_url"] == 1
    assert enricher.counters["batches"] == 1
