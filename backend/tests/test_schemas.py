from app.schemas import FilterSpec, NewsEvent, SourceConfig, compact_dict


def make_event(title="Fed cuts rates", urgency=8, cats=("Macro",), tickers=("SPX",)):
    return NewsEvent.model_validate({
        "event_id": "evt_1", "cluster_id": "cls_1",
        "published_at": "2026-10-16T18:05:12.410Z", "ingested_at": "2026-10-16T18:05:13.102Z",
        "source": {"id": "reuters_wire", "name": "Reuters", "category": "wire",
                   "credibility_tier": 1, "url": "https://example.com"},
        "content": {"raw_title": title, "flash_headline": title,
                     "executive_summary": ["a", "b", "c"], "clean_markdown": ""},
        "enrichment": {"categories": list(cats), "tickers": list(tickers),
                        "entities": [{"name": "Fed", "type": "ORG"}], "locations": ["US"],
                        "sentiment": 0.3, "urgency": urgency, "novelty": 0.9,
                        "verification_status": "DEVELOPING"},
    })


def test_schema_roundtrip():
    ev = make_event()
    d = ev.model_dump(mode="json")
    ev2 = NewsEvent.model_validate(d)
    assert ev2 == ev


def test_compact_dict_shape():
    d = make_event().model_dump(mode="json")
    c = compact_dict(d)
    assert set(c.keys()) == {"i", "c", "p", "s", "n", "t", "u", "v", "w", "z", "k", "x", "l"}
    assert c["t"] == "Fed cuts rates"
    assert c["u"] == 8
    # compact must be dramatically smaller
    import json
    assert len(json.dumps(c)) < len(json.dumps(d)) * 0.5


def test_filter_match_category():
    ev = make_event(cats=("Macro",))
    f = FilterSpec(categories=["Macro"])
    assert f.match(ev)
    f2 = FilterSpec(categories=["Crypto"])
    assert not f2.match(ev)


def test_filter_match_urgency():
    ev = make_event(urgency=7)
    assert FilterSpec(min_urgency=5).match(ev)
    assert not FilterSpec(min_urgency=9).match(ev)


def test_filter_match_query():
    ev = make_event(title="NVIDIA Blackwell launch")
    assert FilterSpec(q="nvidia").match(ev)
    assert not FilterSpec(q="apple").match(ev)


def test_filter_match_tickers():
    ev = make_event(tickers=("NVDA",))
    assert FilterSpec(tickers=["NVDA"]).match(ev)
    assert not FilterSpec(tickers=["BTC"]).match(ev)


def test_source_config_effective_poll_clamped():
    cfg = SourceConfig(id="x", name="x", url="u", poll_interval_sec=5, min_poll_sec=30, max_poll_sec=100)
    assert cfg.effective_poll() == 30
    cfg.poll_interval_sec = 500
    assert cfg.effective_poll() == 100
