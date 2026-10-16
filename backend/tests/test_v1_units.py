from omnipulse.enrich.canonical import canonicalize
from omnipulse.enrich.simhash import SimHashIndex, hamming, simhash
from omnipulse.enrich.summarize import executive_summary, flash_headline
from omnipulse.enrich.tagger import tag
from omnipulse.filters import EventFilter, compact
from omnipulse.ingest.extract import html_to_markdown
from omnipulse.schema import SourceConfig


def test_canonicalize_strips_tracking_and_normalises():
    a = canonicalize("http://www.Example.com/news/story/?utm_source=x&id=7&fbclid=abc#frag")
    b = canonicalize("https://example.com/news/story?id=7")
    assert a == b == "https://example.com/news/story?id=7"
    assert canonicalize("https://m.example.com/a/amp") == "https://example.com/a"


def test_simhash_near_duplicates():
    t1 = "Federal Reserve raises interest rates by 25 basis points to fight persistent inflation, Powell says"
    t2 = "Federal Reserve raises interest rates by 25 basis points to fight persistent inflation, Powell said"
    t3 = "Bitcoin slides below $60,000 as crypto liquidations mount across exchanges"
    assert hamming(simhash(t1), simhash(t2)) <= 12
    assert hamming(simhash(t1), simhash(t3)) > 15
    idx = SimHashIndex(max_distance=3)
    h = simhash(t1)
    idx.add("e1", h, 0.0, {"cluster_id": "c1"})
    assert idx.find(h)[1]["cluster_id"] == "c1"
    assert idx.find(simhash(t3)) is None
    idx.prune(1.0)
    assert idx.find(h) is None


def test_filter_dsl_parse_and_match():
    f = EventFilter.parse('u>=7 cat:crypto,macro $BTC src:reut loc:US tier<=2 sent<0 "rate cut" -opinion')
    assert f.min_urgency == 7 and f.max_tier == 2 and f.max_sentiment == 0
    assert f.categories == ["Crypto", "Macro"] and f.tickers == ["BTC"] and f.text == ["rate cut"]
    ev = {
        "event_id": "evt_1", "cluster_id": "cls_1", "published_at": "2026-10-16T18:05:12.410Z",
        "source": {"id": "reuters_wire", "name": "Reuters", "category": "wire", "credibility_tier": 1, "url": "u"},
        "content": {"raw_title": "Fed signals rate cut", "flash_headline": "Fed signals rate cut", "executive_summary": []},
        "enrichment": {"categories": ["Macro"], "tickers": ["BTC"], "entities": [], "locations": ["US"],
                       "sentiment": -0.2, "urgency": 8, "novelty": 0.9, "verification_status": "CONFIRMED"},
    }
    assert f.matches(ev)
    assert not EventFilter.parse("u>=9").matches(ev)
    assert not EventFilter.parse("-fed").matches(ev)
    assert EventFilter.parse("vs:confirmed tier:1").matches(ev)
    c = compact(ev)
    assert c["h"] == "Fed signals rate cut" and c["vs"] == "C" and c["tk"] == ["BTC"]


def test_tagger_finance_headline():
    src = SourceConfig(id="reuters_wire", name="Reuters", type="rss", category="wire", credibility_tier=1)
    t = tag("BREAKING: Nvidia shares plunge 9% after China bans H20 chip sales; $NVDA",
            "Nvidia Corp shares tumbled in premarket trading on Tuesday after Beijing ordered companies to "
            "halt purchases. Analysts at Morgan Stanley cut their price target.", src)
    assert "NVDA" in t.tickers
    assert "AI/Tech" in t.categories or "Equities" in t.categories
    assert "Breaking" in t.categories and t.urgency >= 8
    assert t.sentiment < -0.3
    assert "CN" in t.locations
    assert any(e["name"] == "Nvidia" and e["type"] == "ORG" for e in t.entities)


def test_tagger_low_value_and_pronoun_us():
    t = tag("Opinion: what to know about the best headphones for us this year", "", None)
    assert t.urgency <= 2
    assert "US" not in t.locations


def test_tagger_cve_and_crypto():
    t = tag("Critical CVE-2026-12345 in Fortinet VPN actively exploited by ransomware gangs", "", None)
    assert "Cybersecurity" in t.categories and any(e["type"] == "CVE" for e in t.entities)
    c = tag("Bitcoin tops $150,000 as ETF inflows surge", "Ethereum also rallied.", None)
    assert "Crypto" in c.categories and "BTC" in c.tickers and c.sentiment > 0.3


def test_summary_and_headline():
    assert flash_headline("BREAKING: Markets rally on Fed pivot - Reuters") == "Markets rally on Fed pivot"
    text = ("The Federal Reserve cut rates by 50 basis points on Wednesday. Chair Jerome Powell said the move "
            "was insurance against a slowing labor market. Markets rallied, with the S&P 500 up 1.8%. "
            "Treasury yields fell to their lowest since 2024.")
    bullets = executive_summary("Fed cuts rates by 50bp", text, ["fallback"])
    assert len(bullets) == 3 and all(len(b) <= 221 for b in bullets)


def test_html_to_markdown_strips_boilerplate():
    html = ('<div><p>Real <b>content</b> here with a <a href="https://x.com/a?utm_source=rss&id=2">link</a>.</p>'
            '<script>evil()</script><div class="share-buttons">Share!</div>'
            '<img src="https://feeds.feedburner.com/pixel" width="1" height="1"/>'
            '<p>The post Real content appeared first on Example.</p></div>')
    md = html_to_markdown(html)
    assert "evil" not in md and "Share!" not in md and "appeared first" not in md
    assert "**content**" in md and "utm_source" not in md


def test_company_mentions_in_release_notes_are_not_tickers():
    gh = SourceConfig(id="gh_llamacpp", name="llama.cpp Releases", type="rss", category="ai_lab", credibility_tier=1)
    t = tag("b11700", "Metal: faster kernels on Apple Silicon; SYCL fixes for Intel GPUs", gh)
    assert "AAPL" not in t.tickers and "INTC" not in t.tickers
    assert any(e["name"] == "Apple" for e in t.entities)  # still recognised as an entity
    wire = SourceConfig(id="reuters_wire", name="Reuters", type="rss", category="wire", credibility_tier=1)
    assert "AAPL" in tag("Shares slide after earnings", "Apple reported weaker iPhone demand.", wire).tickers
