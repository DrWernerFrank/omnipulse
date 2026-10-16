from app.enrich import EnrichmentService
from app.schemas import SourceConfig


def mk_source(sid="test_wire", tier=1, url="https://reuters.com/x"):
    return SourceConfig(id=sid, name="Test Wire", type="rss", url=url,
                        category="wire", credibility_tier=tier)


svc = EnrichmentService()
svc.start()


def test_categories_finance():
    out = svc.base.heuristic(
        "Fed cuts interest rates 25 basis points, bonds rally as inflation cools",
        "The Federal Reserve lowered rates. Treasury yields fell.",
        mk_source(),
    )
    assert "Macro" in out["categories"]


def test_categories_crypto():
    out = svc.base.heuristic(
        "Bitcoin ETF inflows surge as crypto rally extends",
        "Spot ETFs saw $500M inflows. BTC jumped 4%.",
        mk_source(),
    )
    assert "Crypto" in out["categories"]
    assert "BTC" in out["tickers"]


def test_categories_cyber():
    out = svc.base.heuristic(
        "Critical zero-day CVE-2024-45779 actively exploited in the wild",
        "CISA added the flaw to its KEV catalog.",
        mk_source(),
    )
    assert "Cybersecurity" in out["categories"]
    assert any(e["name"].startswith("CVE-") for e in out["entities"])


def test_entity_extraction():
    out = svc.base.heuristic(
        "Jensen Huang says NVIDIA Blackwell demand is insane",
        "", mk_source(),
    )
    names = {e["name"] for e in out["entities"]}
    assert "NVIDIA" in names
    assert "Jensen Huang" in names
    assert "NVDA" in out["tickers"]


def test_locations():
    out = svc.base.heuristic(
        "Germany and France push new EU fiscal rules while Japan watches",
        "", mk_source(),
    )
    assert set(out["locations"]) >= {"DE", "FR", "EU", "JP"}


def test_sentiment_negative():
    out = svc.base.heuristic(
        "Stocks plunge as recession fears spark global selloff",
        "Markets crashed. Investors panicked.", mk_source(),
    )
    assert out["sentiment"] < -0.2


def test_sentiment_positive():
    out = svc.base.heuristic(
        "Earnings beat: record profits and guidance raised, shares surge",
        "", mk_source(),
    )
    assert out["sentiment"] > 0.2


def test_urgency_flash():
    out = svc.base.heuristic(
        "BREAKING: massive earthquake strikes, emergency declared",
        "", mk_source(),
    )
    assert out["urgency"] >= 8


def test_urgency_dampened_for_opinion():
    out = svc.base.heuristic(
        "Opinion: why this week in review was a slow news newsletter",
        "Analysis and recap of the week.", mk_source(),
    )
    assert out["urgency"] <= 4


def test_verification_official_source():
    out = svc.base.heuristic("New rule proposed", "", mk_source(url="https://sec.gov/x"))
    assert out["verification_status"] == "CONFIRMED"


def test_flash_and_summary():
    out = svc.base.heuristic(
        "Apple unveils new AI chip for iPhones",
        "Apple announced the chip today. It will ship next year. Pricing is unknown.",
        mk_source(),
    )
    assert out["flash_headline"]
    assert 1 <= len(out["executive_summary"]) <= 3
