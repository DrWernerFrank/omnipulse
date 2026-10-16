"""Tests for the v1→v2 merge surface: DSL filters, headline format, outbound
webhooks, the openai_compatible LLM provider, and the merged connectors."""
from __future__ import annotations

import asyncio

import pytest

from app.config import get_settings
from app.connectors import _cisa_kev, _parse_telegram_page, _telegram_web
from app.dsl import EventFilter, headline_line, parse_dsl
from app.schemas import FilterSpec, NewsEvent, SourceConfig, now_utc


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _event(**over) -> NewsEvent:
    ts = now_utc()
    base = dict(
        event_id="evt_test1",
        cluster_id="cls_test1",
        published_at=ts,
        ingested_at=ts,
        source={"id": "reuters_wire", "name": "Reuters", "category": "wire",
                "credibility_tier": 1, "url": "https://reuters.com"},
        content={"raw_title": "Fed cuts rates", "flash_headline": "Fed cuts rates",
                 "executive_summary": ["a", "b", "c"], "clean_markdown": "body"},
        enrichment={"categories": ["Macro"], "tickers": ["NVDA"], "entities": [],
                    "locations": ["US"], "sentiment": 0.4, "urgency": 8,
                    "novelty": 0.9, "verification_status": "CONFIRMED"},
    )
    base.update(over)
    return NewsEvent(**base)


# --------------------------------------------------------------------------- #
# DSL (merged from v1's filters.py)
# --------------------------------------------------------------------------- #
def test_dsl_parse_and_match():
    f = EventFilter.parse("u>=7 cat:macro $NVDA loc:US vs:confirmed -opinion")
    assert f.min_urgency == 7
    assert f.categories == ["Macro"]
    assert f.tickers == ["NVDA"]
    assert f.locations == ["US"]
    assert f.verification == ["CONFIRMED"]
    assert f.exclude == ["opinion"]
    assert f.matches(_event().model_dump(mode="json"))

    # urgency below floor
    assert not EventFilter.parse("u>=9").matches(_event().model_dump(mode="json"))
    # negative
    assert not EventFilter.parse("fed cuts").matches(
        _event(content={"raw_title": "no match here", "flash_headline": "no match here",
                        "executive_summary": [], "clean_markdown": ""}).model_dump(mode="json")
    )
    # exclusion hits
    assert not EventFilter.parse("-fed").matches(_event().model_dump(mode="json"))


def test_dsl_numeric_operators_and_aliases():
    f = EventFilter.parse("u>7 sent>0.3 nov>=0.5 tier<=1")
    assert f.min_urgency == 8
    assert f.min_sentiment == 0.3
    assert f.min_novelty == 0.5
    assert f.max_tier == 1
    # category aliases resolve through the v2 taxonomy
    assert EventFilter.parse("cat:crypto").categories == ["Crypto"]
    assert EventFilter.parse("cat:tech").categories == ["AI/Tech"]
    # parse cache returns the same compiled filter
    assert parse_dsl("u>=7") is parse_dsl("u>=7")


def test_filterspec_dsl_composes_with_structured_filters():
    from app.db import MemoryStore

    store = MemoryStore(get_settings())
    hi = _event()
    lo = _event(
        event_id="evt_test2", cluster_id="cls_test2",
        enrichment={"categories": ["Crypto"], "tickers": ["BTC"], "entities": [],
                    "locations": ["US"], "sentiment": 0.0, "urgency": 3,
                    "novelty": 0.5, "verification_status": "UNVERIFIED"},
    )
    asyncio.run(store.add_event(hi, 0, [0.0] * 8))
    asyncio.run(store.add_event(lo, 0, [0.0] * 8))

    got = asyncio.run(store.query_events(FilterSpec(dsl="u>=7 cat:macro")))
    assert [e.event_id for e in got] == ["evt_test1"]
    got = asyncio.run(store.query_events(FilterSpec(dsl="cat:crypto")))
    assert [e.event_id for e in got] == ["evt_test2"]


def test_headline_line_format():
    line = headline_line(_event().model_dump(mode="json"))
    assert "U8 " in line
    assert "[Reuters|T1]" in line
    assert "$NVDA" in line
    assert "evt_test1" in line


# --------------------------------------------------------------------------- #
# outbound webhooks (merged from v1)
# --------------------------------------------------------------------------- #
def test_webhook_sign_matches_v1_wire_format():
    import hashlib
    import hmac as _hmac

    from app.webhooks import sign

    body = b'{"type": "EVENT_CREATED"}'
    expect = "sha256=" + _hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
    assert sign("s3cret", body) == expect


def test_webhook_dispatcher_register_and_deliver(monkeypatch):
    from app.bus import Bus
    from app.db import MemoryStore
    from app.dsl import parse_dsl
    from app.webhooks import WebhookDispatcher

    calls: list[tuple[str, dict | None]] = []

    class _Resp:
        status_code = 200

        def raise_for_status(self): ...

    class _Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, content=None, headers=None, json=None):
            calls.append((url, headers))
            return _Resp()

        async def aclose(self): ...

    store = MemoryStore(get_settings())
    bus = Bus()
    disp = WebhookDispatcher(store, bus)
    monkeypatch.setattr(disp, "_make_client", lambda: _Client())

    async def run():
        hook = await disp.register("https://example.test/hook", secret="s3cret", dsl="u>=7 cat:macro")
        assert hook.id.startswith("hook_")
        # DSL gate: a low-urgency crypto event must be filtered out before delivery
        low = _event(
            event_id="evt_low", cluster_id="cls_low",
            enrichment={"categories": ["Crypto"], "tickers": ["BTC"], "entities": [],
                        "locations": ["US"], "sentiment": 0.0, "urgency": 2,
                        "novelty": 0.5, "verification_status": "UNVERIFIED"},
        ).model_dump(mode="json")
        assert not parse_dsl(hook.dsl).matches(low)
        # the registered hook only delivers matching events
        await disp._deliver(hook, _event().model_dump(mode="json"))
        return hook

    hook = asyncio.run(run())
    assert len(calls) == 1
    url, headers = calls[0]
    assert url == "https://example.test/hook"
    assert headers["X-OmniPulse-Webhook"] == hook.id
    assert headers["X-OmniPulse-Event"] == "EVENT_CREATED"
    assert headers["X-OmniPulse-Signature"].startswith("sha256=")
    assert hook.delivered == 1
    assert hook.failures == 0

    # list/remove round-trips through the store
    listed = asyncio.run(store.list_webhooks())
    assert [h.id for h in listed] == [hook.id]
    assert asyncio.run(disp.remove(hook.id))


# --------------------------------------------------------------------------- #
# openai_compatible LLM provider (env-only config; values never committed)
# --------------------------------------------------------------------------- #
def test_openai_compatible_refiner(monkeypatch):
    from app import enrich as E

    posts: list[tuple[str, dict, dict]] = []

    class _Resp:
        def raise_for_status(self): ...

        def json(self):
            return {"choices": [{"message": {"content":
                '{"flash_headline": "Fed cuts rates by 25bp", "urgency": 9, "tickers": ["NVDA"]}'}}]}

    class _Client:
        def __init__(self, *a, **k): ...

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def post(self, url, headers=None, json=None):
            posts.append((url, headers, json))
            return _Resp()

    monkeypatch.setattr(E.httpx, "AsyncClient", _Client)
    monkeypatch.setenv("OMNIPULSE_LLM_PROVIDER", "openai_compatible")
    monkeypatch.setenv("OMNIPULSE_LLM_BASE_URL", "https://llm.example.test/v1")
    monkeypatch.setenv("OMNIPULSE_LLM_API_KEY", "k-local-test")
    monkeypatch.setenv("OMNIPULSE_LLM_MODEL", "m-local-test")
    get_settings.cache_clear()

    svc = E.Enricher()
    assert svc.resolve_llm() == "openai_compatible"

    refiner = E.LLMRefiner("openai_compatible")
    out = asyncio.run(refiner.refine({
        "raw_title": "Fed cuts rates",
        "clean_markdown": "body text",
        "categories": ["Macro"],
    }))
    assert out and out["flash_headline"] == "Fed cuts rates by 25bp"
    assert out["urgency"] == 9

    url, headers, payload = posts[0]
    assert url == "https://llm.example.test/v1/chat/completions"
    assert headers["Authorization"] == "Bearer k-local-test"
    assert payload["model"] == "m-local-test"
    assert payload["response_format"] == {"type": "json_object"}

    get_settings.cache_clear()
    monkeypatch.delenv("OMNIPULSE_LLM_PROVIDER")
    monkeypatch.delenv("OMNIPULSE_LLM_BASE_URL")
    monkeypatch.delenv("OMNIPULSE_LLM_API_KEY")
    monkeypatch.delenv("OMNIPULSE_LLM_MODEL")


# --------------------------------------------------------------------------- #
# merged connectors: CISA KEV + telegram web preview
# --------------------------------------------------------------------------- #
_KEV_DOC = {
    "title": "CISA Catalog of Known Exploited Vulnerabilities",
    "vulnerabilities": [
        {"cveID": "CVE-2026-1234", "vendorProject": "Acme", "product": "Router",
         "vulnerabilityName": "RCE", "dateAdded": now_utc().date().isoformat(),
         "shortDescription": "Actively exploited", "requiredAction": "Patch",
         "dueDate": "2026-10-30", "knownRansomwareCampaignUse": "Known"},
        {"cveID": "CVE-2020-0001", "vendorProject": "Old", "product": "Thing",
         "vulnerabilityName": "Old bug", "dateAdded": "2020-01-01",
         "shortDescription": "Historic", "requiredAction": "Patch", "dueDate": "2020-02-01",
         "knownRansomwareCampaignUse": "Unknown"},
    ],
}


def test_cisa_kev_connector_filters_by_date(monkeypatch):
    class _Pool:
        async def fetch_json(self, url, source_headers=None, source_id=None):
            return _KEV_DOC

    class _HttpPool:
        @staticmethod
        def get():
            return _Pool()

    monkeypatch.setattr("app.connectors.HttpPool", _HttpPool)
    cfg = SourceConfig(
        id="cisa_kev", name="CISA KEV", type="cisa_kev",
        url="https://example.test/kev.json", category="cyber", credibility_tier=1,
        params={"days": 3},
    )
    items = asyncio.run(_cisa_kev(cfg))
    assert len(items) == 1
    assert items[0].external_id == "kev:CVE-2026-1234"
    assert "CVE-2026-1234" in items[0].title
    assert items[0].url == "https://nvd.nist.gov/vuln/detail/CVE-2026-1234"
    assert items[0].extra["hint_categories"] == ["Cybersecurity"]


_TG_PAGE = """
<html><body>
<div class="tgme_widget_message" data-post="durov/123">
  <div class="tgme_widget_message_text">Big announcement<br/>second line</div>
  <a class="tgme_widget_message_date"><time datetime="2026-10-15T10:00:00+00:00"></time></a>
</div>
<div class="tgme_widget_message" data-post="durov/124">
  <div class="tgme_widget_message_text">short</div>
</div>
</body></html>
"""


def test_telegram_web_parser_and_connector(monkeypatch):
    posts = _parse_telegram_page(_TG_PAGE)
    assert posts and posts[0]["post"] == "durov/123"
    assert posts[0]["text"].startswith("Big announcement")
    assert posts[0]["time"] == "2026-10-15T10:00:00+00:00"
    # the text-less "short" post is dropped by the connector (len < 10)
    assert posts[1]["text"] == "short"

    class _Pool:
        async def fetch_text(self, url, source_headers=None, source_id=None):
            return _TG_PAGE

    class _HttpPool:
        @staticmethod
        def get():
            return _Pool()

    monkeypatch.setattr("app.connectors.HttpPool", _HttpPool)
    cfg = SourceConfig(
        id="tg_test", name="TG", type="telegram_web", category="osint",
        credibility_tier=3, params={"channels": ["durov"]},
    )
    items = asyncio.run(_telegram_web(cfg))
    assert len(items) == 1
    assert items[0].external_id == "tg:durov/123"
    assert items[0].url == "https://t.me/durov/123"
    assert items[0].published_at is not None
