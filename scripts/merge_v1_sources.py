"""One-shot: merge curated v1 sources into the v2 registry (config/default_sources.json).

v1 URLs are taken verbatim from scripts/gen_sources_v1.py (a working registry);
cadences follow v2's per-category conventions. Idempotent: skips ids already present.
Run from the repo root:  python scripts/merge_v1_sources.py
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REG = ROOT / "config" / "default_sources.json"

GN = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US%3Aen"

# (id, name, type, url, category, tier, credibility_tier, poll, min, max, params)
ADDITIONS = [
    # ---- wires & world (v1 group A) ----
    ("reuters_world", "Reuters (World/Conflict)", "rss",
     GN.format(q="when%3A1h+site%3Areuters.com+war+OR+minister+OR+military+OR+election+OR+president"),
     "wire", "A", 1, 20, 12, 120, {"hint_categories": ["Geopolitics"]}),
    ("ap_politics", "AP (Politics)", "rss",
     GN.format(q="when%3A1h+site%3Aapnews.com+congress+OR+senate+OR+election+OR+governor+OR+court"),
     "wire", "A", 1, 20, 12, 120, {"hint_categories": ["Geopolitics"]}),
    ("ft_markets", "Financial Times (Markets)", "rss", "https://www.ft.com/markets?format=rss",
     "finance", "A", 1, 30, 15, 120, None),
    ("ft_companies", "Financial Times (Companies)", "rss", "https://www.ft.com/companies?format=rss",
     "finance", "A", 1, 30, 15, 120, None),
    ("wsj_markets", "WSJ (Markets)", "rss", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
     "finance", "A", 1, 30, 15, 120, None),
    ("wsj_tech", "WSJ (Technology)", "rss", "https://feeds.content.dowjones.io/public/rss/RSSWSJD",
     "ai", "A", 1, 60, 40, 900, None),
    ("wsj_economy", "WSJ (Economy)", "rss", "https://feeds.content.dowjones.io/public/rss/socialeconomyfeed",
     "macro", "A", 1, 60, 40, 300, None),
    ("bbc_business", "BBC News (Business)", "rss", "https://feeds.bbci.co.uk/news/business/rss.xml",
     "macro", "A", 1, 30, 15, 120, None),
    ("bbc_mideast", "BBC News (Middle East)", "rss", "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml",
     "wire", "A", 1, 30, 15, 120, {"hint_categories": ["Geopolitics"]}),
    ("bbc_asia", "BBC News (Asia)", "rss", "https://feeds.bbci.co.uk/news/world/asia/rss.xml",
     "wire", "A", 1, 30, 15, 120, {"hint_categories": ["Geopolitics"]}),
    ("bbc_europe", "BBC News (Europe)", "rss", "https://feeds.bbci.co.uk/news/world/europe/rss.xml",
     "wire", "A", 1, 30, 15, 120, {"hint_categories": ["Geopolitics"]}),
    ("dw_business", "Deutsche Welle (Business)", "rss", "https://rss.dw.com/rdf/rss-en-bus",
     "macro", "A", 2, 60, 30, 300, None),
    ("france24_mideast", "France24 (Middle East)", "rss", "https://www.france24.com/en/middle-east/rss",
     "wire", "A", 2, 30, 15, 90, {"hint_categories": ["Geopolitics"]}),
    ("france24_asia", "France24 (Asia-Pacific)", "rss", "https://www.france24.com/en/asia-pacific/rss",
     "wire", "A", 2, 30, 15, 90, {"hint_categories": ["Geopolitics"]}),

    # ---- finance/macro/regulatory (v1 group B) ----
    ("fed_speeches", "Federal Reserve (Speeches)", "rss", "https://www.federalreserve.gov/feeds/speeches.xml",
     "macro", "B", 1, 60, 60, 300, None),
    ("bea_releases", "US Bureau of Economic Analysis", "rss", "https://apps.bea.gov/rss/rss.xml",
     "macro", "B", 1, 120, 90, 900, None),
    ("eurostat_releases", "Eurostat (Data Releases)", "rss",
     "https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss",
     "macro", "B", 1, 900, 300, 3600, {"max_items_per_poll": 10}),
    ("prnewswire_all", "PR Newswire (All Releases)", "rss", "https://www.prnewswire.com/rss/news-releases-list.rss",
     "finance", "B", 2, 60, 40, 300, {"hint_categories": ["Equities"]}),
    ("businesswire_all", "Business Wire (All Releases)", "rss",
     "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFtRWA==",
     "finance", "B", 2, 60, 40, 300, {"hint_categories": ["Equities"]}),
    ("cnbc_economy", "CNBC (Economy)", "rss", "https://www.cnbc.com/id/20910258/device/rss/rss.html",
     "macro", "B", 2, 60, 40, 300, None),
    ("marketwatch_pulse", "MarketWatch (Market Pulse)", "rss",
     "https://feeds.content.dowjones.io/public/rss/mw_marketpulse",
     "finance", "B", 2, 60, 40, 300, None),
    ("yahoo_finance", "Yahoo Finance (Index Headlines)", "rss",
     "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC,%5EDJI,%5EIXIC,%5EVIX,CL%3DF,GC%3DF&region=US&lang=en-US",
     "finance", "B", 2, 60, 40, 300, None),

    # ---- AI/tech/science (v1 group C) ----
    ("mit_tech_review", "MIT Technology Review", "rss", "https://www.technologyreview.com/feed/",
     "ai", "C", 2, 120, 60, 900, None),
    ("google_ai_blog", "Google (AI Blog)", "rss", "https://blog.google/technology/ai/rss/",
     "official", "C", 1, 180, 120, 1800, {"hint_categories": ["AI/Tech"]}),
    ("meta_engineering", "Meta (Engineering & AI)", "rss", "https://engineering.fb.com/feed/",
     "official", "C", 1, 300, 180, 3600, None),
    ("nvidia_blog", "NVIDIA (Blog)", "rss", "https://blogs.nvidia.com/feed/",
     "official", "C", 1, 300, 180, 3600, None),
    ("github_vllm_releases", "GitHub Releases (vLLM)", "github_releases",
     "https://github.com/vllm-project/vllm/releases.atom",
     "community", "C", 2, 900, 300, 3600, None),
    ("github_llamacpp_releases", "GitHub Releases (llama.cpp)", "github_releases",
     "https://github.com/ggml-org/llama.cpp/releases.atom",
     "community", "C", 2, 1800, 600, 7200, None),
    ("nature_news", "Nature", "rss", "https://www.nature.com/nature.rss",
     "science", "C", 1, 300, 180, 1800, {"hint_categories": ["Science"]}),
    ("sciencedaily", "ScienceDaily (Top Science)", "rss", "https://www.sciencedaily.com/rss/top/science.xml",
     "science", "C", 2, 300, 180, 1800, {"hint_categories": ["Science"]}),

    # ---- crypto (v1 group D) ----
    ("coindesk", "CoinDesk (Native Feed)", "rss", "https://www.coindesk.com/arc/outboundfeeds/rss/",
     "crypto", "B", 2, 20, 15, 90, {"hint_categories": ["Crypto"]}),

    # ---- cybersecurity (v1 group E) ----
    ("cisa_kev", "CISA (Known Exploited Vulnerabilities)", "cisa_kev",
     "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json",
     "cyber", "B", 1, 300, 120, 1800, {"days": 3, "max_items": 25, "hint_categories": ["Cybersecurity"]}),
    ("openai_status", "OpenAI (Status)", "rss", "https://status.openai.com/feed.rss",
     "infra", "B", 1, 90, 60, 900, None),

    # ---- social (v1 group F) ----
    ("telegram_preview", "Telegram Channel (Keyless Web Preview)", "telegram_web", "",
     "osint", "C", 3, 120, 60, 900,
     {"channels": ["durov"], "hint_categories": ["Breaking", "Geopolitics"]}),
]


def main() -> None:
    data = json.loads(REG.read_text(encoding="utf-8"))
    have = {s["id"] for s in data}
    added = 0
    for (sid, name, typ, url, cat, tier, cred, poll, mn, mx, params) in ADDITIONS:
        if sid in have:
            continue
        entry = {
            "id": sid, "name": name, "type": typ, "url": url,
            "category": cat, "tier": tier, "credibility_tier": cred,
            "poll_interval_sec": poll, "min_poll_sec": mn, "max_poll_sec": mx,
        }
        if params:
            entry["params"] = params
        if sid == "telegram_preview":
            entry["enabled"] = False  # template — enable at runtime via /api/sources
        data.append(entry)
        added += 1
    ids = [s["id"] for s in data]
    assert len(ids) == len(set(ids)), "duplicate ids"
    REG.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"{added} added -> {len(data)} total")


if __name__ == "__main__":
    main()
