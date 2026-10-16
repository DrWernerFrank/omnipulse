"""Generates config/default_sources.json. Edit the table below, then: python scripts/gen_sources.py"""
import json, sys

GN = "https://news.google.com/rss/search?q={q}+when:1h&hl=en-US&gl=US&ceid=US:en"
SEC = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={t}&company=&dateb=&owner=include&start=0&count=40&output=atom"
REDDIT = "https://www.reddit.com/r/{s}/new/.rss"

def src(id, name, type, url, category, tier, group, cats=(), **kw):
    d = {"id": id, "name": name, "type": type, "url": url, "category": category,
         "credibility_tier": tier, "group": group}
    if cats:
        d["default_categories"] = list(cats)
    d.update(kw)
    return d

def gnews(id, name, q, publisher, category, tier, group, cats=(), **kw):
    return src(id, name, "rss", GN.format(q=q), category, tier, group, cats,
               params={"aggregator": "gnews", "publisher": publisher, "full_article": False}, **kw)

S = [
    # ---------------- A. Tier-1 breaking wires & global news ----------------
    gnews("reuters_wire", "Reuters", "site:reuters.com", "Reuters", "wire", 1, "A"),
    gnews("reuters_markets", "Reuters Markets", "site:reuters.com+(stocks+OR+markets+OR+shares+OR+bonds+OR+oil)", "Reuters", "wire", 1, "A", ["Equities"]),
    gnews("reuters_world", "Reuters World", "site:reuters.com+(war+OR+minister+OR+military+OR+election+OR+president)", "Reuters", "wire", 1, "A", ["Geopolitics"]),
    gnews("ap_top", "Associated Press", "site:apnews.com", "AP News", "wire", 1, "A"),
    gnews("ap_world", "AP World", "site:apnews.com+(world+OR+war+OR+minister+OR+military)", "AP News", "wire", 1, "A", ["Geopolitics"]),
    gnews("ap_politics", "AP Politics", "site:apnews.com+(congress+OR+senate+OR+election+OR+governor+OR+court)", "AP News", "wire", 1, "A", ["Geopolitics"]),
    src("bloomberg_markets", "Bloomberg Markets", "rss", "https://feeds.bloomberg.com/markets/news.rss", "wire", 1, "A", ["Equities"], params={"full_article": False}),
    src("bloomberg_tech", "Bloomberg Technology", "rss", "https://feeds.bloomberg.com/technology/news.rss", "wire", 1, "A", ["AI/Tech"], params={"full_article": False}),
    src("bloomberg_economics", "Bloomberg Economics", "rss", "https://feeds.bloomberg.com/economics/news.rss", "wire", 1, "A", ["Macro"], params={"full_article": False}),
    src("bloomberg_politics", "Bloomberg Politics", "rss", "https://feeds.bloomberg.com/politics/news.rss", "wire", 1, "A", ["Geopolitics"], params={"full_article": False}),
    gnews("bloomberg_crypto", "Bloomberg Crypto", "site:bloomberg.com+(bitcoin+OR+crypto+OR+stablecoin+OR+ether)", "Bloomberg", "wire", 1, "A", ["Crypto"]),
    src("ft_world", "Financial Times World", "rss", "https://www.ft.com/world?format=rss", "wire", 1, "A", ["Geopolitics"]),
    src("ft_markets", "Financial Times Markets", "rss", "https://www.ft.com/markets?format=rss", "wire", 1, "A", ["Equities"]),
    src("ft_companies", "Financial Times Companies", "rss", "https://www.ft.com/companies?format=rss", "wire", 1, "A", ["Equities"]),
    src("ft_global_economy", "Financial Times Global Economy", "rss", "https://www.ft.com/global-economy?format=rss", "wire", 1, "A", ["Macro"]),
    src("wsj_markets", "WSJ Markets", "rss", "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain", "wire", 1, "A", ["Equities"]),
    src("wsj_world", "WSJ World", "rss", "https://feeds.content.dowjones.io/public/rss/RSSWorldNews", "wire", 1, "A", ["Geopolitics"]),
    src("wsj_tech", "WSJ Technology", "rss", "https://feeds.content.dowjones.io/public/rss/RSSWSJD", "wire", 1, "A", ["AI/Tech"]),
    src("wsj_economy", "WSJ Economy", "rss", "https://feeds.content.dowjones.io/public/rss/socialeconomyfeed", "wire", 1, "A", ["Macro"]),
    src("bbc_world", "BBC News World", "rss", "https://feeds.bbci.co.uk/news/world/rss.xml", "wire", 1, "A", ["Geopolitics"]),
    src("bbc_business", "BBC News Business", "rss", "https://feeds.bbci.co.uk/news/business/rss.xml", "wire", 1, "A", ["Macro"]),
    src("bbc_middle_east", "BBC News Middle East", "rss", "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml", "wire", 1, "A", ["Geopolitics"]),
    src("bbc_asia", "BBC News Asia", "rss", "https://feeds.bbci.co.uk/news/world/asia/rss.xml", "wire", 1, "A", ["Geopolitics"]),
    src("bbc_europe", "BBC News Europe", "rss", "https://feeds.bbci.co.uk/news/world/europe/rss.xml", "wire", 1, "A", ["Geopolitics"]),
    src("aljazeera_all", "Al Jazeera", "rss", "https://www.aljazeera.com/xml/rss/all.xml", "wire", 2, "A", ["Geopolitics"]),
    src("dw_world", "Deutsche Welle World", "rss", "https://rss.dw.com/rdf/rss-en-world", "wire", 2, "A", ["Geopolitics"]),
    src("dw_business", "Deutsche Welle Business", "rss", "https://rss.dw.com/rdf/rss-en-bus", "wire", 2, "A", ["Macro"]),
    src("france24_world", "France 24", "rss", "https://www.france24.com/en/rss", "wire", 2, "A", ["Geopolitics"]),
    src("france24_mideast", "France 24 Middle East", "rss", "https://www.france24.com/en/middle-east/rss", "wire", 2, "A", ["Geopolitics"]),
    src("france24_asia", "France 24 Asia-Pacific", "rss", "https://www.france24.com/en/asia-pacific/rss", "wire", 2, "A", ["Geopolitics"]),
    src("gdelt_global", "GDELT Global Events", "gdelt", "", "aggregator", 2, "A",
        poll_interval=300, min_interval=120, max_interval=900,
        params={"query": "sourcelang:english (theme:ARMEDCONFLICT OR theme:TERROR OR theme:ECON_STOCKMARKET OR theme:NATURAL_DISASTER OR theme:ELECTION)",
                "timespan": "15min", "maxrecords": 75, "full_article": False}),

    # ---------------- B. Finance, macro & regulatory ----------------
    src("sec_8k", "SEC EDGAR 8-K", "sec_edgar", SEC.format(t="8-K"), "regulatory", 1, "B", ["Regulatory"]),
    src("sec_form4", "SEC EDGAR Form 4", "sec_edgar", SEC.format(t="4"), "regulatory", 1, "B", ["Regulatory"]),
    src("sec_10q", "SEC EDGAR 10-Q", "sec_edgar", SEC.format(t="10-Q"), "regulatory", 1, "B", ["Regulatory"], poll_interval=60),
    src("sec_10k", "SEC EDGAR 10-K", "sec_edgar", SEC.format(t="10-K"), "regulatory", 1, "B", ["Regulatory"], poll_interval=60),
    src("sec_13f", "SEC EDGAR 13F", "sec_edgar", SEC.format(t="13F-HR"), "regulatory", 1, "B", ["Regulatory"], poll_interval=120),
    src("fed_press", "Federal Reserve Press Releases", "rss", "https://www.federalreserve.gov/feeds/press_all.xml", "macro", 1, "B", ["Macro"]),
    src("fed_speeches", "Federal Reserve Speeches", "rss", "https://www.federalreserve.gov/feeds/speeches.xml", "macro", 1, "B", ["Macro"], poll_interval=60),
    src("ecb_press", "ECB Press", "rss", "https://www.ecb.europa.eu/rss/press.html", "macro", 1, "B", ["Macro"]),
    src("boj_news", "Bank of Japan", "rss", "https://www.boj.or.jp/en/rss/whatsnew.xml", "macro", 1, "B", ["Macro"]),
    src("bls_latest", "US Bureau of Labor Statistics", "rss", "https://www.bls.gov/feed/bls_latest.rss", "macro", 1, "B", ["Macro"], params={"contact_ua": True}),
    src("bea_releases", "US Bureau of Economic Analysis", "rss", "https://apps.bea.gov/rss/rss.xml", "macro", 1, "B", ["Macro"]),
    src("eurostat_releases", "Eurostat Data Releases", "rss", "https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss", "macro", 1, "B", ["Macro"],
        poll_interval=900, min_interval=300, max_interval=3600, params={"max_items_per_poll": 10, "full_article": False}),
    src("prnewswire_all", "PR Newswire", "rss", "https://www.prnewswire.com/rss/news-releases-list.rss", "finance", 2, "B", ["Equities"]),
    src("businesswire_all", "Business Wire", "rss", "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeEFtRWA==", "finance", 2, "B", ["Equities"]),
    src("cnbc_top", "CNBC Top News", "rss", "https://www.cnbc.com/id/100003114/device/rss/rss.html", "finance", 2, "B", ["Equities"]),
    src("cnbc_markets", "CNBC Investing", "rss", "https://www.cnbc.com/id/15839069/device/rss/rss.html", "finance", 2, "B", ["Equities"]),
    src("cnbc_economy", "CNBC Economy", "rss", "https://www.cnbc.com/id/20910258/device/rss/rss.html", "finance", 2, "B", ["Macro"]),
    src("marketwatch_top", "MarketWatch Top Stories", "rss", "https://feeds.content.dowjones.io/public/rss/mw_topstories", "finance", 2, "B", ["Equities"]),
    src("marketwatch_pulse", "MarketWatch Market Pulse", "rss", "https://feeds.content.dowjones.io/public/rss/mw_marketpulse", "finance", 2, "B", ["Equities"]),
    src("yahoo_finance", "Yahoo Finance", "rss", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC,%5EDJI,%5EIXIC,%5EVIX,CL%3DF,GC%3DF&region=US&lang=en-US", "finance", 2, "B", ["Equities"]),
    src("polymarket_shifts", "Polymarket", "polymarket", "", "prediction", 2, "B",
        poll_interval=30, min_interval=15, max_interval=120, params={"threshold": 0.05, "limit": 150, "min_volume_24h": 50000}),
    src("kalshi_shifts", "Kalshi", "kalshi", "", "prediction", 2, "B",
        poll_interval=30, min_interval=15, max_interval=120, params={"threshold": 0.05, "min_volume": 1000}),

    # ---------------- C. AI, technology & science ----------------
    src("hn_front", "Hacker News Front Page", "hackernews", "", "community", 2, "C", ["AI/Tech"],
        params={"list": "topstories", "top_n": 30, "min_score": 150, "full_article": True}),
    src("hn_show", "Show HN", "hackernews", "", "community", 2, "C", ["AI/Tech"],
        params={"list": "showstories", "top_n": 30, "min_score": 80}),
    src("arxiv_ai", "arXiv cs.AI / cs.LG / cs.CL", "rss", "https://rss.arxiv.org/rss/cs.AI+cs.LG+cs.CL", "science", 2, "C", ["AI/Tech", "Science"],
        poll_interval=600, min_interval=300, max_interval=3600, params={"max_items_per_poll": 40, "full_article": False}),
    src("arxiv_qfin", "arXiv q-fin", "rss", "https://rss.arxiv.org/rss/q-fin", "science", 2, "C", ["Science", "Macro"],
        poll_interval=900, min_interval=300, max_interval=3600, params={"max_items_per_poll": 20, "full_article": False}),
    src("techcrunch", "TechCrunch", "rss", "https://techcrunch.com/feed/", "tech", 2, "C", ["AI/Tech"]),
    src("the_verge", "The Verge", "rss", "https://www.theverge.com/rss/index.xml", "tech", 2, "C", ["AI/Tech"]),
    src("ars_technica", "Ars Technica", "rss", "https://feeds.arstechnica.com/arstechnica/index", "tech", 2, "C", ["AI/Tech"]),
    src("wired", "WIRED", "rss", "https://www.wired.com/feed/rss", "tech", 2, "C", ["AI/Tech"]),
    src("mit_tech_review", "MIT Technology Review", "rss", "https://www.technologyreview.com/feed/", "tech", 2, "C", ["AI/Tech"]),
    src("semianalysis", "SemiAnalysis", "rss", "https://newsletter.semianalysis.com/feed", "tech", 2, "C", ["AI/Tech"], poll_interval=600),
    src("openai_news", "OpenAI News", "rss", "https://openai.com/news/rss.xml", "ai_lab", 1, "C", ["AI/Tech"]),
    src("anthropic_news", "Anthropic News", "rss", GN.format(q="site:anthropic.com").replace("when:1h", "when:7d"), "ai_lab", 1, "C", ["AI/Tech"], poll_interval=300,
        params={"aggregator": "gnews", "publisher": "Anthropic", "full_article": False}),
    src("deepmind_blog", "Google DeepMind Blog", "rss", "https://deepmind.google/blog/rss.xml", "ai_lab", 1, "C", ["AI/Tech"]),
    src("google_ai_blog", "Google AI Blog", "rss", "https://blog.google/technology/ai/rss/", "ai_lab", 1, "C", ["AI/Tech"]),
    src("meta_engineering", "Meta Engineering & AI", "rss", "https://engineering.fb.com/feed/", "ai_lab", 1, "C", ["AI/Tech"]),
    src("huggingface_blog", "Hugging Face Blog", "rss", "https://huggingface.co/blog/feed.xml", "ai_lab", 1, "C", ["AI/Tech"]),
    src("nvidia_newsroom", "NVIDIA Newsroom", "rss", "https://nvidianews.nvidia.com/releases.xml", "ai_lab", 1, "C", ["AI/Tech"]),
    src("nvidia_blog", "NVIDIA Blog", "rss", "https://blogs.nvidia.com/feed/", "ai_lab", 1, "C", ["AI/Tech"]),
    src("github_trending", "GitHub Trending", "github_trending", "https://github.com/trending", "community", 2, "C", ["AI/Tech"],
        poll_interval=900, min_interval=600, max_interval=3600, params={"since": "daily", "min_stars_today": 150}),
    src("gh_ollama", "Ollama Releases", "rss", "https://github.com/ollama/ollama/releases.atom", "ai_lab", 1, "C", ["AI/Tech"], poll_interval=300, min_interval=120),
    src("gh_vllm", "vLLM Releases", "rss", "https://github.com/vllm-project/vllm/releases.atom", "ai_lab", 1, "C", ["AI/Tech"], poll_interval=300, min_interval=120),
    src("gh_transformers", "Transformers Releases", "rss", "https://github.com/huggingface/transformers/releases.atom", "ai_lab", 1, "C", ["AI/Tech"], poll_interval=300, min_interval=120),
    src("gh_llamacpp", "llama.cpp Releases", "rss", "https://github.com/ggml-org/llama.cpp/releases.atom", "ai_lab", 1, "C", ["AI/Tech"], poll_interval=600, min_interval=300, params={"max_items_per_poll": 3}),
    src("nature_news", "Nature", "rss", "https://www.nature.com/nature.rss", "science", 1, "C", ["Science"], poll_interval=300),
    src("sciencedaily", "ScienceDaily", "rss", "https://www.sciencedaily.com/rss/top/science.xml", "science", 2, "C", ["Science"], poll_interval=300),

    # ---------------- D. Crypto & Web3 ----------------
    src("coindesk", "CoinDesk", "rss", "https://www.coindesk.com/arc/outboundfeeds/rss/", "crypto", 2, "D", ["Crypto"]),
    src("theblock", "The Block", "rss", "https://www.theblock.co/rss.xml", "crypto", 2, "D", ["Crypto"]),
    src("decrypt", "Decrypt", "rss", "https://decrypt.co/feed", "crypto", 2, "D", ["Crypto"]),
    src("blockworks", "Blockworks", "rss", "https://blockworks.co/feed", "crypto", 2, "D", ["Crypto"]),
    src("cointelegraph", "Cointelegraph", "rss", "https://cointelegraph.com/rss", "crypto", 2, "D", ["Crypto"]),
    src("whale_alert", "Whale Alert", "whale_alert", "", "crypto", 2, "D", ["Crypto"],
        requires_key="whale_alert_key", poll_interval=30, params={"min_value": 10000000}),

    # ---------------- E. Cybersecurity & infrastructure ----------------
    src("cisa_advisories", "CISA Advisories", "rss", "https://www.cisa.gov/cybersecurity-advisories/all.xml", "security", 1, "E", ["Cybersecurity"]),
    src("cisa_kev", "CISA Known Exploited Vulnerabilities", "cisa_kev", "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json", "security", 1, "E", ["Cybersecurity"],
        poll_interval=300, min_interval=120, max_interval=1800, params={"days": 3}),
    src("nvd_cves", "NIST NVD (CVSS >= 7)", "nvd", "", "security", 1, "E", ["Cybersecurity"],
        poll_interval=120, min_interval=60, max_interval=900, params={"min_cvss": 7.0}),
    src("bleepingcomputer", "BleepingComputer", "rss", "https://www.bleepingcomputer.com/feed/", "security", 2, "E", ["Cybersecurity"]),
    src("krebsonsecurity", "KrebsOnSecurity", "rss", "https://krebsonsecurity.com/feed/", "security", 2, "E", ["Cybersecurity"]),
    src("thehackernews", "The Hacker News", "rss", "https://feeds.feedburner.com/TheHackersNews", "security", 2, "E", ["Cybersecurity"]),
    src("darkreading", "Dark Reading", "rss", "https://www.darkreading.com/rss.xml", "security", 2, "E", ["Cybersecurity"]),
    src("status_aws", "AWS Health", "rss", "https://status.aws.amazon.com/rss/all.rss", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),
    src("status_cloudflare", "Cloudflare Status", "rss", "https://www.cloudflarestatus.com/history.atom", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),
    src("status_github", "GitHub Status", "rss", "https://www.githubstatus.com/history.atom", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),
    src("status_openai", "OpenAI Status", "rss", "https://status.openai.com/feed.rss", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),
    src("status_anthropic", "Anthropic Status", "rss", "https://status.anthropic.com/history.atom", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),
    src("status_vercel", "Vercel Status", "rss", "https://www.vercel-status.com/history.atom", "status", 1, "E", ["Cybersecurity"], params={"full_article": False}),

    # ---------------- F. Social & community firehoses ----------------
    src("bluesky_breaking", "Bluesky", "bluesky", "wss://jetstream2.us-east.bsky.network/subscribe", "social", 3, "F",
        params={"langs": ["en"], "min_length": 40, "max_per_minute": 30, "dids": [], "only_dids": False,
                "full_article": False}),
] + [
    src(f"reddit_{s.lower()}", f"r/{s}", "rss", REDDIT.format(s=s), "social", 3, "F", cats,
        poll_interval=180, min_interval=120, max_interval=1800, params={"aggregator": "reddit", "full_article": False})
    for s, cats in [("worldnews", ["Geopolitics"]), ("LocalLLaMA", ["AI/Tech"]), ("MachineLearning", ["AI/Tech"]),
                    ("wallstreetbets", ["Equities"]), ("Economics", ["Macro"]), ("netsec", ["Cybersecurity"]),
                    ("geopolitics", ["Geopolitics"])]
] + [
    src("telegram_example", "Telegram channel (template)", "telegram", "", "social", 3, "F", enabled=False,
        params={"channel": "durov", "full_article": False}),
    src("newsapi_top", "NewsAPI Top Headlines", "newsapi", "", "aggregator", 2, "A", requires_key="newsapi_key",
        poll_interval=300, min_interval=120, params={"language": "en", "category": "general"}),
    src("x_list", "X / Twitter List", "x_list", "", "social", 3, "F", requires_key="x_bearer_token", enabled=False,
        poll_interval=120, params={"list_id": "REPLACE_WITH_LIST_ID"}),
    src("discord_inbox", "Discord / Webhook Inbox", "webhook", "", "social", 3, "F",
        params={"secret": "", "full_article": False}),
]

doc = {
    "version": 1,
    "description": "OmniPulse default source registry. Zero-key: every enabled source without 'requires_key' works anonymously. Groups set default cadence; per-source fields override. Add/pause/remove at runtime via /v1/sources or the MCP tools.",
    "groups": {
        "A": {"label": "Tier-1 breaking wires & global news", "poll_interval": 20, "min_interval": 10, "max_interval": 300},
        "B": {"label": "Finance, macro & regulatory filings", "poll_interval": 15, "min_interval": 5, "max_interval": 300},
        "C": {"label": "AI, technology & science", "poll_interval": 60, "min_interval": 30, "max_interval": 900},
        "D": {"label": "Crypto & Web3", "poll_interval": 20, "min_interval": 15, "max_interval": 300},
        "E": {"label": "Cybersecurity & infrastructure", "poll_interval": 45, "min_interval": 30, "max_interval": 900},
        "F": {"label": "Real-time social & community firehoses", "poll_interval": 120, "min_interval": 60, "max_interval": 900},
    },
    "sources": S,
}
ids = [s["id"] for s in S]
assert len(ids) == len(set(ids)), "dup ids"
out = sys.argv[1] if len(sys.argv) > 1 else str(__import__("pathlib").Path(__file__).resolve().parents[1] / "config" / "default_sources.json")
json.dump(doc, open(out, "w", encoding="utf-8"), indent=2, ensure_ascii=False)
print(len(S), "sources")
