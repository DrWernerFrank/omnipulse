"""Content extraction: strip ads/trackers/boilerplate and emit clean, LLM-ready Markdown."""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment
from markdownify import markdownify

from ..enrich.canonical import strip_tracking

log = logging.getLogger(__name__)

_DROP_TAGS = ("script", "style", "noscript", "iframe", "form", "svg", "button", "input", "select",
              "nav", "aside", "footer", "header", "object", "embed", "canvas", "video", "audio", "picture")
_BOILERPLATE_CLASS = re.compile(
    r"(^|[\s_-])(ad|ads|advert\w*|promo\w*|sponsor\w*|share\w*|social|newsletter|subscribe\w*|related|"
    r"recommend\w*|outbrain|taboola|paywall|cookie\w*|banner|popup|modal|feedflare|comments?)([\s_-]|$)",
    re.I,
)
_TRAILERS = re.compile(
    r"(The post .{0,200}? appeared first on .{0,120}?\.?$|Continue reading\W*$|Read more\W*$|"
    r"\[\s*link\s*\]|\[\s*comments\s*\]|submitted by\s+/u/\S+)",
    re.I | re.M,
)
# Sites where full-article fetches are paywalled, script-rendered, or disallowed.
NO_FETCH_DOMAINS = (
    "ft.com", "wsj.com", "bloomberg.com", "news.google.com", "reddit.com", "bsky.app", "t.me", "x.com",
    "twitter.com", "polymarket.com", "kalshi.com", "nvd.nist.gov", "github.com", "sec.gov",
    "news.ycombinator.com", "barrons.com", "economist.com", "nytimes.com",
)


def html_to_markdown(fragment: str, max_chars: int = 20000) -> str:
    if not fragment:
        return ""
    if "<" not in fragment:
        return _tidy(fragment)[:max_chars]
    soup = BeautifulSoup(fragment, "lxml")
    for c in soup.find_all(string=lambda s: isinstance(s, Comment)):
        c.extract()
    for t in soup.find_all(_DROP_TAGS):
        t.decompose()
    for el in soup.find_all(True):
        if el.decomposed or el.attrs is None:
            continue
        cls = " ".join(el.get("class", [])) + " " + (el.get("id") or "")
        if cls.strip() and _BOILERPLATE_CLASS.search(cls):
            el.decompose()
            continue
        if el.name == "img":
            w, h = el.get("width", ""), el.get("height", "")
            src = el.get("src", "")
            if w in ("0", "1") or h in ("0", "1") or "pixel" in src or "feedburner" in src or "tracking" in src:
                el.decompose()
                continue
        if el.name == "a" and el.get("href"):
            el["href"] = strip_tracking(el["href"])
        for attr in [a for a in el.attrs if a not in ("href", "src", "alt", "title")]:
            del el[attr]
    md = markdownify(str(soup), heading_style="ATX", bullets="-", strip=["img"])
    return _tidy(md)[:max_chars]


def _tidy(md: str) -> str:
    md = _TRAILERS.sub("", md)
    md = re.sub(r"[ \t\xa0]+", " ", md)
    md = re.sub(r" *\n *", "\n", md)
    md = re.sub(r"\n{3,}", "\n\n", md)
    return md.strip()


def markdown_to_plain(md: str) -> str:
    text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", md)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[#*_`>|]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def fetchable(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return bool(host) and not any(host == d or host.endswith("." + d) for d in NO_FETCH_DOMAINS)


def _trafilatura_markdown(page: str, url: str) -> str:
    import trafilatura

    return trafilatura.extract(
        page, url=url, output_format="markdown", include_comments=False, include_tables=True,
        include_images=False, include_links=False, favor_precision=True, deduplicate=True,
    ) or ""


async def fetch_article_markdown(fetcher, url: str, max_chars: int = 20000) -> str:
    """Download and extract the main article body. Returns '' when unavailable."""
    if not fetchable(url):
        return ""
    try:
        res = await fetcher.get(url, headers={"Accept": "text/html,application/xhtml+xml"}, max_bytes=4_000_000)
    except Exception as exc:
        log.debug("article fetch failed %s: %s", url, exc)
        return ""
    ctype = res.headers.get("content-type", "")
    if "html" not in ctype:
        return ""
    try:
        md = await asyncio.to_thread(_trafilatura_markdown, res.text(), res.url)
    except Exception as exc:  # trafilatura is defensive but not bulletproof
        log.debug("extraction failed %s: %s", url, exc)
        return ""
    return _tidy(md)[:max_chars]
