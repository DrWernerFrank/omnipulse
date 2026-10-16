"""Text utilities: canonical URLs, shingles, SimHash/MinHash-style fingerprints,
HTML boilerplate stripping and Markdown conversion."""
from __future__ import annotations

import hashlib
import html
import math
import re
from html.parser import HTMLParser
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

RE_HTML_TAG = re.compile(r"<[^>]+>")
RE_WS = re.compile(r"\s+")
RE_SCRIPT_STYLE = re.compile(r"<(script|style|noscript|iframe|svg|form|nav|footer|aside)[^>]*>.*?</\1>", re.S | re.I)
RE_COMMENTS = re.compile(r"<!--.*?-->", re.S)
RE_TICKER = re.compile(r"\$([A-Z]{1,5})\b")
RE_CASHTAG = re.compile(r"\$(BTC|ETH|SOL|XRP|DOGE|ADA|AVAX|LINK|BNB|TRX|DOT|MATIC|LTC|BCH|TON|ADA)\b", re.I)


def canonical_url(url: str) -> str:
    """Normalize a URL for dedup: lowercase scheme/host, strip tracking params
    and fragments, collapse amp;utm_*;utm_*, sort remaining params."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
    except ValueError:
        return url
    host = (parts.netloc or "").lower()
    host = host.replace("www.", "", 1) if host.startswith("www.") else host
    path = quote(parts.path or "/", safe="/%:@")
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not k.lower().startswith(("utm_", "ref", "fbclid", "gclid", "mc_", "cmp", "sfc", "ns_"))]
    query.sort()
    return urlunsplit((parts.scheme.lower() or "https", host, path,
                       "&".join(f"{k}={v}" for k, v in query) if query else "", ""))


_TOKEN_RE = re.compile(r"[a-z0-9']+")


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


STOPWORDS = frozenset(
    "a an and are as at be but by for from has have he her his if in into is it its of on or "
    "she that the their there they this to was were what when where which who will with you your "
    "said says told after before over more than new report reports says about amid up down out "
    "how why not no can could would should we us our i me my".split()
)


def content_tokens(text: str) -> list[str]:
    return [t for t in tokenize(text) if t not in STOPWORDS and len(t) > 2]


def shingles(text: str, size: int = 3) -> set[tuple[str, ...]]:
    toks = content_tokens(text)
    if len(toks) < size:
        return {tuple(toks)} if toks else set()
    return {tuple(toks[i:i + size]) for i in range(len(toks) - size + 1)}


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / (len(a) + len(b) - inter)


# --------------------------------------------------------------------------- #
# SimHash (lexically robust exact-duplicate / syndication fingerprint)
# --------------------------------------------------------------------------- #
def _token_weights(tokens: list[str]) -> dict[str, int]:
    weights: dict[str, int] = {}
    for t in tokens:
        weights[t] = weights.get(t, 0) + 1
    return weights


def simhash64(text: str) -> int:
    bits = [0] * 64
    for tok, weight in _token_weights(content_tokens(text)).items():
        h = int.from_bytes(hashlib.blake2b(tok.encode(), digest_size=8).digest(), "big")
        for i in range(64):
            if (h >> i) & 1:
                bits[i] += weight
            else:
                bits[i] -= weight
    out = 0
    for i in range(64):
        if bits[i] > 0:
            out |= 1 << i
    return out


def hamming(a: int, b: int) -> int:
    return (a ^ b).bit_count()


# --------------------------------------------------------------------------- #
# MinHash sketch (cheap set-similarity prefilter for clustering)
# --------------------------------------------------------------------------- #
def minhash(text: str, perms: int = 64) -> list[int]:
    toks = content_tokens(text)
    if not toks:
        return [0] * perms
    sketch = []
    for p in range(perms):
        h = hashlib.blake2b(f"{p}".encode(), digest_size=4).digest()
        seed = int.from_bytes(h, "big")
        sketch.append(min((seed * 0x9E3779B1 ^ hash(t)) & 0xFFFFFFFFFFFF for t in toks))
    return sketch


def minhash_jaccard(a: list[int], b: list[int]) -> float:
    if len(a) != len(b):
        return 0.0
    return sum(1 for x, y in zip(a, b) if x == y) / len(a) if a else 0.0


# --------------------------------------------------------------------------- #
# HTML -> clean text / markdown
# --------------------------------------------------------------------------- #
_BLOCK_TAGS = {
    "p", "div", "section", "article", "header", "main", "ul", "ol", "li", "blockquote",
    "h1", "h2", "h3", "h4", "h5", "h6", "table", "tr", "figure", "figcaption", "pre",
}


class _TextExtractor(HTMLParser):
    """Collects block-level text into markdown-ish paragraphs. Skips the usual
    boilerplate containers (comments/share boxes, related-story widgets)."""

    _BOILERPLATE = re.compile(
        r"(share|social|comment|related|newsletter|subscribe|advert|promo|sidebar|"
        r"footer|nav|menu|breadcrumb|cookie|consent|sign-?in|paywall|trending|most.?read|tag)s?",
        re.I,
    )

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._buf: list[str] = []
        self._skip_depth = 0
        self._skip_class: str | None = None
        self._links: list[tuple[str, str]] = []   # (text, href) for link preservation
        self._link_depth = 0
        self._link_text: list[str] = []
        self._link_href = ""

    def _attrs(self, attrs) -> dict[str, str]:
        return {k.lower(): v or "" for k, v in attrs}

    def handle_starttag(self, tag, attrs):
        a = self._attrs(attrs)
        if tag in ("script", "style", "noscript", "iframe", "svg", "form", "button", "aside"):
            self._skip_depth += 1
            return
        cls = a.get("class", "") + " " + a.get("id", "")
        if cls and self._BOILERPLATE.search(cls) and tag in _BLOCK_TAGS:
            self._skip_depth += 1
            self._skip_class = tag
            return
        if tag == "a" and self._skip_depth == 0:
            self._link_depth += 1
            self._link_text = []
            self._link_href = a.get("href", "")
        if tag in _BLOCK_TAGS and self._skip_depth == 0 and not self._buf:
            pass  # block start; buffer fills as text arrives

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript", "iframe", "svg", "form", "button", "aside"):
            if self._skip_depth:
                self._skip_depth -= 1
            return
        if self._skip_depth and self._skip_class == tag:
            self._skip_depth -= 1
            self._skip_class = None
            return
        if tag == "a" and self._link_depth:
            self._link_depth -= 1
            text = "".join(self._link_text).strip()
            if text and self._link_href.startswith("http"):
                self._links.append((text, self._link_href))
            return
        if tag in _BLOCK_TAGS and self._skip_depth == 0:
            chunk = "".join(self._buf).strip()
            self._buf = []
            if chunk:
                self._chunks.append(chunk)

    def handle_data(self, data):
        if self._skip_depth:
            return
        if self._link_depth:
            self._link_text.append(data)
        self._buf.append(data)

    @property
    def text(self) -> str:
        parts = list(self._chunks)
        tail = "".join(self._buf).strip()
        if tail:
            parts.append(tail)
        return "\n\n".join(parts)


def strip_html(raw_html: str) -> str:
    """Ad/tracker/boilerplate-tolerant HTML -> plain text with paragraph breaks."""
    if not raw_html:
        return ""
    cleaned = RE_COMMENTS.sub(" ", raw_html)
    cleaned = RE_SCRIPT_STYLE.sub(" ", cleaned)
    parser = _TextExtractor()
    try:
        parser.feed(cleaned)
        text = parser.text
    except Exception:
        text = RE_HTML_TAG.sub(" ", cleaned)
    text = html.unescape(text)
    return RE_WS.sub(" ", text).strip()


def html_to_markdown(raw_html: str) -> str:
    """Best-effort markdown conversion using structural heuristics; falls back
    to clean paragraphs. Zero heavy dependencies by design."""
    if not raw_html:
        return ""
    text = strip_html(raw_html)
    paras = [p.strip() for p in text.split("\n\n") if p.strip()]
    out: list[str] = []
    for i, p in enumerate(paras[:80]):
        p = RE_WS.sub(" ", p)
        if len(p) < 40 and i > 0 and not p.endswith((".", "!", "?", ":", ";")):
            continue  # stray label
        out.append(p)
    return "\n\n".join(out)


# --------------------------------------------------------------------------- #
# Summary helpers
# --------------------------------------------------------------------------- #
def leading_sentences(text: str, n: int = 3) -> list[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text)
    out: list[str] = []
    total = 0
    for p in parts:
        p = p.strip()
        if not p or len(p) < 25:
            continue
        out.append(p if p.endswith((".", "!", "?")) else p + ".")
        total += len(p)
        if len(out) >= n or total > 420:
            break
    return out


def truncate(text: str, n: int) -> str:
    if len(text) <= n:
        return text
    return text[: n - 1].rstrip() + "…"


def clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def entropy_norm(text: str) -> float:
    if not text:
        return 0.0
    freq: dict[str, int] = {}
    for t in tokenize(text):
        freq[t] = freq.get(t, 0) + 1
    n = sum(freq.values())
    if n == 0:
        return 0.0
    ent = -sum((c / n) * math.log(c / n) for c in freq.values())
    return clamp(ent / 7.0)  # ~7 bits is a rich English text
