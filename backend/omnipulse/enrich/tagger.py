"""Zero-key NLP tagger: taxonomy, NER (orgs, people, tickers, crypto, CVEs, ISO locations),
finance-aware sentiment and urgency scoring. Typical cost: ~1 ms per item."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any

from ..schema import TAXONOMY, SourceConfig
from . import lexicons as L


def _alternation(phrases: list[str], flags: int = re.I) -> re.Pattern:
    alts = sorted({p for p in phrases if p}, key=len, reverse=True)
    return re.compile(r"(?<![\w$])(" + "|".join(re.escape(p) for p in alts) + r")(?![\w])", flags)


_CAT_RE = {cat: _alternation([p for p, _ in kws]) for cat, kws in L.TAXONOMY_KEYWORDS.items()}
_CAT_W = {cat: {p.lower(): w for p, w in kws} for cat, kws in L.TAXONOMY_KEYWORDS.items()}

_COUNTRY_FORMS: dict[str, str] = {}
for _iso, _forms in L.COUNTRIES.items():
    for _f in _forms:
        _COUNTRY_FORMS.setdefault(_f, _iso)
_COUNTRY_RE = _alternation(list(_COUNTRY_FORMS))
_COUNTRY_NAME = {iso: forms[0].title().replace("U.S.", "US") for iso, forms in L.COUNTRIES.items()}
_COUNTRY_NAME.update({"US": "United States", "GB": "United Kingdom", "EU": "European Union", "AE": "UAE"})

_ORG_RE = _alternation(list(L.ORGS))
_PEOPLE_RE = _alternation(list(L.PEOPLE))
_CRYPTO_RE = _alternation(list(L.CRYPTO_ASSETS))
_HIGH_RE = _alternation([p for p, _ in L.HIGH_IMPACT])
_HIGH_W = {p: w for p, w in L.HIGH_IMPACT}

_CASHTAG = re.compile(r"(?<![\w$])\$([A-Z]{1,5}(?:\.[A-Z])?)(?![\w])")
_EXCHANGE = re.compile(r"\((?:NASDAQ|Nasdaq|NYSE|NYSE American|AMEX|TSX|LSE|OTCQX|OTC|TSXV)\s*:\s*([A-Z][A-Z.]{0,6})\)")
_CVE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.I)
_TITLED_PERSON = re.compile(
    r"\b(?i:" + "|".join(re.escape(t) for t in L.PERSON_TITLES) + r")\s+([A-Z][a-zA-Z'\-]+(?:\s+[A-Z][a-zA-Z'\-]+){1,2})"
)
_CRYPTO_SYMBOLS = frozenset(L.CRYPTO_ASSETS.values())
_SAID_PERSON = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z'\-]+){1,2})\s+(?:said|says|told|wrote|warned|announced)\b")
_WORD = re.compile(r"[A-Za-z][A-Za-z'\-]*")
_BIG_MONEY = re.compile(r"\$\s?\d+(?:\.\d+)?\s?(?:billion|bn|trillion|tn)\b|\b\d{2,}(?:\.\d+)?\s?%", re.I)
_BREAKING_PREFIX = re.compile(r"^\s*[\[(]?\s*(BREAKING|FLASH|URGENT|JUST IN|ALERT|DEVELOPING)\b", re.I)


@dataclass
class Tags:
    categories: list[str] = field(default_factory=list)
    tickers: list[str] = field(default_factory=list)
    entities: list[dict[str, str]] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)
    sentiment: float = 0.0
    urgency: int = 3
    breaking: bool = False


def _case_ok(surface: str, matched: str) -> bool:
    if surface in L.CASE_SENSITIVE_ORGS or len(surface) <= 3:
        if len(surface.replace(".", "")) <= 3:
            return matched.replace(".", "").isupper()
        return matched[0].isupper()
    return True


def categorize(title: str, body: str, src: SourceConfig | None, hints: dict[str, Any]) -> dict[str, float]:
    scores: dict[str, float] = {c: 0.0 for c in TAXONOMY}
    for cat, rx in _CAT_RE.items():
        w = _CAT_W[cat]
        for m in rx.finditer(title):
            if _case_ok(m.group(1).lower(), m.group(1)):
                scores[cat] += 2.0 * w.get(m.group(1).lower(), 1.0)
        for m in rx.finditer(body):
            if _case_ok(m.group(1).lower(), m.group(1)):
                scores[cat] += 0.6 * w.get(m.group(1).lower(), 1.0)
    if src:
        for c in src.default_categories:
            scores[c] += 2.5
    for c in hints.get("categories", ()):
        if c in scores:
            scores[c] += 6.0
    return scores


def sentiment(title: str, body: str) -> float:
    total = 0.0
    for text, weight in ((title, 2.0), (body[:800], 1.0)):
        words = [w.lower() for w in _WORD.findall(text)]
        for i, w in enumerate(words):
            v = L.SENTIMENT.get(w)
            if v is None:
                continue
            if i and words[i - 1] in L.INTENSIFIERS:
                v *= L.INTENSIFIERS[words[i - 1]]
            if any(x in L.NEGATORS for x in words[max(0, i - 3):i]):
                v *= -0.6
            total += v * weight
    return round(max(-1.0, min(1.0, total / math.sqrt(total * total + 15.0))), 3)


# Source categories whose ledes are about companies/markets; elsewhere (release notes, research,
# community posts) a company mention in the body ("Apple Silicon", "Intel GPUs") is not a ticker signal.
_MARKET_SOURCES = {"wire", "finance", "regulatory", "macro", "aggregator", "crypto", "prediction"}


def extract_entities(title: str, body: str, src: SourceConfig | None = None,
                     infer_org_tickers: bool = True) -> tuple[list[dict[str, str]], list[str], list[str]]:
    text = f"{title}\n{body[:4000]}"
    market = src is None or src.category in _MARKET_SOURCES
    lead = f"{title}\n{body[:600]}" if market else title
    entities: dict[tuple[str, str], None] = {}
    tickers: dict[str, None] = {}
    locations: dict[str, None] = {}

    for m in _CASHTAG.finditer(text):
        t = m.group(1)
        if t not in L.TICKER_STOPLIST:
            tickers[t] = None
    for m in _EXCHANGE.finditer(text):
        tickers[m.group(1).rstrip(".")] = None

    for m in _ORG_RE.finditer(text):
        surface = m.group(1).lower()
        if not _case_ok(surface, m.group(1)):
            continue
        name, ticker = L.ORGS[surface]
        entities[(name, "ORG")] = None
        if ticker and infer_org_tickers and m.start() < len(lead):
            tickers[ticker] = None

    for m in _CRYPTO_RE.finditer(text):
        surface = m.group(1).lower()
        if len(surface) <= 4 and not m.group(1).isupper():
            continue  # "eth", "sol" lowercase are too ambiguous
        sym = L.CRYPTO_ASSETS[surface]
        entities[(surface.title() if len(surface) > 4 else sym, "CRYPTO")] = None
        if m.start() < len(lead):
            tickers[sym] = None

    for m in _PEOPLE_RE.finditer(text):
        surface = m.group(1)
        if surface[0].isupper():
            entities[(L.PEOPLE[surface.lower()], "PERSON")] = None
    known_people = {n for n, t in entities if t == "PERSON"}
    for rx in (_TITLED_PERSON, _SAID_PERSON):
        for m in rx.finditer(text):
            name = m.group(1).strip()
            low = name.lower()
            if low in L.ORGS or low in _COUNTRY_FORMS or any(p.endswith(name.split()[-1]) for p in known_people):
                continue
            if any(w.lower() in L.ORGS or w.lower() in _COUNTRY_FORMS for w in name.split()):
                continue
            entities[(name, "PERSON")] = None

    for m in _CVE.finditer(text):
        entities[(m.group(0).upper(), "CVE")] = None

    for m in _COUNTRY_RE.finditer(text):
        surface = m.group(1).lower()
        if len(surface.replace(".", "")) <= 3 and not m.group(1).replace(".", "").isupper():
            continue  # "us", "eu" as pronouns/words
        iso = _COUNTRY_FORMS[surface]
        locations[iso] = None
        entities[(_COUNTRY_NAME.get(iso, iso), "COUNTRY")] = None

    ents = [{"name": n, "type": t} for n, t in entities]
    return ents[:25], list(tickers)[:12], list(locations)[:10]


def urgency(title: str, body: str, src: SourceConfig | None, scores: dict[str, float], hints: dict[str, Any]) -> tuple[int, bool]:
    low = title.lower()
    u = 3.0
    breaking = bool(_BREAKING_PREFIX.search(title)) or "BREAKING" in title
    if breaking:
        u += 4
    if src:
        u += {1: 1.0, 2: 0.0, 3: -0.5}.get(src.credibility_tier, 0.0)
    bonus = 0.0
    for m in _HIGH_RE.finditer(title):
        bonus += _HIGH_W.get(m.group(1).lower(), 1)
    for m in _HIGH_RE.finditer(body[:600]):
        bonus += 0.3 * _HIGH_W.get(m.group(1).lower(), 1)
    u += min(bonus, 4.0)
    if _BIG_MONEY.search(title):
        u += 1
    if scores.get("Macro", 0) >= 6 and src and src.category in ("macro", "regulatory"):
        u += 2  # official data release / central-bank decision
    if any(x in low for x in L.LOW_VALUE) or title.rstrip().endswith("?"):
        u -= 2.5
    u += float(hints.get("urgency_bonus", 0))
    u = max(u, float(hints.get("urgency_floor", 1)))
    return int(max(1, min(10, round(u)))), breaking


def tag(title: str, body: str, src: SourceConfig | None, hints: dict[str, Any] | None = None) -> Tags:
    hints = hints or {}
    scores = categorize(title, body, src, hints)
    # Connectors with authoritative symbols (SEC CIK map) pass hints['tickers'], even if empty.
    ents, tickers, locs = extract_entities(title, body, src, infer_org_tickers="tickers" not in hints)
    for e in hints.get("entities", ()):
        if e not in ents:
            ents.insert(0, e)
    for t in hints.get("tickers", ()):
        if t and t not in tickers:
            tickers.insert(0, t)
    if any(t in _CRYPTO_SYMBOLS for t in tickers):
        scores["Crypto"] += 1.5
    if any(e["type"] == "CVE" for e in ents):
        scores["Cybersecurity"] += 4
    u, breaking = urgency(title, body, src, scores, hints)
    ranked = sorted((c for c in TAXONOMY if c != "Breaking"), key=lambda c: -scores[c])
    cats = [c for c in ranked if scores[c] >= 3.0][:3]
    if not cats and scores[ranked[0]] > 0:
        cats = [ranked[0]]
    if breaking or u >= 9:
        cats.append("Breaking")
    return Tags(
        categories=cats,
        tickers=tickers,
        entities=ents,
        locations=locs,
        sentiment=sentiment(title, body),
        urgency=u,
        breaking=breaking,
    )
