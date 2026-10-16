"""Enrichment: taxonomy, NER, tickers, locations, sentiment, urgency,
flash headlines and executive summaries.

Two paths:
- HeuristicEnricher: pure-local gazetteer + lexicon + regex NLP. Always on,
  zero cost, ~microseconds. The default so the system runs with no API keys.
- LLM enricher (optional): OpenAI / Anthropic / Ollama called with a strict-JSON
  prompt for headline/summary/category refinement on high-signal items only,
  with silent fallback to heuristics on any error or timeout.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any

import httpx
import orjson

from .config import get_settings
from .gazetteer import (
    CATEGORY_KEYWORDS, COUNTRIES, CRYPTO_MAP, FLASH, NEGATIVE, OFFICIAL_SOURCE_HINTS,
    ORGS, PEOPLE, POSITIVE, PRODUCTS, TICKER_MAP, US_STATES, WIRE_SOURCES,
)
from .schemas import Entity, SourceConfig
from .text import RE_TICKER, content_tokens, leading_sentences, tokenize, truncate

log = logging.getLogger("omnipulse.enrich")

RE_CASHTAG = re.compile(r"\$([A-Z]{1,6})\b")
RE_CVE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.I)
RE_PERCENT = re.compile(r"([+-]?\d+(?:\.\d+)?)\s?%")
RE_BPS = re.compile(r"(\d+)\s?(?:basis points|bps|b\.p\.)", re.I)
RE_DOLLAR = re.compile(r"\$\s?(\d+(?:\.\d+)?\s?(?:billion|million|trillion|bn|mn|tn|k|b|m|t)?)", re.I)


class Enricher:
    """Heuristic enricher; LLM refinement layered on top when configured."""

    def __init__(self) -> None:
        s = get_settings()
        self.llm_provider = s.llm_provider
        self._llm_ready = False
        self._sem = asyncio.Semaphore(4)     # concurrent LLM calls
        self._cache: dict[str, dict[str, Any]] = {}
        self._cache_order: list[str] = []
        # precompile keyword matchers
        self._cat_matchers: list[tuple[str, frozenset[str]]] = [
            (cat, frozenset(kws)) for cat, kws in CATEGORY_KEYWORDS.items()
        ]

    # ------------------------------------------------------------------ #
    def resolve_llm(self) -> str | None:
        """Pick the LLM provider once at startup. Returns None => heuristics only."""
        s = get_settings()
        if self.llm_provider == "none":
            return None
        if self.llm_provider == "openai":
            return "openai" if s.openai_api_key else None
        if self.llm_provider == "anthropic":
            return "anthropic" if s.anthropic_api_key else None
        if self.llm_provider == "ollama":
            return "ollama" if s.ollama_url else None
        # auto
        if s.openai_api_key:
            return "openai"
        if s.anthropic_api_key:
            return "anthropic"
        if s.ollama_url:
            return "ollama"
        return None

    # ------------------------------------------------------------------ #
    # Heuristic enrichment
    # ------------------------------------------------------------------ #
    def heuristic(
        self,
        title: str,
        body_markdown: str,
        source: SourceConfig,
        cluster_hint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = f"{title}. {body_markdown}"[:8000]
        toks = tokenize(text)
        tokset = set(toks)
        low = text.lower()

        categories = self._categories(low, tokset)
        entities = self._entities(title, text)
        tickers = self._tickers(low, entities, tokset)
        locations = self._locations(low)
        sentiment = self._sentiment(toks)
        urgency = self._urgency(low, tokset, source, categories)
        verification = self._verification(source, cluster_hint)

        flash = self._flash_headline(title, entities, tickers)
        summary = self._summary(title, body_markdown, sentiment, urgency)

        return {
            "categories": categories,
            "tickers": tickers,
            "entities": [{"name": n, "type": t} for n, t in entities[:12]],
            "locations": locations,
            "sentiment": sentiment,
            "urgency": urgency,
            "novelty": None,  # filled by the pipeline (cluster-aware)
            "verification_status": verification,
            "flash_headline": flash,
            "executive_summary": summary,
            "enricher": "heuristic",
        }

    # ---- components ----
    def _categories(self, low: str, tokset: set[str]) -> list[str]:
        scores: dict[str, float] = {}
        for cat, kws in self._cat_matchers:
            hits = 0.0
            for kw in kws:
                if " " in kw or "-" in kw:
                    if kw in low:
                        hits += 2.0 if " " in kw else 1.0
                elif kw in tokset:
                    hits += 1.0
            if hits:
                scores[cat] = hits
        out = [c for c, _ in sorted(scores.items(), key=lambda t: -t[1]) if scores[c] >= 1]
        if not out:
            return ["Breaking"] if "breaking" in tokset else []
        return out[:3]

    def _entities(self, title: str, text: str) -> list[tuple[str, str]]:
        found: list[tuple[str, str]] = []
        seen: set[str] = set()
        low = text.lower()
        title_low = title.lower()

        def add(name: str, etype: str, boost_title: bool = False):
            if name.lower() in seen:
                return
            # prefer entities mentioned in the title
            if boost_title and name.lower() not in title_low:
                return
            seen.add(name.lower())
            found.append((name, etype))

        # substring prefilter (C-speed) before the word-boundary regex:
        # skips ~99% of gazetteer keys without entering the regex engine
        for key, display in PEOPLE.items():
            if key in low and re.search(rf"\b{re.escape(key)}\b", low):
                add(display, "PERSON", boost_title=len(found) < 3)
        for key, display in ORGS.items():
            if key in low and re.search(rf"\b{re.escape(key)}\b", low):
                add(display, "ORG", boost_title=len(found) < 3)
        for key, display in PRODUCTS.items():
            if key in low:
                add(display, "PRODUCT", boost_title=len(found) < 3)
        for m in RE_CVE.finditer(text):
            add(m.group(0).upper(), "CVE")
        return found

    def _tickers(self, low: str, entities: list[tuple[str, str]], tokset: set[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()

        def add(t: str):
            if t not in seen:
                seen.add(t)
                out.append(t)

        for m in RE_CASHTAG.finditer(low.upper() if "$" in low else ""):
            add(m.group(1))
        for t in content_tokens(" ".join(n for n, _ in entities)):
            if t in TICKER_MAP:
                add(TICKER_MAP[t])
            if t in CRYPTO_MAP:
                add(CRYPTO_MAP[t])
        for w in ("bitcoin", "ethereum", "solana", "xrp", "dogecoin"):
            if w in tokset:
                add(CRYPTO_MAP[w])
        # macro proxies
        if {"sp", "500"} & tokset and ("s&p" in low):
            add("SPX")
        if "nasdaq" in tokset:
            add("NDX")
        if "russell" in tokset and "2000" in tokset:
            add("RTY")
        if "dow" in tokset and "jones" in low:
            add("DJI")
        if "treasury" in tokset or "yields" in tokset or "yield curve" in low:
            add("US10Y")
        return out[:10]

    def _locations(self, low: str) -> list[str]:
        found: list[str] = []
        for key, iso in COUNTRIES.items():
            if key.strip() in low and re.search(rf"\b{re.escape(key.strip())}\b", low):
                if iso not in found:
                    found.append(iso)
            if len(found) >= 5:
                break
        if "US" not in found:
            for key in US_STATES:
                if key in low and re.search(rf"\b{re.escape(key)}\b", low):
                    found.append("US")
                    break
        return found[:5]

    def _sentiment(self, toks: list[str]) -> float:
        score = 0.0
        for t in toks:
            score += NEGATIVE.get(t, 0.0) * -1.0
            score += POSITIVE.get(t, 0.0)
        if not toks:
            return 0.0
        norm = score / (len(toks) ** 0.5 * 1.5)
        return round(max(-1.0, min(1.0, norm)), 3)

    def _urgency(self, low: str, tokset: set[str], source: SourceConfig, categories: list[str]) -> int:
        score = 1.0
        for kw, w in FLASH.items():
            if kw.endswith("-"):
                # prefix families, e.g. "cve-" must match tokens like "cve-2024-45779"
                for tok in tokset:
                    if tok.startswith(kw):
                        score += w
                        break
            elif " " in kw or "-" in kw:
                if kw in low:
                    score += w
            elif kw in tokset:
                score += w
        # tier-1 wire source => intrinsically higher signal
        if source.credibility_tier == 1:
            score += 1.5
        elif source.credibility_tier == 2:
            score += 0.75
        if "Breaking" in categories:
            score += 2.5
        if RE_PERCENT.search(low) or RE_BPS.search(low):
            score += 1.0
        if RE_CVE.search(low):
            score += 1.5
        # opinion / recap / analysis dampeners
        for damp in ("opinion", "analysis", "explainer", "recap", "review", "preview",
                     "week in review", "newsletter", "podcast", "editorial", "column"):
            if damp in tokset:
                score -= 2.0
        return int(max(1, min(10, round(score))))

    def _verification(self, source: SourceConfig, cluster_hint: dict[str, Any] | None) -> str:
        url = (source.url or "") + (source.id or "")
        if any(h in url.lower() for h in OFFICIAL_SOURCE_HINTS):
            return "CONFIRMED"
        if cluster_hint and cluster_hint.get("event_count", 0) >= 2:
            return "CONFIRMED"  # corroborated by 2+ independent sources
        if source.credibility_tier == 1 and any(w in source.id for w in WIRE_SOURCES):
            return "DEVELOPING"
        if source.credibility_tier <= 2:
            return "DEVELOPING"
        return "UNVERIFIED"

    def _flash_headline(self, title: str, entities: list[tuple[str, str]], tickers: list[str]) -> str:
        flash = re.sub(r"\s+", " ", title).strip()
        if tickers:
            flash = flash[:150]
            return flash
        return truncate(flash, 160)

    def _summary(self, title: str, body: str, sentiment: float, urgency: int) -> list[str]:
        sents = leading_sentences(body, 3)
        if not sents:
            sents = [re.sub(r"\s+", " ", title).strip() + "."]
        bullets = [truncate(s, 220) for s in sents[:3]]
        return bullets


# --------------------------------------------------------------------------- #
# LLM refinement
# --------------------------------------------------------------------------- #
LLM_SYSTEM = (
    "You are the enrichment engine of a low-latency news terminal. Given a news item, "
    "return STRICT JSON only (no markdown) with keys: flash_headline (one sentence, "
    "max 18 words, information-dense, no clickbait), executive_summary (array of exactly "
    "3 short bullets, each max 20 words), categories (array, pick 1-3 from: Macro, "
    "Equities, Crypto, AI/Tech, Geopolitics, Cybersecurity, Science, Regulatory, "
    "Breaking), sentiment (float -1.0..1.0 market-relevant tone), urgency (int 1-10, "
    "10 = market-moving flash event), tickers (array of stock/crypto tickers mentioned, "
    "e.g. NVDA or BTC, empty if none)."
)


class LLMRefiner:
    def __init__(self, provider: str):
        self.provider = provider
        s = get_settings()
        self.timeout = 8.0
        self.min_urgency_for_llm = 4

    def _body(self, title: str, body: str, hint_categories: list[str]) -> list[dict[str, str]]:
        content = f"TITLE: {title}\n\nBODY:\n{truncate(body, 4000)}\n\nHeuristic categories: {', '.join(hint_categories)}"
        return [{"role": "user", "content": content}]

    async def refine(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        try:
            if self.provider == "openai":
                return await self._openai(payload)
            if self.provider == "anthropic":
                return await self._anthropic(payload)
            if self.provider == "ollama":
                return await self._ollama(payload)
        except Exception as e:
            log.debug("llm refine failed (%s): %s", self.provider, e)
        return None

    async def _openai(self, p: dict[str, Any]) -> dict[str, Any] | None:
        s = get_settings()
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {s.openai_api_key}"},
                json={
                    "model": s.openai_model,
                    "messages": [{"role": "system", "content": LLM_SYSTEM}, *self._body_messages(p)],
                    "temperature": 0.1,
                    "response_format": {"type": "json_object"},
                },
            )
            r.raise_for_status()
            return self._parse(r.json()["choices"][0]["message"]["content"])

    async def _anthropic(self, p: dict[str, Any]) -> dict[str, Any] | None:
        s = get_settings()
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": s.anthropic_api_key, "anthropic-version": "2023-06-01"},
                json={
                    "model": s.anthropic_model,
                    "max_tokens": 500,
                    "system": LLM_SYSTEM,
                    "messages": self._body_messages(p),
                },
            )
            r.raise_for_status()
            return self._parse(r.json()["content"][0]["text"])

    async def _ollama(self, p: dict[str, Any]) -> dict[str, Any] | None:
        s = get_settings()
        async with httpx.AsyncClient(timeout=self.timeout) as c:
            r = await c.post(
                f"{s.ollama_url.rstrip('/')}/api/chat",
                json={
                    "model": s.ollama_model,
                    "messages": [{"role": "system", "content": LLM_SYSTEM}, *self._body_messages(p)],
                    "stream": False,
                    "format": "json",
                },
            )
            r.raise_for_status()
            return self._parse(r.json()["message"]["content"])

    def _body_messages(self, p: dict[str, Any]) -> list[dict[str, str]]:
        return [{
            "role": "user",
            "content": (
                f"TITLE: {p.get('raw_title','')}\n\n"
                f"BODY:\n{truncate(p.get('clean_markdown',''), 4000)}\n\n"
                f"Heuristic categories: {', '.join(p.get('categories', []))}"
            ),
        }]

    @staticmethod
    def _parse(content: str) -> dict[str, Any] | None:
        try:
            data = orjson.loads(content)
        except Exception:
            m = re.search(r"\{.*\}", content, re.S)
            if not m:
                return None
            try:
                data = orjson.loads(m.group(0))
            except Exception:
                return None
        if not isinstance(data, dict):
            return None
        allowed = ("flash_headline", "executive_summary", "categories", "sentiment",
                   "urgency", "tickers")
        out = {k: data[k] for k in allowed if k in data}
        if not out.get("flash_headline"):
            return None
        return out


class EnrichmentService:
    """Facade: heuristic always; LLM refinement for high-signal items when available."""

    def __init__(self) -> None:
        self.base = Enricher()
        self.llm: LLMRefiner | None = None
        self.llm_enabled = False

    def start(self) -> None:
        provider = self.base.resolve_llm()
        if provider:
            self.llm = LLMRefiner(provider)
            self.llm_enabled = True
            log.info("enrichment: heuristic + %s refinement", provider)
        else:
            log.info("enrichment: heuristic only (set LLM keys for optional refinement)")

    async def enrich(
        self,
        raw_title: str,
        body_markdown: str,
        source: SourceConfig,
        cluster_hint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        out = self.base.heuristic(raw_title, body_markdown, source, cluster_hint)

        if self.llm_enabled and self.llm and out["urgency"] >= self.llm.min_urgency_for_llm:
            payload = {
                "raw_title": raw_title,
                "clean_markdown": body_markdown,
                "categories": out["categories"],
            }
            refined = await self.llm.refine(payload)
            if refined:
                out["flash_headline"] = str(refined.get("flash_headline", out["flash_headline"]))[:220]
                if isinstance(refined.get("executive_summary"), list) and refined["executive_summary"]:
                    out["executive_summary"] = [str(b)[:240] for b in refined["executive_summary"][:3]]
                if isinstance(refined.get("categories"), list) and refined["categories"]:
                    out["categories"] = [str(c) for c in refined["categories"][:3]]
                if isinstance(refined.get("tickers"), list) and refined["tickers"]:
                    merged = list(dict.fromkeys(out["tickers"] + [str(t).upper() for t in refined["tickers"][:8]]))
                    out["tickers"] = merged[:10]
                if isinstance(refined.get("sentiment"), (int, float)):
                    out["sentiment"] = round(max(-1.0, min(1.0, float(refined["sentiment"]))), 3)
                if isinstance(refined.get("urgency"), int):
                    out["urgency"] = int(max(1, min(10, refined["urgency"])))
                out["enricher"] = f"heuristic+{self.llm.provider}"
        return out
