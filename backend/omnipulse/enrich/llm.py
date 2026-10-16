"""Optional fast-LLM tagger (OpenAI, Anthropic, or local Ollama).

Runs *after* the heuristic event has been published, so it never adds latency to the
firehose; improved fields are pushed as an EVENT_UPDATED envelope.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from typing import Any

import httpx
import orjson

from ..schema import TAXONOMY

log = logging.getLogger(__name__)

DEFAULT_MODELS = {"openai": "gpt-4o-mini", "anthropic": "claude-haiku-5-5", "ollama": "llama3.1"}

SYSTEM = (
    "You are a newswire desk editor. Return ONLY a JSON object with keys: "
    '"flash_headline" (one factual sentence, <=120 chars, no clickbait), '
    '"executive_summary" (array of exactly 3 short factual bullets, <=25 words each), '
    f'"categories" (1-3 of {list(TAXONOMY)}), '
    '"entities" (array of {"name","type"} with type in PERSON|ORG|COUNTRY|LOCATION|CRYPTO|PRODUCT), '
    '"tickers" (array of exchange tickers or crypto symbols, uppercase, no $), '
    '"locations" (ISO 3166-1 alpha-2 codes, or EU), '
    '"sentiment" (-1.0 bearish/negative .. 1.0 bullish/positive), '
    '"urgency" (1-10; 10 = market-moving flash headline, 1 = opinion/recap). '
    "Use only facts present in the text."
)


class LLMEnricher:
    def __init__(self, settings) -> None:
        self.provider = settings.llm_provider
        self.model = settings.llm_model or DEFAULT_MODELS.get(self.provider, "")
        self.settings = settings
        self.sem = asyncio.Semaphore(settings.llm_concurrency)
        self.calls: deque[float] = deque()
        self.client = httpx.AsyncClient(timeout=30)

    @property
    def enabled(self) -> bool:
        s = self.settings
        if self.provider == "openai":
            return bool(s.openai_api_key)
        if self.provider == "anthropic":
            return bool(s.anthropic_api_key)
        if self.provider == "openai_compatible":
            return bool(s.llm_base_url and s.llm_api_key and self.model)
        return self.provider == "ollama"

    def _budget_ok(self) -> bool:
        now = time.monotonic()
        while self.calls and now - self.calls[0] > 60:
            self.calls.popleft()
        if len(self.calls) >= self.settings.llm_max_per_minute:
            return False
        self.calls.append(now)
        return True

    async def _complete(self, user: str) -> str:
        s = self.settings
        if self.provider == "openai":
            r = await self.client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {s.openai_api_key}"},
                json={"model": self.model, "temperature": 0, "response_format": {"type": "json_object"},
                      "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]},
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        if self.provider == "openai_compatible":
            r = await self.client.post(
                f"{s.llm_base_url.rstrip('/')}/chat/completions",
                headers={"Authorization": f"Bearer {s.llm_api_key}"},
                json={"model": self.model, "temperature": 0, "response_format": {"type": "json_object"},
                      "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]},
            )
            r.raise_for_status()
            return r.json()["choices"][0]["message"]["content"]
        if self.provider == "anthropic":
            r = await self.client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": s.anthropic_api_key, "anthropic-version": "2023-06-01"},
                json={"model": self.model, "max_tokens": 700, "system": SYSTEM,
                      "messages": [{"role": "user", "content": user}]},
            )
            r.raise_for_status()
            return "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text")
        r = await self.client.post(
            f"{s.ollama_url.rstrip('/')}/api/chat",
            json={"model": self.model, "stream": False, "format": "json",
                  "messages": [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user}]},
        )
        r.raise_for_status()
        return r.json()["message"]["content"]

    async def enrich(self, title: str, text: str, source_name: str) -> dict[str, Any] | None:
        if not self.enabled or not self._budget_ok():
            return None
        user = f"Source: {source_name}\nTitle: {title}\n\nText:\n{text[:6000]}"
        async with self.sem:
            try:
                raw = await self._complete(user)
            except Exception as exc:
                log.info("LLM enrichment failed (%s): %s", self.provider, exc)
                return None
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return None
        try:
            data = orjson.loads(m.group(0))
        except orjson.JSONDecodeError:
            return None
        return _validate(data)


def _validate(d: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(d.get("flash_headline"), str) and d["flash_headline"].strip():
        out["flash_headline"] = d["flash_headline"].strip()[:160]
    summ = d.get("executive_summary")
    if isinstance(summ, list):
        bullets = [str(b).strip()[:240] for b in summ if str(b).strip()][:3]
        if bullets:
            out["executive_summary"] = bullets
    cats = [c for c in d.get("categories", []) if c in TAXONOMY] if isinstance(d.get("categories"), list) else []
    if cats:
        out["categories"] = cats[:3]
    ents = d.get("entities")
    if isinstance(ents, list):
        ok = {"PERSON", "ORG", "COUNTRY", "LOCATION", "CRYPTO", "PRODUCT"}
        out["entities"] = [
            {"name": str(e["name"])[:80], "type": e["type"]}
            for e in ents if isinstance(e, dict) and e.get("name") and e.get("type") in ok
        ][:25]
    if isinstance(d.get("tickers"), list):
        out["tickers"] = [str(t).upper().lstrip("$")[:10] for t in d["tickers"] if re.fullmatch(r"\$?[A-Za-z.]{1,10}", str(t))][:12]
    if isinstance(d.get("locations"), list):
        out["locations"] = [str(x).upper() for x in d["locations"] if re.fullmatch(r"[A-Za-z]{2}", str(x))][:10]
    try:
        out["sentiment"] = round(max(-1.0, min(1.0, float(d["sentiment"]))), 3)
    except (KeyError, TypeError, ValueError):
        pass
    try:
        out["urgency"] = int(max(1, min(10, round(float(d["urgency"])))))
    except (KeyError, TypeError, ValueError):
        pass
    return out
