"""Text embedders. All return L2-normalised float32 matrices of shape (n, dim).

* ``hash``      — zero-dependency feature-hashing embedder (unigrams + bigrams, signed hashing).
                  Deterministic, ~20 µs/text; lexical rather than semantic.
* ``fastembed`` — local ONNX BAAI/bge-small-en-v1.5 (384-d), no API key, downloads once (~70 MB).
* ``openai``    — text-embedding-3-small with ``dimensions`` truncation.
* ``ollama``    — any local Ollama embedding model (Matryoshka-truncated to ``embed_dim``).

Similarity distributions differ per model, so each carries a calibrated default cluster
threshold (0.88 for bge-small per spec); ``OMNI_CLUSTER_THRESHOLD`` overrides.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import re
from typing import Protocol

import numpy as np

log = logging.getLogger(__name__)

STOPWORDS = frozenset(
    """a an the and or but if then else of to in on at by for with from as is are was were be been being
    it its this that these those he she they we you i his her their our your them him us me my not no
    so than too very can could will would should may might must do does did done has have had having
    into over under about after before between through during up down out off again further once here
    there when where why how all any both each few more most other some such only own same just also
    new says said say report reports according amid after update live latest via what which who whom
    """.split()
)
_WORD = re.compile(r"[A-Za-z][A-Za-z0-9'\-\.]*[A-Za-z0-9]|\d+(?:\.\d+)?%?|[A-Za-z]")


def _stem(w: str) -> str:
    for suf, rep in (("ies", "y"), ("ing", ""), ("ed", ""), ("es", ""), ("s", "")):
        if len(w) > len(suf) + 3 and w.endswith(suf):
            return w[: -len(suf)] + rep
    return w


def content_tokens(text: str) -> list[str]:
    out = []
    for m in _WORD.finditer(text):
        w = m.group(0).lower().strip("'.-")
        if not w or w in STOPWORDS or len(w) < 2 and not w.isdigit():
            continue
        out.append(_stem(w))
    return out


class Embedder(Protocol):
    name: str
    dim: int
    default_threshold: float

    async def start(self) -> None: ...
    async def embed(self, texts: list[str]) -> np.ndarray: ...


def _normalize(m: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(m, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return (m / norms).astype(np.float32)


def _fit_dim(m: np.ndarray, dim: int) -> np.ndarray:
    if m.shape[1] > dim:
        m = m[:, :dim]
    elif m.shape[1] < dim:
        m = np.pad(m, ((0, 0), (0, dim - m.shape[1])))
    return _normalize(m)


class HashingEmbedder:
    name = "hash"
    default_threshold = 0.6

    def __init__(self, dim: int = 384) -> None:
        self.dim = dim
        self._cache: dict[str, tuple[int, float]] = {}

    async def start(self) -> None:
        return None

    def _slot(self, feat: str) -> tuple[int, float]:
        hit = self._cache.get(feat)
        if hit is None:
            h = int.from_bytes(hashlib.blake2b(feat.encode(), digest_size=8).digest(), "little")
            hit = (h % self.dim, 1.0 if (h >> 63) & 1 else -1.0)
            if len(self._cache) > 400_000:
                self._cache.clear()
            self._cache[feat] = hit
        return hit

    def embed_one(self, text: str) -> np.ndarray:
        v = np.zeros(self.dim, dtype=np.float32)
        toks = content_tokens(text)
        counts: dict[str, float] = {}
        for t in toks:
            counts[t] = counts.get(t, 0.0) + 1.0
        for a, b in zip(toks, toks[1:]):
            k = f"{a}_{b}"
            counts[k] = counts.get(k, 0.0) + 0.7
        for feat, c in counts.items():
            idx, sign = self._slot(feat)
            w = 1.0 + math.log(c)
            if feat[0].isdigit():
                w *= 1.3  # numbers ("25bp", "4.2%") are strong story identifiers
            v[idx] += sign * w
        n = float(np.linalg.norm(v))
        return v / n if n else v

    async def embed(self, texts: list[str]) -> np.ndarray:
        return np.stack([self.embed_one(t) for t in texts]) if texts else np.zeros((0, self.dim), np.float32)


class FastEmbedEmbedder:
    name = "fastembed"
    default_threshold = 0.88

    def __init__(self, dim: int = 384, model: str = "") -> None:
        self.dim = dim
        self.model_name = model or "BAAI/bge-small-en-v1.5"
        self._model = None

    async def start(self) -> None:
        from fastembed import TextEmbedding

        self._model = await asyncio.to_thread(TextEmbedding, self.model_name)
        log.info("fastembed model %s loaded", self.model_name)

    def _run(self, texts: list[str]) -> np.ndarray:
        return np.stack(list(self._model.embed(texts, batch_size=32)))

    async def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.dim), np.float32)
        return _fit_dim(await asyncio.to_thread(self._run, texts), self.dim)


class OpenAIEmbedder:
    name = "openai"
    default_threshold = 0.78

    def __init__(self, api_key: str, dim: int = 384, model: str = "") -> None:
        import httpx

        self.dim = dim
        self.model = model or "text-embedding-3-small"
        self._client = httpx.AsyncClient(
            base_url="https://api.openai.com/v1", headers={"Authorization": f"Bearer {api_key}"}, timeout=20
        )

    async def start(self) -> None:
        await self.embed(["ping"])

    async def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.dim), np.float32)
        r = await self._client.post("/embeddings", json={"model": self.model, "input": texts, "dimensions": self.dim})
        r.raise_for_status()
        data = sorted(r.json()["data"], key=lambda d: d["index"])
        return _fit_dim(np.array([d["embedding"] for d in data], dtype=np.float32), self.dim)


class OllamaEmbedder:
    name = "ollama"
    default_threshold = 0.8

    def __init__(self, url: str, dim: int = 384, model: str = "") -> None:
        import httpx

        self.dim = dim
        self.model = model or "nomic-embed-text"
        self._client = httpx.AsyncClient(base_url=url.rstrip("/"), timeout=30)

    async def start(self) -> None:
        await self.embed(["ping"])

    async def embed(self, texts: list[str]) -> np.ndarray:
        if not texts:
            return np.zeros((0, self.dim), np.float32)
        r = await self._client.post("/api/embed", json={"model": self.model, "input": texts})
        r.raise_for_status()
        return _fit_dim(np.array(r.json()["embeddings"], dtype=np.float32), self.dim)


async def make_embedder(settings) -> Embedder:
    choice = settings.embedder
    dim = settings.embed_dim
    candidates: list[str] = ["fastembed", "hash"] if choice == "auto" else [choice, "hash"]
    for name in candidates:
        try:
            if name == "hash":
                emb: Embedder = HashingEmbedder(dim)
            elif name == "fastembed":
                emb = FastEmbedEmbedder(dim, settings.embed_model)
            elif name == "openai":
                if not settings.openai_api_key:
                    raise RuntimeError("OPENAI_API_KEY not set")
                emb = OpenAIEmbedder(settings.openai_api_key, dim, settings.embed_model)
            elif name == "ollama":
                emb = OllamaEmbedder(settings.ollama_url, dim, settings.embed_model)
            else:
                continue
            await emb.start()
            log.info("embedder: %s (dim=%d)", emb.name, dim)
            return emb
        except Exception as exc:
            log.warning("embedder %s unavailable (%s); falling back", name, exc)
    return HashingEmbedder(dim)
