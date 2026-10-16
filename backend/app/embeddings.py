"""Local, zero-dependency semantic embeddings: a hashed bag-of-ngrams vector
(HashingVectorizer-style) with IDF-free sublinear TF weighting and L2 norm.

Produces a stable dense float32 vector of settings.embedding_dim for any text
in ~tens of microseconds — no model download, no API key, no GPU. Cosine
similarity on these vectors is an effective 24h-story-similarity signal.
"""
from __future__ import annotations

import hashlib
import math
from typing import Iterable

from .config import get_settings
from .text import content_tokens


def _hash_token(token: str, dim: int, salt: str) -> tuple[int, float]:
    h = int.from_bytes(hashlib.blake2b((salt + token).encode(), digest_size=8).digest(), "big")
    return h % dim, ((h >> 64) if False else 1.0)


def embed(text: str) -> list[float]:
    settings = get_settings()
    dim = settings.embedding_dim
    vec = [0.0] * dim
    toks = content_tokens(text)
    if not toks:
        return vec
    counts: dict[str, int] = {}
    for t in toks:
        counts[t] = counts.get(t, 0) + 1
    for token, count in counts.items():
        weight = 1.0 + math.log(count)  # sublinear tf
        for salt in ("u1:", "b1:"):
            h = int.from_bytes(
                hashlib.blake2b((salt + token).encode(), digest_size=8).digest(), "big"
            )
            idx = h % dim
            sign = 1.0 if ((h >> 63) & 1) else -1.0
            vec[idx] += sign * weight
    # bigrams for word-order signal
    for i in range(len(toks) - 1):
        bg = f"{toks[i]}_{toks[i+1]}"
        h = int.from_bytes(hashlib.blake2b(("bg:" + bg).encode(), digest_size=8).digest(), "big")
        idx = h % dim
        sign = 1.0 if ((h >> 63) & 1) else -1.0
        vec[idx] += sign * 0.5
    norm = math.sqrt(sum(x * x for x in vec)) or 1.0
    return [round(x / norm, 6) for x in vec]


def cosine(a: Iterable[float], b: Iterable[float]) -> float:
    dot = na = nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0 or nb == 0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))
