"""Extractive summarisation: a one-sentence flash headline and a 3-bullet executive summary."""

from __future__ import annotations

import html
import re

from .embed import content_tokens

_PREFIX = re.compile(r"^\s*[\[(]?\s*(BREAKING|FLASH|URGENT|JUST IN|ALERT|DEVELOPING|UPDATE \d*|EXCLUSIVE)\s*[\])]?\s*[:\-—|]+\s*", re.I)
_SUFFIX = re.compile(r"\s+[|\-–—]\s+(Reuters|AP News|AP|Bloomberg|CNBC|BBC News|Al Jazeera|DW|France 24|"
                     r"The Verge|TechCrunch|Ars Technica|WIRED|Financial Times|WSJ|MarketWatch|Yahoo Finance)\s*$", re.I)
_SENT_SPLIT = re.compile(r"(?<=[.!?])[\"'”’)]?\s+(?=[A-Z0-9\"“'$])")
_ABBREV = re.compile(r"\b(?:U\.S|U\.K|E\.U|Mr|Mrs|Ms|Dr|Inc|Corp|Ltd|Co|St|No|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec|vs|approx|est)\.$")


def flash_headline(title: str, max_len: int = 140) -> str:
    t = html.unescape(title)
    t = re.sub(r"\s+", " ", t).strip()
    t = _PREFIX.sub("", t)
    t = _SUFFIX.sub("", t).strip(" -|")
    if len(t) > max_len:
        cut = t[:max_len].rsplit(" ", 1)[0].rstrip(",;:-—")
        t = cut + "…"
    return t or title.strip()[:max_len]


def split_sentences(text: str) -> list[str]:
    parts = _SENT_SPLIT.split(text)
    out: list[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if out and _ABBREV.search(out[-1]):
            out[-1] = f"{out[-1]} {p}"
        else:
            out.append(p)
    return out


def _jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def executive_summary(title: str, plain: str, fallback: list[str], n: int = 3, max_len: int = 220) -> list[str]:
    title_toks = set(content_tokens(title))
    sentences = [s for s in split_sentences(plain[:6000]) if 30 <= len(s) <= 600]
    scored = []
    for idx, s in enumerate(sentences[:40]):
        toks = set(content_tokens(s))
        if not toks:
            continue
        overlap = len(toks & title_toks) / (len(title_toks) or 1)
        if _jaccard(toks, title_toks) > 0.7:
            continue  # restates the headline
        position = 1.0 / (1 + idx * 0.35)
        signal = 0.25 * bool(re.search(r"\d", s)) + 0.15 * bool(re.search(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", s))
        scored.append((position + overlap + signal, idx, s))
    picked = sorted(sorted(scored, reverse=True)[:n], key=lambda x: x[1])
    bullets: list[str] = []
    for _, _, s in picked:
        if len(s) > max_len:
            s = s[:max_len].rsplit(" ", 1)[0].rstrip(",;:") + "…"
        if not any(_jaccard(set(content_tokens(s)), set(content_tokens(b))) > 0.6 for b in bullets):
            bullets.append(s)
    for f in fallback:
        if len(bullets) >= n:
            break
        if f and f not in bullets:
            bullets.append(f)
    return bullets[:n]
