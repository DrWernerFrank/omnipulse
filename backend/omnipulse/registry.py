"""Source registry bootstrap: loads config/default_sources.json and seeds the store."""

from __future__ import annotations

import logging
import re
from pathlib import Path

import orjson

from .schema import SourceConfig

log = logging.getLogger(__name__)


def resolve_path(path: str) -> Path:
    p = Path(path)
    if p.is_absolute() or p.exists():
        return p
    for base in (Path.cwd(), Path(__file__).resolve().parents[1], Path(__file__).resolve().parents[2]):
        cand = base / path
        if cand.exists():
            return cand
    return p


def load_source_file(path: str) -> list[SourceConfig]:
    p = resolve_path(path)
    if not p.exists():
        log.warning("source registry %s not found; starting with no default sources", p)
        return []
    doc = orjson.loads(p.read_bytes())
    groups = doc.get("groups", {})
    out: list[SourceConfig] = []
    for raw in doc.get("sources", []):
        g = groups.get(raw.get("group", ""), {})
        merged = {k: v for k, v in g.items() if k in ("poll_interval", "min_interval", "max_interval")} | raw
        try:
            out.append(SourceConfig.model_validate(merged))
        except Exception as exc:
            log.error("invalid source %s: %s", raw.get("id"), exc)
    ids = [s.id for s in out]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        raise ValueError(f"duplicate source ids in {p}: {sorted(dupes)}")
    return out


async def seed_defaults(store, settings) -> int:
    sources = load_source_file(settings.sources_file)
    n = await store.seed_sources(sources)
    if n:
        log.info("seeded %d default sources", n)
    return n


def slugify(name: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return (s or "source")[:60]
