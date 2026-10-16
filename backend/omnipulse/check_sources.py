"""Probe every configured source once through its real connector and report health.

    python -m omnipulse.check_sources [--only id1,id2] [--json]
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time

import orjson

from .ingest.base import ConnectorContext
from .ingest.engine import CONNECTORS
from .ingest.http import Fetcher
from .registry import load_source_file
from .settings import get_settings


async def probe(cfg, ctx, sem) -> dict:
    cls = CONNECTORS.get(cfg.type)
    if cfg.type == "webhook":
        return {"id": cfg.id, "status": "push", "items": 0}
    if cfg.requires_key and not ctx.settings.credential(cfg.requires_key):
        return {"id": cfg.id, "status": "needs-key", "items": 0, "note": cfg.requires_key}
    if not cfg.enabled:
        return {"id": cfg.id, "status": "disabled", "items": 0}
    async with sem:
        t0 = time.perf_counter()
        conn = cls(cfg, ctx, {})
        try:
            if cls.streaming:
                got: list = []

                async def emit(item):
                    got.append(item)
                    if len(got) >= 1:
                        raise asyncio.CancelledError

                try:
                    await asyncio.wait_for(conn.stream(emit), timeout=45)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
                items = got
                status = "ok" if items else "connected"
            else:
                items = await asyncio.wait_for(conn.poll(), timeout=60)
                status = "ok" if items else "empty"
        except Exception as exc:
            return {"id": cfg.id, "status": "FAIL", "items": 0, "error": f"{type(exc).__name__}: {exc}"[:160],
                    "ms": round((time.perf_counter() - t0) * 1000)}
        return {"id": cfg.id, "status": status, "items": len(items), "ms": round((time.perf_counter() - t0) * 1000),
                "sample": items[0].title[:90] if items else ""}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()
    s = get_settings()
    sources = load_source_file(s.sources_file)
    if args.only:
        wanted = set(args.only.split(","))
        sources = [x for x in sources if x.id in wanted]
    fetcher = Fetcher(s.user_agent, s.http_timeout)
    ctx = ConnectorContext(fetcher, s)
    sem = asyncio.Semaphore(16)
    results = await asyncio.gather(*(probe(c, ctx, sem) for c in sources))
    await fetcher.close()
    if args.json:
        print(orjson.dumps(results, option=orjson.OPT_INDENT_2).decode())
    else:
        for r in sorted(results, key=lambda r: (r["status"] != "FAIL", r["id"])):
            extra = r.get("error") or r.get("sample") or r.get("note") or ""
            print(f"{r['status']:<10} {r['id']:<26} {r['items']:>4} {r.get('ms', 0):>6}ms  {extra}")
    working = sum(r["status"] in ("ok", "empty", "connected") for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    print(f"\n{working} working / {failed} failing / {len(results)} total", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
