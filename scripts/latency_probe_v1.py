"""End-to-end latency probe: push a uniquely-worded item into the inbound webhook while subscribed
to the SSE firehose, and time push -> EVENT_CREATED on the stream.

    python scripts/latency_probe.py [--base http://localhost:8000] [--n 10] [--source discord_inbox]
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid

import httpx


async def probe(client: httpx.AsyncClient, base: str, source: str) -> float:
    token = "probe" + uuid.uuid4().hex[:10]
    arrived = asyncio.Event()

    async def listen() -> None:
        async with client.stream("GET", f"{base}/v1/stream/sse", params={"q": token, "format": "headline"},
                                 timeout=None) as r:
            async for line in r.aiter_lines():
                if line.startswith("data:") and token in line:
                    arrived.set()
                    return

    task = asyncio.create_task(listen())
    await asyncio.sleep(0.3)  # let the subscription register
    t0 = time.perf_counter()
    await client.post(f"{base}/v1/ingest/{source}",
                      json={"title": f"Latency probe {token} reports test event", "url": f"https://probe.invalid/{token}"})
    await asyncio.wait_for(arrived.wait(), 30)
    dt = (time.perf_counter() - t0) * 1000
    task.cancel()
    return dt


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--source", default="discord_inbox")
    a = ap.parse_args()
    async with httpx.AsyncClient() as client:
        samples = [await probe(client, a.base, a.source) for _ in range(a.n)]
    samples.sort()
    print(f"push -> stream over {a.n} probes: p50 {statistics.median(samples):.1f} ms, "
          f"min {samples[0]:.1f} ms, max {samples[-1]:.1f} ms")


if __name__ == "__main__":
    asyncio.run(main())
