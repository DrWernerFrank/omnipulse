"""Live smoke test of the OmniPulse streaming surfaces."""
import asyncio
import json
import sys

import httpx
import websockets

BASE = "http://127.0.0.1:8000"


async def main() -> int:
    failures = []

    # --- SSE ---
    got_event = False
    try:
        # trust_env=False: never route localhost through a system proxy
        async with httpx.AsyncClient(timeout=12.0, trust_env=False) as c:
            async with c.stream("GET", f"{BASE}/stream/events?compact=true") as r:
                r.raise_for_status()
                assert r.headers["content-type"].startswith("text/event-stream")
                async for line in r.aiter_lines():
                    if line.startswith("event:"):
                        continue
                    if line.startswith("data:"):
                        got_event = True
                        break
    except Exception as e:
        failures.append(f"SSE: {type(e).__name__} {e}")
    print(f"SSE  events channel: {'OK' if got_event else 'FAIL'}")

    # --- SSE stats ---
    got_stats = False
    try:
        async with httpx.AsyncClient(timeout=12.0, trust_env=False) as c:
            async with c.stream("GET", f"{BASE}/stream/stats") as r:
                async for line in r.aiter_lines():
                    if line.startswith("data:"):
                        got_stats = True
                        break
    except Exception as e:
        failures.append(f"SSE stats: {type(e).__name__} {e}")
    print(f"SSE  stats channel:  {'OK' if got_stats else 'FAIL'}")

    # --- WebSocket ---
    ws_ok = False
    frames = 0
    try:
        async with websockets.connect(
            "ws://127.0.0.1:8000/ws?min_urgency=1", open_timeout=10, proxy=None
        ) as ws:
            hello = json.loads(await asyncio.wait_for(ws.recv(), 10))
            assert hello["t"] == "hello", hello
            # resubscribe protocol
            await ws.send(json.dumps({
                "type": "resubscribe",
                "filters": {"q": "", "min_urgency": 1},
            }))
            while frames < 3:
                frame = json.loads(await asyncio.wait_for(ws.recv(), 15))
                if frame["t"] == "event":
                    frames += 1
                elif frame["t"] == "hello":
                    ws_ok = True  # resubscribe ack
        ws_ok = ws_ok and frames >= 1
    except Exception as e:
        failures.append(f"WS: {type(e).__name__} {e}")
    print(f"WS   firehose:       {'OK' if ws_ok else 'FAIL'} ({frames} events)")

    # --- REST surface ---
    async with httpx.AsyncClient(timeout=15.0, trust_env=False) as c:
        for name, path, check in [
            ("events full", "/api/events?limit=1", lambda r: r[0]["enrichment"]["urgency"] >= 1),
            ("compact", "/api/events?compact=true&limit=1", lambda r: set(r[0]) == {"i","c","p","s","n","t","u","v","w","z","k","x","l"}),
            ("filter cat", "/api/events?categories=Macro&limit=5", lambda r: isinstance(r, list)),
            ("min_urgency", "/api/events?min_urgency=5&limit=5", lambda r: all(e["enrichment"]["urgency"] >= 5 for e in r)),
            ("jsonl", "/api/events.jsonl?compact=true&limit=10", None),
            ("clusters", "/api/clusters?limit=5", lambda r: r[0]["cluster_id"].startswith("cls_")),
            ("sources", "/api/sources", lambda r: len(r) > 50),
            ("dlq", "/api/dlq", None),
            ("schema", "/api/schema", lambda r: "NewsEvent" in r),
            ("openapi", "/openapi.json", lambda r: r["openapi"] == "3.1.0"),
        ]:
            try:
                r = await c.get(f"{BASE}{path}")
                r.raise_for_status()
                if ".jsonl" in path:
                    lines = [l for l in r.text.splitlines() if l.strip()]
                    assert len(lines) >= 1
                    data = None
                else:
                    data = r.json()
                    if check:
                        assert check(data), f"check failed for {name}"
                print(f"REST {name:12}: OK")
            except Exception as e:
                failures.append(f"{name}: {type(e).__name__} {e}")
                print(f"REST {name:12}: FAIL ({e})")

        # runtime source add/pause/resume/delete
        try:
            cfg = {
                "id": "smoke_test_feed", "name": "Smoke Test", "type": "rss",
                "url": "https://www.cisa.gov/cybersecurity-advisories/all.xml",
                "category": "cyber", "poll_interval_sec": 30,
            }
            r = await c.post(f"{BASE}/api/sources", json=cfg)
            assert r.status_code == 201, r.text
            r = await c.post(f"{BASE}/api/sources/smoke_test_feed/pause")
            assert r.status_code == 200
            r = await c.post(f"{BASE}/api/sources/smoke_test_feed/resume")
            assert r.status_code == 200
            r = await c.delete(f"{BASE}/api/sources/smoke_test_feed")
            assert r.status_code == 200
            print("REST source CRUD : OK")
        except Exception as e:
            failures.append(f"source CRUD: {e}")
            print(f"REST source CRUD : FAIL ({e})")

    if failures:
        print(f"\nFAILURES: {failures}")
        return 1
    print("\nALL SMOKE TESTS PASSED")
    return 0


sys.exit(asyncio.run(main()))
