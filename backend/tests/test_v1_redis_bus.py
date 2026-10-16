"""RedisBus semantics against fakeredis (consumer groups, pending replay, tail, SET NX)."""

import asyncio

import pytest

fakeredis = pytest.importorskip("fakeredis")

from omnipulse.bus import RedisBus  # noqa: E402


@pytest.fixture
def bus():
    b = RedisBus.__new__(RedisBus)
    b._redis = fakeredis.FakeAsyncRedis()
    b.block_ms = None  # fakeredis blocks the loop on BLOCK; poll instead
    return b


async def _take(agen, n, timeout=5):
    out = []

    async def run():
        async for item in agen:
            out.append(item)
            if len(out) >= n:
                return

    await asyncio.wait_for(run(), timeout)
    return out


async def test_consumer_group_delivers_and_replays_pending(bus):
    consumer = bus.consume("s", "g", "c1")
    first = asyncio.ensure_future(_take(consumer, 1))
    await asyncio.sleep(0.05)  # group is created at "$": publish after it exists
    await bus.publish("s", {"n": 1})
    (msg_id, payload), = await first
    assert payload == {"n": 1}
    # not acked -> a restarted consumer with the same name gets it again from its pending list
    replay = await _take(bus.consume("s", "g", "c1"), 1)
    assert replay[0][0] == msg_id
    await bus.ack("s", "g", msg_id)


async def test_tail_and_recent(bus):
    tail = asyncio.ensure_future(_take(bus.tail("e"), 2))
    await asyncio.sleep(0.05)
    await bus.publish("e", {"a": 1})
    await bus.publish("e", {"a": 2})
    got = await tail
    assert [p["a"] for _, p in got] == [1, 2]
    recent = await bus.recent("e", 1)
    assert recent[0][1] == {"a": 2}


async def test_claim_once(bus):
    assert await bus.claim_once("k", 60)
    assert not await bus.claim_once("k", 60)
