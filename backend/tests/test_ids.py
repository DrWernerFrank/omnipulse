import pytest

from app.ids import new_id, ulid


def test_ulid_monotonic_within_ms():
    ids = [ulid() for _ in range(500)]
    assert all(a < b for a, b in zip(ids, ids[1:]))


def test_new_id_prefixes():
    e = new_id("evt")
    c = new_id("cls")
    s = new_id("src")
    assert e.startswith("evt_") and len(e) == 30
    assert c.startswith("cls_")
    assert s.startswith("src_")


def test_new_id_rejects_unknown_prefix():
    with pytest.raises(AssertionError):
        new_id("xxx")
