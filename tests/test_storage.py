"""storage 的小步验证。"""
import json
import os

from telemetry.storage import LocalStore


def test_wal_append_replay_and_snapshot(tmp_path):
    store = LocalStore(str(tmp_path))
    assert list(store.read_events()) == []
    store.append_event({"idempotency_key": "k1", "value": 1})
    store.append_event({"idempotency_key": "k2", "value": 2})
    replayed = list(store.read_events())
    assert [r["idempotency_key"] for r in replayed] == ["k1", "k2"]

    assert store.read_snapshot() is None
    store.write_snapshot({"cursor_ms": 1000, "n": 2})
    assert store.read_snapshot() == {"cursor_ms": 1000, "n": 2}
    assert not os.path.exists(os.path.join(str(tmp_path), "snapshot.json.tmp"))

    store.truncate_wal()
    assert list(store.read_events()) == []
