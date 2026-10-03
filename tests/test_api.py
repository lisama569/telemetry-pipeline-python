"""HTTP 层冒烟测试：使用独立数据目录启动 ASGI 应用。"""
import importlib
import os
import sys

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("TELEMETRY_DATA_DIR", str(tmp_path / "api-data"))
    monkeypatch.setenv("TELEMETRY_WINDOW_MS", "1000")
    monkeypatch.setenv("TELEMETRY_LATENESS_MS", "0")
    sys.modules.pop("main", None)
    main = importlib.import_module("main")
    with TestClient(main.app) as c:
        yield c


def _metric(key, ts, value, name="cpu", labels=None):
    return {"name": name, "value": value, "timestamp_ms": ts,
            "idempotency_key": key, "labels": labels or {}}


def test_metric_dedup_rule_and_flush_flow(client):
    r = client.post("/metrics", json=_metric("k1", 100, 9.0))
    assert r.status_code == 200 and r.json()["window_start_ms"] == 0
    dup = client.post("/metrics", json=_metric("k1", 100, 9.0))
    assert dup.status_code == 429
    assert dup.json()["detail"]["reason"] == "duplicate"

    client.post("/alerts/rules", json={
        "rule_id": "hi", "series": "metric:cpu", "comparator": ">",
        "threshold": 5.0, "for_windows": 1,
    })
    fr = client.post("/flush", json={"watermark_ms": 2000})
    assert fr.status_code == 200
    sealed = fr.json()["sealed"]
    assert sealed and sealed[0]["window_start_ms"] == 0
    evals = fr.json()["evaluations"]
    assert evals and evals[0]["triggered"] is True

    # 窗口已封口，迟到拒绝
    late = client.post("/metrics", json=_metric("late", 500, 1.0))
    assert late.status_code == 429 and late.json()["detail"]["reason"] == "window_closed"

    aggs = client.get("/aggregates").json()["aggregates"]
    assert any(a["sealed"] for a in aggs)

    r = client.post("/export")
    assert r.status_code == 200
    r = client.post("/snapshot")
    assert r.status_code == 200


def test_log_and_bad_level(client):
    ok = client.post("/logs", json={
        "message": "boom", "level": "ERROR", "timestamp_ms": 10,
        "idempotency_key": "l1",
    })
    assert ok.status_code == 200
    bad = client.post("/logs", json={
        "message": "x", "level": "NOPE", "timestamp_ms": 10,
        "idempotency_key": "l2",
    })
    assert bad.status_code == 400


def test_backfill_batch_endpoint_per_item(client):
    # 先在线写一条并封口窗口 0、1
    client.post("/metrics", json=_metric("online-1", 100, 1.0))
    client.post("/flush", json={"watermark_ms": 2000})

    payload = {"items": [
        {"kind": "metric", "name": "cpu", "value": 5.0,
         "timestamp_ms": 2500, "idempotency_key": "bf-ok"},
        {"kind": "metric", "name": "cpu", "value": 9.0,
         "timestamp_ms": 200, "idempotency_key": "bf-old"},       # window_closed
        {"kind": "metric", "name": "cpu", "value": 9.0,
         "timestamp_ms": 2600, "idempotency_key": "online-1"},    # duplicate
        {"kind": "log", "message": "boom", "level": "ERROR",
         "timestamp_ms": 2700, "idempotency_key": "bf-log"},
        {"kind": "metric", "name": "cpu", "value": 1.0,
         "timestamp_ms": 2800, "idempotency_key": ""},            # bad_input
    ]}
    r = client.post("/backfill", json=payload)
    # 整批不因个别坏条目整体失败
    assert r.status_code == 200
    data = r.json()
    assert data["total"] == 5 and data["accepted"] == 2
    assert data["rejected"] == 3
    assert data["reason_counts"] == {
        "window_closed": 1, "duplicate": 1, "bad_input": 1,
    }
    by_key = {row["idempotency_key"]: row for row in data["results"]}
    assert by_key["bf-ok"]["status"] == "accepted"
    assert by_key["bf-old"]["reason"] == "window_closed"
    assert by_key["online-1"]["reason"] == "duplicate"
    assert by_key["bf-log"]["status"] == "accepted"

    # 同一批重复补报：不再接受任何新条目
    r2 = client.post("/backfill", json=payload)
    assert r2.json()["accepted"] == 0


def test_backfill_bad_shape_is_400(client):
    r = client.post("/backfill", json={"nope": []})
    assert r.status_code == 400
