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


def test_backfill_endpoint_per_item_verdicts(client):
    """批量补报 HTTP 入口：逐条结论 + 汇总，重复补报幂等。"""
    payload = {
        "batch_id": "ops-1",
        "metrics": [
            _metric("bf-1", 100, 1.0),
            _metric("bf-2", 200, 2.0),
        ],
        "logs": [
            {"message": "disk full", "level": "ERROR", "timestamp_ms": 300,
             "idempotency_key": "bf-3"},
        ],
    }
    r = client.post("/backfill", json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["batch_id"] == "ops-1"
    assert body["total"] == 3 and body["accepted"] == 3 and body["rejected"] == 0
    assert [x["idempotency_key"] for x in body["results"]] == ["bf-1", "bf-2", "bf-3"]
    assert all(x["status"] == "accepted" for x in body["results"])

    # 同一批重复补报：全部判 duplicate，不重复计数
    again = client.post("/backfill", json=payload).json()
    assert again["accepted"] == 0 and again["rejected"] == 3
    assert again["rejected_by_reason"] == {"duplicate": 3}
    aggs = client.get("/aggregates").json()["aggregates"]
    cpu = [a for a in aggs if a["series"] == "metric:cpu"][0]
    assert cpu["count"] == 2 and cpu["sum"] == 3.0
