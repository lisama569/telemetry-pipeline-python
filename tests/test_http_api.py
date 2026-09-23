"""HTTP API：上报去重、限流原因透传、关窗 409、规则版本冲突 409。"""
from __future__ import annotations

import importlib

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch):
    # 每个测试使用独立数据目录，重新加载 main 以重建 pipeline
    monkeypatch.setenv("TELEMETRY_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("TELEMETRY_MAX_RATE", "2")
    monkeypatch.setenv("TELEMETRY_MAX_CARDINALITY", "50")
    import main
    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def test_metric_dedup_via_http(client):
    payload = {"name": "cpu", "value": 90, "ts_ms": 100,
               "tags": {"host": "a"}, "idempotency_key": "k1"}
    r1 = client.post("/metrics", json=payload)
    r2 = client.post("/metrics", json=payload)
    assert r1.json()["accepted"] is True
    assert r2.json()["accepted"] is False
    assert r2.json()["reason"] == "duplicate"


def test_rate_limit_via_http(client):
    for i in range(2):
        assert client.post("/metrics", json={
            "name": "m", "value": 1, "ts_ms": 100, "idempotency_key": f"k{i}"}
        ).json()["accepted"] is True
    r = client.post("/metrics", json={
        "name": "m", "value": 1, "ts_ms": 100, "idempotency_key": "kX"})
    assert r.json()["reason"] == "rate_limited"


def test_watermark_backward_returns_409(client):
    client.post("/watermark", json={"watermark_ms": 1000})
    r = client.post("/watermark", json={"watermark_ms": 999})
    assert r.status_code == 409
    assert "回退" in r.json()["detail"]


def test_closed_window_write_rejected_via_http(client):
    client.post("/metrics", json={"name": "cpu", "value": 1, "ts_ms": 100,
                                  "idempotency_key": "a"})
    client.post("/watermark", json={"watermark_ms": 1000})
    r = client.post("/metrics", json={"name": "cpu", "value": 1, "ts_ms": 500,
                                      "idempotency_key": "b"})
    assert r.json()["reason"] == "window_closed"


def test_rule_version_conflict_returns_409(client):
    rule = {"rule_id": "r1", "metric_name": "cpu", "op": ">", "threshold": 80,
            "window_ms": 1000, "version": 1}
    assert client.put("/alerts/rules", json=rule).status_code == 200
    assert client.put("/alerts/rules", json=rule).status_code == 409


def test_full_flow_export_and_stats(client):
    client.put("/alerts/rules", json={
        "rule_id": "r1", "metric_name": "cpu", "op": ">", "threshold": 80,
        "window_ms": 1000, "version": 1})
    client.post("/metrics", json={"name": "cpu", "value": 90, "ts_ms": 100,
                                  "idempotency_key": "x"})
    wm = client.post("/watermark", json={"watermark_ms": 1000}).json()
    assert wm["closed_count"] == 1
    exports = client.get("/exports").json()["records"]
    assert {r["type"] for r in exports} == {"aggregate", "alert"}
    stats = client.get("/stats").json()
    assert stats["accepted"] == 1
    assert client.get("/aggregates").json()["aggregates"][0]["closed"] is True
