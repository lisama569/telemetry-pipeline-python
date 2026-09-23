"""ingest 的小步验证：去重、基数、限流、并发结论一致。"""
import threading

import pytest

from telemetry.ingest import IngestionConfig, IngestionManager, TokenBucket
from telemetry.models import IngestStatus, LogEvent, LogLevel, MetricPoint, RejectReason


def _metric(i, ts=1000, name="cpu", labels=None):
    return MetricPoint(
        name=name, value=float(i), timestamp_ms=ts,
        idempotency_key=f"k{i}", labels=labels or {},
    )


def test_happy_path_and_duplicate():
    mgr = IngestionManager()
    r1 = mgr.ingest(_metric(1))
    r2 = mgr.ingest(_metric(1))  # 同幂等键
    assert r1.status is IngestStatus.ACCEPTED
    assert r2.status is IngestStatus.REJECTED and r2.reason is RejectReason.DUPLICATE


def test_bad_inputs():
    mgr = IngestionManager()
    assert mgr.ingest(MetricPoint("cpu", 1.0, 1000, "")).reason is RejectReason.BAD_INPUT
    assert mgr.ingest(MetricPoint("", 1.0, 1000, "k")).reason is RejectReason.BAD_INPUT
    assert mgr.ingest(LogEvent("", LogLevel.INFO, 1000, "k2")).reason is RejectReason.BAD_INPUT


def test_cardinality_limit():
    mgr = IngestionManager(IngestionConfig(max_cardinality=2))
    assert mgr.ingest(_metric(1, name="a")).accepted
    assert mgr.ingest(_metric(2, name="b")).accepted
    r3 = mgr.ingest(_metric(3, name="c"))
    assert r3.reason is RejectReason.CARDINALITY_LIMIT
    # 已有序列仍可写，未污染聚合
    assert mgr.ingest(_metric(4, name="a")).accepted


def test_rate_limit():
    mgr = IngestionManager(IngestionConfig(rate_burst=3, rate_limit_per_sec=0.0))
    for i in range(3):
        assert mgr.ingest(_metric(i, name="a")).accepted
    assert mgr.ingest(_metric(9, name="a")).reason is RejectReason.RATE_LIMIT


def test_sink_rejection_does_not_commit():
    mgr = IngestionManager()
    mgr.set_sink(lambda rec: RejectReason.WINDOW_CLOSED)
    r = mgr.ingest(_metric(1))
    assert r.reason is RejectReason.WINDOW_CLOSED
    # 下游拒绝后幂等键不能被占用 -> 换 sink 后同键应可接受
    mgr.set_sink(lambda rec: None)
    assert mgr.ingest(_metric(1)).accepted


def test_concurrent_duplicates_accepted_once():
    mgr = IngestionManager(IngestionConfig(rate_limit_per_sec=100000, rate_burst=100000))
    point = _metric(1)
    results = []

    def worker():
        for _ in range(100):
            results.append(mgr.ingest(point))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    accepted = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]
    assert len(accepted) == 1
    assert len(rejected) == 799
    assert all(r.reason is RejectReason.DUPLICATE for r in rejected)


def test_token_bucket_refills():
    import time as _time
    b = TokenBucket(capacity=2, refill_per_sec=1000)
    assert b.try_consume() and b.try_consume()
    assert not b.try_consume()
    _time.sleep(0.01)
    assert b.try_consume()
