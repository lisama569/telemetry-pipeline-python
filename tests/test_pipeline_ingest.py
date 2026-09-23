"""采集入库：并发上报、幂等去重、基数限制、速率限制、拒绝原因可区分。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from telemetry.models import RejectReason

from .conftest import log, log_result


def test_concurrent_writes_same_idempotency_key_accepted_once(pipeline):
    """并发：同一幂等键的 N 次并发上报，恰好 1 次接受，其余全部 duplicate。

    聚合结果只被计入一次（不被重复数据污染）。
    """
    def write(i):
        point = pipeline.build_metric("req", 1.0, 100, {"k": "v"}, "same-key")
        r = pipeline.write_metric(point)
        return r

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(write, range(200)))

    accepted = [r for r in results if r.accepted]
    dup = [r for r in results if r.reason is RejectReason.DUPLICATE]
    log.info("并发同键: accepted=%s duplicate=%s", len(accepted), len(dup))
    assert len(accepted) == 1
    assert len(dup) == 199
    agg = pipeline.windows.get_aggregate(0, "metric:req{k=v}")
    assert agg.count == 1 and agg.sum == 1.0


def test_concurrent_writes_different_keys_all_accepted(pipeline):
    """并发：不同幂等键全部接受，聚合计数精确。"""
    barrier = threading.Barrier(32)

    def write(i):
        barrier.wait()  # 尽量放大竞争
        point = pipeline.build_metric("req", float(i), 100, {}, f"key-{i}")
        return pipeline.write_metric(point)

    with ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(write, range(32)))

    assert all(r.accepted for r in results)
    agg = pipeline.windows.get_aggregate(0, "metric:req{}")
    assert agg.count == 32
    assert agg.sum == sum(range(32))
    log_window = agg
    log.info("并发异键: count=%s sum=%s", agg.count, agg.sum)


def test_duplicate_reason_is_distinct_and_explainable(pipeline):
    r1 = pipeline.write_metric(pipeline.build_metric("m", 1, 100, {}, "dk"))
    r2 = pipeline.write_metric(pipeline.build_metric("m", 2, 200, {}, "dk"))
    log_result("dk", r1)
    log_result("dk", r2)
    assert r1.accepted
    assert r2.reason is RejectReason.DUPLICATE
    assert "幂等键" in r2.detail


def test_cardinality_limit_reason_is_distinct(pipeline):
    # max_cardinality=5（fixture）
    for i in range(5):
        r = pipeline.write_metric(
            pipeline.build_metric("m", 1, 100, {"host": f"h{i}"}, f"k{i}")
        )
        assert r.accepted
    overflow = pipeline.write_metric(
        pipeline.build_metric("m", 1, 100, {"host": "hX"}, "kX")
    )
    log_result("kX", overflow)
    assert overflow.reason is RejectReason.CARDINALITY_LIMIT
    assert "基数" in overflow.detail
    assert pipeline.dedup.cardinality() == 5


def test_rate_limit_reason_is_distinct(pipeline, tmp_path):
    p = type(pipeline)(
        window_ms=1000,
        allowed_lateness_ms=0,
        max_cardinality=1000,
        max_events_per_sec=3,
        storage_path=str(tmp_path / "s2.json"),
        export_path="",
    )
    rs = [p.write_metric(p.build_metric("m", 1, 100 + i, {}, f"rk{i}")) for i in range(6)]
    reasons = [r.reason for r in rs]
    log.info("限流序列原因: %s", [x.value if x else "accepted" for x in reasons])
    assert sum(1 for r in rs if r.accepted) == 3
    assert all(r is RejectReason.RATE_LIMITED for r in reasons[3:])
    assert "events/s" in rs[3].detail
    # 下一秒自动放开
    assert p.write_metric(p.build_metric("m", 1, 1000, {}, "rk-next")).accepted


def test_rate_limit_bucket_is_per_event_second(pipeline, tmp_path):
    p = type(pipeline)(
        window_ms=1000, allowed_lateness_ms=0,
        max_cardinality=1000, max_events_per_sec=2,
        storage_path=str(tmp_path / "s3.json"), export_path="",
    )
    assert p.write_metric(p.build_metric("m", 1, 999, {}, "a")).accepted
    assert p.write_metric(p.build_metric("m", 1, 999, {}, "b")).accepted
    assert p.write_metric(p.build_metric("m", 1, 999, {}, "c")).reason is RejectReason.RATE_LIMITED
    # 新的一秒，窗口计数重置
    assert p.write_metric(p.build_metric("m", 1, 1000, {}, "d")).accepted


def test_rejected_events_never_pollute_aggregates(pipeline):
    pipeline.write_metric(pipeline.build_metric("m", 10, 100, {}, "ok"))
    # 重复
    pipeline.write_metric(pipeline.build_metric("m", 99, 200, {}, "ok"))
    # 关窗后写入
    pipeline.advance_watermark(1000)
    pipeline.write_metric(pipeline.build_metric("m", 99, 500, {}, "late"))
    agg = pipeline.windows.get_aggregate(0, "metric:m{}")
    assert agg.count == 1 and agg.sum == 10 and agg.max == 10


def test_tag_order_normalized_to_same_series(pipeline):
    r1 = pipeline.write_metric(
        pipeline.build_metric("m", 1, 100, {"a": "1", "b": "2"}, "t1")
    )
    r2 = pipeline.write_metric(
        pipeline.build_metric("m", 2, 200, {"b": "2", "a": "1"}, "t2")
    )
    assert r1.accepted and r2.accepted
    agg = pipeline.windows.get_aggregate(0, "metric:m{a=1,b=2}")
    assert agg is not None and agg.count == 2
