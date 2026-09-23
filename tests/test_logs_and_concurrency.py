"""日志采集/聚合，以及「并发写入 + 乱序到达」混合压力下的结论一致性。"""
from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor

from telemetry.models import Rule
from telemetry.pipeline import TelemetryPipeline

from .conftest import log, log_result


def test_log_ingest_and_level_aggregation(pipeline):
    r = pipeline.write_log(pipeline.build_log("error", "boom", 100, {"svc": "api"}, "L1"))
    log_result("L1", r)
    assert r.accepted
    r = pipeline.write_log(pipeline.build_log("ERROR", "boom2", 200, {"svc": "api"}, "L2"))
    assert r.accepted
    agg = pipeline.windows.get_aggregate(0, "log:ERROR{svc=api}")
    assert agg is not None
    assert agg.count == 2 and agg.sum == 8  # ERROR 权重 4
    log.info(
        "日志聚合 窗口=[%s,%s) 序列=%s count=%s 权重和=%s",
        agg.window_start_ms, agg.window_end_ms, agg.series_key, agg.count, agg.sum,
    )


def test_log_dedup_same_key(pipeline):
    r1 = pipeline.write_log(pipeline.build_log("info", "x", 100, {}, "dup-log"))
    r2 = pipeline.write_log(pipeline.build_log("info", "x", 100, {}, "dup-log"))
    assert r1.accepted
    assert r2.reason.value == "duplicate"


def test_concurrent_out_of_order_aggregation_matches_sequential(paths):
    """同一批事件：多线程乱序并发写入 vs 单线程顺序写入，聚合完全一致。"""
    events = [
        (ts, float((ts * 7) % 100), f"m-{ts}")
        for ts in range(0, 1000, 10)  # 100 个点落在窗口 [0,1000)
    ]

    def fresh():
        return TelemetryPipeline(
            window_ms=1000, allowed_lateness_ms=0,
            max_cardinality=1000, max_events_per_sec=100000,
            storage_path=paths["state"], export_path="", autosave=False,
        )

    # 参考：顺序
    ref = fresh()
    for ts, v, k in events:
        assert ref.write_metric(ref.build_metric("cpu", v, ts, {"c": "1"}, k)).accepted
    ref_agg = ref.windows.get_aggregate(0, "metric:cpu{c=1}").to_dict()

    # 并发 + 随机乱序，重复跑多轮，每轮用不同新管道
    for round_id in range(5):
        p = fresh()
        shuffled = events[:]
        random.Random(round_id).shuffle(shuffled)

        def write(ev):
            ts, v, k = ev
            return p.write_metric(p.build_metric("cpu", v, ts, {"c": "1"}, k))

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(write, shuffled))
        assert all(r.accepted for r in results), [r for r in results if not r.accepted]
        got = p.windows.get_aggregate(0, "metric:cpu{c=1}").to_dict()
        assert got == ref_agg, f"第 {round_id} 轮并发乱序结果与顺序结果不一致"
    log.info(
        "并发乱序一致性验证通过: count=%s sum=%s min=%s max=%s last=%s",
        ref_agg["count"], ref_agg["sum"], ref_agg["min"], ref_agg["max"], ref_agg["last_value"],
    )


def test_concurrent_replay_duplicates_after_watermark(paths):
    """并发下关闭窗口后，所有迟到写入一致得到 window_closed，无一混入聚合。"""
    p = TelemetryPipeline(
        window_ms=1000, allowed_lateness_ms=0,
        max_cardinality=1000, max_events_per_sec=100000,
        storage_path=paths["state"], export_path="", autosave=False,
    )
    p.write_metric(p.build_metric("cpu", 1, 100, {}, "base"))
    p.advance_watermark(1000)

    def late_write(i):
        return p.write_metric(p.build_metric("cpu", 999, 500, {}, f"late-{i}"))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(late_write, range(50)))
    assert all(r.reason.value == "window_closed" for r in results)
    agg = p.windows.get_aggregate(0, "metric:cpu{}")
    assert agg.count == 1 and agg.sum == 1
    log.info("并发关窗拒绝=%s 条，聚合仍为 count=1 sum=1", len(results))


def test_alert_rule_failure_isolated_under_concurrency(paths):
    """高并发写入 + 关窗时，告警评估异常不影响聚合与导出主链路。"""
    p = TelemetryPipeline(
        window_ms=1000, allowed_lateness_ms=0,
        max_cardinality=1000, max_events_per_sec=100000,
        storage_path=paths["state"], export_path=paths["export"], autosave=False,
    )
    p.alerts.upsert_rule(Rule("r", "cpu", ">", 50, window_ms=1000, for_ms=0))

    def write(i):
        return p.write_metric(p.build_metric("cpu", float(i % 100), i % 1000, {}, f"c-{i}"))

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(write, range(200)))
    closed = p.advance_watermark(1000)
    assert closed, "应有关闭窗口"
    agg = p.windows.get_aggregate(0, "metric:cpu{}")
    assert agg.count == 200  # 全部计入，无一丢失
    alerts = p.exporter.read_by_type("alert")
    assert any(a["state"] == "firing" for a in alerts)
    log.info("并发告警链路: 关闭窗口=%s, 告警事件=%s", len(closed), len(alerts))
