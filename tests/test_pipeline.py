"""pipeline 端到端：闭环、拒绝原因可区分、重启恢复、游标/封口拒绝。"""
import threading

import pytest

from telemetry.models import AlertRule, IngestStatus, LogEvent, LogLevel, MetricPoint, RejectReason
from telemetry.pipeline import TelemetryPipeline
from telemetry.ingest import IngestionConfig


def _cpu(i, ts, v=None, labels=None, key=None):
    return MetricPoint("cpu", float(i if v is None else v), ts, key or f"cpu-{i}", labels or {})


def test_end_to_end_seal_and_alert(tmp_path):
    pipe = TelemetryPipeline(str(tmp_path / "s"), window_size_ms=1000)
    pipe.alerts.add_rule(AlertRule("high_cpu", "metric:cpu", ">", 5.0, for_windows=1))

    r1 = pipe.ingest(_cpu(1, 0, 1.0))
    r2 = pipe.ingest(_cpu(2, 500, 11.0))
    assert r1.accepted and r2.accepted and r1.window_start_ms == 0

    # 重复幂等键
    dup = pipe.ingest(_cpu(2, 500, 11.0))
    assert dup.reason is RejectReason.DUPLICATE

    report = pipe.flush(watermark_ms=2000)
    assert [(a.series, a.window_start_ms) for a in report.sealed] == [("metric:cpu", 0)]
    assert report.sealed[0].average == 6.0
    ev = [e for e in report.evaluations if e.rule_id == "high_cpu"][0]
    assert ev.triggered and "6.0" in ev.reason

    # 封口后再写该窗口必须明确拒绝且不落盘
    late = pipe.ingest(MetricPoint("cpu", 2.0, 900, "late-1"))
    assert late.reason is RejectReason.WINDOW_CLOSED
    assert pipe.aggregator.get_aggregate("metric:cpu", 0).count == 2


def test_reject_reasons_are_distinguishable(tmp_path):
    pipe = TelemetryPipeline(
        str(tmp_path / "s"), window_size_ms=1000,
        ingestion_config=IngestionConfig(max_cardinality=1, rate_burst=2, rate_limit_per_sec=0),
    )
    seen = set()
    seen.add(pipe.ingest(MetricPoint("cpu", 1.0, 0, "cpu-1")).reason or "ok")
    seen.add(pipe.ingest(MetricPoint("cpu", 1.0, 0, "cpu-1")).reason or "ok")  # duplicate
    seen.add(pipe.ingest(MetricPoint("mem", 1.0, 0, "m1")).reason or "ok")  # cardinality
    pipe.ingest(_cpu(3, 0, key="c3"))
    seen.add(pipe.ingest(_cpu(4, 0, key="c4")).reason or "ok")        # rate
    assert seen >= {"ok", RejectReason.DUPLICATE, RejectReason.CARDINALITY_LIMIT,
                    RejectReason.RATE_LIMIT}


def test_watermark_backward_rejected_on_pipeline(tmp_path):
    pipe = TelemetryPipeline(str(tmp_path / "s"), window_size_ms=1000)
    pipe.ingest(_cpu(1, 5000, 1.0))
    assert pipe.aggregator.advance_watermark(4000) is RejectReason.CURSOR_BACKWARD


def test_restart_recovers_state_and_still_dedups(tmp_path):
    data_dir = str(tmp_path / "s")

    def fresh():
        return TelemetryPipeline(data_dir, window_size_ms=1000)

    pipe = fresh()
    pipe.alerts.add_rule(AlertRule("high_cpu", "metric:cpu", ">", 5.0))
    pipe.ingest(_cpu(1, 0, 1.0))
    pipe.ingest(_cpu(2, 500, 11.0))
    # 重启场景 1：仅有 WAL，无快照
    pipe2 = fresh()
    pipe2.alerts.add_rule(AlertRule("high_cpu", "metric:cpu", ">", 5.0))
    assert pipe2.aggregator.get_aggregate("metric:cpu", 0).count == 2
    dup = pipe2.ingest(_cpu(2, 500, 11.0))
    assert dup.reason is RejectReason.DUPLICATE  # 幂等键已从 WAL 恢复

    # 推进并封口、产生告警，再做快照
    report = pipe2.flush(2000)
    assert report.evaluations[0].triggered
    pipe2.snapshot()

    # 重启场景 2：快照 + 快照后新 WAL
    pipe2.ingest(_cpu(3, 2000, 3.0))
    pipe3 = fresh()
    assert pipe3.aggregator.get_aggregate("metric:cpu", 0).sealed
    assert pipe3.aggregator.get_aggregate("metric:cpu", 2000).count == 1
    assert pipe3.alerts.get_status("high_cpu").state.value == "firing"
    # 快照里的去重键仍然生效
    assert pipe3.ingest(_cpu(1, 0, 1.0)).reason is RejectReason.DUPLICATE
    assert pipe3.ingest(_cpu(3, 2000, 3.0)).reason is RejectReason.DUPLICATE


def test_concurrent_ingest_consistent_aggregation(tmp_path):
    pipe = TelemetryPipeline(
        str(tmp_path / "s"), window_size_ms=60_000,
        ingestion_config=IngestionConfig(rate_limit_per_sec=1_000_000, rate_burst=1_000_000),
    )
    points = [MetricPoint("cpu", float(i), 1000, f"k{i}") for i in range(500)]

    def worker(chunk):
        for p in chunk:
            r = pipe.ingest(p)
            assert r.accepted

    threads = []
    for i in range(8):
        t = threading.Thread(target=worker, args=(points[i::8],))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    agg = pipe.aggregator.get_aggregate("metric:cpu", 0)
    assert agg.count == 500
    assert agg.total == sum(range(500))
    assert agg.minimum == 0 and agg.maximum == 499

    # 并发重复上报：每个键在另一个线程再写一次，全部应被识别为重复
    duplicates: list = []

    def rewriter(chunk):
        for p in chunk:
            duplicates.append(pipe.ingest(p).reason)

    threads = [threading.Thread(target=rewriter, args=(points[i::8],)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert duplicates == [RejectReason.DUPLICATE] * 500


def test_log_events_are_ingested(tmp_path):
    pipe = TelemetryPipeline(str(tmp_path / "s"), window_size_ms=1000)
    r = pipe.ingest(LogEvent("boom", LogLevel.ERROR, 100, "log-1", {"svc": "a"}))
    assert r.accepted
    agg = pipe.aggregator.get_aggregate("log:ERROR{svc=a}", 0)
    assert agg is not None and agg.count == 1


def test_replay_after_restart_is_order_independent(tmp_path):
    """两个管道以不同到达顺序接受同一组事件，重启重放后聚合必须完全一致。"""
    events = [MetricPoint("cpu", float(i), (i * 613) % 5000, f"o-{i}")
              for i in range(60)]
    dir_a, dir_b = str(tmp_path / "a"), str(tmp_path / "b")
    # 迟到宽限覆盖全部时间跨度：在线期间窗口不封口，乱序事件全部被接受，
    # 此时“同一组事件任意到达顺序 + 重启重放”的统计必须完全一致。
    p_a = TelemetryPipeline(dir_a, window_size_ms=1000, allowed_lateness_ms=10_000)
    p_b = TelemetryPipeline(dir_b, window_size_ms=1000, allowed_lateness_ms=10_000)
    for p in events:
        p_a.ingest(p)
    for p in reversed(events):
        p_b.ingest(p)

    # 重启（仅 WAL，无快照），各自重放。
    # 比较口径：count/sum/min/max/sealed 等统计量与顺序无关必须一致；
    # last_timestamp_ms/last_value 是展示字段，天然与到达顺序相关，不参与比较。
    def stats(p):
        return sorted(
            (
                (a.series, a.window_start_ms, a.count, round(a.total, 12),
                 a.minimum, a.maximum, a.sealed)
                for a in p.aggregator.aggregates()
            )
        )

    # 重启（仅 WAL，无快照），各自重放
    ra = TelemetryPipeline(dir_a, window_size_ms=1000)
    rb = TelemetryPipeline(dir_b, window_size_ms=1000)
    assert stats(ra) == stats(rb)

    # 快照 + 重启仍然一致
    ra.snapshot()
    rb.snapshot()
    assert stats(TelemetryPipeline(dir_a, window_size_ms=1000)) == \
           stats(TelemetryPipeline(dir_b, window_size_ms=1000)) == stats(ra)


def test_alert_only_evaluates_its_own_series(tmp_path):
    """封口多个 series 时，规则只对自己关注的 series 窗口产生判定。"""
    pipe = TelemetryPipeline(str(tmp_path / "s"), window_size_ms=1000)
    pipe.alerts.add_rule(AlertRule("cpu_rule", "metric:cpu", ">", 100.0))
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "c1"))       # 不超标
    pipe.ingest(MetricPoint("mem", 999.0, 100, "m1"))     # 超标但规则不关注
    report = pipe.flush(2000)
    assert len(report.evaluations) == 1
    ev = report.evaluations[0]
    assert ev.rule_id == "cpu_rule" and ev.value == 1.0 and not ev.triggered
