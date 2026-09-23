"""端到端：乱序 + 关窗 + 告警 + 导出闭环 + 进程重启恢复。"""
from __future__ import annotations

import json

import pytest

from telemetry.models import Rule
from telemetry.pipeline import TelemetryPipeline
from telemetry.windows import CursorBackwardError

from .conftest import log, log_alert, log_result


def test_out_of_order_then_close_then_alert_export(pipeline):
    pipeline.alerts.upsert_rule(
        Rule("cpu-hot", "cpu", ">", 80, window_ms=1000, for_ms=0)
    )
    # 乱序上报（含迟到），显式幂等键
    events = [
        (900, 90.0, "e4"), (200, 20.0, "e2"),
        (100, 10.0, "e1"), (300, 85.0, "e3"),
        (250, 5.0, "e5-late"),
    ]
    for ts, v, key in events:
        r = pipeline.write_metric(
            pipeline.build_metric("cpu", v, ts, {"host": "a"}, key)
        )
        log_result(key, r)
        assert r.accepted

    closed = pipeline.advance_watermark(1000)
    assert len(closed) == 1
    agg = closed[0]
    log_window_e2e(agg)
    assert agg.count == 5
    assert agg.sum == 210.0
    assert (agg.min, agg.max) == (5.0, 90.0)
    assert agg.last_value == 90.0  # 事件时间 900 最大

    # 导出文件包含 1 条聚合 + 1 条 firing 告警
    records = pipeline.exporter.read_all()
    kinds = [r["type"] for r in records]
    assert kinds.count("aggregate") == 1
    assert kinds.count("alert") == 1
    alert = records[-1]["payload"]
    assert alert["state"] == "firing" and alert["rule_id"] == "cpu-hot"
    log.info("导出告警: %s", json.dumps(alert, ensure_ascii=False))


def log_window_e2e(agg):
    log.info(
        "关窗窗口=[%s,%s) count=%s sum=%s min=%s max=%s last=%s",
        agg.window_start_ms, agg.window_end_ms,
        agg.count, agg.sum, agg.min, agg.max, agg.last_value,
    )


def test_restart_recovers_all_state(paths):
    p1 = TelemetryPipeline(
        window_ms=1000, allowed_lateness_ms=500,
        max_cardinality=5, max_events_per_sec=100,
        storage_path=paths["state"], export_path=paths["export"],
    )
    p1.alerts.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=0))
    p1.write_metric(p1.build_metric("cpu", 90, 100, {"h": "a"}, "k1"))
    p1.write_metric(p1.build_metric("cpu", 90, 100, {"h": "a"}, "k1"))  # dup
    p1.advance_watermark(1000)
    stats_before = p1.stats()

    # 模拟进程重启：全新实例 + restore
    p2 = TelemetryPipeline(
        window_ms=1000, allowed_lateness_ms=500,
        max_cardinality=5, max_events_per_sec=100,
        storage_path=paths["state"], export_path=paths["export"],
    )
    p2.restore()

    # 水位恢复：已关闭窗口继续拒绝
    r = p2.write_metric(p2.build_metric("cpu", 1, 500, {}, "kx"))
    log_result("kx(重启后写已关窗口)", r)
    assert r.reason.value == "window_closed"
    # 去重状态恢复
    r = p2.write_metric(p2.build_metric("cpu", 1, 1500, {"h": "a"}, "k1"))
    log_result("k1(重启后重放)", r)
    assert r.reason.value == "duplicate"
    # 游标不允许回退
    with pytest.raises(CursorBackwardError):
        p2.advance_watermark(999)
    # 告警状态恢复
    assert p2.alerts.state_of("r", "metric:cpu{h=a}").value == "firing"
    # 聚合数值恢复一致
    agg = p2.windows.get_aggregate(0, "metric:cpu{h=a}")
    assert agg.sum == 90 and agg.count == 1 and agg.closed
    # 导出文件是追加的，历史记录仍在
    assert len(p2.exporter.read_by_type("alert")) == 1
    assert p2.stats()["accepted"] == stats_before["accepted"]


def test_watermark_closes_and_late_within_grace_before_close_ok(paths):
    """宽限语义：未推进水位前，迟到数据随时可入；一旦关窗则明确拒绝。"""
    p = TelemetryPipeline(
        window_ms=1000, allowed_lateness_ms=500,
        max_cardinality=5, max_events_per_sec=100,
        storage_path=paths["state"], export_path="",
    )
    # 先进窗口1的数据，再回补窗口0（只要窗口0还没关就允许）
    assert p.write_metric(p.build_metric("m", 2, 1200, {}, "newer")).accepted
    assert p.write_metric(p.build_metric("m", 1, 100, {}, "older")).accepted
    # 水位只推进到 1000：窗口0关闭，窗口1仍开放
    closed = p.advance_watermark(1000)
    assert [a.window_start_ms for a in closed] == [0]
    late = p.write_metric(p.build_metric("m", 9, 999, {}, "late0"))
    assert late.reason.value == "window_closed"
    # 窗口1迟到点仍可写入
    assert p.write_metric(p.build_metric("m", 3, 1050, {}, "late1")).accepted


def test_cursor_backward_is_rejected_and_counted(pipeline):
    pipeline.advance_watermark(2000)
    with pytest.raises(CursorBackwardError):
        pipeline.advance_watermark(1000)
    assert pipeline.stats()["rejected"].get("cursor_backward") == 1
    # 水位未被改写
    assert pipeline.windows.watermark_ms == 2000
