"""窗口聚合：乱序到达一致性、迟到重放、窗口关闭、游标回退、重启恢复。"""
from __future__ import annotations

import pytest

from telemetry.models import MetricPoint, normalize_tags
from telemetry.windows import CursorBackwardError, WindowManager

from .conftest import log_window


def _p(ts: int, v: float, key: str) -> MetricPoint:
    return MetricPoint("cpu", float(v), ts, normalize_tags(()), key)


VALUES_ORDER_A = [(100, 1.0), (300, 3.0), (200, 2.0), (900, 9.0)]
VALUES_ORDER_B = [(900, 9.0), (200, 2.0), (300, 3.0), (100, 1.0)]


def _fold(order):
    wm = WindowManager(1000, 500)
    for ts, v in order:
        reason, agg = wm.add_metric(_p(ts, v, f"k{ts}"))
        assert reason is None
    return wm.get_aggregate(0, "metric:cpu{}")


def test_out_of_order_aggregation_is_order_independent():
    """结论一致性：同批数据任意到达顺序，聚合结果完全相同。"""
    a = _fold(VALUES_ORDER_A)
    b = _fold(VALUES_ORDER_B)
    log_window(a, "乱序场景 A")
    log_window(b, "乱序场景 B")
    assert a.to_dict() == b.to_dict()
    assert a.count == 4
    assert a.sum == 15.0
    assert (a.min, a.max) == (1.0, 9.0)
    # last_value 以事件时间为准，不随到达顺序改变
    assert a.last_value == 9.0


def test_late_arrival_replays_and_bumps_version():
    """迟到但窗口未关闭：可并入、可重放，version 递增。"""
    wm = WindowManager(1000, 500)
    for ts, v in VALUES_ORDER_B:
        wm.add_metric(_p(ts, v, f"k{ts}"))
    reason, agg = wm.add_metric(_p(150, 7.0, "late-1"))
    assert reason is None
    log_window(agg, "迟到数据并入后")
    assert agg.sum == 22.0
    assert agg.count == 5
    assert agg.version == 5
    assert agg.last_value == 9.0  # 更早的迟到点不改变 last_value


def test_closed_window_rejects_writes():
    """水位推进后关闭窗口，写入被明确拒绝且不污染聚合。"""
    wm = WindowManager(1000, 500)
    wm.add_metric(_p(100, 1.0, "k1"))
    closed = wm.close_through(1000)
    assert len(closed) == 1 and closed[0].closed

    reason, agg = wm.add_metric(_p(999, 2.0, "late-closed"))
    assert reason == "window_closed"
    assert agg is None
    # 已有聚合不被污染
    existing = wm.get_aggregate(0, "metric:cpu{}")
    assert existing.count == 1 and existing.sum == 1.0

    # 宽限期内但窗口 end <= watermark，照样拒绝（关闭优先于宽限）
    reason, _ = wm.add_metric(_p(501, 3.0, "within-grace-but-closed"))
    assert reason == "window_closed"


def test_cursor_backward_rejected():
    wm = WindowManager(1000, 500)
    wm.close_through(2000)
    with pytest.raises(CursorBackwardError):
        wm.close_through(1999)
    # 状态不变
    assert wm.watermark_ms == 2000
    # 同值推进是幂等空操作
    assert wm.close_through(2000) == []


def test_window_restore_after_restart():
    """重启恢复：聚合值、关闭状态、水位全部还原，结论可重放。"""
    wm = WindowManager(1000, 500)
    for ts, v in VALUES_ORDER_A:
        wm.add_metric(_p(ts, v, f"k{ts}"))
    wm.close_through(1000)
    before = wm.get_aggregate(0, "metric:cpu{}").to_dict()
    snap = wm.snapshot()

    wm2 = WindowManager(1, 1)
    wm2.restore(snap)
    after = wm2.get_aggregate(0, "metric:cpu{}")
    assert after.to_dict() == before
    assert after.closed is True
    assert wm2.watermark_ms == 1000
    assert wm2.admit_reason(500) == "window_closed"


def test_window_alignment_groups_same_data_together():
    """窗口与纪元对齐：跨窗口边界的数据被分到不同窗口。"""
    wm = WindowManager(1000, 500)
    wm.add_metric(_p(999, 1.0, "a"))
    wm.add_metric(_p(1000, 2.0, "b"))
    assert wm.get_aggregate(0, "metric:cpu{}").sum == 1.0
    assert wm.get_aggregate(1000, "metric:cpu{}").sum == 2.0
