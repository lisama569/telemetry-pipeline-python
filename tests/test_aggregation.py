"""aggregation：乱序稳定、迟到、封口、游标回退。"""
import pytest

from telemetry.aggregation import WindowAggregator
from telemetry.models import RejectReason


def _rec(series, ts, value, key=None):
    return {
        "kind": "metric", "series": series, "name": series, "labels": {},
        "value": value, "timestamp_ms": ts,
        "window_start_ms": None,
        "idempotency_key": key or f"{series}-{ts}-{value}",
    }


def test_out_of_order_is_stable():
    a = WindowAggregator(1000)
    b = WindowAggregator(1000)
    seq = [(_rec("metric:cpu", 1000, 1.0)), _rec("metric:cpu", 1999, 3.0),
           _rec("metric:cpu", 1500, 2.0)]
    for r in seq:
        assert a.apply(r) is None
    for r in reversed(seq):
        assert b.apply(r) is None
    ga = a.get_aggregate("metric:cpu", 1000)
    gb = b.get_aggregate("metric:cpu", 1000)
    assert ga.to_dict() == gb.to_dict()
    assert (ga.count, ga.minimum, ga.maximum, ga.average) == (3, 1.0, 3.0, 2.0)
    assert ga.last_timestamp_ms == 1999 and ga.last_value == 3.0


def test_late_arrival_before_seal_is_accepted():
    agg = WindowAggregator(1000, allowed_lateness_ms=500)
    assert agg.apply(_rec("metric:cpu", 1000, 1.0)) is None
    assert agg.apply(_rec("metric:cpu", 3000, 9.0)) is None  # 推进水位到 3000
    # 水位 3000：窗口 [1000,2000) 结束+宽限 2500 <= 3000，封口拒绝迟到
    sealed = agg.seal_due_windows()
    assert [(s.series, s.window_start_ms) for s in sealed] == [("metric:cpu", 1000)]
    late = _rec("metric:cpu", 1200, 5.0, key="late1")
    assert agg.apply(late) is RejectReason.WINDOW_CLOSED


def test_late_arrival_within_lateness_window():
    agg = WindowAggregator(1000, allowed_lateness_ms=500)
    agg.apply(_rec("metric:cpu", 1500, 1.0))
    agg.apply(_rec("metric:cpu", 2300, 9.0))  # watermark=2300
    # [1000,2000)+500 = 2500 > 2300，尚未封口，迟到可并入
    assert agg.apply(_rec("metric:cpu", 1100, 7.0, key="late")) is None
    assert agg.get_aggregate("metric:cpu", 1000).count == 2
    sealed = agg.seal_due_windows()
    assert not sealed  # 2300 cutoff 下仍未到期


def test_cursor_backward_rejected():
    agg = WindowAggregator(1000)
    agg.advance_watermark(5000)
    assert agg.advance_watermark(4000) is RejectReason.CURSOR_BACKWARD
    # 用回退游标触发封口也不得改变状态
    assert agg.seal_due_windows(3000) == []
    assert agg.watermark_ms == 5000


def test_seal_is_idempotent_and_immutable():
    agg = WindowAggregator(1000)
    agg.apply(_rec("metric:cpu", 0, 1.0))
    agg.advance_watermark(2000)
    first = agg.seal_due_windows()
    second = agg.seal_due_windows()
    assert len(first) == 1 and second == []
    assert first[0].sealed is True


def test_snapshot_restore_and_trusted_replay():
    agg = WindowAggregator(1000, allowed_lateness_ms=0)
    agg.apply(_rec("metric:cpu", 0, 1.0))
    agg.apply(_rec("metric:cpu", 500, 3.0))
    state = agg.snapshot()

    agg2 = WindowAggregator(1000)
    agg2.restore(state)
    got = agg2.get_aggregate("metric:cpu", 0)
    assert (got.count, got.total, got.minimum, got.maximum) == (2, 4.0, 1.0, 3.0)

    # 重放已封口窗口的历史记录（trusted）也必须被接受
    agg.advance_watermark(2000)
    agg.seal_due_windows()
    state2 = agg.snapshot()
    agg3 = WindowAggregator(1000)
    agg3.restore(state2, replay=[_rec("metric:cpu", 900, 2.0, key="r1")])
    assert agg3.get_aggregate("metric:cpu", 0).count == 3
