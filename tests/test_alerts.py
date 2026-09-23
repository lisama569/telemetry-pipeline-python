"""告警：时间窗口判定、触发/恢复、短抖动抑制、规则更新不改写历史、序列隔离。"""
from __future__ import annotations

import pytest

from telemetry.models import AlertState, Rule, WindowAggregate
from telemetry.alerts import AlertEngine

from .conftest import log_alert


def _agg(start: int, v: float, series: str = "metric:cpu{}", metric: str = "cpu"):
    return WindowAggregate(
        start, start + 1000, f"metric:{metric}", series,
        count=1, sum=v, min=v, max=v, last_value=v, closed=True,
    )


def test_immediate_trigger_and_resolve():
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=0))
    evs = eng.evaluate(_agg(0, 90))
    assert len(evs) == 1 and evs[0].state is AlertState.FIRING
    log_alert(evs[0])
    assert "window=[0,1000)" in evs[0].reason and "observed=90" in evs[0].reason

    evs = eng.evaluate(_agg(1000, 10))
    assert len(evs) == 1 and evs[0].state is AlertState.RESOLVED
    log_alert(evs[0])
    assert eng.state_of("r").value == "resolved"


def test_debounce_suppresses_short_flapping():
    """for_ms=2000：只持续一个窗口就恢复的抖动不产生任何 FIRING。"""
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=2000))
    assert eng.evaluate(_agg(0, 90)) == []        # PENDING
    assert eng.state_of("r") is AlertState.PENDING
    assert eng.evaluate(_agg(1000, 10)) == []      # 提前恢复 -> 抑制
    assert eng.state_of("r") is AlertState.RESOLVED


def test_trigger_only_after_sustained_window():
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=2000))
    eng.evaluate(_agg(0, 90))
    eng.evaluate(_agg(1000, 95))
    evs = eng.evaluate(_agg(2000, 91))
    assert len(evs) == 1 and evs[0].state is AlertState.FIRING
    # 首个满足窗口在 1000ms 结束，至当前窗口结束 3000ms，已持续 2000ms
    assert "持续=2000ms" in evs[0].reason
    log_alert(evs[0])


def test_no_duplicate_firing_while_already_firing():
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=0))
    assert len(eng.evaluate(_agg(0, 90))) == 1
    assert eng.evaluate(_agg(1000, 99)) == []  # 持续异常不重复触发
    assert eng.evaluate(_agg(2000, 100)) == []


def test_rule_update_version_must_advance_and_history_frozen():
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=0))
    historical = eng.evaluate(_agg(0, 90))[0]

    # 同版本/旧版本更新必须拒绝
    with pytest.raises(ValueError):
        eng.upsert_rule(Rule("r", "cpu", ">", 99, window_ms=1000, version=1))

    eng.upsert_rule(Rule("r", "cpu", ">", 99, window_ms=1000, version=2))
    # 历史事件固化在 version=1、threshold=80，不被新规则改写
    assert historical.rule_version == 1
    assert historical.threshold == 80
    # 新规则下 90 不再触发
    assert eng.evaluate(_agg(1000, 90)) == []
    # 99.5 在新阈值下触发，事件记录 version=2
    evs = eng.evaluate(_agg(2000, 99.5))
    assert len(evs) == 1 and evs[0].rule_version == 2 and evs[0].threshold == 99


def test_alert_state_isolated_per_series():
    """A 序列触发不应被 B 序列的正常值错误恢复。"""
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">", 80, window_ms=1000, for_ms=0))
    sa = "metric:cpu{host=a}"
    sb = "metric:cpu{host=b}"
    assert eng.evaluate(_agg(0, 90, sa))[0].state is AlertState.FIRING
    assert eng.evaluate(_agg(0, 10, sb)) == []  # B 正常，无事件
    assert eng.state_of("r", sa) is AlertState.FIRING
    assert eng.state_of("r", sb) is AlertState.RESOLVED
    # 只有 A 自己恢复才产生 RESOLVED
    evs = eng.evaluate(_agg(1000, 5, sa))
    assert len(evs) == 1 and evs[0].state is AlertState.RESOLVED


def test_alert_state_restored_from_snapshot():
    eng = AlertEngine()
    eng.upsert_rule(Rule("r", "cpu", ">=", 80, window_ms=1000, for_ms=0))
    eng.evaluate(_agg(0, 80))
    snap = eng.snapshot()

    eng2 = AlertEngine()
    eng2.restore(snap)
    assert eng2.state_of("r") is AlertState.FIRING
    evs = eng2.evaluate(_agg(1000, 1))
    assert evs[0].state is AlertState.RESOLVED


def test_ops_and_fields():
    eng = AlertEngine()
    eng.upsert_rule(Rule("lo", "cpu", "<", 5, window_ms=1000, agg_field="min"))
    evs = eng.evaluate(_agg(0, 10))  # min=10 不 <5
    assert evs == []
    a = WindowAggregate(0, 1000, "metric:cpu", "metric:cpu{}",
                        count=2, sum=4, min=2, max=10, last_value=10, closed=True)
    evs = eng.evaluate(a)  # min=2 <5 触发
    assert len(evs) == 1 and evs[0].state is AlertState.FIRING


def test_one_rule_failure_does_not_block_others():
    """单规则/单链路失败不阻塞其它规则。"""
    eng = AlertEngine()
    eng.upsert_rule(Rule("a", "mem", ">", 1, window_ms=1000))
    eng.upsert_rule(Rule("b", "cpu", ">", 1, window_ms=1000))
    evs = eng.evaluate(_agg(0, 5, "metric:cpu{}", metric="cpu"))
    assert [e.rule_id for e in evs] == ["b"]
