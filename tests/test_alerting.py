"""alerting：触发/恢复/抖动抑制、规则更新不改历史、失败隔离。"""
import pytest

from telemetry.alerting import AlertManager
from telemetry.models import AlertRule, AlertState, AlertStatus


def _rule(for_w=1, resolve_w=1, rid="r1", op=">", th=10.0):
    return AlertRule(rid, "metric:cpu", op, th, for_w, resolve_w)


def test_trigger_and_resolve():
    am = AlertManager()
    am.add_rule(_rule(for_w=2, resolve_w=2))
    ev1 = am.evaluate_window("r1", 0, 11)   # 1/2
    assert ev1.state is AlertState.PENDING and not ev1.triggered
    ev2 = am.evaluate_window("r1", 1000, 12)  # 2/2 FIRING
    assert ev2.state is AlertState.FIRING and ev2.triggered
    ev3 = am.evaluate_window("r1", 2000, 9)  # 进入确认期
    assert ev3.state is AlertState.SUPPRESSED and not ev3.resolved
    ev4 = am.evaluate_window("r1", 3000, 8)  # 确认恢复
    assert ev4.state is AlertState.RESOLVED and ev4.resolved
    # 新一轮触发
    ev5 = am.evaluate_window("r1", 4000, 20)
    assert ev5.state is AlertState.PENDING  # 重新累计
    ev6 = am.evaluate_window("r1", 5000, 20)
    assert ev6.state is AlertState.FIRING and ev6.triggered


def test_flapping_suppressed_no_duplicate_trigger():
    am = AlertManager()
    am.add_rule(_rule(for_w=1, resolve_w=2))
    assert am.evaluate_window("r1", 0, 20).triggered
    am.evaluate_window("r1", 1000, 5)            # -> SUPPRESSED
    flap = am.evaluate_window("r1", 2000, 20)    # 抖动：仍超标
    assert flap.state is AlertState.SUPPRESSED and not flap.triggered
    am.evaluate_window("r1", 3000, 5)
    resolved = am.evaluate_window("r1", 4000, 5)
    assert resolved.state is AlertState.RESOLVED and resolved.resolved


def test_missing_window_does_not_change_state():
    am = AlertManager()
    am.add_rule(_rule(for_w=2, resolve_w=2))
    am.evaluate_window("r1", 0, 11)
    ev = am.evaluate_window("r1", 1000, None)
    assert ev.state is AlertState.PENDING and am.get_status("r1").breach_count == 1


def test_rule_update_keeps_history():
    am = AlertManager()
    am.add_rule(_rule(for_w=1))
    am.evaluate_window("r1", 0, 20)
    before = list(am.history)
    am.update_rule(_rule(for_w=1, th=100.0))
    assert am.history == before            # 历史判定未改写
    assert am.get_status("r1").state is AlertState.PENDING
    assert am.evaluate_window("r1", 1000, 20).state is AlertState.PENDING  # 新阈值下不超标累计


def test_rule_failure_isolation():
    am = AlertManager()
    am.add_rule(_rule(rid="r1"))
    # 模拟运行期规则损坏（绕过 add_rule 校验），评估时必须隔离不波及 r1
    broken = AlertRule("r2", "metric:mem", "??", 1.0)
    am._rules["r2"] = broken
    am._status["r2"] = AlertStatus("r2")
    results = am.evaluate_all(0, {"r1": 20.0, "r2": 20.0})
    assert len(results) == 2
    by = {e.rule_id: e for e in results}
    assert by["r1"].triggered
    assert "异常" in by["r2"].reason and by["r2"].state is AlertState.PENDING


def test_snapshot_restore_roundtrip():
    am = AlertManager()
    am.add_rule(_rule(for_w=2, resolve_w=2))
    am.evaluate_window("r1", 0, 11)
    am.evaluate_window("r1", 1000, 12)
    am.evaluate_window("r1", 2000, 9)

    state = am.snapshot()
    am2 = AlertManager()
    am2.restore(state)
    assert am2.get_status("r1").state is AlertState.SUPPRESSED
    assert am2.history == am.history
    ev = am2.evaluate_window("r1", 3000, 9)
    assert ev.resolved
