"""告警规则判定与状态流转。

状态机（每个窗口判定一次，判定依据写入历史且不可改写）::

    PENDING ──连续 for_windows 个窗口超阈值──▶ FIRING(触发事件)
    FIRING  ──窗口恢复──▶ SUPPRESSED（等待 resolve_windows 确认，期间不恢复）
    SUPPRESSED ──再次超阈值──▶ SUPPRESSED（短时抖动被抑制，不重复触发）
    SUPPRESSED ──连续 resolve_windows 个窗口恢复──▶ RESOLVED(恢复事件)
    RESOLVED ──再次连续超阈值──▶ FIRING（新一轮触发）

- ``triggered=True`` 只在进入 FIRING 的那个窗口出现一次；
- 单条规则评估抛异常会被隔离，只影响该规则，其它规则照常出结论；
- 规则更新只替换现行定义并从 PENDING 重新累计，历史判定记录永不改写。
"""
from __future__ import annotations

import threading
from dataclasses import asdict, replace

from .models import AlertEvaluation, AlertRule, AlertState, AlertStatus

COMPARATORS = {">", "<", ">=", "<="}
ALERT_STATE_VERSION = 1


class AlertManager:
    def __init__(self) -> None:
        self._rules: dict[str, AlertRule] = {}
        self._status: dict[str, AlertStatus] = {}
        self._history: list[AlertEvaluation] = []
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 规则管理
    # ------------------------------------------------------------------
    def add_rule(self, rule: AlertRule) -> None:
        with self._lock:
            if rule.comparator not in COMPARATORS:
                raise ValueError(f"不支持的比较符：{rule.comparator}")
            if rule.for_windows < 1 or rule.resolve_windows < 1:
                raise ValueError("for_windows / resolve_windows 必须 >= 1")
            if rule.rule_id in self._rules:
                raise ValueError(f"规则 {rule.rule_id} 已存在，请用 update_rule")
            self._rules[rule.rule_id] = rule
            self._status[rule.rule_id] = AlertStatus(rule.rule_id)

    def update_rule(self, rule: AlertRule) -> None:
        """更新现行规则：定义被替换，判定计数器清零、状态回 PENDING。

        历史判定（self._history）保持原样，绝不重算或改写。
        """
        with self._lock:
            if rule.rule_id not in self._rules:
                raise KeyError(f"规则 {rule.rule_id} 不存在")
            if rule.comparator not in COMPARATORS:
                raise ValueError(f"不支持的比较符：{rule.comparator}")
            if rule.for_windows < 1 or rule.resolve_windows < 1:
                raise ValueError("for_windows / resolve_windows 必须 >= 1")
            self._rules[rule.rule_id] = rule
            self._status[rule.rule_id] = AlertStatus(rule.rule_id)

    def get_rule(self, rule_id: str) -> AlertRule:
        with self._lock:
            return self._rules[rule_id]

    @property
    def rule_ids(self) -> list[str]:
        with self._lock:
            return list(self._rules)

    def get_status(self, rule_id: str) -> AlertStatus:
        with self._lock:
            st = self._status[rule_id]
            return replace(st)

    @property
    def history(self) -> list[AlertEvaluation]:
        with self._lock:
            return list(self._history)

    # ------------------------------------------------------------------
    # 判定
    # ------------------------------------------------------------------
    @staticmethod
    def _breaches(rule: AlertRule, value: float) -> bool:
        if rule.comparator == ">":
            return value > rule.threshold
        if rule.comparator == "<":
            return value < rule.threshold
        if rule.comparator == ">=":
            return value >= rule.threshold
        if rule.comparator == "<=":
            return value <= rule.threshold
        raise ValueError(f"未知比较符：{rule.comparator!r}")

    def evaluate_window(
        self, rule_id: str, window_start_ms: int, value: float | None
    ) -> AlertEvaluation:
        """评估单条规则在某个窗口的结论。"""
        with self._lock:
            rule = self._rules[rule_id]
            status = self._status[rule_id]

            if value is None:
                ev = AlertEvaluation(
                    rule_id, window_start_ms, None, status.state,
                    triggered=False, resolved=False,
                    reason="窗口无聚合数据，跳过判定且不改变状态",
                )
                self._history.append(ev)
                return ev

            try:
                breach = self._breaches(rule, value)
            except Exception as exc:  # 单规则失败隔离
                ev = AlertEvaluation(
                    rule_id, window_start_ms, value, status.state,
                    triggered=False, resolved=False,
                    reason=f"评估异常已隔离：{exc!r}",
                )
                self._history.append(ev)
                return ev

            triggered = resolved = False
            status.last_value = value
            status.changed_window_ms = window_start_ms

            if breach:
                status.breach_count += 1
                status.ok_count = 0
                if status.state is AlertState.RESOLVED:
                    # 新一轮告警：离开 RESOLVED，从头累计确认窗口
                    status.state = AlertState.PENDING
                if status.state is AlertState.PENDING:
                    if status.breach_count >= rule.for_windows:
                        status.state = AlertState.FIRING
                        triggered = True
                        reason = (
                            f"连续 {status.breach_count} 个窗口 {rule.comparator} "
                            f"{rule.threshold}（当前 {value}），触发告警"
                        )
                    else:
                        reason = (
                            f"超阈值但仅连续 {status.breach_count}/"
                            f"{rule.for_windows} 个窗口，保持 PENDING"
                        )
                elif status.state is AlertState.FIRING:
                    reason = f"告警持续中：{value} {rule.comparator} {rule.threshold}"
                else:  # SUPPRESSED：抖动期内再次超标，抑制重复触发
                    reason = (
                        f"恢复未确认即再次超标（{value}），判定为短时抖动，"
                        "抑制重复触发"
                    )
            else:
                status.ok_count += 1
                status.breach_count = 0
                if status.state is AlertState.FIRING:
                    # 进入恢复确认期，先不恢复，防止单窗口抖动
                    status.state = AlertState.SUPPRESSED
                    reason = (
                        f"窗口恢复（{value}），进入 {rule.resolve_windows} "
                        "窗口恢复确认期"
                    )
                elif status.state is AlertState.SUPPRESSED:
                    if status.ok_count >= rule.resolve_windows:
                        status.state = AlertState.RESOLVED
                        resolved = True
                        reason = (
                            f"连续 {status.ok_count} 个窗口恢复，告警解除"
                        )
                    else:
                        reason = (
                            f"恢复确认中 {status.ok_count}/"
                            f"{rule.resolve_windows}，保持 SUPPRESSED"
                        )
                else:
                    reason = f"窗口未超阈值（{value}），保持 {status.state.value}"

            ev = AlertEvaluation(
                rule_id, window_start_ms, value, status.state,
                triggered, resolved, reason,
            )
            self._history.append(ev)
            return ev

    def evaluate_all(
        self, window_start_ms: int, values: dict[str, float | None]
    ) -> list[AlertEvaluation]:
        """评估全部规则；单条规则失败不阻塞其它规则。"""
        results: list[AlertEvaluation] = []
        with self._lock:
            rule_ids = list(self._rules)
        for rid in rule_ids:
            try:
                results.append(
                    self.evaluate_window(rid, window_start_ms, values.get(rid))
                )
            except Exception as exc:  # 兜底隔离
                results.append(
                    AlertEvaluation(
                        rid, window_start_ms, values.get(rid),
                        self._status.get(rid, AlertStatus(rid)).state,
                        False, False, f"规则评估失败已隔离：{exc!r}",
                    )
                )
        return results

    # ------------------------------------------------------------------
    # 快照 / 恢复
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "version": ALERT_STATE_VERSION,
                "rules": [asdict(r) for r in self._rules.values()],
                "status": [
                    {
                        "rule_id": s.rule_id,
                        "state": s.state.value,
                        "breach_count": s.breach_count,
                        "ok_count": s.ok_count,
                        "changed_window_ms": s.changed_window_ms,
                        "last_value": s.last_value,
                    }
                    for s in self._status.values()
                ],
                "history": [
                    {
                        "rule_id": e.rule_id,
                        "window_start_ms": e.window_start_ms,
                        "value": e.value,
                        "state": e.state.value,
                        "triggered": e.triggered,
                        "resolved": e.resolved,
                        "reason": e.reason,
                    }
                    for e in self._history
                ],
            }

    def restore(self, state: dict) -> None:
        with self._lock:
            self._rules.clear()
            self._status.clear()
            self._history.clear()
            for row in state.get("rules", []):
                rule = AlertRule(**row)
                self._rules[rule.rule_id] = rule
            for row in state.get("status", []):
                self._status[row["rule_id"]] = AlertStatus(
                    rule_id=row["rule_id"],
                    state=AlertState(row["state"]),
                    breach_count=row.get("breach_count", 0),
                    ok_count=row.get("ok_count", 0),
                    changed_window_ms=row.get("changed_window_ms"),
                    last_value=row.get("last_value"),
                )
            # history 已按具名 dict 序列化
            for kwargs in state.get("history", []):
                kwargs = dict(kwargs)
                kwargs["state"] = AlertState(kwargs["state"])
                self._history.append(AlertEvaluation(**kwargs))
