"""告警规则：时间窗口判定、状态流转、抖动抑制、规则版本化。

状态机（每条规则 × 每个匹配序列独立维护，互不干扰）：
    OK ──条件满足──> PENDING（记录首个满足窗口的起点）
    PENDING ──连续满足且持续 >= for_ms──> FIRING（产生触发事件）
    FIRING  ──条件不满足──> RESOLVED（产生恢复事件，随后回到 OK）
    PENDING ──条件在 for_ms 内提前不满足──> OK（短抖动被抑制，不触发）

为什么状态按序列（series_key）隔离：同一指标名下不同标签组合是不同的
时间序列，若共用一个状态机，A 序列触发会被 B 序列的正常值错误恢复。

可解释性：每个 AlertEvent.reason 写明窗口区间、所用聚合字段、观测值、
比较符、阈值、持续时长与状态流转。规则更新只增 version 并替换规则定义；
已产生的历史 AlertEvent 固化 rule_version 与判定快照，永不被改写。
"""
from __future__ import annotations

from dataclasses import dataclass

from telemetry.models import AlertEvent, AlertState, Rule, WindowAggregate

_OPS = {
    ">": lambda v, t: v > t,
    "<": lambda v, t: v < t,
    ">=": lambda v, t: v >= t,
    "<=": lambda v, t: v <= t,
}


@dataclass
class SeriesState:
    """一条规则在一个序列上的运行态。"""
    state: AlertState = AlertState.RESOLVED
    armed: bool = False                  # 是否处于 PENDING 观测中
    first_breach_end: int = -1           # 首个满足窗口的结束时刻（持续计时基准）
    last_eval_ts: int = -1
    last_value: float | None = None
    error: str = ""

    def snapshot(self) -> dict:
        return {
            "state": self.state.value,
            "armed": self.armed,
            "first_breach_end": self.first_breach_end,
            "last_eval_ts": self.last_eval_ts,
            "last_value": self.last_value,
        }

    def restore(self, d: dict) -> None:
        self.state = AlertState(d["state"])
        self.armed = d.get("armed", False)
        self.first_breach_end = d.get("first_breach_end", d.get("first_breach_start", -1))
        self.last_eval_ts = d.get("last_eval_ts", -1)
        self.last_value = d.get("last_value")


class RuleState:
    """单条规则的定义 + 其在所有序列上的运行态。"""

    def __init__(self, rule: Rule) -> None:
        self.rule = rule
        self.series: dict[str, SeriesState] = {}

    def state_of(self, series_key: str) -> AlertState:
        return self.series.get(series_key, SeriesState()).state

    def overall_state(self) -> AlertState:
        """跨序列汇总：任一 FIRING 即 FIRING，否则任一 PENDING 即 PENDING。"""
        states = {s.state for s in self.series.values()}
        if AlertState.FIRING in states:
            return AlertState.FIRING
        if AlertState.PENDING in states:
            return AlertState.PENDING
        return AlertState.RESOLVED

    def snapshot(self) -> dict:
        return {
            sk: st.snapshot() for sk, st in sorted(self.series.items())
        }

    def restore_series(self, states: dict) -> None:
        for sk, d in states.items():
            st = SeriesState()
            st.restore(d)
            self.series[sk] = st


class AlertEngine:
    def __init__(self) -> None:
        self._rules: dict[str, RuleState] = {}

    # ---- 规则管理 ----------------------------------------------------------

    def upsert_rule(self, rule: Rule) -> int:
        """新增或更新规则。同 rule_id 更新时版本号必须更大，返回生效版本。

        更新只替换规则定义并重置其运行态；历史 AlertEvent 不受影响。
        """
        if rule.op not in _OPS:
            raise ValueError(f"不支持的比较符: {rule.op}")
        if rule.agg_field not in _FIELD:
            raise ValueError(f"不支持的聚合字段: {rule.agg_field}")
        existing = self._rules.get(rule.rule_id)
        if existing is not None and rule.version <= existing.rule.version:
            raise ValueError(
                f"规则版本必须单调递增: {rule.rule_id} "
                f"current={existing.rule.version} got={rule.version}"
            )
        self._rules[rule.rule_id] = RuleState(rule)
        return rule.version

    def remove_rule(self, rule_id: str) -> None:
        self._rules.pop(rule_id, None)

    def get_rule(self, rule_id: str) -> Rule | None:
        rs = self._rules.get(rule_id)
        return rs.rule if rs else None

    # ---- 判定 --------------------------------------------------------------

    def evaluate(self, agg: WindowAggregate) -> list[AlertEvent]:
        """用一个已关闭/刷新的窗口评估所有匹配规则。

        单条规则抛错会被隔离到其序列状态上，不影响其它规则/序列
        （单条链路或单个规则失败不阻塞其它链路）。
        """
        events: list[AlertEvent] = []
        for rule_id in sorted(self._rules):
            rs = self._rules[rule_id]
            try:
                ev = self._evaluate_one(rs, agg)
            except Exception as exc:
                st = rs.series.setdefault(agg.series_key, SeriesState())
                st.error = str(exc)
                ev = None
            if ev:
                events.append(ev)
        return events

    def _evaluate_one(self, rs: RuleState, agg: WindowAggregate) -> AlertEvent | None:
        rule = rs.rule
        # 只评估匹配 metric 名称、且同窗口长度的聚合
        if agg.kind != f"metric:{rule.metric_name}":
            return None
        if agg.window_end_ms - agg.window_start_ms != rule.window_ms:
            return None

        st = rs.series.setdefault(agg.series_key, SeriesState())
        value = _FIELD[rule.agg_field](agg)
        st.last_eval_ts = agg.window_end_ms
        st.last_value = value
        breached = _OPS[rule.op](value, rule.threshold)

        base_reason = (
            f"rule={rule.rule_id} series={agg.series_key} "
            f"window=[{agg.window_start_ms},{agg.window_end_ms}) "
            f"field={rule.agg_field} observed={round(value, 6)} "
            f"{rule.op} threshold={rule.threshold} -> breached={breached}"
        )

        if breached:
            if not st.armed:
                st.armed = True
                st.first_breach_end = agg.window_end_ms
                st.state = AlertState.PENDING
            # 从首个满足窗口的结束时刻起算已持续时长
            sustained = agg.window_end_ms - st.first_breach_end
            if st.state != AlertState.FIRING and sustained >= rule.for_ms:
                st.state = AlertState.FIRING
                return AlertEvent(
                    rule_id=rule.rule_id,
                    state=AlertState.FIRING,
                    ts_ms=agg.window_end_ms,
                    window_start_ms=agg.window_start_ms,
                    window_end_ms=agg.window_end_ms,
                    observed_value=value,
                    threshold=rule.threshold,
                    reason=(
                        f"TRIGGER {base_reason}; 持续={sustained}ms "
                        f">= for={rule.for_ms}ms，状态 PENDING->FIRING"
                    ),
                    rule_version=rule.version,
                )
            return None  # 仍在持续观测窗口内（抖动抑制中）

        # 条件不满足
        if st.state == AlertState.FIRING:
            st.state = AlertState.RESOLVED
            ev = AlertEvent(
                rule_id=rule.rule_id,
                state=AlertState.RESOLVED,
                ts_ms=agg.window_end_ms,
                window_start_ms=agg.window_start_ms,
                window_end_ms=agg.window_end_ms,
                observed_value=value,
                threshold=rule.threshold,
                reason=(
                    f"RESOLVE {base_reason}; 条件不再满足，状态 FIRING->RESOLVED"
                ),
                rule_version=rule.version,
            )
            st.armed = False
            st.first_breach_end = -1
            return ev
        if st.armed:
            # PENDING 期间提前恢复：短抖动被抑制，不产生告警事件
            st.armed = False
            st.first_breach_end = -1
            st.state = AlertState.RESOLVED
        return None

    def state_of(self, rule_id: str, series_key: str | None = None) -> AlertState | None:
        rs = self._rules.get(rule_id)
        if rs is None:
            return None
        if series_key is None:
            return rs.overall_state()
        return rs.state_of(series_key)

    # ---- 持久化 ------------------------------------------------------------

    def snapshot(self) -> dict:
        rules: list[dict] = []
        states: dict[str, dict] = {}
        for rule_id in sorted(self._rules):
            rs = self._rules[rule_id]
            r = rs.rule
            rules.append(
                {
                    "rule_id": r.rule_id,
                    "metric_name": r.metric_name,
                    "op": r.op,
                    "threshold": r.threshold,
                    "window_ms": r.window_ms,
                    "for_ms": r.for_ms,
                    "version": r.version,
                    "agg_field": r.agg_field,
                    "created_at_ms": r.created_at_ms,
                }
            )
            states[rule_id] = rs.snapshot()
        return {"rules": rules, "states": states}

    def restore(self, state: dict) -> None:
        self._rules = {}
        for rd in state.get("rules", []):
            rule = Rule(
                rule_id=rd["rule_id"],
                metric_name=rd["metric_name"],
                op=rd["op"],
                threshold=rd["threshold"],
                window_ms=rd["window_ms"],
                for_ms=rd.get("for_ms", 0),
                version=rd["version"],
                agg_field=rd.get("agg_field", "max"),
                created_at_ms=rd.get("created_at_ms", 0),
            )
            rs = RuleState(rule)
            rs.restore_series(state.get("states", {}).get(rule.rule_id, {}))
            self._rules[rule.rule_id] = rs


def _avg(agg: WindowAggregate) -> float:
    return agg.sum / agg.count if agg.count else 0.0


_FIELD = {
    "max": lambda a: a.max,
    "min": lambda a: a.min,
    "last_value": lambda a: a.last_value,
    "avg": _avg,
    "count": lambda a: float(a.count),
    "sum": lambda a: a.sum,
}
