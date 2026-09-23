"""数据模型：指标、日志、写入回执、窗口聚合、告警事件。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class EventType(str, Enum):
    METRIC = "metric"
    LOG = "log"


class RejectReason(str, Enum):
    """写入被拒绝的可区分原因。"""
    DUPLICATE = "duplicate"                 # 幂等键重复
    CARDINALITY_LIMIT = "cardinality_limit" # 超出基数上限
    RATE_LIMITED = "rate_limited"           # 超出写入速率
    WINDOW_CLOSED = "window_closed"         # 写入已关闭窗口
    CURSOR_BACKWARD = "cursor_backward"     # 游标回退
    INVALID = "invalid"                     # 参数非法


@dataclass(frozen=True)
class MetricPoint:
    name: str
    value: float
    ts_ms: int
    tags: tuple[tuple[str, str], ...] = ()
    idempotency_key: str = ""

    def event_type(self) -> EventType:
        return EventType.METRIC


@dataclass(frozen=True)
class LogRecord:
    level: str
    message: str
    ts_ms: int
    labels: tuple[tuple[str, str], ...] = ()
    idempotency_key: str = ""

    def event_type(self) -> EventType:
        return EventType.LOG


@dataclass(frozen=True)
class WriteResult:
    accepted: bool
    reason: RejectReason | None = None
    idempotency_key: str = ""
    detail: str = ""


@dataclass
class WindowAggregate:
    window_start_ms: int
    window_end_ms: int
    kind: str                       # metric:<name> 或 log:<level>
    series_key: str                 # 含标签的序列键
    count: int = 0
    sum: float = 0.0
    min: float = 0.0
    max: float = 0.0
    last_value: float = 0.0
    closed: bool = False
    version: int = 0                # 每次迟到数据重放后 +1

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "kind": self.kind,
            "series_key": self.series_key,
            "count": self.count,
            "sum": round(self.sum, 6),
            "min": self.min,
            "max": self.max,
            "last_value": self.last_value,
            "closed": self.closed,
            "version": self.version,
        }


class AlertState(str, Enum):
    PENDING = "pending"     # 观测到异常但尚未持续满足
    FIRING = "firing"       # 已触发
    RESOLVED = "resolved"   # 已恢复


@dataclass
class AlertEvent:
    rule_id: str
    state: AlertState
    ts_ms: int
    window_start_ms: int
    window_end_ms: int
    observed_value: float
    threshold: float
    reason: str             # 判定依据的人类可读说明
    rule_version: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "state": self.state.value,
            "ts_ms": self.ts_ms,
            "window": [self.window_start_ms, self.window_end_ms],
            "observed_value": self.observed_value,
            "threshold": self.threshold,
            "reason": self.reason,
            "rule_version": self.rule_version,
        }


@dataclass
class Rule:
    rule_id: str
    metric_name: str
    op: str                 # ">" / "<" / ">=" / "<="
    threshold: float
    window_ms: int
    for_ms: int = 0             # 持续满足多久才 FIRING（抖动抑制）
    version: int = 1
    agg_field: str = "max"      # 用窗口的哪个统计量判定：max/min/last_value/avg/count
    created_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    # 规则更新只增 version；历史告警事件固化 rule_version，不被改写。


def normalize_tags(tags: Any) -> tuple[tuple[str, str], ...]:
    """标签排序归一化，保证 {a=1,b=2} 与 {b=2,a=1} 属同一序列。"""
    return tuple(sorted((str(k), str(v)) for k, v in tags))


def render_tags(tags: tuple[tuple[str, str], ...]) -> str:
    return "{" + ",".join(f"{k}={v}" for k, v in tags) + "}"


def metric_series_key(name: str, tags: tuple[tuple[str, str], ...]) -> str:
    return f"metric:{name}{render_tags(tags)}"


def log_series_key(level: str, labels: tuple[tuple[str, str], ...]) -> str:
    return f"log:{level.upper()}{render_tags(labels)}"


def now_ms() -> int:
    return int(time.time() * 1000)
