"""数据模型：事件、采集结果、窗口聚合、告警状态等类型定义。"""
from __future__ import annotations

import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# 枚举
# ---------------------------------------------------------------------------
class RejectReason(enum.Enum):
    """写入被拒绝的可区分原因（不做静默丢弃）。"""

    DUPLICATE = "duplicate"              # 幂等键重复
    CARDINALITY_LIMIT = "cardinality"    # 序列基数超限
    RATE_LIMIT = "rate_limit"           # 写入速率超限
    WINDOW_CLOSED = "window_closed"     # 写入已关闭/封口的窗口
    CURSOR_BACKWARD = "cursor_backward"  # 游标回退
    BAD_INPUT = "bad_input"             # 输入不合法


class IngestStatus(enum.Enum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class EventKind(enum.Enum):
    METRIC = "metric"
    LOG = "log"


class AlertState(enum.Enum):
    """告警状态机：待触发 -> 触发 -> （抖动抑制）-> 已恢复。"""

    PENDING = "pending"
    FIRING = "firing"
    SUPPRESSED = "suppressed"
    RESOLVED = "resolved"


class LogLevel(enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


# ---------------------------------------------------------------------------
# 采集侧模型
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class MetricPoint:
    """单个指标数据点。

    idempotency_key 为幂等键：同一 key 的重复上报必须被去重。
    timestamp_ms 为事件时间（毫秒），允许乱序/延迟到达。
    """

    name: str
    value: float
    timestamp_ms: int
    idempotency_key: str
    labels: dict[str, str] = field(default_factory=dict)
    kind: EventKind = field(default=EventKind.METRIC)

    @staticmethod
    def new_key() -> str:
        return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class LogEvent:
    """单条日志事件，按 level 计入错误率等聚合。"""

    message: str
    level: LogLevel
    timestamp_ms: int
    idempotency_key: str
    labels: dict[str, str] = field(default_factory=dict)
    kind: EventKind = field(default=EventKind.LOG)

    @staticmethod
    def new_key() -> str:
        return uuid.uuid4().hex


@dataclass(frozen=True, slots=True)
class IngestResult:
    """一次写入的结论，必须可解释：接受或带明确原因的拒绝。"""

    status: IngestStatus
    idempotency_key: str
    reason: RejectReason | None = None
    detail: str = ""
    window_start_ms: int | None = None

    @property
    def accepted(self) -> bool:
        return self.status is IngestStatus.ACCEPTED


# ---------------------------------------------------------------------------
# 批量补报侧模型
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class BackfillItemResult:
    """批量补报中单条数据的结论（按输入下标对齐，逐条可解释）。"""

    index: int
    result: IngestResult


@dataclass(frozen=True, slots=True)
class BackfillReport:
    """一次批量补报的汇总：收了多少、挡了多少、各原因多少、逐条结论。"""

    total: int
    accepted: int
    rejected: int
    reason_counts: dict[str, int]
    results: list[BackfillItemResult] = field(default_factory=list)

    def result_for(self, index: int) -> IngestResult:
        return self.results[index].result


# ---------------------------------------------------------------------------
# 聚合侧模型
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class WindowKey:
    """一个时间窗口的身份：序列 + 窗口起点（左闭右开 [start, end)）。"""

    series: str
    window_start_ms: int
    window_size_ms: int

    @property
    def window_end_ms(self) -> int:
        return self.window_start_ms + self.window_size_ms


@dataclass
class Aggregate:
    """单个窗口内单条序列的聚合结果。

    结论一致：min/max/sum/count/avg 与输入顺序无关；
    last_timestamp_ms 仅用于可解释性，不影响统计结论。
    """

    series: str
    window_start_ms: int
    window_end_ms: int
    count: int = 0
    total: float = 0.0
    minimum: float | None = None
    maximum: float | None = None
    last_timestamp_ms: int | None = None
    last_value: float | None = None
    sealed: bool = False

    @property
    def average(self) -> float | None:
        return self.total / self.count if self.count else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "series": self.series,
            "window_start_ms": self.window_start_ms,
            "window_end_ms": self.window_end_ms,
            "count": self.count,
            "sum": round(self.total, 12),
            "min": self.minimum,
            "max": self.maximum,
            "avg": round(self.average, 12) if self.average is not None else None,
            "last_timestamp_ms": self.last_timestamp_ms,
            "sealed": self.sealed,
        }


# ---------------------------------------------------------------------------
# 告警侧模型
# ---------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class AlertRule:
    """告警规则：对某序列的窗口聚合值做阈值判定。

    for_windows 为连续满足条件多少个窗口才 FIRING（去瞬时抖动）；
    resolve_windows 为连续不满足多少个窗口才 RESOLVED；
    短时抖动通过 SUPPRESSED 状态抑制，不产生新的触发事件。
    """

    rule_id: str
    series: str
    comparator: str          # ">" | "<" | ">=" | "<="
    threshold: float
    for_windows: int = 1
    resolve_windows: int = 1


@dataclass
class AlertStatus:
    """规则的运行时状态（可解释、可恢复、规则更新不改写历史）。"""

    rule_id: str
    state: AlertState = AlertState.PENDING
    breach_count: int = 0
    ok_count: int = 0
    changed_window_ms: int | None = None
    last_value: float | None = None


@dataclass(frozen=True, slots=True)
class AlertEvaluation:
    """单个规则针对单个窗口的判定结论（判定依据可解释）。"""

    rule_id: str
    window_start_ms: int
    value: float | None
    state: AlertState
    triggered: bool
    resolved: bool
    reason: str


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------
def now_ms() -> int:
    return int(time.time() * 1000)
