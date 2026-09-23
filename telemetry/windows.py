"""按时间窗口的聚合，支持乱序到达、迟到重放、窗口关闭与状态持久化。

语义：
- 窗口按 window_ms 与纪元对齐：start = (ts // window_ms) * window_ms；
  因此无论数据以什么顺序到达，同一批数据必然落到同一个窗口，
  聚合结果只取决于数据集合本身，与到达顺序无关（count/sum/min/max 天然可交换）。
- last_value 取「窗口内事件时间戳最大」的值，同时间戳后到者覆盖，保证重放稳定。
- watermark（水位）单调推进；一旦某窗口 end <= watermark，窗口即 closed，
  此后再向其写入（即使在 allowed_lateness_ms 宽限内）一律拒绝 window_closed。
- 迟到但窗口尚未关闭的数据可以正常并入，窗口 version +1，
  使「窗口刷新 + 延迟到达」的结果可重放、可解释。
"""
from __future__ import annotations

from telemetry.models import (
    LogRecord,
    MetricPoint,
    RejectReason,
    WindowAggregate,
    log_series_key,
    metric_series_key,
)


class WindowManager:
    def __init__(self, window_ms: int, allowed_lateness_ms: int) -> None:
        if window_ms <= 0:
            raise ValueError("window_ms 必须为正数")
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms 不能为负")
        self.window_ms = window_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        # key = (window_start_ms, series_key) -> aggregate
        self._agg: dict[tuple[int, str], WindowAggregate] = {}
        self._watermark_ms = -1  # 已关闭到的时间点（单调）

    # ---- 内部工具 ----------------------------------------------------------

    def _window_start(self, ts_ms: int) -> int:
        return (ts_ms // self.window_ms) * self.window_ms

    def _get_or_create(self, start: int, kind: str, series_key: str) -> WindowAggregate:
        key = (start, series_key)
        agg = self._agg.get(key)
        if agg is None:
            agg = WindowAggregate(
                window_start_ms=start,
                window_end_ms=start + self.window_ms,
                kind=kind,
                series_key=series_key,
            )
            self._agg[key] = agg
        return agg

    def _admit(self, ts_ms: int, series_key: str) -> tuple[str | None, int]:
        """检查时间戳是否可入库。返回 (拒绝原因, 窗口起点)。"""
        if ts_ms < 0:
            return RejectReason.INVALID.value, 0
        start = self._window_start(ts_ms)
        end = start + self.window_ms
        if end <= self._watermark_ms:
            return RejectReason.WINDOW_CLOSED.value, start
        return None, start

    def admit_reason(self, ts_ms: int) -> str | None:
        """只读准入检查（不产生任何状态变更）。"""
        reason, _ = self._admit(ts_ms, "")
        return reason

    @staticmethod
    def _fold_numeric(agg: WindowAggregate, ts_ms: int, value: float) -> None:
        if agg.count == 0:
            agg.min = value
            agg.max = value
            agg.last_value = value
            agg._last_ts = ts_ms  # type: ignore[attr-defined]
        else:
            agg.min = min(agg.min, value)
            agg.max = agg.max if agg.max >= value else value
            last_ts = getattr(agg, "_last_ts", -1)
            if ts_ms >= last_ts:
                agg.last_value = value
                agg._last_ts = ts_ms  # type: ignore[attr-defined]
        agg.count += 1
        agg.sum += value
        agg.version += 1

    # ---- 写入 --------------------------------------------------------------

    def add_metric(self, point: MetricPoint) -> tuple[str | None, WindowAggregate | None]:
        series_key = metric_series_key(point.name, point.tags)
        reason, start = self._admit(point.ts_ms, series_key)
        if reason is not None:
            return reason, None
        agg = self._get_or_create(start, f"metric:{point.name}", series_key)
        self._fold_numeric(agg, point.ts_ms, point.value)
        return None, agg

    def add_log(self, record: LogRecord) -> tuple[str | None, WindowAggregate | None]:
        series_key = log_series_key(record.level, record.labels)
        reason, start = self._admit(record.ts_ms, series_key)
        if reason is not None:
            return reason, None
        # 日志聚合：count 为条数，sum/min/max 记录级别权重（DEBUG=1..ERROR=5），
        # last_value 始终为最新一条的级别权重。
        weight = _LEVEL_WEIGHT.get(record.level.upper(), 0)
        agg = self._get_or_create(start, f"log:{record.level.upper()}", series_key)
        self._fold_numeric(agg, record.ts_ms, float(weight))
        return None, agg

    # ---- 窗口推进 / 关闭 ----------------------------------------------------

    def close_through(self, watermark_ms: int) -> list[WindowAggregate]:
        """把水位推进到 watermark_ms，返回本次新关闭的窗口（按键排序，稳定）。

        水位只允许单调前进，回退返回 CURSOR_BACKWARD 且不改变任何状态。
        """
        if watermark_ms < self._watermark_ms:
            raise CursorBackwardError(self._watermark_ms, watermark_ms)
        if watermark_ms == self._watermark_ms:
            return []
        self._watermark_ms = watermark_ms
        newly_closed: list[WindowAggregate] = []
        for agg in self._agg.values():
            if not agg.closed and agg.window_end_ms <= watermark_ms:
                agg.closed = True
                newly_closed.append(agg)
        newly_closed.sort(key=lambda a: (a.window_start_ms, a.series_key))
        return newly_closed

    @property
    def watermark_ms(self) -> int:
        return self._watermark_ms

    # ---- 查询 --------------------------------------------------------------

    def get_aggregate(self, window_start_ms: int, series_key: str) -> WindowAggregate | None:
        return self._agg.get((window_start_ms, series_key))

    def all_aggregates(self, include_open: bool = True) -> list[WindowAggregate]:
        items = [a for a in self._agg.values() if include_open or a.closed]
        return sorted(items, key=lambda a: (a.window_start_ms, a.series_key))

    # ---- 持久化 ------------------------------------------------------------

    def snapshot(self) -> dict:
        aggs = []
        for (start, series_key), agg in sorted(self._agg.items()):
            d = agg.to_dict()
            d["last_ts"] = getattr(agg, "_last_ts", -1)
            aggs.append(d)
        return {
            "window_ms": self.window_ms,
            "allowed_lateness_ms": self.allowed_lateness_ms,
            "watermark_ms": self._watermark_ms,
            "aggregates": aggs,
        }

    def restore(self, state: dict) -> None:
        self.window_ms = state.get("window_ms", self.window_ms)
        self.allowed_lateness_ms = state.get("allowed_lateness_ms", self.allowed_lateness_ms)
        self._watermark_ms = state.get("watermark_ms", -1)
        self._agg = {}
        for d in state.get("aggregates", []):
            agg = WindowAggregate(
                window_start_ms=d["window_start_ms"],
                window_end_ms=d["window_end_ms"],
                kind=d["kind"],
                series_key=d["series_key"],
                count=d["count"],
                sum=d["sum"],
                min=d["min"],
                max=d["max"],
                last_value=d["last_value"],
                closed=d["closed"],
                version=d["version"],
            )
            agg._last_ts = d.get("last_ts", -1)  # type: ignore[attr-defined]
            self._agg[(agg.window_start_ms, agg.series_key)] = agg


class CursorBackwardError(ValueError):
    def __init__(self, old_ms: int, new_ms: int) -> None:
        super().__init__(
            f"水位/游标不允许回退: 当前={old_ms}ms, 请求={new_ms}ms"
        )
        self.old_ms = old_ms
        self.new_ms = new_ms


_LEVEL_WEIGHT = {"DEBUG": 1, "INFO": 2, "WARN": 3, "WARNING": 3, "ERROR": 4, "FATAL": 5}
