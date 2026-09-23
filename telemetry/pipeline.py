"""管道门面：并发安全地串联采集、去重、限流、聚合、告警与导出。

写入判定顺序（全部在同一把锁内串行，保证并发下结论一致）：
    1. 参数合法性          -> invalid
    2. 幂等键去重          -> duplicate
    3. 基数上限            -> cardinality_limit
    4. 写入速率            -> rate_limited
    5. 窗口状态/时间戳     -> window_closed / invalid
任何一步拒绝都不会登记后续状态、不会触碰聚合，绝不污染已有结果。

水位（watermark）推进时关闭到期窗口：关闭的聚合被导出并送告警引擎评估，
产生的告警事件同样导出。水位只进不退，回退抛 CursorBackwardError。
"""
from __future__ import annotations

import hashlib
import threading
from collections import Counter

from telemetry.alerts import AlertEngine
from telemetry.dedup import Deduplicator
from telemetry.exporter import LocalExporter
from telemetry.models import (
    LogRecord,
    MetricPoint,
    RejectReason,
    WindowAggregate,
    WriteResult,
    log_series_key,
    metric_series_key,
    normalize_tags,
)
from telemetry.ratelimit import RateLimiter
from telemetry.storage import LocalStorage
from telemetry.windows import CursorBackwardError, WindowManager


def derive_idempotency_key(kind: str, raw: str) -> str:
    """未显式提供幂等键时，由事件内容派生稳定键（同一内容天然去重）。"""
    h = hashlib.sha256(f"{kind}|{raw}".encode("utf-8")).hexdigest()[:16]
    return f"auto:{h}"


class TelemetryPipeline:
    def __init__(
        self,
        window_ms: int = 1000,
        allowed_lateness_ms: int = 500,
        max_cardinality: int = 100,
        max_events_per_sec: int = 1000,
        storage_path: str = "",
        export_path: str = "",
        autosave: bool = True,
    ) -> None:
        self._lock = threading.RLock()
        self.window_ms = window_ms
        self.windows = WindowManager(window_ms, allowed_lateness_ms)
        self.dedup = Deduplicator(max_cardinality)
        self.ratelimit = RateLimiter(max_events_per_sec)
        self.alerts = AlertEngine()
        self.storage = LocalStorage(storage_path) if storage_path else None
        self.exporter = LocalExporter(export_path) if export_path else None
        self.autosave = autosave
        self._reject_stats: Counter[str] = Counter()
        self._accepted = 0

    # ---- 构造规范化事件 ------------------------------------------------------

    @staticmethod
    def build_metric(
        name: str,
        value: float,
        ts_ms: int,
        tags: dict | tuple | None = None,
        idempotency_key: str = "",
    ) -> MetricPoint:
        ntags = normalize_tags(tags.items() if isinstance(tags, dict) else (tags or ()))
        raw = f"{name}|{value}|{ts_ms}|{ntags}"
        key = idempotency_key or derive_idempotency_key("metric", raw)
        return MetricPoint(name, float(value), int(ts_ms), ntags, key)

    @staticmethod
    def build_log(
        level: str,
        message: str,
        ts_ms: int,
        labels: dict | tuple | None = None,
        idempotency_key: str = "",
    ) -> LogRecord:
        nl = normalize_tags(labels.items() if isinstance(labels, dict) else (labels or ()))
        raw = f"{level}|{message}|{ts_ms}|{nl}"
        key = idempotency_key or derive_idempotency_key("log", raw)
        return LogRecord(level.upper(), message, int(ts_ms), nl, key)

    # ---- 写入主路径 ----------------------------------------------------------

    def write_metric(self, point: MetricPoint) -> WriteResult:
        return self._write(
            point,
            metric_series_key(point.name, point.tags),
            self.windows.add_metric,
        )

    def write_log(self, record: LogRecord) -> WriteResult:
        return self._write(
            record,
            log_series_key(record.level, record.labels),
            self.windows.add_log,
        )

    def _write(self, event, series_key: str, sink) -> WriteResult:
        with self._lock:
            # 1. 参数合法性
            if event.ts_ms < 0:
                return self._reject(
                    RejectReason.INVALID, event.idempotency_key, "ts_ms 不能为负"
                )
            # 5（提前只读检查，避免被拒绝事件登记去重状态）：窗口是否已关闭
            closed_reason = self.windows.admit_reason(event.ts_ms)
            if closed_reason is not None:
                return self._reject(
                    RejectReason(closed_reason),
                    event.idempotency_key,
                    f"事件时间 {event.ts_ms}ms 落入已关闭窗口 "
                    f"(watermark={self.windows.watermark_ms}ms)",
                )
            # 2+3. 幂等去重与基数
            dedup_reason = self.dedup.check(event.idempotency_key, series_key)
            if dedup_reason is not None:
                reason = RejectReason(dedup_reason)
                if reason is RejectReason.CARDINALITY_LIMIT:
                    detail = (
                        f"序列 {series_key} 被拒：基数已达上限 "
                        f"{self.dedup.max_cardinality}"
                    )
                else:
                    detail = f"幂等键 {event.idempotency_key} 已存在，重复上报被去重"
                return self._reject(reason, event.idempotency_key, detail)
            # 4. 速率限制（用事件时间所在秒计数）
            rate_reason = self.ratelimit.check(event.ts_ms)
            if rate_reason is not None:
                used, limit = self.ratelimit.current_usage(event.ts_ms)
                return self._reject(
                    RejectReason.RATE_LIMITED,
                    event.idempotency_key,
                    f"事件秒 {event.ts_ms // 1000} 写入量已达上限 "
                    f"({used}/{limit} events/s)",
                )
            # 入库（窗口侧此前已校验，此处必成功）
            reason, agg = sink(event)
            if reason is not None:
                # 理论不可达：与 admit_reason 保持一致的防御性分支
                return self._reject(RejectReason(reason), event.idempotency_key, "窗口拒绝")
            self._accepted += 1
            self._maybe_save()
            return WriteResult(
                accepted=True,
                idempotency_key=event.idempotency_key,
                detail=(
                    f"已接受: 窗口 [{agg.window_start_ms},{agg.window_end_ms}) "
                    f"序列 {series_key} version={agg.version}"
                ),
            )

    def _reject(self, reason: RejectReason, key: str, detail: str) -> WriteResult:
        self._reject_stats[reason.value] += 1
        return WriteResult(
            accepted=False, reason=reason, idempotency_key=key, detail=detail
        )

    # ---- 水位推进：关窗 -> 导出 -> 告警 --------------------------------------

    def advance_watermark(self, watermark_ms: int) -> list[WindowAggregate]:
        with self._lock:
            try:
                closed = self.windows.close_through(watermark_ms)
            except CursorBackwardError:
                self._reject_stats[RejectReason.CURSOR_BACKWARD.value] += 1
                raise
            for agg in closed:
                self._safe_export_aggregate(agg)
                try:
                    events = self.alerts.evaluate(agg)
                except Exception:
                    events = []
                for ev in events:
                    self._safe_export_alert(ev)
            if closed:
                self._maybe_save()
            return closed

    def _safe_export_aggregate(self, agg: WindowAggregate) -> None:
        if self.exporter is None:
            return
        try:
            self.exporter.export_aggregate(agg)
        except OSError as exc:
            # 导出失败只计数，不阻塞内存链路
            self._reject_stats["export_error"] += 1
            self._last_export_error = str(exc)  # type: ignore[attr-defined]

    def _safe_export_alert(self, event) -> None:
        if self.exporter is None:
            return
        try:
            self.exporter.export_alert(event)
        except OSError as exc:
            self._reject_stats["export_error"] += 1
            self._last_export_error = str(exc)  # type: ignore[attr-defined]

    # ---- 可观测性 ------------------------------------------------------------

    def stats(self) -> dict:
        with self._lock:
            return {
                "accepted": self._accepted,
                "rejected": dict(self._reject_stats),
                "cardinality": self.dedup.cardinality(),
                "watermark_ms": self.windows.watermark_ms,
            }

    def query_aggregates(self, include_open: bool = True) -> list[dict]:
        with self._lock:
            return [a.to_dict() for a in self.windows.all_aggregates(include_open)]

    # ---- 持久化 --------------------------------------------------------------

    def _snapshot(self) -> dict:
        return {
            "window_ms": self.window_ms,
            "accepted": self._accepted,
            "reject_stats": dict(self._reject_stats),
            "windows": self.windows.snapshot(),
            "dedup": self.dedup.snapshot(),
            "alerts": self.alerts.snapshot(),
        }

    def _maybe_save(self) -> None:
        if self.storage is not None and self.autosave:
            self.storage.save(self._snapshot())

    def save(self) -> None:
        with self._lock:
            if self.storage is not None:
                self.storage.save(self._snapshot())

    def restore(self) -> None:
        """进程重启后从本地状态恢复。无状态文件时为空操作。"""
        if self.storage is None:
            return
        with self._lock:
            state = self.storage.load()
            if not state:
                return
            self._accepted = state.get("accepted", 0)
            self._reject_stats = Counter(state.get("reject_stats", {}))
            if "windows" in state:
                self.windows.restore(state["windows"])
            if "dedup" in state:
                self.dedup.restore(state["dedup"])
            if "alerts" in state:
                self.alerts.restore(state["alerts"])
