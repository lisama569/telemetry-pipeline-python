"""运维批量补报/历史重放入口。

补报与在线采集的差异：
- 输入是一批事件时间在过去的离线数据，允许乱序、允许整批重复提交、
  允许横跨已经封口的窗口；
- 每条独立给结论（个别坏数据不拖垮整批），最后汇总收/挡数量与原因；
- 判定是"输入集合 + 已落盘事实"的纯函数：与线程调度、机器快慢无关，
  同一份输入逐条结论与最终聚合必须完全一致；
- 补报额度（配额）随快照持久化，重启不清零。

固定且唯一的判定优先级（一条数据同时踩多个条件时只给一个结论）::

    BAD_INPUT > DUPLICATE > WINDOW_CLOSED > CARDINALITY_LIMIT > RATE_LIMIT(补报额度)

即"窗口已关"是事实性结论，永远优先于"写得太快/额度不足"，两者绝不混用。
"""
from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass

from .ingest import IngestionManager, _series_key
from .models import (
    BackfillItemResult,
    BackfillReport,
    IngestResult,
    IngestStatus,
    LogEvent,
    LogLevel,
    MetricPoint,
    RejectReason,
)

logger = logging.getLogger("telemetry.backfill")

# 窗口分类器：纯读取，返回拒绝原因或 None，不改状态
WindowClassifier = Callable[[dict], RejectReason | None]
# sink：规范化记录 -> 拒绝原因或 None（由管道装配为聚合+WAL）
BackfillSink = Callable[[dict], RejectReason | None]

BACKFILL_STATE_VERSION = 1


@dataclass
class BackfillConfig:
    """补报额度配置。

    quota_total 为本份本地状态允许"补报接受"的总条数；-1 表示不限。
    已用额度随快照持久化，重启后继续累计，绝不因重启而清零放行。
    """

    quota_total: int = -1


@dataclass
class _Prepared:
    """通过校验、等待在锁内按规范序裁决的一条候选。"""

    index: int
    point: MetricPoint | LogEvent
    series: str
    record: dict


class BackfillManager:
    """确定性的批量补报入口。

    结论确定性来自三点：
    1. 全部候选先按 (事件时间, 幂等键, 序列, 输入下标) 规范化排序再裁决，
       与调用方线程如何切分/调度无关；
    2. 整批在 IngestionManager 的同一把写锁内裁决并提交，
       与在线并发上报互相串行化；
    3. 窗口是否已关是纯事实判断（封口标记 + 水位），不依赖墙钟、
       不依赖令牌桶瞬时余量；补报额度是持久化的确定性计数器。
    """

    def __init__(
        self,
        ingestion: IngestionManager,
        config: BackfillConfig | None = None,
        *,
        sink: BackfillSink | None = None,
        window_classifier: WindowClassifier | None = None,
        window_size_ms: int | None = None,
        lock: threading.RLock | None = None,
    ) -> None:
        self.config = config or BackfillConfig()
        self._ingestion = ingestion
        self._sink: BackfillSink = sink or (lambda _record: None)
        self._classify_window: WindowClassifier = (
            window_classifier or (lambda _record: None)
        )
        self._window_size_ms = window_size_ms
        self._quota_used = 0
        # 与管道/采集共用同一把可重入锁：整批裁决与在线上报、快照严格互斥，
        # 且只有一把锁，不存在相反加锁序
        self._lock = lock if lock is not None else threading.RLock()

    def set_sink(self, sink: BackfillSink) -> None:
        self._sink = sink

    def set_window_classifier(self, classifier: WindowClassifier) -> None:
        self._classify_window = classifier

    # ------------------------------------------------------------------
    # 快照 / 恢复
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "version": BACKFILL_STATE_VERSION,
                "quota_total": self.config.quota_total,
                "quota_used": self._quota_used,
            }

    def restore(self, state: dict | None, *, wal_records: list[dict]) -> None:
        """从快照恢复已用额度，再叠加快照之后 WAL 中来源为补报的已接受记录。

        因此"重启一次就清零额度"不会发生：快照口径 + 快照后补报条数，
        与重启前的实际用量逐一对齐。
        """
        with self._lock:
            if state is not None:
                self.config = BackfillConfig(
                    quota_total=state.get("quota_total", self.config.quota_total)
                )
                self._quota_used = int(state.get("quota_used", 0))
            else:
                self._quota_used = 0
            # 快照截断之后、重启之前落盘的补报记录仍要计入已用额度
            self._quota_used += sum(
                1 for r in wal_records if r.get("source") == "backfill"
            )

    @property
    def quota_used(self) -> int:
        with self._lock:
            return self._quota_used

    @property
    def quota_total(self) -> int:
        return self.config.quota_total

    # ------------------------------------------------------------------
    # 批量补报
    # ------------------------------------------------------------------
    def backfill(self, points: list[MetricPoint | LogEvent]) -> BackfillReport:
        # 1) 逐条校验（BAD_INPUT 在锁外纯函数判定，互不影响、不拖垮整批）
        prepared: list[_Prepared] = []
        results: dict[int, IngestResult] = {}
        for i, point in enumerate(points):
            bad = IngestionManager._validate(point)
            if bad is not None:
                results[i] = IngestResult(
                    IngestStatus.REJECTED,
                    getattr(point, "idempotency_key", ""),
                    RejectReason.BAD_INPUT,
                    bad,
                )
                continue
            series = _series_key(
                point.kind.value,
                point.name if isinstance(point, MetricPoint) else point.level.value,
                point.labels,
            )
            prepared.append(
                _Prepared(i, point, series, self._to_record(point, series))
            )

        # 2) 规范化排序：结论只取决于输入集合，不取决于到达/调度顺序
        prepared.sort(
            key=lambda p: (
                p.record["timestamp_ms"],
                p.record["idempotency_key"],
                p.series,
                p.index,
            )
        )

        # 3) 整批在写锁内裁决（与在线上报互斥），逐条按固定优先级给唯一结论
        with self._ingestion.exclusive() as gate, self._lock:
            for prep in prepared:
                rec = prep.record
                key = rec["idempotency_key"]

                # (a) 幂等去重：此前已接受（含本批更早的条目、含重启恢复）
                if gate.has_key(key):
                    results[prep.index] = self._reject(
                        prep, RejectReason.DUPLICATE,
                        f"幂等键 {key} 已接受过，补报重复提交不再计数",
                    )
                    continue

                # (b) 窗口已关：事实性结论，优先于基数/额度，绝不与限流混用
                closed = self._classify_window(rec)
                if closed is not None:
                    results[prep.index] = self._reject(
                        prep, closed,
                        self._window_detail(closed, rec),
                    )
                    continue

                # (c) 基数上限
                is_new_series = not gate.has_series(prep.series)
                if is_new_series and gate.series_count >= gate.max_cardinality:
                    results[prep.index] = self._reject(
                        prep, RejectReason.CARDINALITY_LIMIT,
                        f"序列基数已达上限 {gate.max_cardinality}，"
                        f"拒绝新序列 {prep.series}",
                    )
                    continue

                # (d) 补报额度：持久化的确定性配额（与墙钟/令牌桶无关）
                if self.config.quota_total >= 0 and \
                        self._quota_used >= self.config.quota_total:
                    results[prep.index] = self._reject(
                        prep, RejectReason.RATE_LIMIT,
                        f"补报额度已用尽：{self._quota_used}/"
                        f"{self.config.quota_total}（窗口仍在接收，"
                        "区别于窗口已关）",
                    )
                    continue

                # (e) 下游：聚合并入 + WAL 落盘；成功后才提交键/基数/额度
                rejected = self._sink(rec)
                if rejected is not None:
                    # 理论上窗口已在 (b) 预判；兜底保留下游事实性结论
                    results[prep.index] = self._reject(
                        prep, rejected, f"下游拒绝写入：{rejected.value}"
                    )
                    continue

                gate.commit(key, prep.series)
                self._quota_used += 1
                results[prep.index] = IngestResult(
                    IngestStatus.ACCEPTED, key, None,
                    f"补报已入库：series={prep.series}",
                    rec.get("window_start_ms"),
                )

        return self._build_report(len(points), results)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------
    def _reject(self, prep: _Prepared, reason: RejectReason,
                detail: str) -> IngestResult:
        ws = prep.record.get("window_start_ms")
        return IngestResult(
            IngestStatus.REJECTED, prep.record["idempotency_key"],
            reason, detail, ws if isinstance(ws, int) else None,
        )

    def _window_detail(self, reason: RejectReason, rec: dict) -> str:
        ws = rec.get("window_start_ms")
        if isinstance(ws, int) and self._window_size_ms:
            rng = f"窗口=[{ws},{ws + self._window_size_ms})"
        else:
            rng = "窗口未知"
        return f"{rng} 已封口/越过水位迟到宽限，事实性拒绝（{reason.value}）"

    def _build_report(
        self, total: int, results: dict[int, IngestResult]
    ) -> BackfillReport:
        ordered = [
            BackfillItemResult(i, results[i]) for i in range(total)
        ]
        accepted = sum(1 for r in ordered if r.result.accepted)
        reason_counts: dict[str, int] = {}
        for item in ordered:
            if item.result.reason is not None:
                rv = item.result.reason.value
                reason_counts[rv] = reason_counts.get(rv, 0) + 1

        # 可观测日志：窗口区间 / 幂等键 / 每条判定依据
        logger.info(
            "批量补报汇总 总数=%d 接受=%d 拒绝=%d 原因分布=%s 额度=%s/%s",
            total, accepted, total - accepted, reason_counts,
            self._quota_used,
            self.config.quota_total if self.config.quota_total >= 0 else "∞",
        )
        for item in ordered:
            r = item.result
            ws = r.window_start_ms
            rng = (
                f"[{ws},{ws + self._window_size_ms})"
                if isinstance(ws, int) and self._window_size_ms else "-"
            )
            verdict = r.status.value
            logger.info(
                "补报逐条 下标=%d 幂等键=%s 窗口=%s 结论=%s 原因=%s 依据=%s",
                item.index, r.idempotency_key, rng, verdict,
                r.reason.value if r.reason else "-", r.detail,
            )

        return BackfillReport(
            total=total,
            accepted=accepted,
            rejected=total - accepted,
            reason_counts=reason_counts,
            results=ordered,
        )

    @staticmethod
    def _to_record(point: MetricPoint | LogEvent, series: str) -> dict:
        if isinstance(point, MetricPoint):
            return {
                "kind": "metric",
                "source": "backfill",
                "series": series,
                "name": point.name,
                "labels": point.labels,
                "value": point.value,
                "timestamp_ms": point.timestamp_ms,
                "window_start_ms": "__BY_AGGREGATOR__",
                "idempotency_key": point.idempotency_key,
            }
        return {
            "kind": "log",
            "source": "backfill",
            "series": series,
            "name": point.level.value,
            "labels": point.labels,
            "value": 1.0 if point.level is LogLevel.ERROR else 0.0,
            "message": point.message,
            "timestamp_ms": point.timestamp_ms,
            "window_start_ms": "__BY_AGGREGATOR__",
            "idempotency_key": point.idempotency_key,
        }
