"""管道装配：采集 -> WAL 落盘 -> 窗口聚合 -> 封口 -> 告警 -> 导出。

闭环与恢复：
- 写入：仅当聚合窗口接受时，事件才追加到 WAL，再提交幂等状态；
  被去重/限流/关闭窗口拒绝的事件不落盘、不污染聚合。
- 封口：``flush`` 先把**水位标记**追加到 WAL（先落盘、后生效），
  再推进封口地平线、封口到期窗口，并对每个封口窗口驱动一次全部规则判定；
  单条规则失败仅记录隔离原因，不阻塞其它链路。
- 恢复：加载 ``snapshot.json``（聚合+告警+已接受幂等键+限流余量），
  随后重放快照之后的 WAL（事件走 trusted 通道，水位标记恢复封口地平线），
  结论可重放且与重启前一致——"窗口已关"的事实不会被重启抹掉。
- 快照：聚合状态（含封口地平线）+ 告警状态 + 幂等键集合 + 限流余量
  原子落盘，然后清空 WAL。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .aggregation import WindowAggregator
from .alerting import AlertManager
from .backfill import BackfillReport, run_backfill
from .ingest import IngestionConfig, IngestionManager
from .models import (
    Aggregate,
    AlertEvaluation,
    AlertState,
    IngestResult,
    LogEvent,
    MetricPoint,
    RejectReason,
)
from .storage import LocalStore, is_watermark_marker, make_watermark_marker

PIPELINE_STATE_VERSION = 1


@dataclass
class FlushReport:
    watermark_ms: int
    sealed: list[Aggregate] = field(default_factory=list)
    evaluations: list[AlertEvaluation] = field(default_factory=list)


class TelemetryPipeline:
    """面向应用的门面（facade）。"""

    def __init__(
        self,
        store_dir: str,
        window_size_ms: int = 1000,
        allowed_lateness_ms: int = 0,
        ingestion_config: IngestionConfig | None = None,
    ) -> None:
        self.store = LocalStore(store_dir)
        self.aggregator = WindowAggregator(window_size_ms, allowed_lateness_ms)
        self.alerts = AlertManager()
        self.ingestion = IngestionManager(ingestion_config)
        self._lock = threading.RLock()
        self._last_eval_window: int | None = None
        # sink：聚合校验通过 -> 落 WAL -> 返回 None
        self.ingestion.set_sink(self._sink)
        # precheck：窗口已关的事实性预检，优先于限流等瞬时判定
        self.ingestion.set_precheck(self.aggregator.check_window_open)
        self._recover()

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def _sink(self, record: dict) -> RejectReason | None:
        with self._lock:
            rejected = self.aggregator.apply(record)
            if rejected is not None:
                return rejected
            # 聚合接受后才落盘，保证 WAL 与聚合状态一致
            self.store.append_event(record)
            return None

    def ingest(self, point: MetricPoint | LogEvent) -> IngestResult:
        return self.ingestion.ingest(point)

    def backfill(
        self,
        events: list[MetricPoint | LogEvent | IngestResult],
        *,
        batch_id: str | None = None,
    ) -> BackfillReport:
        """批量补报入口：整批投递历史事件，逐条给出结论并汇总。

        走补报专用判定通道（不占用在线限流配额、判定不含墙钟），
        保证同一份输入无论怎么切分/并发/在什么机器上跑，结论都一致。
        ``events`` 中允许混入预先判定好的 ``IngestResult``
        （例如 HTTP 层解析失败的条目），原样计入报告。
        """
        import uuid

        return run_backfill(
            self.ingestion.ingest_backfill,
            list(events),
            batch_id=batch_id or uuid.uuid4().hex,
            window_size_ms=self.aggregator.window_size_ms,
        )

    def export(self, path: str, *, include_open_windows: bool = True,
               include_events: bool = False) -> str:
        """导出聚合/告警报告到本地 JSON 文件。"""
        from .exporter import Exporter

        return Exporter(self.aggregator, self.alerts, self.store).export(
            path,
            include_open_windows=include_open_windows,
            include_events=include_events,
        )

    # ------------------------------------------------------------------
    # 封口 + 告警
    # ------------------------------------------------------------------
    def flush(self, watermark_ms: int | None = None) -> FlushReport:
        """推进水位、封口窗口，并对每个新封口窗口评估全部告警规则。

        先把水位标记写入 WAL 再改动内存状态（先落盘、后生效），
        保证崩溃/重启后"窗口已关"的事实性结论可以重放出来。
        """
        with self._lock:
            target = self.aggregator.plan_seal_target(watermark_ms)
            if target is not None:
                self.store.append_event(make_watermark_marker(target))
            sealed = self.aggregator.seal_due_windows(watermark_ms)
            wm = self.aggregator.watermark_ms
            evaluations: list[AlertEvaluation] = []
            # 按规则关注的 series 分组封口窗口：规则只对自己 series 的窗口判定，
            # 其它 series 的窗口不产生该规则的判定（避免错配）。
            windows_by_series: dict[str, list[Aggregate]] = {}
            for agg in sealed:
                windows_by_series.setdefault(agg.series, []).append(agg)
            for rid in self.alerts.rule_ids:
                try:
                    rule = self.alerts.get_rule(rid)
                    for agg in windows_by_series.get(rule.series, []):
                        evaluations.append(
                            self.alerts.evaluate_window(
                                rid, agg.window_start_ms, agg.average
                            )
                        )
                except Exception as exc:  # 单条链路失败不阻塞其它规则
                    evaluations.append(
                        AlertEvaluation(
                            rid, -1, None, AlertState.PENDING,
                            False, False, f"规则链路失败已隔离：{exc!r}",
                        )
                    )
            self._last_eval_window = max(
                (a.window_start_ms for a in sealed), default=self._last_eval_window
            )
            return FlushReport(watermark_ms=wm, sealed=sealed, evaluations=evaluations)

    # ------------------------------------------------------------------
    # 快照 / 恢复
    # ------------------------------------------------------------------
    def snapshot(self) -> None:
        with self._lock:
            state = {
                "version": PIPELINE_STATE_VERSION,
                "aggregation": self.aggregator.snapshot(),
                "alerts": self.alerts.snapshot(),
                "seen_keys": sorted(self.ingestion.seen_keys),
                "series": sorted(self.ingestion.series),
                # 限流配额随快照延续：重启不会把已用掉的量清零
                "rate_limiter": self.ingestion.rate_limiter_snapshot(),
            }
            self.store.write_snapshot(state)
            self.store.truncate_wal()

    def _recover(self) -> None:
        """进程重启恢复：快照 + WAL 重放（事件 + 水位标记）。"""
        with self._lock:
            snap = self.store.read_snapshot()
            replay = list(self.store.read_events())
            if snap is not None:
                self.aggregator.restore(snap["aggregation"], replay=replay)
                self.alerts.restore(snap["alerts"])
                # 快照后的 WAL 已接受事件也要补进去重/基数集合
                events = [r for r in replay if not is_watermark_marker(r)]
                extra_keys = {r["idempotency_key"] for r in events}
                extra_series = {r["series"] for r in events}
                self.ingestion.restore(
                    set(snap.get("seen_keys", ())) | extra_keys,
                    set(snap.get("series", ())) | extra_series,
                )
                # 恢复限流配额余量：重启前已用掉的量继续有效
                limiter = snap.get("rate_limiter")
                if limiter is not None:
                    self.ingestion.restore_rate_limiter(limiter)
            else:
                keys: set[str] = set()
                series: set[str] = set()
                for record in replay:
                    if is_watermark_marker(record):
                        # 水位标记：恢复封口地平线，
                        # "窗口已关"的事实性结论跨重启稳定
                        self.aggregator.apply_watermark_marker(
                            record["watermark_ms"]
                        )
                        continue
                    # 无快照：全部历史都是首次重放
                    self.aggregator.apply(record, trusted=True)
                    keys.add(record["idempotency_key"])
                    series.add(record["series"])
                # 地平线恢复后补打 sealed 标志，与快照路径对齐
                self.aggregator.reseal_due()
                self.ingestion.restore(keys, series)

    def rebuild_dedup_from_wal(self) -> None:
        """从快照 + WAL 重建幂等键与基数集合（测试/运维辅助）。"""
        with self._lock:
            events = [
                r for r in self.store.read_events()
                if not is_watermark_marker(r)
            ]
            keys = {r["idempotency_key"] for r in events}
            series = {r["series"] for r in events}
            snap = self.store.read_snapshot()
            if snap is not None:
                keys |= set(snap.get("seen_keys", ()))
                series |= set(snap.get("series", ()))
            self.ingestion.restore(keys, series)
