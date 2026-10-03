"""管道装配：采集 -> WAL 落盘 -> 窗口聚合 -> 封口 -> 告警 -> 导出。

闭环与恢复：
- 写入：仅当聚合窗口接受时，事件才追加到 WAL，再提交幂等状态；
  被去重/限流/关闭窗口拒绝的事件不落盘、不污染聚合。
- 恢复：加载 ``snapshot.json``（聚合+告警+已接受幂等键），
  随后以 trusted 通道重放快照之后的 WAL，结论可重放且一致。
- 封口：``flush`` 推进水位、封口到期窗口，并对每个封口窗口驱动一次
  全部规则判定；单条规则失败仅记录隔离原因，不阻塞其它链路。
- 快照：聚合状态 + 告警状态 + 幂等键集合原子落盘，然后清空 WAL。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .aggregation import WindowAggregator
from .alerting import AlertManager
from .backfill import BackfillConfig, BackfillManager
from .ingest import IngestionConfig, IngestionManager
from .models import (
    Aggregate,
    AlertEvaluation,
    AlertState,
    BackfillReport,
    IngestResult,
    LogEvent,
    MetricPoint,
    RejectReason,
)
from .storage import LocalStore

PIPELINE_STATE_VERSION = 2


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
        backfill_config: BackfillConfig | None = None,
    ) -> None:
        self.store = LocalStore(store_dir)
        # 单一共享可重入锁：采集、补报、聚合封口、快照共用同一把锁，
        # 统一加锁序，杜绝"在线/补报/快照"之间的多锁环。
        self._lock = threading.RLock()
        self.aggregator = WindowAggregator(window_size_ms, allowed_lateness_ms)
        self.alerts = AlertManager()
        self.ingestion = IngestionManager(ingestion_config, lock=self._lock)
        self.backfills = BackfillManager(
            self.ingestion,
            backfill_config,
            sink=self._backfill_sink,
            window_classifier=self._classify_backfill_window,
            window_size_ms=window_size_ms,
            lock=self._lock,
        )
        self._last_eval_window: int | None = None
        # sink：聚合校验通过 -> 落 WAL -> 返回 None
        self.ingestion.set_sink(self._sink)
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

    def _classify_backfill_window(self, record: dict) -> RejectReason | None:
        """补报专用：纯读预判窗口是否已关（事实性结论，不推进水位）。"""
        with self._lock:
            return self.aggregator.classify_apply(record)

    def _backfill_sink(self, record: dict) -> RejectReason | None:
        """补报专用 sink：并入聚合并落 WAL，但**不推进水位**。

        这样补报事件时间跨度再大、切块再乱，也不会因某条高时间戳数据先被
        处理而把其它低时间戳数据误判成窗口已关，结论只取决于 flush 决定的
        水位事实与封口状态，可跨线程/跨机器复现。
        """
        with self._lock:
            rejected = self.aggregator.apply(record, advance_watermark=False)
            if rejected is not None:
                return rejected
            self.store.append_event(record)
            return None

    def ingest(self, point: MetricPoint | LogEvent) -> IngestResult:
        return self.ingestion.ingest(point)

    def backfill(self, points: list[MetricPoint | LogEvent]) -> BackfillReport:
        """一次性补报一批离线历史数据。

        每条独立给结论、整批不因个别坏数据失败；判定与聚合均确定可复现。
        """
        return self.backfills.backfill(points)

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
        """推进水位、封口窗口，并对每个新封口窗口评估全部告警规则。"""
        with self._lock:
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
                "backfill": self.backfills.snapshot(),
            }
            self.store.write_snapshot(state)
            self.store.truncate_wal()

    def _recover(self) -> None:
        """进程重启恢复：快照 + WAL 重放。"""
        with self._lock:
            snap = self.store.read_snapshot()
            replay = list(self.store.read_events())
            if snap is not None:
                self.aggregator.restore(snap["aggregation"], replay=replay)
                self.alerts.restore(snap["alerts"])
                # 快照后的 WAL 已接受事件也要补进去重/基数集合
                extra_keys = {r["idempotency_key"] for r in replay}
                extra_series = {r["series"] for r in replay}
                self.ingestion.restore(
                    set(snap.get("seen_keys", ())) | extra_keys,
                    set(snap.get("series", ())) | extra_series,
                )
                # 补报额度：快照口径 + 快照后 WAL 中来源为补报的条数，重启不清零
                self.backfills.restore(snap.get("backfill"), wal_records=replay)
            else:
                keys: set[str] = set()
                series: set[str] = set()
                for record in replay:
                    # 无快照：全部历史都是首次重放
                    self.aggregator.apply(record, trusted=True)
                    keys.add(record["idempotency_key"])
                    series.add(record["series"])
                self.ingestion.restore(keys, series)
                self.backfills.restore(None, wal_records=replay)

    def rebuild_dedup_from_wal(self) -> None:
        """从快照 + WAL 重建幂等键、基数集合与补报额度（测试/运维辅助）。"""
        with self._lock:
            records = list(self.store.read_events())
            keys = {r["idempotency_key"] for r in records}
            series = {r["series"] for r in records}
            snap = self.store.read_snapshot()
            if snap is not None:
                keys |= set(snap.get("seen_keys", ()))
                series |= set(snap.get("series", ()))
            self.ingestion.restore(keys, series)
            self.backfills.restore(
                snap.get("backfill") if snap is not None else None,
                wal_records=records,
            )
