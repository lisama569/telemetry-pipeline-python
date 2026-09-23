"""导出层：把当前聚合、告警状态与事件导出为本地 JSON 文件。

导出物只包含已接受的事实与已封口/进行中的聚合，不接触任何被拒绝事件；
按 (window_start_ms, series) 确定排序，使同一份状态的导出字节级可复现。
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict

from .aggregation import WindowAggregator
from .alerting import AlertManager
from .storage import LocalStore


def _atomic_write_json(path: str, payload: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2, sort_keys=True)
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


class Exporter:
    """把管道状态导出到本地目录，供离线复核与外部系统抓取。"""

    def __init__(self, aggregator: WindowAggregator, alerts: AlertManager,
                 store: LocalStore) -> None:
        self.aggregator = aggregator
        self.alerts = alerts
        self.store = store

    def build_report(self, *, include_open_windows: bool = True,
                     include_events: bool = False) -> dict:
        aggregates = [
            a.to_dict()
            for a in self.aggregator.aggregates(include_open=include_open_windows)
        ]
        alert_snapshot = self.alerts.snapshot()
        report = {
            "watermark_ms": self.aggregator.watermark_ms,
            "aggregates": aggregates,
            "alert_rules": alert_snapshot["rules"],
            "alert_status": alert_snapshot["status"],
            "alert_history": alert_snapshot["history"],
        }
        if include_events:
            report["events"] = list(self.store.read_events())
        return report

    def export(self, path: str, *, include_open_windows: bool = True,
               include_events: bool = False) -> str:
        report = self.build_report(
            include_open_windows=include_open_windows,
            include_events=include_events,
        )
        _atomic_write_json(path, report)
        return path
