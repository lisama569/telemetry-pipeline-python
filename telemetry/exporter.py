"""导出闭环：把聚合与告警事件以 JSON Lines 追加写入本地文件。

每条记录占一行：{"type": "aggregate"|"alert", "payload": {...}}。
追加后 fsync，保证进程崩溃时已导出的数据不丢；导出文件只追加、
不修改，历史判定永久留痕。
"""
from __future__ import annotations

import json
import os

from telemetry.models import AlertEvent, WindowAggregate


class LocalExporter:
    def __init__(self, path: str) -> None:
        self.path = path
        if path:
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)

    def _append(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())

    def export_aggregate(self, agg: WindowAggregate) -> None:
        self._append({"type": "aggregate", "payload": agg.to_dict()})

    def export_alert(self, event: AlertEvent) -> None:
        self._append({"type": "alert", "payload": event.to_dict()})

    def read_all(self) -> list[dict]:
        if not os.path.exists(self.path):
            return []
        records: list[dict] = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def read_by_type(self, record_type: str) -> list[dict]:
        return [r["payload"] for r in self.read_all() if r.get("type") == record_type]
