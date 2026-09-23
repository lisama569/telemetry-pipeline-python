"""本地持久化层。

设计目标（崩溃可恢复、结论可重放）：
- ``events.log``  仅追加 JSONL 日志：只有 *已接受* 的事件才落盘，
  被拒绝的事件不会污染 WAL，因此重放结果与在线聚合一致。
- ``snapshot.json`` 原子快照：保存聚合结果、告警状态与水位游标，
  重启时优先加载快照，再重放快照之后的 WAL 记录即可恢复。
- 线程锁保证同一进程内并发写入的串行化；文件写入采用
  "临时文件 + fsync + os.replace" 的原子替换，避免半截文件。
"""
from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator
from typing import Any

WAL_NAME = "events.log"
SNAPSHOT_NAME = "snapshot.json"


class LocalStore:
    """目录型本地存储。"""

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._wal_path = os.path.join(directory, WAL_NAME)
        self._snapshot_path = os.path.join(directory, SNAPSHOT_NAME)
        self.lock = threading.RLock()

    # ------------------------------------------------------------------
    # 仅追加日志（WAL）
    # ------------------------------------------------------------------
    def append_event(self, record: dict[str, Any]) -> None:
        """把一条已接受事件原子追加到 WAL。"""
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self.lock:
            with open(self._wal_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    def read_events(self) -> Iterator[dict[str, Any]]:
        """顺序重放 WAL。文件不存在时产生空迭代器。"""
        if not os.path.exists(self._wal_path):
            return
        with open(self._wal_path, "r", encoding="utf-8") as fh:
            for line_no, raw in enumerate(fh, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    yield json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"WAL 损坏：{self._wal_path}:{line_no}"
                    ) from exc

    def truncate_wal(self) -> None:
        """快照成功后安全清空 WAL。"""
        with self.lock:
            if os.path.exists(self._wal_path):
                os.remove(self._wal_path)

    # ------------------------------------------------------------------
    # 原子快照
    # ------------------------------------------------------------------
    def write_snapshot(self, state: dict[str, Any]) -> None:
        """用临时文件 + 原子替换写入快照。"""
        tmp_path = self._snapshot_path + ".tmp"
        payload = json.dumps(state, ensure_ascii=False, sort_keys=True)
        with self.lock:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self._snapshot_path)
            self._fsync_directory()

    def read_snapshot(self) -> dict[str, Any] | None:
        if not os.path.exists(self._snapshot_path):
            return None
        with open(self._snapshot_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _fsync_directory(self) -> None:
        try:
            dir_fd = os.open(self.directory, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)
