"""本地持久化层。

设计目标（崩溃可恢复、结论可重放）：
- ``events.log``  仅追加 JSONL 日志：只有 *已接受* 的事件才落盘，
  被拒绝的事件不会污染 WAL，因此重放结果与在线聚合一致。
  除事件记录外，WAL 还允许追加 **水位标记**（``{"type": "watermark", ...}``）：
  显式 flush 推进的水位随 WAL 持久化，仅 WAL 恢复时也能精确还原水位，
  不会出现"重启后水位回退、已翻篇窗口又被打开"的情况。
- ``snapshot.json`` 原子快照：保存聚合结果、告警状态与水位游标，
  重启时优先加载快照，再重放快照之后的 WAL 记录即可恢复。
- ``limiter.json`` 限流状态：每次成功消费令牌后原子落盘，
  进程停掉再起来（即便还没来得及做快照）已用掉的额度也不会被清零。
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
LIMITER_NAME = "limiter.json"

# WAL 中水位标记记录的类型字段值（事件记录没有 "type" 字段）
RECORD_TYPE_WATERMARK = "watermark"


class LocalStore:
    """目录型本地存储。"""

    def __init__(self, directory: str) -> None:
        self.directory = directory
        os.makedirs(directory, exist_ok=True)
        self._wal_path = os.path.join(directory, WAL_NAME)
        self._snapshot_path = os.path.join(directory, SNAPSHOT_NAME)
        self._limiter_path = os.path.join(directory, LIMITER_NAME)
        self.lock = threading.RLock()

    # ------------------------------------------------------------------
    # 仅追加日志（WAL）
    # ------------------------------------------------------------------
    def append_event(self, record: dict[str, Any]) -> None:
        """把一条已接受事件原子追加到 WAL。"""
        self._append_line(record)

    def append_watermark(self, watermark_ms: int) -> None:
        """把一次显式水位推进作为标记记录追加到 WAL。

        水位标记与事件记录共享同一 WAL 顺序，重放时按序应用即可
        精确还原重启前的水位（包括"flush 推进超过最大事件时间"的部分）。
        """
        self._append_line({"type": RECORD_TYPE_WATERMARK,
                           "watermark_ms": int(watermark_ms)})

    def _append_line(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self.lock:
            with open(self._wal_path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())

    def read_records(self) -> Iterator[dict[str, Any]]:
        """顺序重放 WAL 中的全部记录（事件 + 水位标记）。

        文件不存在时产生空迭代器。调用方按 ``type`` 字段区分记录种类。
        """
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

    def read_events(self) -> Iterator[dict[str, Any]]:
        """顺序重放 WAL 中的事件记录（滤掉水位标记）。"""
        return (
            r for r in self.read_records()
            if r.get("type") != RECORD_TYPE_WATERMARK
        )

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

    # ------------------------------------------------------------------
    # 限流状态（每次成功消费令牌后落盘，重启不清零已用掉的额度）
    # ------------------------------------------------------------------
    def write_limiter_state(self, state: dict[str, Any]) -> None:
        """原子写入限流状态（临时文件 + fsync + 原子替换）。"""
        tmp_path = self._limiter_path + ".tmp"
        payload = json.dumps(state, ensure_ascii=False, sort_keys=True)
        with self.lock:
            with open(tmp_path, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self._limiter_path)

    def read_limiter_state(self) -> dict[str, Any] | None:
        if not os.path.exists(self._limiter_path):
            return None
        with open(self._limiter_path, "r", encoding="utf-8") as fh:
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
