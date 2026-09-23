"""本地持久化存储（JSON 文件 + fsync），进程重启后恢复状态。

写入采用「临时文件 + fsync + 原子 rename」，保证状态文件要么是旧版本、
要么是新版本，不会出现半截 JSON。
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any


class LocalStorage:
    def __init__(self, path: str) -> None:
        self.path = path

    def save(self, state: dict[str, Any]) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
            # fsync 目录，确保 rename 落盘
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def load(self) -> dict[str, Any]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return {}
        if not isinstance(data, dict):
            raise ValueError(f"状态文件格式非法: {self.path}")
        return data
