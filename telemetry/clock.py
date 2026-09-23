"""时间与窗口对齐工具。

窗口为固定大小、左闭右开区间 [start, start+size)；
事件时间戳向下取整到窗口边界，保证乱序到达也映射到同一窗口。
"""
from __future__ import annotations

from .models import now_ms


def floor_window(timestamp_ms: int, window_size_ms: int) -> int:
    """返回 timestamp 所属窗口的起点（闭区间起点）。"""
    if window_size_ms <= 0:
        raise ValueError("window_size_ms 必须为正整数")
    return (timestamp_ms // window_size_ms) * window_size_ms


def next_window_start(timestamp_ms: int, window_size_ms: int) -> int:
    """返回 timestamp 之后的下一个窗口起点。"""
    return floor_window(timestamp_ms, window_size_ms) + window_size_ms


class Clock:
    """可替换的时钟，便于测试注入确定性时间。"""

    __slots__ = ("_override",)

    def __init__(self, fixed_ms: int | None = None) -> None:
        self._override = fixed_ms

    def now_ms(self) -> int:
        return self._override if self._override is not None else now_ms()

    def set(self, timestamp_ms: int) -> None:
        self._override = timestamp_ms
