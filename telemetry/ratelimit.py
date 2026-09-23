"""写入速率限制（按秒的固定窗口计数器）。

时间由调用方注入（事件摄入时间），同一秒内超过 max_events_per_sec 的写入
判为 rate_limited。被限流的事件不登记、不入库。限流计数器不做持久化：
进程重启即重新计数（只限制瞬时摄入压力，不参与聚合正确性）。
"""
from __future__ import annotations

from telemetry.models import RejectReason


class RateLimiter:
    def __init__(self, max_events_per_sec: int) -> None:
        if max_events_per_sec <= 0:
            raise ValueError("max_events_per_sec 必须为正整数")
        self.max_events_per_sec = max_events_per_sec
        self._bucket_sec = -1
        self._count = 0
        self._rejected = 0

    def allow(self, ts_ms: int) -> bool:
        sec = ts_ms // 1000
        if sec != self._bucket_sec:
            self._bucket_sec = sec
            self._count = 0
        if self._count >= self.max_events_per_sec:
            self._rejected += 1
            return False
        self._count += 1
        return True

    @property
    def rejected_count(self) -> int:
        return self._rejected

    def current_usage(self, ts_ms: int) -> tuple[int, int]:
        """返回 (当前秒已接受数, 上限)，便于可解释的拒绝信息。"""
        sec = ts_ms // 1000
        return (self._count if sec == self._bucket_sec else 0), self.max_events_per_sec

    def check(self, ts_ms: int) -> str | None:
        """与 Deduplicator 风格一致的接口。"""
        if self.allow(ts_ms):
            return None
        return RejectReason.RATE_LIMITED.value
