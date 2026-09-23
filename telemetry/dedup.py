"""幂等去重与基数限制。

- 幂等键（idempotency_key）相同的重复上报一律判为 duplicate；
- 每个不同的序列键（series_key = 名称/级别 + 标签组合）占用一个基数槽，
  新序列在槽位已满时判为 cardinality_limit；
- 被拒绝的事件不登记幂等键，也不会进入任何聚合，绝不污染已有结果。
"""
from __future__ import annotations

from telemetry.models import RejectReason


class Deduplicator:
    def __init__(self, max_cardinality: int) -> None:
        if max_cardinality <= 0:
            raise ValueError("max_cardinality 必须为正整数")
        self.max_cardinality = max_cardinality
        self._seen_keys: set[str] = set()
        self._series: set[str] = set()

    def check(self, key: str, series_key: str) -> str | None:
        """返回拒绝原因（RejectReason 的 value），None 表示放行并完成登记。"""
        # 重复优先判定：同一幂等键的任何重报都是 duplicate
        if key in self._seen_keys:
            return RejectReason.DUPLICATE.value
        # 新序列需要占用基数槽
        if series_key not in self._series and len(self._series) >= self.max_cardinality:
            return RejectReason.CARDINALITY_LIMIT.value
        # 放行并登记（只有被接受的事件才占用状态）
        self._seen_keys.add(key)
        self._series.add(series_key)
        return None

    def cardinality(self) -> int:
        return len(self._series)

    def seen_count(self) -> int:
        return len(self._seen_keys)

    def snapshot(self) -> dict:
        return {
            "max_cardinality": self.max_cardinality,
            "seen_keys": sorted(self._seen_keys),
            "series": sorted(self._series),
        }

    def restore(self, state: dict) -> None:
        self.max_cardinality = state.get("max_cardinality", self.max_cardinality)
        self._seen_keys = set(state.get("seen_keys", []))
        self._series = set(state.get("series", []))
