"""时间窗口聚合层。

语义约定：
- 固定大小窗口，左闭右开 ``[start, start+size)``，事件时间戳向下取整对齐，
  因此乱序到达的同一批事件必定落到同一窗口；
- 聚合只做 count/sum/min/max/avg（全部与输入顺序无关），
  窗口可反复重放，结论稳定一致；
- 水位线 ``watermark_ms`` = 已观察到的最大事件时间，单调不允许回退，
  显式回退游标 -> ``CURSOR_BACKWARD``；
- 当 ``window_end + allowed_lateness <= watermark`` 时窗口封口（sealed）：
  封口结果冻结为不可变快照，再写入 -> ``WINDOW_CLOSED``；
- 封口前允许迟到数据（out-of-order）就地并入，下次封口时结论仍是确定的。
"""
from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import asdict

from .clock import floor_window
from .models import Aggregate, RejectReason

AGG_STATE_VERSION = 1


class WindowAggregator:
    """线程安全的按序列×窗口聚合器。"""

    def __init__(self, window_size_ms: int, allowed_lateness_ms: int = 0) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms 必须为正数")
        if allowed_lateness_ms < 0:
            raise ValueError("allowed_lateness_ms 不能为负数")
        self.window_size_ms = window_size_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self._windows: dict[tuple[str, int], Aggregate] = {}
        self._watermark_ms: int = -1
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 水位游标
    # ------------------------------------------------------------------
    @property
    def watermark_ms(self) -> int:
        with self._lock:
            return self._watermark_ms

    def advance_watermark(self, timestamp_ms: int) -> RejectReason | None:
        """显式推进水位；试图回退游标必须被明确拒绝。"""
        with self._lock:
            if timestamp_ms < self._watermark_ms:
                return RejectReason.CURSOR_BACKWARD
            if timestamp_ms > self._watermark_ms:
                self._watermark_ms = timestamp_ms
            return None

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def apply(self, record: dict, *, trusted: bool = False,
              advance_watermark: bool = True) -> RejectReason | None:
        """并入一条事件记录。

        ``trusted=True`` 用于重启重放：记录是过去已接受的事实，
        跳过水位/封口检查，但仍保持聚合运算的确定性。

        ``advance_watermark=False`` 用于批量补报：补报是离线历史重放，
        事件时间跨度可能很大且到达顺序由调用方/调度决定；若让每条补报
        推进水位，先到的高时间戳切块会把后处理切块的低时间戳事件误判成
        WINDOW_CLOSED，结论就依赖线程调度。补报因此**只并入聚合、不推进
        水位**；水位由在线数据与显式 ``flush`` 决定，保证补报可重放。
        """
        with self._lock:
            ts = record["timestamp_ms"]
            ws = floor_window(ts, self.window_size_ms)
            record["window_start_ms"] = ws
            series = record["series"]
            key = (series, ws)

            if not trusted:
                agg = self._windows.get(key)
                if agg is not None and agg.sealed:
                    return RejectReason.WINDOW_CLOSED
                if self._watermark_ms >= 0 and ws + self.window_size_ms + self.allowed_lateness_ms <= self._watermark_ms:
                    return RejectReason.WINDOW_CLOSED

            self._merge(key, series, ws, record)

            # 在线写入随事件推进水位（只增不减）；补报/可重放通道不推进
            if advance_watermark and ts > self._watermark_ms:
                self._watermark_ms = ts
            return None

    def classify_apply(self, record: dict) -> RejectReason | None:
        """纯读取的写入预判：只返回该记录 *当前* 是否会被窗口层拒绝。

        与 :meth:`apply`（``trusted=False``）的封口/水位预判保持同一套事实
        判定，但**不推进水位、不并入聚合**，供批量补报在整批裁决前把
        "窗口已关"稳定识别为事实性结论——无论写入压力或限流如何，说法都不会
        变成别的。调用方持锁时，与随后真正的 ``apply`` 结论一致。
        """
        with self._lock:
            ts = record["timestamp_ms"]
            ws = floor_window(ts, self.window_size_ms)
            record["window_start_ms"] = ws
            key = (record["series"], ws)
            agg = self._windows.get(key)
            if agg is not None and agg.sealed:
                return RejectReason.WINDOW_CLOSED
            if self._watermark_ms >= 0 and ws + self.window_size_ms \
                    + self.allowed_lateness_ms <= self._watermark_ms:
                return RejectReason.WINDOW_CLOSED
            return None

    def _merge(self, key: tuple[str, int], series: str, ws: int, record: dict) -> None:
        agg = self._windows.get(key)
        value = float(record["value"])
        ts = record["timestamp_ms"]
        if agg is None:
            agg = Aggregate(
                series=series,
                window_start_ms=ws,
                window_end_ms=ws + self.window_size_ms,
                count=1,
                total=value,
                minimum=value,
                maximum=value,
                last_timestamp_ms=ts,
                last_value=value,
            )
            self._windows[key] = agg
            return
        agg.count += 1
        agg.total += value
        agg.minimum = value if agg.minimum is None else min(agg.minimum, value)
        agg.maximum = value if agg.maximum is None else max(agg.maximum, value)
        # last_* 仅用于可解释展示，不参与统计结论
        if agg.last_timestamp_ms is None or ts >= agg.last_timestamp_ms:
            agg.last_timestamp_ms = ts
            agg.last_value = value

    # ------------------------------------------------------------------
    # 封口（窗口刷新）
    # ------------------------------------------------------------------
    def seal_due_windows(self, watermark_ms: int | None = None) -> list[Aggregate]:
        """把已超过迟到宽限的窗口封口，返回本次新封口的聚合快照。

        封口是幂等操作：重复调用不会重复产出或改变已封口结果。
        """
        with self._lock:
            if watermark_ms is not None:
                if watermark_ms < self._watermark_ms:
                    # 游标回退不改变任何状态
                    return []
                self._watermark_ms = max(self._watermark_ms, watermark_ms)
            cutoff = self._watermark_ms - self.allowed_lateness_ms
            sealed: list[Aggregate] = []
            for (series, ws), agg in self._windows.items():
                if not agg.sealed and ws + self.window_size_ms <= cutoff:
                    agg.sealed = True
                    sealed.append(agg)
            sealed.sort(key=lambda a: (a.window_start_ms, a.series))
            return sealed

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_aggregate(self, series: str, window_start_ms: int) -> Aggregate | None:
        with self._lock:
            return self._windows.get((series, window_start_ms))

    def aggregates(self, *, include_open: bool = True) -> list[Aggregate]:
        with self._lock:
            items = [a for a in self._windows.values() if include_open or a.sealed]
        items.sort(key=lambda a: (a.window_start_ms, a.series))
        return items

    # ------------------------------------------------------------------
    # 快照与恢复
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "version": AGG_STATE_VERSION,
                "window_size_ms": self.window_size_ms,
                "allowed_lateness_ms": self.allowed_lateness_ms,
                "watermark_ms": self._watermark_ms,
                "windows": [asdict(a) for a in self._windows.values()],
            }

    def restore(self, state: dict, replay: Iterable[dict] = ()) -> None:
        """从快照恢复，并重放快照之后的 WAL 事件（trusted 通道）。"""
        with self._lock:
            self._windows.clear()
            self._watermark_ms = state.get("watermark_ms", -1)
            for row in state.get("windows", []):
                a = Aggregate(
                    series=row["series"],
                    window_start_ms=row["window_start_ms"],
                    window_end_ms=row["window_end_ms"],
                    count=row["count"],
                    total=row["total"],
                    minimum=row["minimum"],
                    maximum=row["maximum"],
                    last_timestamp_ms=row.get("last_timestamp_ms"),
                    last_value=row.get("last_value"),
                    sealed=row.get("sealed", False),
                )
                self._windows[(a.series, a.window_start_ms)] = a
        # 重放走 trusted，保证“已接受事实”不因重启后水位变高而被拒
        for record in replay:
            self.apply(record, trusted=True)
