"""时间窗口聚合层。

语义约定：
- 固定大小窗口，左闭右开 ``[start, start+size)``，事件时间戳向下取整对齐，
  因此乱序到达的同一批事件必定落到同一窗口；
- 聚合只做 count/sum/min/max/avg（全部与输入顺序无关），
  窗口可反复重放，结论稳定一致；
- 水位游标 ``watermark_ms`` = 已观察到的最大事件时间（或被显式推进到的值），
  单调不允许回退，显式回退游标 -> ``CURSOR_BACKWARD``；
- **封口地平线** ``closed_horizon_ms`` 与水位游标分离：它只在显式
  ``flush``/水位标记回放时单调推进，**不随单条事件的到达而移动**。
  因此"窗口是否已关"只取决于显式的封口操作，同一批事件无论乱序、
  分批还是并发投递，``WINDOW_CLOSED`` 的判定都完全一致；
- 当 ``window_end + allowed_lateness <= closed_horizon`` 时窗口封口（sealed）：
  封口结果冻结为不可变快照，再写入 -> ``WINDOW_CLOSED``；
- 封口前允许迟到数据（out-of-order）就地并入，下次封口时结论仍是确定的。
"""
from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import asdict

from .clock import floor_window
from .models import Aggregate, RejectReason
from .storage import is_watermark_marker

AGG_STATE_VERSION = 2


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
        # 水位游标：已观察到的最大事件时间 / 显式推进值（单调，信息性）
        self._watermark_ms: int = -1
        # 封口地平线：仅随显式 flush / 水位标记回放单调推进。
        # 与事件到达解耦 —— WINDOW_CLOSED 判定只看它，保证结论与
        # 到达顺序、分批方式、并发调度、机器快慢全部无关。
        self._closed_horizon_ms: int = -1
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 水位游标
    # ------------------------------------------------------------------
    @property
    def watermark_ms(self) -> int:
        with self._lock:
            return self._watermark_ms

    @property
    def closed_horizon_ms(self) -> int:
        """封口地平线：只随显式 flush / 水位标记回放单调推进。"""
        with self._lock:
            return self._closed_horizon_ms

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
    def check_window_open(self, series: str, timestamp_ms: int) -> RejectReason | None:
        """非破坏性预检：该事件时间对应的窗口是否还在接收数据。

        只读、不改动任何状态；判定只依赖**封口地平线**（单调、仅随显式
        flush 推进），与事件到达顺序无关。因此一旦返回 ``WINDOW_CLOSED``，
        随后的正式写入也必然得到同一结论——该事实性判定不随并发/限流压力
        漂移，也不会因为同批其它事件先到达而改口。
        """
        with self._lock:
            ws = floor_window(timestamp_ms, self.window_size_ms)
            agg = self._windows.get((series, ws))
            if agg is not None and agg.sealed:
                return RejectReason.WINDOW_CLOSED
            if (
                self._closed_horizon_ms >= 0
                and ws + self.window_size_ms + self.allowed_lateness_ms
                <= self._closed_horizon_ms
            ):
                return RejectReason.WINDOW_CLOSED
            return None

    def apply(self, record: dict, *, trusted: bool = False) -> RejectReason | None:
        """并入一条事件记录。

        ``trusted=True`` 用于重启重放：记录是过去已接受的事实，
        跳过水位/封口检查，但仍保持聚合运算的确定性。
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
                if self._closed_horizon_ms >= 0 and ws + self.window_size_ms + self.allowed_lateness_ms <= self._closed_horizon_ms:
                    return RejectReason.WINDOW_CLOSED

            self._merge(key, series, ws, record)

            # 水位游标随事件推进（只增不减，信息性；
            # 注意：它不影响 WINDOW_CLOSED 判定，判定只看封口地平线）
            if ts > self._watermark_ms:
                self._watermark_ms = ts
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
    def plan_seal_target(self, watermark_ms: int | None = None) -> int | None:
        """预算一次 flush 会把封口地平线推进到哪儿；不会推进则返回 None。

        供管道在改动内存状态之前先把水位标记写入 WAL（先落盘、后生效），
        保证无快照重启后仍能重放出相同的封口地平线。
        """
        with self._lock:
            if watermark_ms is not None and watermark_ms < self._watermark_ms:
                return None  # 游标回退：本次 flush 是无操作
            target = (
                max(self._watermark_ms, watermark_ms)
                if watermark_ms is not None
                else self._watermark_ms
            )
            return target if target > self._closed_horizon_ms else None

    def seal_due_windows(self, watermark_ms: int | None = None) -> list[Aggregate]:
        """把已超过迟到宽限的窗口封口，返回本次新封口的聚合快照。

        封口是幂等操作：重复调用不会重复产出或改变已封口结果。
        封口地平线只在这里（以及水位标记回放）单调推进，
        不随单条事件到达而移动。
        """
        with self._lock:
            if watermark_ms is not None:
                if watermark_ms < self._watermark_ms:
                    # 游标回退不改变任何状态
                    return []
                self._watermark_ms = watermark_ms
            target = self._watermark_ms
            if target <= self._closed_horizon_ms:
                return []
            self._closed_horizon_ms = target
            return self._seal_due_locked()

    def _seal_due_locked(self) -> list[Aggregate]:
        """按当前封口地平线补打 sealed 标志（调用方须持锁）。"""
        cutoff = self._closed_horizon_ms - self.allowed_lateness_ms
        sealed: list[Aggregate] = []
        for (series, ws), agg in self._windows.items():
            if not agg.sealed and ws + self.window_size_ms <= cutoff:
                agg.sealed = True
                sealed.append(agg)
        sealed.sort(key=lambda a: (a.window_start_ms, a.series))
        return sealed

    def apply_watermark_marker(self, watermark_ms: int) -> None:
        """回放 WAL 水位标记：单调推进封口地平线与水网游标。"""
        with self._lock:
            if watermark_ms > self._closed_horizon_ms:
                self._closed_horizon_ms = watermark_ms
            if watermark_ms > self._watermark_ms:
                self._watermark_ms = watermark_ms

    def reseal_due(self) -> None:
        """恢复后按已恢复的封口地平线补打 sealed 标志（幂等）。

        使"仅 WAL 重放"与"快照恢复"两种路径得到的 sealed 标志一致。
        """
        with self._lock:
            self._seal_due_locked()

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
                # 封口地平线随快照持久化：重启后"窗口已关"的事实性结论不变
                "closed_horizon_ms": self._closed_horizon_ms,
                "windows": [asdict(a) for a in self._windows.values()],
            }

    def restore(self, state: dict, replay: Iterable[dict] = ()) -> None:
        """从快照恢复，并重放快照之后的 WAL 记录（事件 + 水位标记）。"""
        with self._lock:
            self._windows.clear()
            self._watermark_ms = state.get("watermark_ms", -1)
            # 旧版快照没有 closed_horizon_ms：当时水位游标兼作封口依据，
            # 用水位兜底，保持与旧版一致的"窗口已关"判定口径。
            self._closed_horizon_ms = state.get(
                "closed_horizon_ms", self._watermark_ms
            )
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
        # 重放走 trusted，保证"已接受事实"不因重启后水位变高而被拒；
        # 水位标记恢复封口地平线，保证"窗口已关"的结论跨重启稳定
        for record in replay:
            if is_watermark_marker(record):
                self.apply_watermark_marker(record["watermark_ms"])
            else:
                self.apply(record, trusted=True)
        # 地平线恢复后补打 sealed 标志，与快照路径的冻结状态对齐
        self.reseal_due()
