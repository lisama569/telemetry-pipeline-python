"""采集入库层：并发写入、幂等去重、基数与速率限流。

拒绝原因（见 ``RejectReason``）互相可区分，绝不静默丢弃。判定在同一把锁内
按**固定优先级**依次执行，结论唯一、不随线程调度漂移：
1. ``BAD_INPUT``         —— 字段不合法（在加锁前快速失败）；
2. ``DUPLICATE``         —— 幂等键已被接受过；
3. ``WINDOW_CLOSED``     —— 目标窗口已封口（事实性结论，经 precheck 预检，
   优先于瞬时压力类判定：写入压力再大也不会被说成别的原因）；
4. ``CARDINALITY_LIMIT`` —— 新序列会突破基数上限；
5. ``RATE_LIMIT``        —— 令牌桶不足，超过写入速率配额（瞬时压力，
   与"窗口已关"严格区分，互不混淆）；
随后交给 ``sink`` 回调（由管道装配为存储+聚合），sink 还可能返回
``WINDOW_CLOSED`` / ``CURSOR_BACKWARD`` 这两类拒绝（与预检结论一致）。

全部判定与消费在同一把锁内完成，保证多线程并发上报时结论确定。

通道（channel）：
- ``online``   —— 在线实时上报，使用在线令牌桶；
- ``backfill`` —— 批量补报/重放，使用**独立的补报令牌桶**，
  与在线配额互不挤占；补报记录在 WAL 中带 ``channel`` 标记，
  聚合层据此不随补报事件推进水位（见 ``aggregation.apply``）。
两个令牌桶的时钟均可注入（默认单调时钟），测试用固定时钟即可得到
与机器快慢无关的限流结论；每次成功消费令牌后通过 persister 回调落盘，
进程重启不会把已用掉的额度清零。
"""
from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from .models import (
    IngestResult,
    IngestStatus,
    LogEvent,
    LogLevel,
    MetricPoint,
    RejectReason,
)

# sink 接收规范化记录，返回拒绝原因或 None 表示接受
Sink = Callable[[dict], RejectReason | None]

# precheck 接收 (series, timestamp_ms)，返回事实性拒绝原因（如 WINDOW_CLOSED）
# 或 None；在限流等瞬时压力判定之前执行，保证事实性结论稳定优先。
Precheck = Callable[[str, int], RejectReason | None]

# 采集通道：在线实时上报 / 批量补报（重放）
CHANNEL_ONLINE = "online"
CHANNEL_BACKFILL = "backfill"
CHANNELS = (CHANNEL_ONLINE, CHANNEL_BACKFILL)


def _monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


class TokenBucket:
    """简单令牌桶：以固定速率补充令牌，容量为 burst。

    时钟可注入（``now_ms``，默认单调时钟）：注入固定/手动时钟后，
    限流结论与机器快慢、调度抖动无关，可精确复现。
    """

    __slots__ = ("capacity", "refill_per_sec", "_tokens", "_last_ms", "_lock",
                 "_now_ms")

    def __init__(self, capacity: int, refill_per_sec: float,
                 now_ms: Callable[[], int] | None = None) -> None:
        if capacity <= 0:
            raise ValueError("令牌桶容量必须为正数")
        if refill_per_sec < 0:
            raise ValueError("补充速率不能为负数")
        self.capacity = float(capacity)
        self.refill_per_sec = refill_per_sec
        self._tokens = float(capacity)
        self._now_ms = now_ms or _monotonic_ms
        self._last_ms = self._now_ms()
        self._lock = threading.Lock()

    def try_consume(self, amount: float = 1.0) -> bool:
        with self._lock:
            now = self._now_ms()
            elapsed = (now - self._last_ms) / 1000.0
            if elapsed > 0:
                self._tokens = min(
                    self.capacity, self._tokens + elapsed * self.refill_per_sec
                )
                self._last_ms = now
            if self._tokens >= amount:
                self._tokens -= amount
                return True
            return False

    # ------------------------------------------------------------------
    # 配额跨重启延续：快照保存剩余令牌，恢复时从该余量继续补充，
    # 不会出现"重启一次等于限额清零、已用掉的量被抹掉"的情况。
    # ------------------------------------------------------------------
    def snapshot(self) -> dict:
        with self._lock:
            return {
                "tokens": self._tokens,
                "capacity": self.capacity,
                "refill_per_sec": self.refill_per_sec,
            }

    def restore(self, state: dict) -> None:
        with self._lock:
            tokens = float(state.get("tokens", self.capacity))
            # 余量不超出当前配置容量（配置可能在重启间调小）
            self._tokens = min(self.capacity, max(0.0, tokens))
            self._last_ms = self._now_ms()


def _series_key(kind: str, name: str, labels: dict[str, str]) -> str:
    """规范化序列名：标签按 key 排序，保证结论与字典顺序无关。"""
    if labels:
        joined = ",".join(f"{k}={labels[k]}" for k in sorted(labels))
        return f"{kind}:{name}{{{joined}}}"
    return f"{kind}:{name}"


@dataclass
class IngestionConfig:
    max_cardinality: int = 1000          # 最多不同序列数
    rate_limit_per_sec: float = 1000.0   # 在线通道每秒写入配额
    rate_burst: int = 1000               # 在线通道突发容量
    # 补报通道使用独立令牌桶，与在线配额互不挤占；
    # 缺省（None）回落到在线通道的对应配置。
    backfill_rate_limit_per_sec: float | None = None
    backfill_rate_burst: int | None = None


class IngestionManager:
    """线程安全的采集入口（在线/补报双通道，各自独立限流配额）。"""

    def __init__(self, config: IngestionConfig | None = None,
                 sink: Sink | None = None,
                 clock: Callable[[], int] | None = None) -> None:
        self.config = config or IngestionConfig()
        self._sink: Sink = sink or (lambda _record: None)
        self._precheck: Precheck = lambda _series, _ts: None
        self._seen_keys: set[str] = set()
        self._series: set[str] = set()
        self._clock = clock or _monotonic_ms
        bf_rate = self.config.backfill_rate_limit_per_sec
        bf_burst = self.config.backfill_rate_burst
        self._buckets: dict[str, TokenBucket] = {
            CHANNEL_ONLINE: TokenBucket(
                self.config.rate_burst, self.config.rate_limit_per_sec,
                now_ms=self._clock,
            ),
            CHANNEL_BACKFILL: TokenBucket(
                bf_burst if bf_burst is not None else self.config.rate_burst,
                bf_rate if bf_rate is not None else self.config.rate_limit_per_sec,
                now_ms=self._clock,
            ),
        }
        # 限流状态持久化回调：每次成功消费令牌后调用（由管道装配到本地存储）
        self._limiter_persister: Callable[[dict], None] | None = None
        self._lock = threading.RLock()

    def set_sink(self, sink: Sink) -> None:
        self._sink = sink

    def set_precheck(self, precheck: Precheck) -> None:
        """注册事实性预检（窗口是否已关），在限流判定之前执行。"""
        self._precheck = precheck

    def set_limiter_persister(self, persister: Callable[[dict], None]) -> None:
        """注册限流状态持久化回调：成功消费令牌后落盘，重启不清零额度。"""
        self._limiter_persister = persister

    # ------------------------------------------------------------------
    # 恢复支持（重启后由管道注入历史状态，见 pipeline.py）
    # ------------------------------------------------------------------
    def restore(self, seen_keys: set[str], series: set[str]) -> None:
        with self._lock:
            self._seen_keys = set(seen_keys)
            self._series = set(series)

    @property
    def seen_keys(self) -> set[str]:
        with self._lock:
            return set(self._seen_keys)

    @property
    def series(self) -> set[str]:
        with self._lock:
            return set(self._series)

    # ------------------------------------------------------------------
    # 限流配额快照（跨重启延续已用掉的量，见 TokenBucket.snapshot）
    # ------------------------------------------------------------------
    def rate_limiter_snapshot(self) -> dict:
        """两个通道的令牌桶状态（在线/补报分开记录）。"""
        return {channel: bucket.snapshot()
                for channel, bucket in self._buckets.items()}

    def restore_rate_limiter(self, state: dict) -> None:
        """恢复限流状态；兼容旧版单桶格式（``{"tokens": ...}`` → 在线桶）。"""
        if "online" in state or "backfill" in state:
            for channel, bucket_state in state.items():
                bucket = self._buckets.get(channel)
                if bucket is not None and isinstance(bucket_state, dict):
                    bucket.restore(bucket_state)
        elif "tokens" in state:
            self._buckets[CHANNEL_ONLINE].restore(state)

    # ------------------------------------------------------------------
    # 校验
    # ------------------------------------------------------------------
    @staticmethod
    def _validate(point: MetricPoint | LogEvent) -> str | None:
        if not point.idempotency_key or not isinstance(point.idempotency_key, str):
            return "幂等键不能为空"
        if not isinstance(point.timestamp_ms, int) or point.timestamp_ms < 0:
            return "timestamp_ms 必须为非负整数"
        if isinstance(point, MetricPoint):
            if not point.name:
                return "指标名不能为空"
            if isinstance(point.value, bool) or not isinstance(point.value, (int, float)):
                return "指标值必须为数值"
            if point.value != point.value:  # NaN
                return "指标值不能为 NaN"
        elif isinstance(point, LogEvent):
            if not point.message:
                return "日志内容不能为空"
            if not isinstance(point.level, LogLevel):
                return "日志级别非法"
        else:
            return "不支持的事件类型"
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in point.labels.items()):
            return "labels 必须全部为字符串键值"
        return None

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------
    def ingest(self, point: MetricPoint | LogEvent, *,
               channel: str = CHANNEL_ONLINE) -> IngestResult:
        if channel not in CHANNELS:
            raise ValueError(f"未知采集通道：{channel!r}")
        bad = self._validate(point)
        if bad is not None:
            return IngestResult(
                IngestStatus.REJECTED,
                getattr(point, "idempotency_key", ""),
                RejectReason.BAD_INPUT,
                bad,
            )

        with self._lock:
            key = point.idempotency_key
            # 1) 幂等去重：重复上报直接识别，不重复落盘、不二次计数
            if key in self._seen_keys:
                return IngestResult(
                    IngestStatus.REJECTED, key, RejectReason.DUPLICATE,
                    f"幂等键 {key} 已存在，按重复上报丢弃",
                )

            series = _series_key(
                point.kind.value,
                point.name if isinstance(point, MetricPoint) else point.level.value,
                point.labels,
            )

            # 2) 事实性预检：窗口已关是稳定结论，必须优先于限流等瞬时判定，
            #    且预检不消耗令牌（窗口已关的补报不应占用在线写入配额）
            closed = self._precheck(series, point.timestamp_ms)
            if closed is not None:
                return IngestResult(
                    IngestStatus.REJECTED, key, closed,
                    "目标窗口已封口，不再接收该事件时间的数据",
                )

            # 3) 基数限流：不得污染已有聚合（新序列尚未加入集合）
            if series not in self._series and len(self._series) >= self.config.max_cardinality:
                return IngestResult(
                    IngestStatus.REJECTED, key, RejectReason.CARDINALITY_LIMIT,
                    f"序列基数已达上限 {self.config.max_cardinality}，拒绝新序列 {series}",
                )

            # 4) 速率限流：按通道各自独立的令牌桶，补报与在线互不挤占配额
            if not self._buckets[channel].try_consume(1.0):
                return IngestResult(
                    IngestStatus.REJECTED, key, RejectReason.RATE_LIMIT,
                    f"写入速率超过 {channel} 通道配额",
                )
            # 令牌已消耗：立即落盘限流状态，进程重启不会把已用额度清零
            if self._limiter_persister is not None:
                self._limiter_persister(self.rate_limiter_snapshot())

            record = self._to_record(point, series, channel)

            # 5) 下游（存储/窗口）可能再拒绝：已关闭窗口、游标回退
            #    （与预检结论一致，此处为兜底权威判定）
            rejected = self._sink(record)
            if rejected is not None:
                return IngestResult(
                    IngestStatus.REJECTED, key, rejected,
                    "下游拒绝写入",
                )

            # 只有 sink 成功后才提交去重键与基数，保证不污染状态
            self._seen_keys.add(key)
            self._series.add(series)
            return IngestResult(
                IngestStatus.ACCEPTED, key, None,
                f"已入库：series={series}",
                record["window_start_ms"],
            )

    @staticmethod
    def _to_record(point: MetricPoint | LogEvent, series: str,
                   channel: str) -> dict:
        if isinstance(point, MetricPoint):
            return {
                "kind": "metric",
                "series": series,
                "name": point.name,
                "labels": point.labels,
                "value": point.value,
                "timestamp_ms": point.timestamp_ms,
                "window_start_ms": "__BY_AGGREGATOR__",
                "idempotency_key": point.idempotency_key,
                # 通道随记录落 WAL：重放时仅 online 记录推进水位
                "channel": channel,
            }
        return {
            "kind": "log",
            "series": series,
            "name": point.level.value,
            "labels": point.labels,
            "value": 1.0 if point.level is LogLevel.ERROR else 0.0,
            "message": point.message,
            "timestamp_ms": point.timestamp_ms,
            "window_start_ms": "__BY_AGGREGATOR__",
            "idempotency_key": point.idempotency_key,
            "channel": channel,
        }
