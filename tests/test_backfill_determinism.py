"""补报/重放确定性：同一份输入，无论怎么排序、分批、并发、机器快慢、
是否重启，逐条结论与最终聚合都必须一致。

日志中显式打印：窗口区间、幂等键、每条判定依据，便于离线复核。
"""
import logging
import random
import threading

from telemetry.ingest import IngestionConfig
from telemetry.models import LogEvent, LogLevel, MetricPoint, RejectReason
from telemetry.pipeline import TelemetryPipeline

logger = logging.getLogger("telemetry.backfill.determinism")

WINDOW_MS = 1000


class ManualClock:
    """手动时钟：限流结论只随显式推进变化，与机器快慢无关。"""

    def __init__(self, start_ms: int = 0) -> None:
        self.t = start_ms

    def now_ms(self) -> int:
        return self.t

    def advance(self, ms: int) -> None:
        self.t += ms


def _pipe(path, allowed_lateness_ms=0, clock=None, **cfg_kw):
    cfg = IngestionConfig(
        max_cardinality=cfg_kw.pop("max_cardinality", 1000),
        rate_limit_per_sec=cfg_kw.pop("rate_limit_per_sec", 1_000_000),
        rate_burst=cfg_kw.pop("rate_burst", 1_000_000),
        backfill_rate_limit_per_sec=cfg_kw.pop("backfill_rate_limit_per_sec", None),
        backfill_rate_burst=cfg_kw.pop("backfill_rate_burst", None),
    )
    return TelemetryPipeline(
        str(path), window_size_ms=WINDOW_MS,
        allowed_lateness_ms=allowed_lateness_ms,
        ingestion_config=cfg, clock=clock,
    )


def _stats(pipe):
    return sorted(
        (a.series, a.window_start_ms, a.count, round(a.total, 12),
         a.minimum, a.maximum, a.sealed)
        for a in pipe.aggregator.aggregates()
    )


def _disposition(reports):
    """多次补报报告的归并结论：被接受的键集合 + 各拒绝原因计数。"""
    accepted = set()
    reasons: dict[str, int] = {}
    for rep in reports:
        for r in rep.results:
            if r.accepted:
                accepted.add(r.idempotency_key)
            else:
                reasons[r.reason.value] = reasons.get(r.reason.value, 0) + 1
    return accepted, reasons


def _scenario_batch():
    """混合批次：已封口窗口 / 开放窗口 / 非法输入 / 批内重复键 / 日志。

    批内重复键的两条内容完全一致（真正的重复发布）：去重是"先到者得"，
    内容相同则无论谁先被接受，聚合结论都一致。
    """
    return [
        MetricPoint("cpu", 1.0, 100, "k-closed"),           # 窗口 [0,1000) 已封口
        MetricPoint("cpu", 2.0, 1500, "k-open-1"),          # 开放窗口
        MetricPoint("cpu", 3.0, 1600, "k-open-2"),          # 开放窗口
        MetricPoint("cpu", float("nan"), 1700, "k-bad"),    # 非法输入
        MetricPoint("cpu", 2.0, 1800, "k-open-1"),          # 批内重复键（同内容）
        LogEvent("boom", LogLevel.ERROR, 1900, "k-log"),    # 日志事件
    ]


def _prepared_pipe(path):
    """在线写入一条并封口 [0,1000)，构造"有已翻篇窗口"的现场。"""
    pipe = _pipe(path)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=WINDOW_MS)
    return pipe


# ---------------------------------------------------------------------------
# 1. 同一批历史数据：换顺序、换切分方式，逐条结论与聚合完全一致
# ---------------------------------------------------------------------------
def test_verdicts_independent_of_order_and_splitting(tmp_path):
    batch = _scenario_batch()
    shuffled = list(batch)
    random.Random(42).shuffle(shuffled)
    plans = {
        "原序整批": [batch],
        "逆序整批": [list(reversed(batch))],
        "打散整批": [shuffled],
        "顺切三批": [batch[:2], batch[2:4], batch[4:]],
        "逆切三批": [list(reversed(batch[4:])), list(reversed(batch[2:4])),
                     list(reversed(batch[:2]))],
    }

    baseline = None
    for name, chunks in plans.items():
        pipe = _prepared_pipe(tmp_path / name)
        reports = [
            pipe.backfill(chunk, batch_id=f"{name}-#{i}")
            for i, chunk in enumerate(chunks)
        ]
        accepted, reasons = _disposition(reports)
        stats = _stats(pipe)
        logger.info("切分方案=%s 接受键=%s 拒绝分类=%s",
                    name, sorted(accepted), reasons)
        outcome = (accepted, reasons, stats)
        if baseline is None:
            baseline = outcome
            # 期望的确定结论：3 个键被接受，closed/bad/duplicate 各 1
            assert accepted == {"k-open-1", "k-open-2", "k-log"}
            assert reasons == {"window_closed": 1, "bad_input": 1,
                               "duplicate": 1}
        else:
            assert outcome == baseline, f"{name} 的结论与基准方案不一致"


# ---------------------------------------------------------------------------
# 2. 多线程并发分批投递：每个键恰好接受一次，聚合与顺序执行一致
# ---------------------------------------------------------------------------
def test_concurrent_split_submission_deterministic(tmp_path):
    batch = [
        MetricPoint("cpu", float(i), 1000 + (i * 61) % (2 * WINDOW_MS),
                    f"cc-{i:03d}")
        for i in range(40)
    ]
    chunks = [batch[i::4] for i in range(4)]

    reports: list = []
    lock = threading.Lock()

    def worker(idx, chunk):
        rep = _pipe_concurrent.backfill(chunk, batch_id=f"chunk-{idx}")
        with lock:
            reports.append(rep)

    _pipe_concurrent = _pipe(tmp_path / "s")
    threads = [threading.Thread(target=worker, args=(i, c))
               for i, c in enumerate(chunks)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted, reasons = _disposition(reports)
    assert accepted == {p.idempotency_key for p in batch}
    assert reasons == {}

    shadow = _pipe(tmp_path / "shadow")
    shadow.backfill(batch, batch_id="sequential")
    assert _stats(_pipe_concurrent) == _stats(shadow)
    logger.info("并发分批补报：%d 个键全部恰好接受一次，聚合与顺序执行一致",
                len(accepted))


# ---------------------------------------------------------------------------
# 3. 补报历史不能把在线仍在接收的窗口提前关掉
# ---------------------------------------------------------------------------
def test_backfill_does_not_close_online_windows(tmp_path):
    pipe = _pipe(tmp_path / "s")
    assert pipe.ingest(MetricPoint("cpu", 1.0, 5000, "live-1")).accepted
    # 补报一条时间戳远超当前水位的历史数据：被接受，但绝不能推进水位
    rep = pipe.backfill([MetricPoint("cpu", 9.0, 999_999, "hist-future")],
                        batch_id="future-dated")
    assert rep.results[0].accepted
    assert pipe.aggregator.watermark_ms == 5000  # 水位不被补报推高
    # 紧接着的实时上报照常接收（旧实现会误判 WINDOW_CLOSED）
    r = pipe.ingest(MetricPoint("cpu", 2.0, 5500, "live-2"))
    assert r.accepted
    logger.info("补报未推进水位（水位=%d），在线窗口未被提前关掉",
                pipe.aggregator.watermark_ms)


# ---------------------------------------------------------------------------
# 4. 无快照重启：已用掉的限流额度不被清零，去重状态不丢
# ---------------------------------------------------------------------------
def test_restart_without_snapshot_preserves_quota_and_dedup(tmp_path):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir, rate_burst=2, rate_limit_per_sec=0)
    assert pipe.ingest(MetricPoint("cpu", 1.0, 100, "q1")).accepted
    assert pipe.ingest(MetricPoint("cpu", 2.0, 200, "q2")).accepted
    blocked = pipe.ingest(MetricPoint("cpu", 3.0, 300, "q3"))
    assert blocked.reason is RejectReason.RATE_LIMIT
    # 进程直接停掉：不做快照、不 flush

    restarted = _pipe(data_dir, rate_burst=2, rate_limit_per_sec=0)
    # 限额没有被重启清零：新写入依旧被挡
    again = restarted.ingest(MetricPoint("cpu", 4.0, 400, "q4"))
    assert again.reason is RejectReason.RATE_LIMIT
    # 去重状态不丢：已接受的键依旧判重
    assert restarted.ingest(MetricPoint("cpu", 1.0, 100, "q1")).reason is \
        RejectReason.DUPLICATE
    logger.info("无快照重启：q4 仍被 %s 拒绝（额度未清零），q1 判重",
                again.reason.value)


# ---------------------------------------------------------------------------
# 5. 无快照重启：显式 flush 推进的水位精确还原，
#    "窗口已关"的事实性结论跨重启稳定
# ---------------------------------------------------------------------------
def test_restart_without_snapshot_preserves_explicit_watermark(tmp_path):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "e1"))   # 最大事件时间 100
    pipe.flush(watermark_ms=10_000)                   # 显式推进远超事件时间
    late = MetricPoint("cpu", 9.0, 8500, "late-1")
    before = pipe.backfill([late], batch_id="before")
    assert before.results[0].reason is RejectReason.WINDOW_CLOSED

    restarted = _pipe(data_dir)  # 仅 WAL 恢复（无快照）
    assert restarted.aggregator.watermark_ms == 10_000
    after = restarted.backfill([late], batch_id="after")
    assert after.results[0].reason is RejectReason.WINDOW_CLOSED
    logger.info("无快照重启：水位=%d 由 WAL 标记还原，late-1 重启前后均判 %s",
                restarted.aggregator.watermark_ms,
                after.results[0].reason.value)


# ---------------------------------------------------------------------------
# 6. 机器快慢无关：注入手动时钟后，同样的输入序列给出逐条一致的限流结论
# ---------------------------------------------------------------------------
def test_rate_limit_verdicts_machine_speed_independent(tmp_path):
    def run(dir_name):
        clock = ManualClock()
        pipe = _pipe(tmp_path / dir_name, clock=clock.now_ms,
                     rate_burst=2, rate_limit_per_sec=10)  # 每 100ms 补 1 个
        verdicts = []
        for i in range(4):  # 突发 2：前 2 条接受，后 2 条被限流
            r = pipe.ingest(MetricPoint("cpu", float(i), 100 + i, f"m-{i}"))
            verdicts.append(r.reason.value if r.reason else "accepted")
        clock.advance(200)  # 补充 2 个令牌
        for i in range(2, 4):  # 重试被限流的 2 条：接受
            r = pipe.ingest(MetricPoint("cpu", float(i), 100 + i, f"m-{i}"))
            verdicts.append(r.reason.value if r.reason else "accepted")
        return verdicts, _stats(pipe)

    v1, s1 = run("fast")   # 模拟"快机器"：同样的逻辑时钟序列
    v2, s2 = run("slow")   # 模拟"慢机器"：结论必须逐条一致
    assert v1 == v2 == ["accepted", "accepted", "rate_limit", "rate_limit",
                        "accepted", "accepted"]
    assert s1 == s2
    logger.info("手动时钟下两轮限流结论逐条一致：%s", v1)


# ---------------------------------------------------------------------------
# 7. 补报与在线配额互不挤占：各自独立的令牌桶
# ---------------------------------------------------------------------------
def test_backfill_and_online_quotas_are_independent(tmp_path):
    pipe = _pipe(tmp_path / "s", rate_burst=1, rate_limit_per_sec=0,
                 backfill_rate_burst=1, backfill_rate_limit_per_sec=0)
    # 补报用掉自己的唯一令牌，之后补报被限流
    rep = pipe.backfill([MetricPoint("cpu", 1.0, 100, "bf-1"),
                         MetricPoint("cpu", 2.0, 200, "bf-2")], batch_id="q")
    assert rep.results[0].accepted
    assert rep.results[1].reason is RejectReason.RATE_LIMIT
    # 在线配额没被补报挤占：在线写入照常接受
    assert pipe.ingest(MetricPoint("cpu", 3.0, 300, "live-1")).accepted
    # 在线令牌也用完后，在线被限流，但互不影响对方已发生的结论
    assert pipe.ingest(MetricPoint("cpu", 4.0, 400, "live-2")).reason is \
        RejectReason.RATE_LIMIT
    logger.info("补报/在线各自独立配额：补报耗尽不影响在线接受，反之亦然")


# ---------------------------------------------------------------------------
# 8. 跨重启重复补报：同一批补报两次（中间隔一次重启），结论逐条对得上
# ---------------------------------------------------------------------------
def test_repeat_backfill_across_restart_matches(tmp_path):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=WINDOW_MS)  # 封口 [0,1000)

    batch = [
        MetricPoint("cpu", 9.0, 100, "r-closed"),  # 窗口已关
        MetricPoint("cpu", 3.0, 1500, "r-1"),      # 接受
        MetricPoint("cpu", 4.0, 1600, "r-2"),      # 接受
    ]
    first = pipe.backfill(batch, batch_id="run-1")
    assert [r.reason for r in first.results] == [
        RejectReason.WINDOW_CLOSED, None, None]

    restarted = _pipe(data_dir)  # 重启（仅 WAL，含水位标记）
    second = restarted.backfill(batch, batch_id="run-2")
    # 逐条对得上：已接受的判 DUPLICATE，窗口已关的仍是 WINDOW_CLOSED
    assert [r.reason for r in second.results] == [
        RejectReason.WINDOW_CLOSED, RejectReason.DUPLICATE,
        RejectReason.DUPLICATE]
    # 第三次（不再重启）结论与第二次完全一致
    third = restarted.backfill(batch, batch_id="run-3")
    assert [r.reason for r in third.results] == \
        [r.reason for r in second.results]
    logger.info("跨重启重复补报：三次结论逐条一致 %s",
                [r.reason.value if r.reason else "accepted"
                 for r in third.results])
