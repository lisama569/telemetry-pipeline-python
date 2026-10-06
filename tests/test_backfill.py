"""批量补报（backfill）与重放：逐条结论、并发确定、重复幂等、跨重启恢复。

日志中显式打印：窗口区间、幂等键、每条判定依据，便于离线复核。
"""
import logging
import random
import threading

import pytest

from telemetry.ingest import IngestionConfig
from telemetry.models import LogEvent, LogLevel, MetricPoint, RejectReason
from telemetry.pipeline import TelemetryPipeline

logger = logging.getLogger("telemetry.backfill.tests")

WINDOW_MS = 1000


def _pipe(path, allowed_lateness_ms=0, **cfg_kw):
    cfg = IngestionConfig(
        max_cardinality=cfg_kw.pop("max_cardinality", 1000),
        rate_limit_per_sec=cfg_kw.pop("rate_limit_per_sec", 1_000_000),
        rate_burst=cfg_kw.pop("rate_burst", 1_000_000),
    )
    return TelemetryPipeline(
        str(path), window_size_ms=WINDOW_MS,
        allowed_lateness_ms=allowed_lateness_ms,
        ingestion_config=cfg,
    )


def _stats(pipe):
    return sorted(
        (a.series, a.window_start_ms, a.count, round(a.total, 12),
         a.minimum, a.maximum, a.sealed)
        for a in pipe.aggregator.aggregates()
    )


# ---------------------------------------------------------------------------
# 1. 整批混合条目：逐条结论 + 汇总，单条失败不影响整批
# ---------------------------------------------------------------------------
def test_backfill_batch_mixed_outcomes(tmp_path, caplog):
    pipe = _pipe(tmp_path / "s")
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=WINDOW_MS)  # 封口窗口 [0,1000)

    batch = [
        MetricPoint("cpu", 2.0, 100, "bf-closed"),          # 落在已封口窗口
        MetricPoint("cpu", 3.0, 1500, "bf-ok-1"),           # 开放窗口，接受
        MetricPoint("cpu", float("nan"), 1600, "bf-bad"),   # 非法输入
        MetricPoint("cpu", 4.0, 1700, "bf-ok-1"),           # 批内重复键
        LogEvent("boom", LogLevel.ERROR, 1800, "bf-ok-2"),  # 日志，接受
    ]
    with caplog.at_level(logging.INFO, logger="telemetry.backfill"):
        report = pipe.backfill(batch, batch_id="batch-1")

    # 逐条结论，顺序与输入一致
    assert [r.idempotency_key for r in report.results] == [
        "bf-closed", "bf-ok-1", "bf-bad", "bf-ok-1", "bf-ok-2",
    ]
    assert report.results[0].reason is RejectReason.WINDOW_CLOSED
    assert report.results[1].accepted
    assert report.results[2].reason is RejectReason.BAD_INPUT
    assert report.results[3].reason is RejectReason.DUPLICATE
    assert report.results[4].accepted

    # 汇总：收了多少、挡了多少、每种原因各多少
    assert report.total == 5
    assert report.accepted == 2
    assert report.rejected == 3
    assert report.rejected_by_reason == {
        "window_closed": 1, "bad_input": 1, "duplicate": 1,
    }

    # 只有被接受的条目进入聚合
    agg = pipe.aggregator.get_aggregate("metric:cpu", 1000)
    assert agg.count == 1 and agg.total == 3.0
    log_agg = pipe.aggregator.get_aggregate("log:ERROR", 1000)
    assert log_agg.count == 1

    # 日志可见：窗口区间、幂等键、每条判定依据
    text = caplog.text
    assert "batch=batch-1" in text
    assert "幂等键=bf-closed" in text and "窗口=[0,1000)" in text
    assert "原因=window_closed" in text
    assert "幂等键=bf-bad" in text and "原因=bad_input" in text
    assert "补报汇总" in text and "接受=2" in text and "拒绝=3" in text


# ---------------------------------------------------------------------------
# 2. WINDOW_CLOSED 是事实性结论：写入压力再大也不会被说成 RATE_LIMIT；
#    补报不消耗在线限流配额（判定不含墙钟，结论与机器快慢无关）；
#    在线通道上 WINDOW_CLOSED 与 RATE_LIMIT 严格区分
# ---------------------------------------------------------------------------
def test_backfill_window_closed_never_masked_by_rate_limit(tmp_path, caplog):
    pipe = _pipe(tmp_path / "s", rate_burst=2, rate_limit_per_sec=0)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))   # 消耗 1 令牌
    pipe.flush(watermark_ms=WINDOW_MS)                      # 封口 [0,1000)

    with caplog.at_level(logging.INFO, logger="telemetry.backfill"):
        report = pipe.backfill([
            MetricPoint("cpu", 9.0, 100, "c1"),    # 窗口已关 -> WINDOW_CLOSED
            MetricPoint("cpu", 9.0, 200, "c2"),    # 窗口已关 -> WINDOW_CLOSED
            MetricPoint("cpu", 5.0, 1500, "o1"),   # 窗口开放 -> 接受
            MetricPoint("cpu", 6.0, 1600, "o2"),   # 窗口开放 -> 接受（补报不限流）
            MetricPoint("cpu", 9.0, 300, "c3"),    # 仍是 WINDOW_CLOSED
        ], batch_id="batch-pressure")

    reasons = [r.reason for r in report.results]
    assert reasons == [
        RejectReason.WINDOW_CLOSED,
        RejectReason.WINDOW_CLOSED,
        None,                       # accepted
        None,                       # accepted：补报不占用在线令牌桶，
                                    # 不会因为"当时正赶上限流"而改口
        RejectReason.WINDOW_CLOSED, # 窗口已关的结论稳定，不受写入压力影响
    ]
    assert report.rejected_by_reason == {"window_closed": 3}
    assert pipe.aggregator.get_aggregate("metric:cpu", 1000).total == 11.0

    # 补报没有消耗在线配额：burst=2 只被 online-1 用了 1 个，
    # 在线再写 1 条仍有令牌，第 2 条才被 RATE_LIMIT 挡下
    assert pipe.ingest(MetricPoint("cpu", 7.0, 1700, "live-1")).accepted
    blocked = pipe.ingest(MetricPoint("cpu", 8.0, 1800, "live-2"))
    assert blocked.reason is RejectReason.RATE_LIMIT
    # 在线通道上两类原因同样分得清：令牌耗尽时，
    # 落已关窗口的仍判 WINDOW_CLOSED（预检优先、不耗令牌），
    # 落开放窗口的才判 RATE_LIMIT
    closed = pipe.ingest(MetricPoint("cpu", 9.0, 400, "live-closed"))
    assert closed.reason is RejectReason.WINDOW_CLOSED
    logger.info(
        "限流压力下原因可区分：补报窗口已关=window_closed 在线开放窗口=%s "
        "在线已关窗口=%s",
        blocked.reason.value, closed.reason.value,
    )
    # 日志中两类原因各自可区分
    assert "原因=window_closed" in caplog.text


# ---------------------------------------------------------------------------
# 3. 并发补报：同一批被多线程同时投递，每个幂等键恰好接受一次，
#    聚合结论与顺序执行一致
# ---------------------------------------------------------------------------
def test_backfill_concurrent_submission_accepted_once(tmp_path):
    pipe = _pipe(tmp_path / "s")
    batch = [
        MetricPoint("cpu", float(i), (i * 37) % WINDOW_MS, f"bf-{i:03d}")
        for i in range(60)
    ]

    reports: list = []
    lock = threading.Lock()

    def worker():
        r = pipe.backfill(batch, batch_id=f"t-{threading.get_ident()}")
        with lock:
            reports.append(r)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 全部报告合计：每个幂等键恰好被接受一次，其余都是 DUPLICATE
    accepted_keys = [
        res.idempotency_key for rep in reports for res in rep.results if res.accepted
    ]
    assert sorted(accepted_keys) == sorted(p.idempotency_key for p in batch)
    total_accepted = sum(rep.accepted for rep in reports)
    total_duplicates = sum(
        rep.rejected_by_reason.get("duplicate", 0) for rep in reports
    )
    assert total_accepted == len(batch)
    assert total_duplicates == 3 * len(batch)

    # 聚合与顺序执行的影子管道一致
    shadow = _pipe(tmp_path / "shadow")
    for p in batch:
        shadow.ingest(p)
    assert _stats(pipe) == _stats(shadow)
    agg = pipe.aggregator.get_aggregate("metric:cpu", 0)
    logger.info("并发补报 窗口=[0,1000) count=%d sum=%.1f 与顺序执行一致",
                agg.count, agg.total)


# ---------------------------------------------------------------------------
# 4. 同一批重复补报：第二次全部判 DUPLICATE，不重复计数，两次结论对得上
# ---------------------------------------------------------------------------
def test_backfill_repeat_submission_is_idempotent(tmp_path):
    pipe = _pipe(tmp_path / "s")
    batch = [MetricPoint("cpu", float(i), i * 10, f"rp-{i}") for i in range(20)]

    first = pipe.backfill(batch, batch_id="run-1")
    assert first.accepted == 20 and first.rejected == 0
    stats_after_first = _stats(pipe)

    second = pipe.backfill(batch, batch_id="run-2")
    assert second.accepted == 0 and second.rejected == 20
    assert second.rejected_by_reason == {"duplicate": 20}
    # 两次结论对得上：第一次接受的键 == 第二次判重的键
    assert sorted(r.idempotency_key for r in first.results if r.accepted) == \
        sorted(r.idempotency_key for r in second.results
               if r.reason is RejectReason.DUPLICATE)
    # 聚合没有被重复计数
    assert _stats(pipe) == stats_after_first


# ---------------------------------------------------------------------------
# 5. 跨重启恢复：重放同一段历史，逐条结论与聚合都和重启前对得上
#    （分别覆盖 仅 WAL 与 快照+WAL 两条恢复路径）
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("use_snapshot", [False, True])
def test_backfill_restart_replay_consistent(tmp_path, use_snapshot):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=WINDOW_MS)  # 封口 [0,1000)

    batch = [
        MetricPoint("cpu", 9.0, 100, "b-closed"),  # 窗口已关
        MetricPoint("cpu", 3.0, 1500, "b-1"),      # 接受
        MetricPoint("cpu", 4.0, 1600, "b-2"),      # 接受
    ]
    before = pipe.backfill(batch, batch_id="run-before")
    assert [r.reason for r in before.results] == [
        RejectReason.WINDOW_CLOSED, None, None,
    ]
    stats_before = _stats(pipe)
    if use_snapshot:
        pipe.snapshot()

    # 重启后重放同一段历史：逐条结论与重启前对得上
    restarted = _pipe(data_dir)
    after = restarted.backfill(batch, batch_id="run-after")
    assert [r.reason for r in after.results] == [
        RejectReason.WINDOW_CLOSED,   # 窗口已关的结论跨重启稳定
        RejectReason.DUPLICATE,       # 已接受的不重复计数
        RejectReason.DUPLICATE,
    ]
    # 仅 WAL 恢复时 sealed 标志由水位重新推导，flush 一次后与重启前一致
    restarted.flush()
    assert _stats(restarted) == stats_before
    logger.info("跨重启重放（snapshot=%s）逐条结论=%s 聚合一致",
                use_snapshot,
                [r.reason.value if r.reason else "accepted" for r in after.results])


# ---------------------------------------------------------------------------
# 6. 限流配额跨重启延续：重启不会把已用掉的量清零
# ---------------------------------------------------------------------------
def test_backfill_quota_persists_across_restart(tmp_path):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir, rate_burst=2, rate_limit_per_sec=0)
    assert pipe.ingest(MetricPoint("cpu", 1.0, 100, "q1")).accepted
    assert pipe.ingest(MetricPoint("cpu", 2.0, 200, "q2")).accepted
    # 配额已耗尽
    blocked = pipe.ingest(MetricPoint("cpu", 3.0, 300, "q3"))
    assert blocked.reason is RejectReason.RATE_LIMIT
    pipe.snapshot()

    # 重启：限额不清零，q3 依旧被挡；已接受的键依旧判重
    restarted = _pipe(data_dir, rate_burst=2, rate_limit_per_sec=0)
    again = restarted.ingest(MetricPoint("cpu", 3.0, 300, "q3"))
    assert again.reason is RejectReason.RATE_LIMIT
    assert restarted.ingest(MetricPoint("cpu", 1.0, 100, "q1")).reason is \
        RejectReason.DUPLICATE
    logger.info("跨重启配额延续：q3 仍被 %s 拒绝，未因重启放行", again.reason.value)


# ---------------------------------------------------------------------------
# 7. 补报与在线采集兼容：补报历史不会把在线仍在接收的窗口提前关掉，
#    在线事件也不被补报干扰；窗口只随显式 flush 关闭
# ---------------------------------------------------------------------------
def test_backfill_coexists_with_online_ingest(tmp_path):
    pipe = _pipe(tmp_path / "s", allowed_lateness_ms=2000)
    # 在线实时上报（事件时间 5000）
    assert pipe.ingest(MetricPoint("cpu", 1.0, 5000, "live-1")).accepted
    # 补报历史：没有任何显式 flush，再早的窗口也不会被在线水位提前关掉
    report = pipe.backfill([
        MetricPoint("cpu", 2.0, 4500, "hist-1"),   # 窗口 [4000,5000)
        MetricPoint("cpu", 3.0, 500, "hist-2"),    # 窗口 [0,1000)：未 flush 不关闭
    ], batch_id="mixed")
    assert all(r.accepted for r in report.results)
    # 在线链路继续可用，不受补报影响
    assert pipe.ingest(MetricPoint("cpu", 4.0, 5100, "live-2")).accepted
    assert pipe.aggregator.get_aggregate("metric:cpu", 0).total == 3.0
    assert pipe.aggregator.get_aggregate("metric:cpu", 4000).total == 2.0
    assert pipe.aggregator.get_aggregate("metric:cpu", 5000).total == 5.0

    # 显式 flush 推进水位到 5100：迟到宽限 2000 -> 地平线 5100，
    # [0,1000)…[2000,3000) 封口；[4000,5000) 与在线当前窗口仍开放
    pipe.flush(watermark_ms=5100)
    late = pipe.backfill([
        MetricPoint("cpu", 9.0, 500, "hist-3"),    # 已翻篇 -> WINDOW_CLOSED
        MetricPoint("cpu", 6.0, 4600, "hist-4"),   # 宽限内仍开放 -> 接受
    ], batch_id="after-flush")
    assert late.results[0].reason is RejectReason.WINDOW_CLOSED
    assert late.results[1].accepted
    # 在线实时上报不被补报与封口干扰
    assert pipe.ingest(MetricPoint("cpu", 7.0, 5200, "live-3")).accepted
    logger.info(
        "补报与在线共存：hist-3 结论=%s hist-4 结论=accepted 在线窗口=[5000,6000) 正常",
        late.results[0].reason.value,
    )


# ---------------------------------------------------------------------------
# 8. 确定性（核心回归）：同一份输入无论换序/分批/并发/在限流配置极小的
#    "慢机器"上跑，逐条结论与最终聚合都完全一致；重复补报结论对得上
# ---------------------------------------------------------------------------
def _verdicts(pipe_reports):
    """把若干次补报报告汇总成 {幂等键: [结论...]}，便于跨方式比较。"""
    out: dict[str, list[str]] = {}
    for rep in pipe_reports:
        for r in rep.results:
            label = "accepted" if r.accepted else r.reason.value
            out.setdefault(r.idempotency_key, []).append(label)
    return out


def test_backfill_deterministic_under_reorder_split_concurrency(tmp_path):
    # 30 条历史数据，事件时间横跨 5 个窗口（乱序场景的高发形态）；
    # 限流配置调到最小：若补报走墙钟令牌桶，burst=1 时只有 1 条能被接受——
    # 用来证明补报结论与机器快慢/令牌补充无关
    batch = [
        MetricPoint("cpu", float(i), (i * 137) % 5000, f"det-{i:02d}")
        for i in range(30)
    ]
    expected_keys = sorted(p.idempotency_key for p in batch)

    # 顺序影子管道：作为聚合结论的基准
    shadow = _pipe(tmp_path / "shadow", rate_burst=1, rate_limit_per_sec=0)
    for p in batch:
        assert shadow.ingestion.ingest_backfill(p).accepted
    stats_shadow = _stats(shadow)

    def run_one(name, submit):
        pipe = _pipe(tmp_path / name, rate_burst=1, rate_limit_per_sec=0)
        reports = submit(pipe)
        verdicts = _verdicts(reports)
        # 每个键恰好被判定一次，且全部接受（无 flush、无重复键、无坏数据）
        assert sorted(verdicts) == expected_keys
        assert all(v == ["accepted"] for v in verdicts.values()), verdicts
        # 同一批立刻重复补报一次：全部稳定判 DUPLICATE，两次结论对得上
        again = pipe.backfill(batch, batch_id=f"{name}-rerun")
        assert again.rejected_by_reason == {"duplicate": len(batch)}
        assert _stats(pipe) == stats_shadow
        return verdicts

    # 方式 1：正序整批
    v1 = run_one("forward", lambda p: [p.backfill(batch, batch_id="f")])
    # 方式 2：倒序整批
    v2 = run_one("reverse", lambda p: [p.backfill(batch[::-1], batch_id="r")])
    # 方式 3：固定种子打乱
    shuffled = batch[:]
    random.Random(20261006).shuffle(shuffled)
    v3 = run_one("shuffled", lambda p: [p.backfill(shuffled, batch_id="s")])
    # 方式 4：切成 5 批依次投递
    def in_chunks(p):
        return [
            p.backfill(batch[i::5], batch_id=f"chunk-{i}") for i in range(5)
        ]
    v4 = run_one("chunks", in_chunks)

    assert v1 == v2 == v3 == v4
    logger.info(
        "换序/分批确定性：4 种投递方式逐条结论完全一致，键数=%d", len(v1),
    )

    # 方式 5：4 线程并发同时投同一批——每个键恰好被接受一次，
    # 其余 3 次全部稳定判 DUPLICATE，聚合与顺序执行一致
    pipe = _pipe(tmp_path / "concurrent", rate_burst=1, rate_limit_per_sec=0)
    reports: list = []
    lock = threading.Lock()

    def worker(n):
        r = pipe.backfill(batch, batch_id=f"par-{n}")
        with lock:
            reports.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    verdicts = _verdicts(reports)
    assert sorted(verdicts) == expected_keys
    for key, labels in sorted(verdicts.items()):
        assert sorted(labels) == ["accepted", "duplicate", "duplicate",
                                  "duplicate"], (key, labels)
    assert _stats(pipe) == stats_shadow

    # 方式 6：5 线程并发各投一个不相交分片（并集 = 同一批）——
    # 每个键恰好被接受一次，聚合与顺序执行一致
    pipe6 = _pipe(tmp_path / "concurrent-chunks",
                  rate_burst=1, rate_limit_per_sec=0)
    reports6: list = []

    def chunk_worker(n):
        r = pipe6.backfill(batch[n::5], batch_id=f"slice-{n}")
        with lock:
            reports6.append(r)

    threads = [threading.Thread(target=chunk_worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    verdicts6 = _verdicts(reports6)
    assert sorted(verdicts6) == expected_keys
    assert all(v == ["accepted"] for v in verdicts6.values()), verdicts6
    assert _stats(pipe6) == stats_shadow
    logger.info(
        "并发确定性：整批重复投/分片并发投，每个幂等键都恰好 accepted 一次，"
        "聚合与顺序执行一致（窗口数=%d）",
        len(pipe.aggregator.aggregates()),
    )


# ---------------------------------------------------------------------------
# 9. 并发 + 窗口已关混合：已关窗口的结论不随线程调度漂移，
#    开放窗口的条目恰好接受一次
# ---------------------------------------------------------------------------
def test_backfill_concurrent_closed_and_open_verdicts_stable(tmp_path):
    pipe = _pipe(tmp_path / "s")
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=2 * WINDOW_MS)   # 封口 [0,1000) 与 [1000,2000)

    closed_items = [
        MetricPoint("cpu", 9.0, 100 + i, f"closed-{i}") for i in range(10)
    ]
    open_items = [
        MetricPoint("cpu", float(i), 2500 + i, f"open-{i}") for i in range(10)
    ]
    batch = [x for pair in zip(closed_items, open_items) for x in pair]

    reports: list = []
    lock = threading.Lock()

    def worker(n):
        r = pipe.backfill(batch, batch_id=f"w-{n}")
        with lock:
            reports.append(r)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    verdicts = _verdicts(reports)
    for i in range(10):
        # 窗口已关：4 次投递全部稳定判 WINDOW_CLOSED，不会改口
        assert verdicts[f"closed-{i}"] == ["window_closed"] * 4
        # 窗口开放：恰好接受一次，其余判 DUPLICATE
        assert sorted(verdicts[f"open-{i}"]) == [
            "accepted", "duplicate", "duplicate", "duplicate",
        ]
    agg = pipe.aggregator.get_aggregate("metric:cpu", 2000)
    assert agg.count == 10 and agg.total == sum(range(10))
    logger.info(
        "并发混合场景：closed 键 4 次均=window_closed，open 键恰好接受一次，"
        "窗口=[2000,3000) count=%d", agg.count,
    )


# ---------------------------------------------------------------------------
# 10. 无快照崩溃重启：flush 的封口事实由 WAL 水位标记恢复，
#     连"新序列写旧窗口"也稳定判 WINDOW_CLOSED
# ---------------------------------------------------------------------------
def test_backfill_restart_restores_closed_horizon_from_wal(tmp_path):
    data_dir = tmp_path / "s"
    pipe = _pipe(data_dir)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))
    pipe.flush(watermark_ms=WINDOW_MS)       # 封口 [0,1000)，水位标记落 WAL
    before = pipe.backfill(
        [MetricPoint("cpu", 2.0, 100, "b-1")], batch_id="run-before"
    )
    assert before.results[0].reason is RejectReason.WINDOW_CLOSED
    # 不做快照，直接模拟崩溃重启
    restarted = _pipe(data_dir)

    # 已存在序列：窗口已关结论跨重启稳定（sealed 标志也由重放补齐）
    assert restarted.aggregator.get_aggregate("metric:cpu", 0).sealed
    again = restarted.backfill(
        [MetricPoint("cpu", 2.0, 100, "b-1")], batch_id="run-after"
    )
    assert again.results[0].reason is RejectReason.WINDOW_CLOSED
    # 新序列写同一个已翻篇的窗口：同样必须判 WINDOW_CLOSED——
    # 这依赖封口地平线（而非单个聚合的 sealed 标志）被正确恢复
    new_series = restarted.backfill(
        [MetricPoint("mem", 5.0, 100, "m-1")], batch_id="new-series"
    )
    assert new_series.results[0].reason is RejectReason.WINDOW_CLOSED
    # 开放窗口不受影响
    assert restarted.backfill(
        [MetricPoint("mem", 5.0, 1500, "m-2")], batch_id="open-window"
    ).results[0].accepted
    logger.info(
        "无快照重启：水位标记恢复封口地平线，b-1/m-1 均稳定判 window_closed",
    )
