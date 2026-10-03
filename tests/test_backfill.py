"""批量补报（backfill）与重放：逐条结论、并发确定、重复幂等、跨重启恢复。

日志中显式打印：窗口区间、幂等键、每条判定依据，便于离线复核。
"""
import logging
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
# 2. WINDOW_CLOSED 是事实性结论：限流压力再大也不会被说成 RATE_LIMIT，
#    且窗口已关的补报不消耗在线写入配额；两类原因严格区分
# ---------------------------------------------------------------------------
def test_backfill_window_closed_never_masked_by_rate_limit(tmp_path, caplog):
    pipe = _pipe(tmp_path / "s", rate_burst=2, rate_limit_per_sec=0)
    pipe.ingest(MetricPoint("cpu", 1.0, 100, "online-1"))   # 消耗 1 令牌
    pipe.flush(watermark_ms=WINDOW_MS)                      # 封口 [0,1000)

    with caplog.at_level(logging.INFO, logger="telemetry.backfill"):
        report = pipe.backfill([
            MetricPoint("cpu", 9.0, 100, "c1"),    # 窗口已关 -> WINDOW_CLOSED（不耗令牌）
            MetricPoint("cpu", 9.0, 200, "c2"),    # 窗口已关 -> WINDOW_CLOSED
            MetricPoint("cpu", 5.0, 1500, "o1"),   # 窗口开放 -> 接受（用掉最后 1 令牌）
            MetricPoint("cpu", 6.0, 1600, "o2"),   # 窗口开放但配额耗尽 -> RATE_LIMIT
            MetricPoint("cpu", 9.0, 300, "c3"),    # 仍是 WINDOW_CLOSED，而非 RATE_LIMIT
        ], batch_id="batch-pressure")

    reasons = [r.reason for r in report.results]
    assert reasons == [
        RejectReason.WINDOW_CLOSED,
        RejectReason.WINDOW_CLOSED,
        None,                       # accepted
        RejectReason.RATE_LIMIT,
        RejectReason.WINDOW_CLOSED, # 限流耗尽后，窗口已关的结论依然稳定
    ]
    assert report.rejected_by_reason == {"window_closed": 3, "rate_limit": 1}
    # 已关窗口的补报没有消耗配额：o1 得以接受
    assert pipe.aggregator.get_aggregate("metric:cpu", 1000).total == 5.0
    # 日志中两类原因各自可区分
    assert "原因=window_closed" in caplog.text
    assert "原因=rate_limit" in caplog.text


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
# 7. 补报与在线采集兼容：补报历史不影响在线窗口，在线事件也不被补报干扰
# ---------------------------------------------------------------------------
def test_backfill_coexists_with_online_ingest(tmp_path):
    # 迟到宽限 2000ms：水位 5000 时窗口 [4000,5000) 仍开放，[0,1000) 已翻篇
    pipe = _pipe(tmp_path / "s", allowed_lateness_ms=2000)
    # 在线实时上报
    assert pipe.ingest(MetricPoint("cpu", 1.0, 5000, "live-1")).accepted
    # 补报历史（落在更早的开放窗口内，迟到宽限外的不放行）
    report = pipe.backfill([
        MetricPoint("cpu", 2.0, 4500, "hist-1"),   # 窗口 [4000,5000) 仍开放
        MetricPoint("cpu", 3.0, 500, "hist-2"),    # 窗口 [0,1000) 已被水位翻过
    ], batch_id="mixed")
    assert report.results[0].accepted
    assert report.results[1].reason is RejectReason.WINDOW_CLOSED
    # 在线链路继续可用
    assert pipe.ingest(MetricPoint("cpu", 4.0, 5100, "live-2")).accepted
    assert pipe.aggregator.get_aggregate("metric:cpu", 4000).total == 2.0
    assert pipe.aggregator.get_aggregate("metric:cpu", 5000).total == 5.0
