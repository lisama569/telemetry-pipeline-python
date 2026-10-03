"""运维批量补报/历史重放测试。

覆盖：
- 批量逐条结论、坏数据不拖垮整批、收/挡汇总与各原因计数；
- 唯一且固定的判定优先级（窗口已关 vs 限流必须分得清清楚楚）；
- 同一份输入乱序/切块/多线程并发，逐条结论与最终聚合完全一致；
- 整批重复补报不重复计数，两次结论对得上；
- 补报额度跨重启延续，不因子进程重启清零而放行；
- 与在线采集并存、不回归（在线键同样参与去重）。

日志显式打印：窗口区间、幂等键、每条判定依据（由 backfill 模块 INFO 输出，
pytest 配置 --log-cli-level=INFO，跑测时在控制台可见）。
"""
from __future__ import annotations

import logging
import threading

import pytest

from telemetry.backfill import BackfillConfig
from telemetry.ingest import IngestionConfig
from telemetry.models import (
    LogEvent,
    LogLevel,
    MetricPoint,
    RejectReason,
)
from telemetry.pipeline import TelemetryPipeline

logger = logging.getLogger("telemetry.test_backfill")
W = 1000


def _mk(key, ts, v=1.0, name="cpu", labels=None):
    return MetricPoint(name, v, ts, key, labels or {})


def _open_pipe(tmp, *, quota=-1, cardinality=1000, lateness=0):
    return TelemetryPipeline(
        str(tmp / "bf"),
        window_size_ms=W,
        allowed_lateness_ms=lateness,
        ingestion_config=IngestionConfig(
            max_cardinality=cardinality, rate_burst=10_000,
            rate_limit_per_sec=10_000,
        ),
        backfill_config=BackfillConfig(quota_total=quota),
    )


def _keyed(report):
    """按幂等键返回结论，便于跨输入顺序对比。"""
    return {r.result.idempotency_key: r.result for r in report.results}


# ---------------------------------------------------------------------------
# 1) 批量逐条结论 + 坏数据隔离 + 汇总
# ---------------------------------------------------------------------------
def test_batch_per_item_results_and_summary(tmp_path):
    pipe = _open_pipe(tmp_path)
    pipe.ingest(_mk("online-1", 100, 1.0))
    pipe.flush(W)  # 封口 [0,1000)

    batch = [
        _mk("bf-ok-1", 1500, 5.0),
        _mk("bf-closed", 200, 9.0),                 # 窗口已关
        _mk("online-1", 1500, 5.0),                 # 与在线键重复
        _mk("", 1600, 5.0),                         # 坏输入：空幂等键
        LogEvent("boom", LogLevel.ERROR, 1600, "bf-log"),  # 日志也能补
        MetricPoint("cpu", float("nan"), 1600, "bf-nan"),  # 坏输入：NaN
    ]
    rep = pipe.backfill(batch)

    assert rep.total == 6
    assert rep.accepted == 2
    assert rep.rejected == 4
    assert rep.reason_counts == {
        "window_closed": 1, "duplicate": 1, "bad_input": 2,
    }
    # 逐条按下标对齐，坏条目不拖垮其它条目
    by_idx = {r.index: r.result for r in rep.results}
    assert by_idx[0].accepted and by_idx[0].window_start_ms == 1000
    assert by_idx[1].reason is RejectReason.WINDOW_CLOSED
    assert by_idx[2].reason is RejectReason.DUPLICATE
    assert by_idx[3].reason is RejectReason.BAD_INPUT
    assert by_idx[4].accepted  # 日志条目接受
    assert by_idx[5].reason is RejectReason.BAD_INPUT

    # 汇总与聚合一致：接受的两条并入 [1000,2000)
    agg = pipe.aggregator.get_aggregate("metric:cpu", 1000)
    assert agg is not None and agg.count == 1
    log_agg = pipe.aggregator.get_aggregate("log:ERROR", 1000)
    assert log_agg is not None and log_agg.count == 1

    logger.info(
        "窗口=[0,1000) 已封口；开放窗口=[1000,2000) 汇总=%s", rep.reason_counts
    )


# ---------------------------------------------------------------------------
# 2) 固定唯一优先级：WINDOW_CLOSED 永远先于限流/基数
# ---------------------------------------------------------------------------
def test_closed_window_never_reported_as_rate_limit(tmp_path):
    # 额度只给 1：制造"窗口已关 + 额度也已用尽"同时成立
    pipe = _open_pipe(tmp_path, quota=1)
    pipe.ingest(_mk("seed", 100, 1.0))
    pipe.flush(W)  # 封 [0,1000)
    # 用掉唯一补报额度（开放窗口）
    used = pipe.backfill([_mk("q-1", 1500, 1.0)])
    assert used.accepted == 1

    closed_and_exhausted = _mk("old", 300, 1.0)
    open_and_exhausted = _mk("new", 1500, 1.0)
    rep = pipe.backfill([closed_and_exhausted, open_and_exhausted])
    by_key = _keyed(rep)
    # 同一压力（额度为 0）下：关窗的必须是事实结论 window_closed，
    # 仍在接收的窗口才是 rate_limit，两者绝不混用
    assert by_key["old"].reason is RejectReason.WINDOW_CLOSED
    assert by_key["new"].reason is RejectReason.RATE_LIMIT
    logger.info(
        "幂等键=old 窗口=[0,1000) 依据=%s；幂等键=new 窗口=[1000,2000) 依据=%s",
        by_key["old"].detail, by_key["new"].detail,
    )


def test_priority_duplicate_beats_window_closed(tmp_path):
    pipe = _open_pipe(tmp_path)
    pipe.ingest(_mk("k1", 100, 1.0))
    pipe.flush(W)  # k1 所在窗口已封口
    rep = pipe.backfill([_mk("k1", 200, 1.0)])
    # 已接受过的键即使落在关窗窗口，结论也稳定是 DUPLICATE（幂等优先）
    assert _keyed(rep)["k1"].reason is RejectReason.DUPLICATE


def test_cardinality_before_quota_priority(tmp_path):
    pipe = _open_pipe(tmp_path, quota=5, cardinality=1)
    # 已有一个序列 metric:cpu
    pipe.ingest(_mk("seed", 100, 1.0))
    pipe.flush(2000)
    # 新序列落在开放窗口 [3000,4000)；额度充足但基数将超限 -> cardinality
    rep = pipe.backfill([_mk("m1", 3500, 1.0, name="mem")])
    assert _keyed(rep)["m1"].reason is RejectReason.CARDINALITY_LIMIT


# ---------------------------------------------------------------------------
# 3) 同一份输入：乱序 / 切块 / 并发，结论与聚合完全一致
# ---------------------------------------------------------------------------
def _scenario_events():
    events = []
    # 全部落在开放窗口 [2000,5000)，时间戳打散（乱序）；
    # 起点水位为 2000，且这些数据不会把任何窗口越过封口点，
    # 因此"整批一次"与"切块并发"面对的窗口事实完全相同，结论必须一致。
    for i in range(40):
        events.append(_mk(f"e-{i:02d}", 2000 + (i * 271) % 3000, float(i)))
    # 两个坏条目
    events.append(MetricPoint("cpu", float("nan"), 2200, "bad-1"))
    events.append(_mk("", 2300, 1.0))
    return events


def _stats(pipe):
    return sorted(
        (a.series, a.window_start_ms, a.count, round(a.total, 9),
         a.minimum, a.maximum, a.sealed)
        for a in pipe.aggregator.aggregates()
    )


def _indexed(report):
    """按下标返回 (status, reason, window)，跨输入顺序可直接对齐。"""
    return {
        r.index: (r.result.status.value,
                  r.result.reason.value if r.result.reason else None,
                  r.result.window_start_ms)
        for r in report.results
    }


def _verdict_by_key(pipe, events):
    rep = pipe.backfill(list(events))
    return {k: (v.status.value, v.reason.value if v.reason else None,
                v.window_start_ms) for k, v in _keyed(rep).items() if k}, rep


def test_backfill_order_independent(tmp_path):
    events = _scenario_events()
    # 追加：一条落在已封口窗口 0 的旧数据（window_closed），
    # 以及一条与在线已接受键重复的数据（duplicate）
    events.append(_mk("old-closed", 300, 1.0))
    events.append(_mk("seed", 2500, 1.0))

    ref = _open_pipe(tmp_path / "a")
    ref.ingest(_mk("seed", 50, 1.0))
    ref.flush(2000)  # 封掉窗口 [0,1000)、[1000,2000)

    # 参照：按事件时间正序
    ref_v, ref_rep = _verdict_by_key(ref, sorted(events, key=lambda p: p.timestamp_ms))

    # 乱序管道
    shuffled = TelemetryPipeline(
        str(tmp_path / "b"), window_size_ms=W,
        ingestion_config=IngestionConfig(rate_burst=10_000, rate_limit_per_sec=10_000),
        backfill_config=BackfillConfig(-1),
    )
    shuffled.ingest(_mk("seed", 50, 1.0))
    shuffled.flush(2000)
    shuf_v, shuf_rep = _verdict_by_key(shuffled, list(reversed(events)))

    assert shuf_v == ref_v
    assert shuf_rep.accepted == ref_rep.accepted
    assert shuf_rep.reason_counts == ref_rep.reason_counts
    assert _stats(shuffled) == _stats(ref)
    logger.info("乱序补报逐条结论与正序完全一致：%s", ref_rep.reason_counts)


def test_backfill_concurrent_chunks_matches_serial(tmp_path):
    # 干净数据（全部合法、全在开放窗口）：切块并发与整批串行面对的
    # 窗口事实相同，逐条结论、原因计数、聚合必须完全一致。
    events = [_mk(f"c-{i:02d}", 2000 + (i * 271) % 3000, float(i))
              for i in range(48)]

    def fresh(label):
        p = TelemetryPipeline(
            str(tmp_path / label), window_size_ms=W,
            ingestion_config=IngestionConfig(rate_burst=10_000,
                                             rate_limit_per_sec=10_000),
            backfill_config=BackfillConfig(-1),
        )
        p.flush(2000)  # 起点水位固定为 2000
        return p

    serial = fresh("c-serial")
    s_rep = serial.backfill(list(events))
    s_idx = _indexed(s_rep)

    conc = fresh("c-conc")
    reports = []
    barrier = threading.Barrier(8)

    def worker(chunk):
        barrier.wait()
        rep = conc.backfill(chunk)
        # 记录全局事件 -> 结论（切块内下标是相对的，需映射回全局事件）
        reports.append((chunk, rep))

    threads = [threading.Thread(target=worker, args=(events[i::8],)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 合并各切块结论，按全局事件的幂等键对齐
    merged: dict[str, tuple] = {}
    total_acc = 0
    counts: dict[str, int] = {}
    for chunk, rep in reports:
        total_acc += rep.accepted
        for item, point in zip(rep.results, chunk):
            r = item.result
            merged[point.idempotency_key] = (
                r.status.value, r.reason.value if r.reason else None,
                r.window_start_ms,
            )
        for reason, n in rep.reason_counts.items():
            counts[reason] = counts.get(reason, 0) + n

    serial_v = {k: (v.status.value, v.reason.value if v.reason else None,
                    v.window_start_ms) for k, v in _keyed(s_rep).items()}
    assert merged == serial_v
    assert total_acc == s_rep.accepted
    assert counts == s_rep.reason_counts
    assert _stats(conc) == _stats(serial)
    logger.info("并发切块补报合并结论/聚合与串行一致：接受=%d %s", total_acc, counts)


def test_concurrent_same_batch_submissions_agree(tmp_path):
    """同一整批被多个线程同时提交：逐条结论唯一、可接受恰好一次。"""
    events = [_mk(f"d-{i:02d}", 2000 + (i * 199) % 3000, float(i))
              for i in range(32)]
    pipe = _open_pipe(tmp_path / "d")
    pipe.flush(2000)

    reports = []
    barrier = threading.Barrier(6)

    def submit():
        barrier.wait()
        reports.append(pipe.backfill(list(events)))

    threads = [threading.Thread(target=submit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    indexed = [_indexed(r) for r in reports]
    # 每个下标在 6 次提交中：恰好一次 accepted，其余全部 duplicate，
    # 即"同一键无论多少线程同时补，只计数一次"，且结论集合确定。
    for i in range(len(events)):
        verdicts = [m[i] for m in indexed]
        accepted_n = sum(1 for status, _, _ in verdicts if status == "accepted")
        dup_n = sum(1 for _, reason, _ in verdicts if reason == "duplicate")
        assert accepted_n == 1, (i, verdicts)
        assert dup_n == len(verdicts) - 1, (i, verdicts)

    # 接受总数 == 键数（没有重复计数）
    accepted_total = sum(r.accepted for r in reports)
    assert accepted_total == len(events)

    # 再重跑一遍：没有任何新数据被计数，全部 duplicate
    again = pipe.backfill(list(events))
    assert again.accepted == 0
    assert all(v.reason is RejectReason.DUPLICATE for v in _keyed(again).values())
    logger.info("同一整批并发+重跑：唯一接受数=%d，重跑新增接受=%d",
                accepted_total, again.accepted)


# ---------------------------------------------------------------------------
# 4) 整批重复补报不重复计数，两次结论对得上
# ---------------------------------------------------------------------------
def test_repeat_batch_is_idempotent(tmp_path):
    pipe = _open_pipe(tmp_path)
    pipe.ingest(_mk("online-1", 100, 1.0))
    pipe.flush(W)
    batch = [
        _mk("a", 1500, 2.0),
        _mk("b", 200, 3.0),    # closed
        _mk("online-1", 1500, 2.0),
        _mk("c", 1600, 4.0),
    ]
    r1 = pipe.backfill(list(batch))
    accepted_keys = {k for k, v in _keyed(r1).items() if v.accepted}
    total_before = {
        (a.series, a.window_start_ms): (a.count, round(a.total, 9))
        for a in pipe.aggregator.aggregates()
    }

    r2 = pipe.backfill(list(batch))
    # 第一次接受的键第二次必须全部 DUPLICATE；关窗/坏结论保持不变
    for k in accepted_keys:
        assert _keyed(r2)[k].reason is RejectReason.DUPLICATE
    assert r2.accepted == 0
    assert _keyed(r2)["b"].reason is RejectReason.WINDOW_CLOSED
    total_after = {
        (a.series, a.window_start_ms): (a.count, round(a.total, 9))
        for a in pipe.aggregator.aggregates()
    }
    assert total_after == total_before  # 聚合未被二次计数
    logger.info("重复补报幂等：首次接受=%s，二次全部 duplicate，聚合不变",
                sorted(accepted_keys))


# ---------------------------------------------------------------------------
# 5) 额度 + 结论跨重启延续（仅 WAL、快照 两种恢复路径）
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("use_snapshot", [False, True])
def test_quota_and_verdicts_survive_restart(tmp_path, use_snapshot):
    data_dir = str(tmp_path / "store")

    def fresh():
        return TelemetryPipeline(
            data_dir, window_size_ms=W,
            ingestion_config=IngestionConfig(rate_burst=10_000,
                                             rate_limit_per_sec=10_000),
            backfill_config=BackfillConfig(quota_total=3),
        )

    pipe = fresh()
    pipe.ingest(_mk("online-1", 100, 1.0))
    pipe.flush(W)
    batch = [_mk(f"bf-{i}", 1500 + i * 10, float(i + 1)) for i in range(5)]
    r1 = pipe.backfill(batch)
    assert r1.accepted == 3
    assert _keyed(r1)["bf-3"].reason is RejectReason.RATE_LIMIT
    assert _keyed(r1)["bf-4"].reason is RejectReason.RATE_LIMIT

    if use_snapshot:
        pipe.snapshot()
        # 快照后再接受一条补报（额度应继续累计，不落回快照口径）
        # 额度已 3/3，仍应被挡；重启后也一样
    before = pipe.backfills.quota_used

    pipe2 = fresh()
    assert pipe2.backfills.quota_used == before == 3  # 重启没有清零额度
    r2 = pipe2.backfill(batch)
    # 逐条结论与重启前重放一致：前 3 个 duplicate，后 2 个仍是 rate_limit
    k2 = _keyed(r2)
    for i in range(3):
        assert k2[f"bf-{i}"].reason is RejectReason.DUPLICATE
    for i in range(3, 5):
        assert k2[f"bf-{i}"].reason is RejectReason.RATE_LIMIT
    assert r2.accepted == 0
    logger.info("跨重启（snapshot=%s）额度延续 %d/3，逐条结论一致",
                use_snapshot, pipe2.backfills.quota_used)


def test_restart_accepted_backfills_aggregate_identically(tmp_path):
    data_dir = str(tmp_path / "agg")

    def fresh():
        return TelemetryPipeline(
            data_dir, window_size_ms=W,
            ingestion_config=IngestionConfig(rate_burst=10_000,
                                             rate_limit_per_sec=10_000),
            backfill_config=BackfillConfig(-1),
        )

    p1 = fresh()
    p1.ingest(_mk("seed", 100, 1.0))
    p1.flush(W)
    batch = [_mk(f"e-{i}", (i * 137) % 4000 + 1000, float(i)) for i in range(20)]
    rep = p1.backfill(batch)
    accepted_keys = {k for k, v in _keyed(rep).items() if v.accepted}
    before = _stats(p1)

    p1.snapshot()
    p2 = fresh()
    assert _stats(p2) == before  # 补报事件随 WAL/快照恢复，聚合一致
    # 重启后重放同一历史：已接受键稳定 duplicate，无新增计数
    again = p2.backfill(batch)
    assert again.accepted == 0
    assert all(v.reason is RejectReason.DUPLICATE
               for k, v in _keyed(again).items() if k in accepted_keys)
    assert _stats(p2) == before


# ---------------------------------------------------------------------------
# 6) 跨独立实例可复现：整批串行 vs 乱序切块并发，聚合必须一致
# ---------------------------------------------------------------------------
def test_reproducible_across_independent_instances(tmp_path):
    events = [_mk(f"r-{i:02d}", 2000 + (i * 977) % 3000, float(i) * 1.5)
              for i in range(64)]

    serial = TelemetryPipeline(
        str(tmp_path / "inst-serial"), window_size_ms=W,
        ingestion_config=IngestionConfig(rate_burst=10_000, rate_limit_per_sec=10_000),
        backfill_config=BackfillConfig(-1),
    )
    serial.flush(2000)
    serial.backfill(list(events))

    conc = TelemetryPipeline(
        str(tmp_path / "inst-conc"), window_size_ms=W,
        ingestion_config=IngestionConfig(rate_burst=10_000, rate_limit_per_sec=10_000),
        backfill_config=BackfillConfig(-1),
    )
    conc.flush(2000)
    shuffled = list(reversed(events))

    def w(chunk):
        conc.backfill(chunk)

    threads = [threading.Thread(target=w, args=(shuffled[i::8],)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert _stats(conc) == _stats(serial)
    logger.info("跨实例（整批串行 vs 乱序切块并发）聚合字节级一致")


def test_closed_window_verdict_stable_under_write_pressure(tmp_path):
    """无论当时在线写入压力多大、是否触发限流，关窗补报永远是 window_closed。"""
    pipe = TelemetryPipeline(
        str(tmp_path / "pressure"), window_size_ms=W,
        ingestion_config=IngestionConfig(rate_burst=1, rate_limit_per_sec=0.0),
        backfill_config=BackfillConfig(-1),
    )
    pipe.ingest(_mk("seed", 100, 1.0))
    pipe.flush(W)
    old = _mk("old", 300, 1.0)

    verdicts: list = []

    def hammer_online():
        for i in range(50):
            pipe.ingest(_mk(f"press-{threading.get_ident()}-{i}", 5000, 1.0))

    def hammer_backfill():
        for _ in range(50):
            verdicts.append(pipe.backfill([old]).results[0].result.reason)

    threads = [threading.Thread(target=hammer_online) for _ in range(4)]
    threads += [threading.Thread(target=hammer_backfill) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert verdicts, "必须产生补报结论"
    assert set(verdicts) == {RejectReason.WINDOW_CLOSED}
    logger.info("写压力下关窗补报结论恒定=%s（未混入 rate_limit）",
                {r.value for r in set(verdicts)})


# ---------------------------------------------------------------------------
# 7) 补报与在线采集并存：在线限流不影响补报确定性，反之亦然
# ---------------------------------------------------------------------------
def test_backfill_independent_of_online_token_bucket(tmp_path):
    # 在线令牌桶很小，在线写很快被 RATE_LIMIT；补报走独立的持久化额度，
    # 结论只取决于窗口事实与已落盘键，不受在线写压力影响。
    pipe = TelemetryPipeline(
        str(tmp_path / "mix"), window_size_ms=W,
        ingestion_config=IngestionConfig(rate_burst=1, rate_limit_per_sec=0.0),
        backfill_config=BackfillConfig(-1),
    )
    pipe.ingest(_mk("o1", 100, 1.0))
    online_rate_limited = 0
    for i in range(20):
        r = pipe.ingest(_mk(f"o-{i}", 100, 1.0))
        if r.reason is RejectReason.RATE_LIMIT:
            online_rate_limited += 1
    assert online_rate_limited >= 1

    pipe.flush(W)
    # 补报开放窗口：在线桶已空也不影响；关窗的仍是 window_closed
    rep = pipe.backfill([_mk("b1", 1500, 1.0), _mk("b2", 200, 1.0)])
    k = _keyed(rep)
    assert k["b1"].accepted
    assert k["b2"].reason is RejectReason.WINDOW_CLOSED
    logger.info("在线限流压力下补报结论稳定：b1=accepted, b2=window_closed")
