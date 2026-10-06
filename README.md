# 本地可观测数据管道（Python）

用**应用内状态 + 本地文件存储**模拟遥测数据的「采集 → 去重/限流 → 窗口聚合 →
告警判定 → 导出」闭环。重点不是吞吐，而是在**并发写入、乱序到达、写入限流**三类
场景下给出**一致且可解释**的结论：每一次拒绝都有可区分原因，每一次聚合都可重放，
每一次告警都有判定依据。

## 1. 目录结构

```
telemetry/
  models.py       数据模型与枚举（事件、IngestResult、Aggregate、AlertRule/状态…）
  clock.py        窗口对齐与可注入时钟
  storage.py      本地持久化：仅追加 WAL + 原子快照
  ingest.py       采集入口：幂等去重、窗口预检、基数上限、令牌桶限流
  aggregation.py  固定窗口聚合、乱序/迟到、水位游标、封口
  alerting.py     告警规则、PENDING/FIRING/SUPPRESSED/RESOLVED 状态机
  backfill.py     批量补报入口：整批投递、逐条结论、汇总报告
  pipeline.py     门面：装配上述组件，负责闭环与重启恢复
  exporter.py     聚合/告警报告的确定性 JSON 导出
main.py           FastAPI 本地演示服务
tests/            单元测试 + 并发/乱序/限流/补报场景测试（日志打印窗口/幂等键/依据）
```

## 2. 数据模型

| 类型 | 字段要点 | 说明 |
|---|---|---|
| `MetricPoint` | `name, value, timestamp_ms, idempotency_key, labels{str:str}` | 指标点，`timestamp_ms` 是**事件时间**（非处理时间） |
| `LogEvent` | `message, level, timestamp_ms, idempotency_key, labels` | 日志；ERROR 计 1、其余计 0，可用于错误率聚合 |
| `IngestResult` | `status, idempotency_key, reason, detail, window_start_ms` | 每次写入的结论；拒绝时 `reason` 必填 |
| `Aggregate` | `count, total, min, max, avg, last_*, sealed` | 单序列单窗口聚合，全部为顺序无关统计量 |
| `AlertRule` | `series, comparator, threshold, for_windows, resolve_windows` | 阈值 + 触发/恢复所需连续窗口数 |
| `AlertStatus` | `state, breach_count, ok_count, changed_window_ms, last_value` | 规则运行时状态 |
| `AlertEvaluation` | `window_start_ms, value, state, triggered, resolved, reason` | 单窗口判定结论，`reason` 即判定依据 |

**序列（series）规范化命名**：`metric:<name>{k=v,...}` / `log:<LEVEL>{k=v,...}`，
标签按 key 排序后拼接，因此同一组标签无论传入字典顺序如何都映射到同一序列。

本地存储目录（默认 `./data`）：

- `events.log`：仅追加 JSONL（WAL），**只记录已接受事件**；
  事件记录带 `channel`（`online`/`backfill`）标记，另含显式 flush 产生的
  **水位标记**记录（`{"type": "watermark", ...}`）；
- `snapshot.json`：聚合 + 告警 + 已接受幂等键/序列集合的原子快照；
- `limiter.json`：限流令牌桶余量，每次成功消费令牌后原子落盘；
- `report.json`：导出报告（由 `/export` 或 `Exporter` 生成）。

## 3. 窗口与乱序语义

- 固定大小窗口（默认 1000ms），**左闭右开** `[start, start+size)`，
  时间戳向下取整对齐窗口边界 → 乱序到达的同一批事件必然落入同一窗口。
- 聚合只计算 `count/sum/min/max/avg`，均与输入顺序无关；
  `last_timestamp_ms/last_value` 仅用于展示，不参与统计结论。
  因此窗口可以被反复重放，结论稳定（见 `test_out_of_order_is_stable`）。
- **水位线（watermark）单调不减**，只从两个来源推进：
  - **在线（online）通道**的事件随事件时间推进水位（实时数据到了，
    旧窗口自然翻篇）；
  - `flush(watermark_ms)` 显式推进（只能前进不能后退，
    显式回拨游标 → `CURSOR_BACKWARD`，状态不变）。
  **补报（backfill）通道的事件只读水位、绝不推进**——这是补报确定性的
  基石：同一批历史数据无论怎么排序/分批/并发投递，窗口开闭判定都相同；
  补报里混进时间戳异常大的记录，也不会把在线仍在接收的窗口提前关掉。
  显式 flush 的推进会以水位标记写入 WAL，仅 WAL 恢复时也能精确还原。
- **封口（seal）**：当 `window_end + allowed_lateness <= watermark`
  时窗口封口并冻结为不可变快照，随后驱动一次全部告警规则判定。
  封口幂等：重复 flush 不会重复产出、不改变结果。
- **迟到数据**：
  - 窗口未封口（仍在迟到宽限 `allowed_lateness_ms` 内）→ 正常并入，结论仍确定；
  - 窗口已封口 → 返回 `WINDOW_CLOSED`，事件**不落 WAL、不改动聚合**。

## 4. 去重与限流规则

写入在同一把锁内按**固定优先级**依次判定，拒绝原因互不相同、绝不静默丢弃，
一条数据同时踩到多个条件时结论唯一、不随线程调度漂移：

| 顺序 | 拒绝原因 `RejectReason` | 判定条件 |
|---|---|---|
| 1 | `BAD_INPUT` | 空幂等键/非法时间戳/空指标名/NaN/非字符串标签… |
| 2 | `DUPLICATE` | 幂等键已在此前**成功**写入（含重启后从状态恢复的键） |
| 3 | `WINDOW_CLOSED` | 目标窗口已封口/已被水位翻过（**事实性预检**，见下） |
| 4 | `CARDINALITY_LIMIT` | 新序列会使不同序列数超过 `max_cardinality` |
| 5 | `RATE_LIMIT` | 所在通道的令牌桶令牌不足 |

**双通道独立限流**：在线与补报各用各的令牌桶（在线：`rate_burst` /
`rate_limit_per_sec`；补报：`backfill_rate_burst` /
`backfill_rate_limit_per_sec`，缺省回落到在线配置），
补报再猛也挤不占在线配额，反之亦然。令牌桶时钟可注入
（`TelemetryPipeline(..., clock=...)`，默认单调时钟）：注入固定/手动时钟后，
限流结论与机器快慢、调度抖动完全无关，可逐条复现。

**事实性结论优先于瞬时压力**：`WINDOW_CLOSED` 由窗口层提供只读预检
（`check_window_open`），在去重之后、限流之前判定。窗口封口与水位都是单调的，
因此"窗口已关"一旦成立就永远成立——无论当时写入压力多大、令牌桶是否刚好耗尽，
落在已封口窗口上的数据都稳定判为 `WINDOW_CLOSED`，不会被说成 `RATE_LIMIT`；
反过来，窗口仍在接收、只是写得太快被挡下的，判为 `RATE_LIMIT`，两者严格区分。
窗口已关的补报**不消耗**限流令牌，不会挤占在线写入配额。

关键一致性约定：

- 幂等键、基数集合**只有在下游（聚合+WAL）成功后才提交**，
  因此任何拒绝都不会污染已有聚合或"抢占"幂等键；
- 被拒事件不写 WAL，重放结果与在线聚合天然一致；
- 所有判定在锁内完成，多线程并发下同一幂等键**恰好接受一次**；
- 限流令牌桶余量**每次成功消费后落 `limiter.json`**，并随快照持久化：
  进程停掉再起来（即便没来得及做快照）已用掉的配额继续有效，
  **不会出现重启一次等于限额清零**、原本该挡的写入被放行的情况。

## 5. 告警语义

状态机（每个封口窗口判定一次）：

```
PENDING ──连续 for_windows 个窗口超阈值──▶ FIRING      (triggered=True，仅此一次)
FIRING  ──本窗口恢复──▶ SUPPRESSED                    (进入恢复确认期)
SUPPRESSED ──再次超阈值──▶ SUPPRESSED                 (短时抖动，抑制重复触发)
SUPPRESSED ──连续 resolve_windows 个窗口恢复──▶ RESOLVED (resolved=True)
RESOLVED ──再次连续超阈值──▶ FIRING                   (新一轮)
```

- 窗口无聚合数据（`value=None`）：跳过判定且**不改变**计数/状态；
- 单条规则评估抛异常（如规则定义损坏）→ 该规则产出一条带"异常已隔离"原因的
  结论，**不阻塞其它规则与其它链路**；
- **规则更新只替换现行定义、计数器清零并回到 PENDING，历史判定记录
  （`alert_history`）永不重算、不改写**；
- 判定历史随快照持久化，重启后状态（FIRING/SUPPRESSED…）完整恢复。

## 6. 批量补报与重放（运维补数）

运维手里的离线历史数据（事件时间是过去、可能乱序、可能整批重复提交、
可能横跨已翻篇的窗口）可以**一次性整批投递**：

```python
report = pipe.backfill(events, batch_id="ops-2026-10-01")
# 或 HTTP：POST /backfill {"batch_id": "...", "metrics": [...], "logs": [...]}
```

语义约定：

- **逐条结论，整批不失败**：每条事件独立走判定链
  （校验 → 去重 → 窗口预检 → 基数 → 限流），单条有问题只影响自己；
  返回 `BackfillReport`：总数、接受数、拒绝数、按原因分类的拒绝计数，
  以及每条的状态/原因/依据/目标窗口，日志同步打印窗口区间、幂等键与判定依据；
- **结论确定，与切分/顺序/并发/机器快慢无关**：补报走独立的
  `backfill` 通道——**只读水位、绝不推进水位**，窗口开闭判定只取决于
  在线水位与显式 flush，因此同一批历史数据无论怎么排序、分成几批、
  几线程并发、在快慢不同的机器上跑，每个键的最终处置（接受/拒绝及原因）
  与最终聚合都一模一样；判定优先级固定
  （`BAD_INPUT > DUPLICATE > WINDOW_CLOSED > CARDINALITY_LIMIT > RATE_LIMIT`），
  一条数据同时踩到多个条件时结论唯一，不随线程调度漂移；
- **事实性结论不被瞬时压力掩盖**：落在已封口窗口上的补报稳定判
  `WINDOW_CLOSED`（不消耗令牌、不受限流压力影响）；窗口仍开放只是
  写得太快被挡的，判 `RATE_LIMIT`——两类原因严格分开；
- **重复补报幂等**：同一批再次投递时，已接受条目稳定判 `DUPLICATE`，
  不重复落盘、不重复计数，两次报告的结论逐条对得上；
- **跨重启延续**：去重键、序列集合、聚合状态、水位（含显式 flush 的
  推进，经 WAL 水位标记还原）、限流配额余量（`limiter.json` + 快照）
  都随快照/WAL 恢复，重启后重放同一段历史，逐条结论与最终聚合与重启前一致。

**与在线采集的兼容范围**：补报与在线实时上报共用同一条判定链与同一把锁，
但**配额各自独立**（补报耗尽自己的令牌桶不影响在线写入，反之亦然），
且**补报不推进水位**——补报历史不会把在线仍在接收的窗口提前关掉，
紧接着的实时上报照常接收；补报落在仍开放的迟到窗口内正常并入聚合，
落在已封口窗口上被明确拒绝；告警仍只在 `flush` 封口时判定一次，
补报不会触发重复告警。注意：若在线实时数据已把水位推过某窗口，
并发补报该窗口的数据会被判 `WINDOW_CLOSED`——这是"窗口确实已翻篇"
的事实性结论，而非补报与在线互相干扰。

**限流结论的边界**：`RATE_LIMIT` 本质是背压信号——被拒的条目在配额
恢复后重试可被接受（这正是限流的意义）。注入固定时钟后限流结论可
逐条复现；使用真实时钟时，同一输入在不同机器上只有"恰好压在配额
边界上"的条目可能在 `RATE_LIMIT`/`accepted` 之间变化，其余结论
（去重、窗口已关、非法输入）均与机器快慢无关。

## 7. 进程重启恢复

构造 `TelemetryPipeline` 时自动执行：

1. 读 `snapshot.json`（无快照则从空状态开始）；
2. 按序重放 `events.log`：事件记录走 **trusted 通道**并入聚合
   （历史已接受事实，不受重启后更高水位/封口影响），且只有
   `channel=online` 的记录推进水位（与运行期行为一致）；
   **水位标记**记录精确还原显式 flush 的推进——包括"flush 推进超过
   最大事件时间"的部分，重启前后"窗口已关"的判定保持一致；
   已接受事件同时补入去重与基数集合；
3. 恢复限流状态：优先读 `limiter.json`（每次消费后落盘，是最新状态），
   不存在则回落到快照内嵌的限流字段（兼容旧版单桶格式）——
   重启前已用掉的配额继续有效，重启本身不会重置限额。

运行期调用 `pipeline.snapshot()` 会用「临时文件 + fsync + 原子 rename」
写入快照（聚合 + 告警 + 幂等键/序列集合 + 限流余量）并清空 WAL，
保证不存在半截文件。

## 8. 本地验证

```bash
# 安装依赖（Python >= 3.14）
uv sync

# 全部单测（pytest 会在控制台打印 INFO 日志：窗口区间/幂等键/判定依据）
uv run pytest

# 只看并发+乱序+限流场景
uv run pytest tests/test_observability_scenarios.py -s

# 只看批量补报/重放场景（日志含窗口区间、幂等键、逐条判定依据）
uv run pytest tests/test_backfill.py -s

# 只看补报确定性场景（乱序/分批/并发/跨重启/机器快慢）
uv run pytest tests/test_backfill_determinism.py -s

# 启动 HTTP 演示服务
uv run uvicorn main:app --reload
# 然后可尝试：
#   POST /metrics   上报指标
#   POST /logs      上报日志
#   POST /backfill  批量补报历史事件（逐条结论 + 汇总）
#   POST /flush     封口并跑告警
#   GET  /aggregates、GET /alerts
#   POST /snapshot、POST /export
```

可配置环境变量：`TELEMETRY_DATA_DIR`、`TELEMETRY_WINDOW_MS`、
`TELEMETRY_LATENESS_MS`、`TELEMETRY_MAX_CARDINALITY`、
`TELEMETRY_RATE_PER_SEC`、`TELEMETRY_RATE_BURST`。

### 快速脚本式验证

```python
from telemetry.pipeline import TelemetryPipeline
from telemetry.ingest import IngestionConfig
from telemetry.models import MetricPoint, AlertRule

pipe = TelemetryPipeline("./data", window_size_ms=1000, allowed_lateness_ms=500,
                         ingestion_config=IngestionConfig(max_cardinality=100,
                                                          rate_limit_per_sec=1000))
pipe.alerts.add_rule(AlertRule("cpu_high", "metric:cpu", ">", 5.0, for_windows=2))

pipe.ingest(MetricPoint("cpu", 8.0, 100, "evt-1"))
pipe.ingest(MetricPoint("cpu", 9.0, 1100, "evt-2"))
print(pipe.ingest(MetricPoint("cpu", 8.0, 100, "evt-1")).reason)  # RejectReason.DUPLICATE

# 批量补报：整批投递，逐条结论 + 汇总
bf = pipe.backfill([MetricPoint("cpu", 7.0, 200, "hist-1"),
                    MetricPoint("cpu", 7.0, 200, "hist-1")], batch_id="ops-1")
print(bf.accepted, bf.rejected_by_reason)   # 1 {'duplicate': 1}

report = pipe.flush(watermark_ms=3000)      # 封口 + 告警判定
for e in report.evaluations:
    print(e.window_start_ms, e.state.value, e.reason)
pipe.export("./data/report.json")
```

## 9. 测试覆盖对照

| 需求 | 测试 |
|---|---|
| 并发上报、同键恰好一次 | `test_concurrent_duplicates_accepted_once`、`test_concurrent_ingest_consistent_aggregation` |
| 乱序到达聚合一致 | `test_out_of_order_is_stable`、场景测试与影子管道对比 |
| 迟到/封口拒绝 | `test_late_arrival_before_seal_is_accepted`、`test_late_arrival_within_lateness_window` |
| 游标回退拒绝 | `test_cursor_backward_rejected_on_pipeline` 等 |
| 限流/基数/去重原因可区分 | `test_*_limit`、`test_reject_reasons_are_distinguishable`、`test_reason_separation_under_concurrency` |
| 告警触发/恢复/抖动抑制 | `test_trigger_and_resolve`、`test_flapping_suppressed_no_duplicate_trigger` |
| 规则更新不改写历史 | `test_rule_update_keeps_history` |
| 单规则失败隔离 | `test_rule_failure_isolation` |
| 重启恢复（WAL / 快照+WAL） | `test_restart_recovers_state_and_still_dedups` |
| 导出确定性 | `test_export_report_is_deterministic` |
| 批量补报逐条结论与汇总 | `test_backfill_batch_mixed_outcomes`、`test_backfill_endpoint_per_item_verdicts` |
| 窗口已关不被限流掩盖、不耗配额 | `test_backfill_window_closed_never_masked_by_rate_limit` |
| 并发补报同键恰好一次 | `test_backfill_concurrent_submission_accepted_once` |
| 同一批重复补报幂等 | `test_backfill_repeat_submission_is_idempotent` |
| 补报跨重启重放一致（WAL / 快照） | `test_backfill_restart_replay_consistent` |
| 限流配额跨重启延续 | `test_backfill_quota_persists_across_restart`、`test_restart_without_snapshot_preserves_quota_and_dedup` |
| 补报与在线采集兼容 | `test_backfill_coexists_with_online_ingest`、`test_backfill_and_online_quotas_are_independent` |
| 补报结论与排序/切分无关 | `test_verdicts_independent_of_order_and_splitting` |
| 并发分批补报确定性 | `test_concurrent_split_submission_deterministic` |
| 补报不提前关掉在线窗口 | `test_backfill_does_not_close_online_windows` |
| 显式水位跨重启精确还原 | `test_restart_without_snapshot_preserves_explicit_watermark` |
| 限流结论与机器快慢无关（注入时钟） | `test_rate_limit_verdicts_machine_speed_independent` |
| 跨重启重复补报逐条一致 | `test_repeat_backfill_across_restart_matches` |
