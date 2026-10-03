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
  ingest.py       采集入口：幂等去重、基数上限、令牌桶限流
  aggregation.py  固定窗口聚合、乱序/迟到、水位游标、封口
  alerting.py     告警规则、PENDING/FIRING/SUPPRESSED/RESOLVED 状态机
  backfill.py     运维批量补报/历史重放：逐条结论、固定优先级、持久化额度
  pipeline.py     门面：装配上述组件，负责闭环与重启恢复
  exporter.py     聚合/告警报告的确定性 JSON 导出
main.py           FastAPI 本地演示服务（含 POST /backfill）
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
  补报事件带 `"source": "backfill"`，在线事件无该字段；
- `snapshot.json`：聚合 + 告警 + 已接受幂等键/序列集合 + **补报已用额度**
  的原子快照；
- `report.json`：导出报告（由 `/export` 或 `Exporter` 生成）。

## 3. 窗口与乱序语义

- 固定大小窗口（默认 1000ms），**左闭右开** `[start, start+size)`，
  时间戳向下取整对齐窗口边界 → 乱序到达的同一批事件必然落入同一窗口。
- 聚合只计算 `count/sum/min/max/avg`，均与输入顺序无关；
  `last_timestamp_ms/last_value` 仅用于展示，不参与统计结论。
  因此窗口可以被反复重放，结论稳定（见 `test_out_of_order_is_stable`）。
- **水位线（watermark）= 已观察到的最大事件时间，单调不减**：
  - 显式把游标往回拨 → 返回 `CURSOR_BACKWARD`，状态不变；
  - `flush(watermark_ms)` 只能前进不能后退。
- **封口（seal）**：当 `window_end + allowed_lateness <= watermark`
  时窗口封口并冻结为不可变快照，随后驱动一次全部告警规则判定。
  封口幂等：重复 flush 不会重复产出、不改变结果。
- **迟到数据**：
  - 窗口未封口（仍在迟到宽限 `allowed_lateness_ms` 内）→ 正常并入，结论仍确定；
  - 窗口已封口 → 返回 `WINDOW_CLOSED`，事件**不落 WAL、不改动聚合**。

## 4. 去重与限流规则

写入在同一把锁内依次做 4 类判定，拒绝原因互不相同、绝不静默丢弃：

| 顺序 | 拒绝原因 `RejectReason` | 判定条件 |
|---|---|---|
| 1 | `BAD_INPUT` | 空幂等键/非法时间戳/空指标名/NaN/非字符串标签… |
| 2 | `DUPLICATE` | 幂等键已在此前**成功**写入（含重启后从状态恢复的键） |
| 3 | `CARDINALITY_LIMIT` | 新序列会使不同序列数超过 `max_cardinality` |
| 4 | `RATE_LIMIT` | 令牌桶（容量 `rate_burst`，补充 `rate_limit_per_sec`）令牌不足 |
| 5 | `WINDOW_CLOSED` / `CURSOR_BACKWARD` | 下游窗口层拒绝 |

关键一致性约定：

- 幂等键、基数集合**只有在下游（聚合+WAL）成功后才提交**，
  因此任何拒绝都不会污染已有聚合或"抢占"幂等键；
- 被拒事件不写 WAL，重放结果与在线聚合天然一致；
- 所有判定在锁内完成，多线程并发下同一幂等键**恰好接受一次**。

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

## 6. 运维批量补报与历史重放

除在线采集（`ingest`）外，管道提供一次性批量补报入口
`pipeline.backfill([...])`（HTTP：`POST /backfill`），面向"运维手里有一批
离线导出的历史上报"的场景：事件时间在过去、可能乱序、可能整批重复提交、
也可能横跨已经封口的窗口。

### 6.1 逐条结论，整批不整体失败

- 每条数据**单独**给出 `IngestResult`，按输入下标对齐（`BackfillReport.results`）；
- 个别条目坏（空幂等键/NaN/非法时间戳…）只判该条 `BAD_INPUT`，
  **不拖垮整批**，其余条目照常处理；
- 汇总给出 `total / accepted / rejected / reason_counts`
  （收了多少、挡了多少、每种原因各多少）。

### 6.2 固定且唯一的判定优先级

一条数据同时踩到多个条件时，结论唯一、优先级固定，**不随线程调度漂移**：

| 顺序 | 结论 | 含义 |
|---|---|---|
| 1 | `BAD_INPUT` | 字段不合法 |
| 2 | `DUPLICATE` | 幂等键此前已接受（含本批更早条目、在线写入、重启恢复） |
| 3 | `WINDOW_CLOSED` | 目标窗口已封口 / 越过水位迟到宽限（**事实性结论**） |
| 4 | `CARDINALITY_LIMIT` | 新序列会突破基数上限 |
| 5 | `RATE_LIMIT` | 补报额度（配额）已用尽——窗口**仍在接收** |

要点：

- **"窗口已关"与"写得太快/额度不足"严格分开**。`WINDOW_CLOSED` 是由封口
  标记 + 水位决定的事实，判定在额度之前，因此无论写入压力多大、是否正好触发
  限流，关窗数据永远报 `window_closed`，绝不会改口成 `rate_limit`；
  反过来仍在接收窗口、只是额度用尽的，报 `rate_limit`，两者不混用。
- 补报额度是**持久化的确定性计数器**，不是在线那套随墙钟补充的令牌桶，
  因此结论与机器快慢、调用时刻无关。

### 6.3 结论稳定、可复现（并发/乱序/跨机器一致）

判定是"**输入集合 + 已落盘事实**"的纯函数：

1. 候选先按 `(事件时间, 幂等键, 序列, 输入下标)` **规范化排序**再裁决，
   与调用方如何切分批次、线程如何调度无关；
2. 整批在管道的同一把写锁内裁决并提交，与在线并发上报严格串行化，
   同一幂等键在任意并发度下**恰好接受一次**；
3. 补报写入**只并入聚合、不推进水位**（水位只由在线数据与显式 `flush` 决定）。
   否则先处理到高时间戳的切块会把后处理切块的低时间戳事件误判成关窗，
   结论就会依赖调度顺序。
4. 聚合本身只有 count/sum/min/max/avg 等顺序无关统计量。

因此：同一份输入，多线程并发、切块提交、或换一台快慢不同的机器，
逐条接受/拒绝判定与最终聚合完全一致（见
`test_backfill_concurrent_chunks_matches_serial`、
`test_reproducible_across_independent_instances`）。

### 6.4 整批重复补报不重复计数

同一批数据再次 `backfill`：上一轮已接受的键稳定判为 `DUPLICATE`，
关窗的仍是 `WINDOW_CLOSED`、额度不足的仍是 `RATE_LIMIT`、坏的仍是
`BAD_INPUT`，`accepted=0`，聚合不发生二次变化，两轮结论对得上
（`test_repeat_batch_is_idempotent`）。

### 6.5 额度跨重启延续

补报已用额度写入 `snapshot.json`，并在恢复时叠加快照之后 WAL 中
`source="backfill"` 的已接受条数。因此：

- 进程重启 / 从本地快照恢复后，**额度不清零**，原本该被额度挡下的补报
  不会因重启被放行；
- 同一历史在重启前后逐条结论与最终聚合一致
  （`test_quota_and_verdicts_survive_restart`，覆盖"仅 WAL"和"快照+WAL"
  两条恢复路径）。

### 6.6 与在线采集的兼容边界

- **共用同一套去重键与序列基数集合**：在线已接受的键，补报会判重复；
  补报接受的键，在线再写也判重复。两者共用同一把写锁，互不覆盖。
- **限流相互独立**：在线仍走令牌桶（墙钟相关，允许突发/恢复）；
  补报走持久化配额（确定性）。在线令牌桶被打满不影响补报对"开放窗口"
  的事实判定（`test_backfill_independent_of_online_token_bucket`）。
- **水位语义**：在线写入随事件时间推进水位；补报不推进水位。
  补报只接受"当前仍开放"窗口的数据；要封掉旧窗口请显式 `flush`。
- 补报接受的事件与在线事件一样落 WAL、进聚合、参与告警与导出，
  并以 `source="backfill"` 标记来源（老数据无该字段，按在线兼容处理）。
- 补报额度由 `BackfillConfig(quota_total=...)` 配置，`-1`（默认）不限；
  HTTP 服务可用环境变量 `TELEMETRY_BACKFILL_QUOTA` 设置。

### 6.7 本地验证

```python
from telemetry.pipeline import TelemetryPipeline
from telemetry.backfill import BackfillConfig
from telemetry.models import MetricPoint

pipe = TelemetryPipeline("./data", window_size_ms=1000, allowed_lateness_ms=0,
                         backfill_config=BackfillConfig(quota_total=1000))
pipe.flush(2000)  # 先把旧窗口封掉，制造"横跨已关窗口"的历史批

batch = [
    MetricPoint("cpu", 5.0, 2500, "bf-1"),  # 开放窗口 -> 接受
    MetricPoint("cpu", 7.0, 300,  "bf-2"),  # 窗口已关 -> WINDOW_CLOSED
    MetricPoint("cpu", 9.0, 2600, "bf-1"),  # 同批键重复 -> DUPLICATE
]
report = pipe.backfill(batch)
print(report.accepted, report.rejected, report.reason_counts)
for item in report.results:
    print(item.index, item.result.idempotency_key,
          item.result.reason, item.result.detail)

pipe.backfill(batch)          # 再来一次：accepted=0，结论与上次对得上
pipe.snapshot()               # 额度/键/聚合落快照，重启后延续
```

HTTP：

```bash
curl -s -X POST localhost:8000/backfill -H 'content-type: application/json' -d '{
  "items": [
    {"kind":"metric","name":"cpu","value":5.0,"timestamp_ms":2500,"idempotency_key":"bf-1"},
    {"kind":"log","message":"boom","level":"ERROR","timestamp_ms":2600,"idempotency_key":"bf-log"}
  ]
}'
```

## 7. 进程重启恢复

构造 `TelemetryPipeline` 时自动执行：

1. 读 `snapshot.json`（无快照则从空状态开始）；
2. 重放 `events.log`：快照之后的已接受事件通过 **trusted 通道**并入聚合
   （它们是历史已接受事实，不受重启后更高水位/封口影响），并补入去重与基数集合；
3. 无快照时，幂等键/序列集合直接由全部 WAL 记录重建；
4. 补报已用额度同样恢复：快照口径 + 快照后 WAL 中 `source="backfill"`
   的条数，重启不清零（无快照时由全部 WAL 中的补报记录计数）。

运行期调用 `pipeline.snapshot()` 会用「临时文件 + fsync + 原子 rename」
写入快照并清空 WAL，保证不存在半截文件。

## 8. 本地验证

```bash
# 安装依赖（Python >= 3.14）
uv sync

# 全部单测（pytest 会在控制台打印 INFO 日志：窗口区间/幂等键/判定依据）
uv run pytest

# 只看并发+乱序+限流场景
uv run pytest tests/test_observability_scenarios.py -s

# 只看批量补报/并发/重复补报/跨重启恢复（-s 查看窗口/幂等键/逐条依据日志）
uv run pytest tests/test_backfill.py -s

# 启动 HTTP 演示服务
uv run uvicorn main:app --reload
# 然后可尝试：
#   POST /metrics   上报指标
#   POST /logs      上报日志
#   POST /backfill  批量补报离线历史（逐条结论 + 收/挡汇总）
#   POST /flush     封口并跑告警
#   GET  /aggregates、GET /alerts
#   POST /snapshot、POST /export
```

可配置环境变量：`TELEMETRY_DATA_DIR`、`TELEMETRY_WINDOW_MS`、
`TELEMETRY_LATENESS_MS`、`TELEMETRY_MAX_CARDINALITY`、
`TELEMETRY_RATE_PER_SEC`、`TELEMETRY_RATE_BURST`、`TELEMETRY_BACKFILL_QUOTA`。

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
| 批量补报逐条结论/坏数据隔离/汇总 | `test_batch_per_item_results_and_summary` |
| 补报优先级（关窗≠限流、去重/基数顺序） | `test_closed_window_never_reported_as_rate_limit`、`test_priority_duplicate_beats_window_closed`、`test_cardinality_before_quota_priority` |
| 补报乱序/切块并发/跨实例可复现 | `test_backfill_order_independent`、`test_backfill_concurrent_chunks_matches_serial`、`test_concurrent_same_batch_submissions_agree`、`test_reproducible_across_independent_instances` |
| 写压力下关窗结论恒定 | `test_closed_window_verdict_stable_under_write_pressure` |
| 整批重复补报幂等 | `test_repeat_batch_is_idempotent` |
| 补报额度/结论跨重启延续 | `test_quota_and_verdicts_survive_restart`、`test_restart_accepted_backfills_aggregate_identically` |
| 补报与在线采集兼容（限流独立/键共用） | `test_backfill_independent_of_online_token_bucket`、`test_backfill_batch_endpoint_per_item` |
| 导出确定性 | `test_export_report_is_deterministic` |
