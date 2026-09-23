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
  pipeline.py     门面：装配上述组件，负责闭环与重启恢复
  exporter.py     聚合/告警报告的确定性 JSON 导出
main.py           FastAPI 本地演示服务
tests/            单元测试 + 并发/乱序/限流场景测试（日志打印窗口/幂等键/依据）
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
- `snapshot.json`：聚合 + 告警 + 已接受幂等键/序列集合的原子快照；
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

## 6. 进程重启恢复

构造 `TelemetryPipeline` 时自动执行：

1. 读 `snapshot.json`（无快照则从空状态开始）；
2. 重放 `events.log`：快照之后的已接受事件通过 **trusted 通道**并入聚合
   （它们是历史已接受事实，不受重启后更高水位/封口影响），并补入去重与基数集合；
3. 无快照时，幂等键/序列集合直接由全部 WAL 记录重建。

运行期调用 `pipeline.snapshot()` 会用「临时文件 + fsync + 原子 rename」
写入快照并清空 WAL，保证不存在半截文件。

## 7. 本地验证

```bash
# 安装依赖（Python >= 3.14）
uv sync

# 全部单测（pytest 会在控制台打印 INFO 日志：窗口区间/幂等键/判定依据）
uv run pytest

# 只看并发+乱序+限流场景
uv run pytest tests/test_observability_scenarios.py -s

# 启动 HTTP 演示服务
uv run uvicorn main:app --reload
# 然后可尝试：
#   POST /metrics   上报指标
#   POST /logs      上报日志
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
report = pipe.flush(watermark_ms=3000)      # 封口 + 告警判定
for e in report.evaluations:
    print(e.window_start_ms, e.state.value, e.reason)
pipe.export("./data/report.json")
```

## 8. 测试覆盖对照

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
