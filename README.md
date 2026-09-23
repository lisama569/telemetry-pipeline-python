# 本地可观测数据管道（Local Telemetry Pipeline）

用**应用内状态 + 本地文件存储**模拟一条可观测数据管道的完整闭环：

```
并发采集(指标/日志)
   │  ① 参数校验  ② 幂等去重  ③ 基数限制  ④ 速率限制  ⑤ 窗口准入
   ▼
按时间窗口聚合（支持乱序到达 / 迟到重放 / 关窗拒绝 / 水位游标）
   │  窗口关闭(watermark)
   ├──────────────► 导出聚合结果（JSONL，append + fsync）
   ▼
告警引擎（按「规则 × 序列」独立的状态机 + 抖动抑制 + 规则版本化）
   │
   └──────────────► 导出告警事件（JSONL）

状态快照（state.json，临时文件 + fsync + 原子 rename）── 进程重启后恢复
```

所有写入与判定在一把 `RLock` 内串行，**并发写入、乱序到达、限流**三种场景下
结论一致且可解释：拒绝一律带可区分的 `reason` 与人类可读的 `detail`，
绝不静默丢弃，也绝不污染已有聚合。

---

## 1. 数据模型（`telemetry/models.py`）

| 模型 | 含义 | 关键字段 |
| --- | --- | --- |
| `MetricPoint` | 一条指标 | `name, value, ts_ms(事件时间), tags, idempotency_key` |
| `LogRecord` | 一条日志 | `level, message, ts_ms, labels, idempotency_key` |
| `WriteResult` | 写入回执 | `accepted, reason, idempotency_key, detail` |
| `WindowAggregate` | 窗口聚合结果 | `window_start_ms/end_ms, kind, series_key, count, sum, min, max, last_value, closed, version` |
| `Rule` | 告警规则 | `rule_id, metric_name, op, threshold, window_ms, for_ms, agg_field, version` |
| `AlertEvent` | 告警事件（不可变） | `state, window[起,止), observed_value, threshold, reason, rule_version` |

- **时间单位统一为毫秒（`ts_ms`）**，事件时间由上报方携带，与处理时间解耦。
- **序列键 `series_key`**：指标为 `metric:<name>{k=v,...}`，日志为
  `log:<LEVEL>{k=v,...}`。标签先按 key 排序再渲染，因此
  `{a=1,b=2}` 与 `{b=2,a=1}` 属于同一序列。
- **幂等键**：显式传入优先；未传时由事件内容做 SHA-256 派生（`auto:...`），
  内容完全相同的事件天然去重。
- 日志聚合除 `count` 外，用级别权重参与 `sum/min/max/last_value`：
  `DEBUG=1, INFO=2, WARN=3, ERROR=4, FATAL=5`。

拒绝原因枚举 `RejectReason`（**彼此可区分**）：

| reason | 触发条件 |
| --- | --- |
| `invalid` | 参数非法（如负时间戳） |
| `duplicate` | 幂等键已存在（重复上报） |
| `cardinality_limit` | 新序列无法获得基数槽（超过 `max_cardinality`） |
| `rate_limited` | 事件所在秒写入量超过 `max_events_per_sec` |
| `window_closed` | 事件落入已关闭窗口（含迟到到已关窗口的数据） |
| `cursor_backward` | 水位/游标试图回退（HTTP 层映射为 409） |

---

## 2. 窗口与乱序语义（`telemetry/windows.py`）

### 窗口对齐
窗口与纪元对齐，长度为 `window_ms`：

```
window_start = (ts_ms // window_ms) * window_ms
window_end   = window_start + window_ms
```

因此窗口归属只由事件时间决定、与到达顺序无关。`count/sum/min/max` 天然满足
交换律与结合律；`last_value` 取**窗口内事件时间戳最大**的值（同时间戳后到覆盖），
保证「同一批数据、任意到达顺序 ⇒ 完全相同的聚合结果」。

### 乱序与迟到（可重放、稳定）
- 窗口**尚未关闭**前，任何迟到数据都可并入，`version` 每并入一条 +1，
  使「窗口刷新 + 延迟到达」可追踪、可重放。
- `allowed_lateness_ms` 描述可接受的迟到宽限；**但只要水位越过窗口结束时刻，
  窗口即永久关闭**，此后（即使仍在宽限时长内）写入一律 `window_closed`。
  即「是否可写」只取决于水位，不取决于墙钟时间，结论确定可复现。

### 水位（watermark）与游标
- `advance_watermark(ts)` 把所有 `window_end <= ts` 的窗口置为 `closed`，
  返回本次新关闭的窗口（按 `(window_start, series_key)` 排序，顺序稳定）。
- 水位**单调不减**：回退抛 `CursorBackwardError`（API 返回 409），状态不变。
- 关窗动作同时触发：导出该聚合 → 送告警引擎评估 → 导出产生的告警事件。

### 重启恢复
每次接受写入 / 关窗后，状态写入 `state.json`（临时文件 + fsync + 原子 rename）。
启动或调用 `restore()` 后恢复：窗口聚合值、`closed` 标志、水位、幂等键集合、
基数集合、告警运行态。恢复后已关闭窗口继续拒绝写入，历史判定不丢失。

---

## 3. 去重与限流规则（`telemetry/dedup.py`、`telemetry/ratelimit.py`）

写入在同一把锁内按下列**固定顺序**判定，任一拒绝都不会登记后续状态、
不会触碰聚合（拒绝的事件不占幂等槽/基数槽，除 `duplicate` 自身外）：

1. **参数校验** → `invalid`
2. **窗口准入（只读预检）** → `window_closed`（先预检，避免被拒事件登记去重状态）
3. **幂等去重 + 基数**：
   - 幂等键已存在 → `duplicate`；
   - 新序列且基数槽已满 → `cardinality_limit`；
   - 通过才把幂等键与序列登记（仅被接受的事件占用状态）。
4. **速率限制** → 按**事件时间所在秒**的固定窗口计数
   （`ts_ms // 1000` 为桶），超过 `max_events_per_sec` 即 `rate_limited`，
   跨秒自动重置。`detail` 中给出 `(已用/上限) events/s`。

> 限流器只用于抑制瞬时摄入压力，不参与聚合正确性，因此不做持久化，
> 重启后重新计数；去重/基数/窗口/告警状态均持久化。

---

## 4. 告警语义（`telemetry/alerts.py`）

### 状态机（每条规则 × 每个序列独立）
```
RESOLVED ──条件满足──► PENDING（记录首个满足窗口的结束时刻）
PENDING  ──连续满足，且 (当前窗口结束 - 首个满足窗口结束) >= for_ms──► FIRING（产生 TRIGGER 事件）
FIRING   ──条件不满足──► RESOLVED（产生 RESOLVE 事件）
PENDING  ──for_ms 内提前不满足──► RESOLVED（短抖动被抑制，不产生任何告警）
FIRING   ──条件持续满足──► 不重复触发
```

- 判定值取自关闭窗口的某一聚合字段 `agg_field`：
  `max / min / last_value / avg / sum / count`（默认 `max`），
  与阈值按 `op ∈ {>, <, >=, <=}` 比较。
- **抖动抑制**：`for_ms` 要求异常连续覆盖足够时长；只在个别窗口闪现、
  随即恢复的情况停留/回退于 `PENDING`，不打扰值班。
- **按序列隔离**：同一规则对不同标签序列分别维护状态。A 序列触发后，
  B 序列的正常值不会把 A 错误恢复（这是常见的跨序列串状态陷阱，已被测试固定）。
- **规则更新不改写历史**：`version` 必须单调递增（否则拒绝，HTTP 409）。
  更新只替换规则定义并重置其运行态；已产生的 `AlertEvent` 固化当时的
  `rule_version / threshold` 与完整判定依据，永不被改写。
- 每条 `AlertEvent.reason` 都写明：规则、序列、**窗口区间**、聚合字段、
  观测值、比较符、阈值、是否越限、已持续时长与状态流转，可直接用于解释结论。
- 单条规则评估异常只记录在该序列状态上，不影响其它规则/序列与聚合、导出主链路。

---

## 5. 本地存储与导出

| 文件 | 内容 | 写入方式 |
| --- | --- | --- |
| `state.json` | 窗口、去重、基数、告警运行态快照 | temp + fsync + 原子 rename |
| `export.jsonl` | 每行一条 `{type: aggregate\|alert, payload: ...}` | append + fsync，只追加不改写 |

导出是闭环的「落库」端：关窗聚合与告警事件都可在 JSONL 中回放、核对。
导出盘故障只计数（`stats().rejected.export_error`），不阻塞内存链路。

---

## 6. 本地验证方法

### 6.1 作为应用内库使用
```python
from telemetry.pipeline import TelemetryPipeline
from telemetry.models import Rule

p = TelemetryPipeline(
    window_ms=1000, allowed_lateness_ms=500,
    max_cardinality=100, max_events_per_sec=1000,
    storage_path="./.telemetry-data/state.json",
    export_path="./.telemetry-data/export.jsonl",
)
p.restore()                       # 进程重启后恢复（无文件则空启动）
p.alerts.upsert_rule(Rule("cpu-hot", "cpu", ">", 80, window_ms=1000, for_ms=2000))

p.write_metric(p.build_metric("cpu", 90, 100, {"host": "a"}, "evt-1"))
p.write_metric(p.build_metric("cpu", 90, 100, {"host": "a"}, "evt-1"))  # duplicate
p.advance_watermark(1000)         # 关窗 -> 导出聚合 -> 评估/导出告警
print(p.stats())
print(p.exporter.read_all())
```

### 6.2 跑测试（38 个，含并发/乱序/限流/恢复）
```bash
uv sync
uv run pytest -q
# 查看带「窗口区间 / 幂等键 / 判定依据」的测试日志：
uv run pytest -s -k "out_of_order_then_close or concurrent"
```

测试覆盖：

- `tests/test_windows.py`：乱序一致性、迟到重放、关窗拒绝、游标回退、重启恢复；
- `tests/test_pipeline_ingest.py`：**并发同键只接受一次**、并发异键计数精确、
  四类拒绝原因可区分、被拒数据不污染聚合、标签归一化；
- `tests/test_alerts.py`：触发/恢复、`for_ms` 抖动抑制、规则版本化与历史固化、
  按序列状态隔离、快照恢复、单规则失败不阻塞他人；
- `tests/test_pipeline_e2e.py`：乱序→关窗→告警→导出闭环、整管道重启恢复；
- `tests/test_logs_and_concurrency.py`：日志聚合，以及多线程随机乱序与顺序
  参考结果逐字段一致、并发关窗拒绝、并发下告警链路不丢数据；
- `tests/test_http_api.py`：HTTP 层去重、限流、409（游标回退/规则版本冲突）。

### 6.3 启动 HTTP 服务手测
```bash
uv run uvicorn main:app --port 8000
# 可调环境变量：TELEMETRY_DATA_DIR / TELEMETRY_WINDOW_MS /
#               TELEMETRY_LATENESS_MS / TELEMETRY_MAX_CARDINALITY / TELEMETRY_MAX_RATE

curl -s -X POST localhost:8000/metrics -H 'Content-Type: application/json' \
  -d '{"name":"cpu","value":90,"ts_ms":100,"tags":{"host":"a"},"idempotency_key":"k1"}'
curl -s -X PUT  localhost:8000/alerts/rules -H 'Content-Type: application/json' \
  -d '{"rule_id":"r1","metric_name":"cpu","op":">","threshold":80,"window_ms":1000,"version":1}'
curl -s -X POST localhost:8000/watermark -H 'Content-Type: application/json' -d '{"watermark_ms":1000}'
curl -s localhost:8000/exports
curl -s localhost:8000/stats
```

| 路由 | 说明 |
| --- | --- |
| `POST /metrics`、`POST /logs` | 上报；回执带 `accepted/reason/detail` |
| `POST /watermark` | 推进水位关窗；回退返回 409 |
| `GET /aggregates` | 查询聚合（可带 `?include_open=`） |
| `PUT /alerts/rules` | 新增/更新规则；版本未递增返回 409 |
| `GET /alerts/state` | 查询告警状态（可按 rule/series 过滤） |
| `GET /exports` | 读取导出的聚合与告警 JSONL |
| `GET /stats` | 接受数、按原因分类的拒绝数、基数、水位 |
| `POST /restore` | 手动触发从本地状态恢复 |

---

## 7. 代码结构
```
telemetry/
  models.py     # 数据模型、拒绝原因、序列键归一化
  storage.py    # 原子持久化状态文件
  dedup.py      # 幂等去重 + 基数限制
  ratelimit.py  # 按秒固定窗口限流
  windows.py    # 时间窗口聚合 / 乱序 / 关窗 / 水位 / 快照
  alerts.py     # 告警状态机 / 抖动抑制 / 规则版本化 / 按序列隔离
  exporter.py   # JSONL 导出（聚合 + 告警）
  pipeline.py   # 并发安全门面，串联上述组件并编排持久化与导出
main.py         # FastAPI HTTP 入口
tests/          # 38 个 pytest 用例
```
