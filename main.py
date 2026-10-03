"""本地可观测数据管道演示服务。

启动::

    uv run uvicorn main:app --reload

接口：
- POST /metrics        上报指标（幂等键去重 / 基数 / 速率限流）
- POST /logs           上报日志
- POST /backfill       批量补报离线历史数据（逐条结论 + 收/挡汇总，整批不整体失败）
- POST /flush          推进水位、封口窗口并驱动告警判定
- POST /alerts/rules   新增告警规则
- PUT  /alerts/rules   更新规则（不改写历史判定）
- GET  /aggregates     查询聚合
- GET  /alerts         查询告警状态与判定历史
- POST /snapshot       落本地快照（重启自动恢复）
- POST /export         导出 JSON 报告

数据全部保存在 ./data 目录（WAL + 原子快照）。
"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from telemetry.backfill import BackfillConfig
from telemetry.ingest import IngestionConfig
from telemetry.models import (
    AlertRule,
    IngestStatus,
    LogEvent,
    LogLevel,
    MetricPoint,
)
from telemetry.pipeline import TelemetryPipeline

DATA_DIR = os.environ.get("TELEMETRY_DATA_DIR", "./data")
WINDOW_MS = int(os.environ.get("TELEMETRY_WINDOW_MS", "1000"))
LATENESS_MS = int(os.environ.get("TELEMETRY_LATENESS_MS", "500"))
BACKFILL_QUOTA = int(os.environ.get("TELEMETRY_BACKFILL_QUOTA", "-1"))

app = FastAPI(title="local-telemetry-pipeline")
pipeline = TelemetryPipeline(
    DATA_DIR,
    window_size_ms=WINDOW_MS,
    allowed_lateness_ms=LATENESS_MS,
    ingestion_config=IngestionConfig(
        max_cardinality=int(os.environ.get("TELEMETRY_MAX_CARDINALITY", "1000")),
        rate_limit_per_sec=float(os.environ.get("TELEMETRY_RATE_PER_SEC", "1000")),
        rate_burst=int(os.environ.get("TELEMETRY_RATE_BURST", "1000")),
    ),
    backfill_config=BackfillConfig(quota_total=BACKFILL_QUOTA),
)


# ---------------------------------------------------------------------------
# 请求模型
# ---------------------------------------------------------------------------
class MetricIn(BaseModel):
    name: str
    value: float
    timestamp_ms: int
    idempotency_key: str
    labels: dict[str, str] = Field(default_factory=dict)


class LogIn(BaseModel):
    message: str
    level: str = "INFO"
    timestamp_ms: int
    idempotency_key: str
    labels: dict[str, str] = Field(default_factory=dict)


class RuleIn(BaseModel):
    rule_id: str
    series: str
    comparator: str
    threshold: float
    for_windows: int = 1
    resolve_windows: int = 1


class BackfillItemIn(BaseModel):
    """补报条目：kind=metric（默认）或 log，二者可混在同一批。"""

    kind: str = "metric"
    name: str | None = None
    value: float | None = None
    message: str | None = None
    level: str = "INFO"
    timestamp_ms: int
    idempotency_key: str
    labels: dict[str, str] = Field(default_factory=dict)


class FlushIn(BaseModel):
    watermark_ms: int | None = None


def _ingest_response(result) -> dict:
    return {
        "status": result.status.value,
        "idempotency_key": result.idempotency_key,
        "reason": result.reason.value if result.reason else None,
        "detail": result.detail,
        "window_start_ms": result.window_start_ms,
    }


@app.post("/metrics")
def post_metric(body: MetricIn) -> dict:
    result = pipeline.ingest(
        MetricPoint(
            name=body.name, value=body.value, timestamp_ms=body.timestamp_ms,
            idempotency_key=body.idempotency_key, labels=body.labels,
        )
    )
    code = 200 if result.status is IngestStatus.ACCEPTED else 429
    payload = _ingest_response(result)
    if code != 200:
        raise HTTPException(status_code=429, detail=payload)
    return payload


@app.post("/logs")
def post_log(body: LogIn) -> dict:
    try:
        level = LogLevel(body.level.upper())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"未知日志级别：{body.level}")
    result = pipeline.ingest(
        LogEvent(
            message=body.message, level=level, timestamp_ms=body.timestamp_ms,
            idempotency_key=body.idempotency_key, labels=body.labels,
        )
    )
    if result.status is IngestStatus.REJECTED:
        raise HTTPException(status_code=429, detail=_ingest_response(result))
    return _ingest_response(result)


@app.post("/backfill")
def post_backfill(body: dict) -> dict:
    """批量补报离线历史数据。

    请求体：``{"items": [ ...metric/log 条目... ]}``。
    与在线上报不同：**整批不会因为个别条目有问题而整体失败**——每条单独
    给出 accepted/rejected 结论（按输入下标对齐），并汇总收/挡数量与各
    拒绝原因计数；只要请求体结构可解析，HTTP 一律 200，结论看逐条 reason。
    结论确定可复现，重复补报同一批不会重复计数。
    """
    raw_items = body.get("items")
    if not isinstance(raw_items, list):
        raise HTTPException(status_code=400, detail="请求体需为 {'items': [...]}")

    points: list = []
    parse_bad: dict[int, str] = {}
    for i, raw in enumerate(raw_items):
        try:
            item = BackfillItemIn(**raw)
            if item.kind == "metric":
                points.append(MetricPoint(
                    name=item.name or "", value=item.value if item.value is not None else 0.0,
                    timestamp_ms=item.timestamp_ms,
                    idempotency_key=item.idempotency_key, labels=item.labels,
                ))
            elif item.kind == "log":
                points.append(LogEvent(
                    message=item.message or "", level=LogLevel(item.level.upper()),
                    timestamp_ms=item.timestamp_ms,
                    idempotency_key=item.idempotency_key, labels=item.labels,
                ))
            else:
                parse_bad[i] = f"不支持的 kind：{item.kind}"
                points.append(MetricPoint(
                    name="", value=0.0, timestamp_ms=item.timestamp_ms,
                    idempotency_key=item.idempotency_key, labels=item.labels,
                ))
        except (TypeError, ValueError) as exc:
            parse_bad[i] = f"条目解析失败：{exc}"
            key = raw.get("idempotency_key", "") if isinstance(raw, dict) else ""
            ts = raw.get("timestamp_ms", -1) if isinstance(raw, dict) else -1
            points.append(MetricPoint(
                name="", value=0.0,
                timestamp_ms=ts if isinstance(ts, int) and ts >= 0 else 0,
                idempotency_key=key if isinstance(key, str) else "",
            ))

    report = pipeline.backfill(points)
    return {
        "total": report.total,
        "accepted": report.accepted,
        "rejected": report.rejected,
        "reason_counts": report.reason_counts,
        "results": [
            {
                "index": item.index,
                **_ingest_response(item.result),
                **({"parse_error": parse_bad[item.index]}
                   if item.index in parse_bad else {}),
            }
            for item in report.results
        ],
    }


@app.post("/flush")
def flush(body: FlushIn) -> dict:
    report = pipeline.flush(body.watermark_ms)
    return {
        "watermark_ms": report.watermark_ms,
        "sealed": [a.to_dict() for a in report.sealed],
        "evaluations": [
            {
                "rule_id": e.rule_id,
                "window": [e.window_start_ms, e.window_start_ms + WINDOW_MS],
                "value": e.value,
                "state": e.state.value,
                "triggered": e.triggered,
                "resolved": e.resolved,
                "reason": e.reason,
            }
            for e in report.evaluations
        ],
    }


@app.post("/alerts/rules")
def create_rule(body: RuleIn) -> dict:
    rule = AlertRule(
        body.rule_id, body.series, body.comparator, body.threshold,
        body.for_windows, body.resolve_windows,
    )
    try:
        pipeline.alerts.add_rule(rule)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"added": body.rule_id}


@app.put("/alerts/rules")
def update_rule(body: RuleIn) -> dict:
    rule = AlertRule(
        body.rule_id, body.series, body.comparator, body.threshold,
        body.for_windows, body.resolve_windows,
    )
    try:
        pipeline.alerts.update_rule(rule)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"updated": body.rule_id, "note": "历史判定记录保持不变"}


@app.get("/aggregates")
def get_aggregates(sealed_only: bool = False) -> dict:
    return {
        "watermark_ms": pipeline.aggregator.watermark_ms,
        "aggregates": [a.to_dict() for a in
                       pipeline.aggregator.aggregates(include_open=not sealed_only)],
    }


@app.get("/alerts")
def get_alerts() -> dict:
    state = pipeline.alerts.snapshot()
    return state


@app.post("/snapshot")
def make_snapshot() -> dict:
    pipeline.snapshot()
    return {"snapshot": "ok"}


@app.post("/export")
def export_report(include_events: bool = False) -> dict:
    path = pipeline.export(os.path.join(DATA_DIR, "report.json"),
                           include_events=include_events)
    return {"exported": path}
