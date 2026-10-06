"""本地可观测数据管道演示服务。

启动::

    uv run uvicorn main:app --reload

接口：
- POST /metrics        上报指标（幂等键去重 / 基数 / 速率限流）
- POST /logs           上报日志
- POST /backfill       批量补报历史事件（逐条结论 + 汇总，整批幂等）
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

from telemetry.ingest import IngestionConfig
from telemetry.models import (
    AlertRule,
    IngestResult,
    IngestStatus,
    LogEvent,
    LogLevel,
    MetricPoint,
    RejectReason,
)
from telemetry.pipeline import TelemetryPipeline

DATA_DIR = os.environ.get("TELEMETRY_DATA_DIR", "./data")
WINDOW_MS = int(os.environ.get("TELEMETRY_WINDOW_MS", "1000"))
LATENESS_MS = int(os.environ.get("TELEMETRY_LATENESS_MS", "500"))

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


class FlushIn(BaseModel):
    watermark_ms: int | None = None


class BackfillIn(BaseModel):
    """批量补报请求：一批历史事件（指标/日志混合），逐条给结论。

    条目按原始 dict 接收：单条缺字段/级别写错只影响自己（判 BAD_INPUT），
    不让整批一起失败。
    """

    batch_id: str | None = None
    metrics: list[dict] = Field(default_factory=list)
    logs: list[dict] = Field(default_factory=list)


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


def _bad_item(key: object, detail: str) -> IngestResult:
    """单条坏数据的预定结论：只影响自己，不拖垮整批。"""
    return IngestResult(
        IngestStatus.REJECTED,
        key if isinstance(key, str) else "",
        RejectReason.BAD_INPUT,
        detail,
    )


def _build_metric(d: dict) -> MetricPoint | IngestResult:
    missing = [f for f in ("name", "value", "timestamp_ms", "idempotency_key")
               if f not in d]
    if missing:
        return _bad_item(d.get("idempotency_key"),
                         f"缺少字段: {', '.join(missing)}")
    return MetricPoint(
        name=d["name"], value=d["value"], timestamp_ms=d["timestamp_ms"],
        idempotency_key=d["idempotency_key"], labels=d.get("labels") or {},
    )


def _build_log(d: dict) -> LogEvent | IngestResult:
    missing = [f for f in ("message", "timestamp_ms", "idempotency_key")
               if f not in d]
    if missing:
        return _bad_item(d.get("idempotency_key"),
                         f"缺少字段: {', '.join(missing)}")
    level = d.get("level", "INFO")
    try:
        level = LogLevel(str(level).upper())
    except ValueError:
        # 级别写错：保留原始值交给采集层校验，单条判 BAD_INPUT
        pass
    return LogEvent(
        message=d["message"], level=level, timestamp_ms=d["timestamp_ms"],
        idempotency_key=d["idempotency_key"], labels=d.get("labels") or {},
    )


@app.post("/backfill")
def backfill(body: BackfillIn) -> dict:
    """批量补报：整批投递历史事件，逐条结论 + 汇总，单条失败不影响整批。"""
    events: list = [_build_metric(m) for m in body.metrics]
    events.extend(_build_log(entry) for entry in body.logs)
    report = pipeline.backfill(events, batch_id=body.batch_id)
    return report.to_dict()


@app.post("/snapshot")
def make_snapshot() -> dict:
    pipeline.snapshot()
    return {"snapshot": "ok"}


@app.post("/export")
def export_report(include_events: bool = False) -> dict:
    path = pipeline.export(os.path.join(DATA_DIR, "report.json"),
                           include_events=include_events)
    return {"exported": path}
