"""本地可观测数据管道 HTTP 入口（同时也是应用内库使用方式的演示）。

路由：
    POST /metrics                 上报指标（支持单条）
    POST /logs                    上报日志
    POST /watermark               推进水位、关闭窗口并触发导出/告警
    GET  /aggregates              查询聚合结果
    GET  /alerts/state            查询告警状态
    PUT  /alerts/rules            新增/更新告警规则（版本必须递增）
    GET  /exports                 读取已导出的 JSONL 记录
    GET  /stats                   接收/拒绝计数等自检信息
    POST /restore                 从本地状态文件恢复（启动时也自动恢复一次）
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from telemetry.alerts import AlertEngine  # noqa: F401  (保证包可导入)
from telemetry.models import Rule
from telemetry.pipeline import TelemetryPipeline
from telemetry.windows import CursorBackwardError

DATA_DIR = os.environ.get("TELEMETRY_DATA_DIR", "./.telemetry-data")
STORAGE_PATH = os.path.join(DATA_DIR, "state.json")
EXPORT_PATH = os.path.join(DATA_DIR, "export.jsonl")

pipeline = TelemetryPipeline(
    window_ms=int(os.environ.get("TELEMETRY_WINDOW_MS", "1000")),
    allowed_lateness_ms=int(os.environ.get("TELEMETRY_LATENESS_MS", "500")),
    max_cardinality=int(os.environ.get("TELEMETRY_MAX_CARDINALITY", "100")),
    max_events_per_sec=int(os.environ.get("TELEMETRY_MAX_RATE", "1000")),
    storage_path=STORAGE_PATH,
    export_path=EXPORT_PATH,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 进程启动即尝试从本地状态恢复（文件不存在则为空启动）
    pipeline.restore()
    yield


app = FastAPI(title="local-telemetry-pipeline", lifespan=lifespan)


class MetricIn(BaseModel):
    name: str
    value: float
    ts_ms: int
    tags: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str = ""


class LogIn(BaseModel):
    level: str
    message: str
    ts_ms: int
    labels: dict[str, str] = Field(default_factory=dict)
    idempotency_key: str = ""


class WatermarkIn(BaseModel):
    watermark_ms: int


class RuleIn(BaseModel):
    rule_id: str
    metric_name: str
    op: str
    threshold: float
    window_ms: int
    for_ms: int = 0
    version: int = 1
    agg_field: str = "max"


def _result_payload(result) -> dict[str, Any]:
    return {
        "accepted": result.accepted,
        "reason": result.reason.value if result.reason else None,
        "idempotency_key": result.idempotency_key,
        "detail": result.detail,
    }


@app.get("/")
def read_root() -> dict:
    return {"service": "local-telemetry-pipeline", "status": "ok"}


@app.post("/metrics")
def post_metric(item: MetricIn) -> dict:
    point = pipeline.build_metric(
        item.name, item.value, item.ts_ms, item.tags, item.idempotency_key
    )
    return _result_payload(pipeline.write_metric(point))


@app.post("/logs")
def post_log(item: LogIn) -> dict:
    record = pipeline.build_log(
        item.level, item.message, item.ts_ms, item.labels, item.idempotency_key
    )
    return _result_payload(pipeline.write_log(record))


@app.post("/watermark")
def post_watermark(item: WatermarkIn) -> dict:
    try:
        closed = pipeline.advance_watermark(item.watermark_ms)
    except CursorBackwardError as exc:
        # 游标回退必须被明确拒绝（409 Conflict）
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {
        "watermark_ms": pipeline.windows.watermark_ms,
        "closed_count": len(closed),
        "closed": [a.to_dict() for a in closed],
    }


@app.get("/aggregates")
def get_aggregates(include_open: bool = True) -> dict:
    return {"aggregates": pipeline.query_aggregates(include_open=include_open)}


@app.get("/alerts/state")
def get_alert_state(rule_id: str = "", series_key: str = "") -> dict:
    if not rule_id:
        return {
            rid: pipeline.alerts.state_of(rid).value
            for rid in sorted(pipeline.alerts._rules)
        }
    state = pipeline.alerts.state_of(rule_id, series_key or None)
    if state is None:
        raise HTTPException(status_code=404, detail=f"规则不存在: {rule_id}")
    return {"rule_id": rule_id, "series_key": series_key, "state": state.value}


@app.put("/alerts/rules")
def put_rule(item: RuleIn) -> dict:
    rule = Rule(
        rule_id=item.rule_id,
        metric_name=item.metric_name,
        op=item.op,
        threshold=item.threshold,
        window_ms=item.window_ms,
        for_ms=item.for_ms,
        version=item.version,
        agg_field=item.agg_field,
    )
    try:
        version = pipeline.alerts.upsert_rule(rule)
    except ValueError as exc:
        # 版本回退 / 非法运算符：明确拒绝
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    pipeline.save()
    return {"rule_id": item.rule_id, "version": version}


@app.get("/exports")
def get_exports() -> dict:
    if pipeline.exporter is None:
        return {"records": []}
    return {"records": pipeline.exporter.read_all()}


@app.get("/stats")
def get_stats() -> dict:
    return pipeline.stats()


@app.post("/restore")
def post_restore() -> dict:
    pipeline.restore()
    return {"restored": True, "stats": pipeline.stats()}
