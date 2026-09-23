"""公共 fixtures 与日志辅助：测试日志打印窗口区间、幂等键与判定依据。"""
from __future__ import annotations

import logging
import os
import tempfile

import pytest

from telemetry.models import AlertEvent, WindowAggregate
from telemetry.pipeline import TelemetryPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("telemetry-tests")


@pytest.fixture
def paths(tmp_path):
    return {
        "state": str(tmp_path / "state.json"),
        "export": str(tmp_path / "export.jsonl"),
    }


@pytest.fixture
def pipeline(paths):
    p = TelemetryPipeline(
        window_ms=1000,
        allowed_lateness_ms=500,
        max_cardinality=5,
        max_events_per_sec=100,
        storage_path=paths["state"],
        export_path=paths["export"],
    )
    return p


def log_window(agg: WindowAggregate, note: str = "") -> None:
    log.info(
        "窗口=[%s,%s) 序列=%s closed=%s version=%s count=%s sum=%s "
        "min=%s max=%s last=%s %s",
        agg.window_start_ms,
        agg.window_end_ms,
        agg.series_key,
        agg.closed,
        agg.version,
        agg.count,
        round(agg.sum, 6),
        agg.min,
        agg.max,
        agg.last_value,
        note,
    )


def log_result(key: str, result, note: str = "") -> None:
    log.info(
        "幂等键=%s accepted=%s reason=%s 判定依据=%s %s",
        key,
        result.accepted,
        result.reason.value if result.reason else "-",
        result.detail,
        note,
    )


def log_alert(ev: AlertEvent) -> None:
    log.info("告警事件 rule=%s state=%s 依据=%s", ev.rule_id, ev.state.value, ev.reason)
