"""exporter：导出内容与排序确定性。"""
import json

from telemetry.exporter import Exporter
from telemetry.models import AlertRule
from telemetry.pipeline import TelemetryPipeline


def test_export_report_is_deterministic(tmp_path):
    pipe = TelemetryPipeline(str(tmp_path / "s"), window_size_ms=1000)
    pipe.alerts.add_rule(AlertRule("high_cpu", "metric:cpu", ">", 5.0))
    from telemetry.models import MetricPoint
    pipe.ingest(MetricPoint("cpu", 8.0, 100, "k1"))
    pipe.ingest(MetricPoint("cpu", 2.0, 1200, "k2"))
    pipe.flush(2000)

    ex = Exporter(pipe.aggregator, pipe.alerts, pipe.store)
    p1 = ex.export(str(tmp_path / "a.json"))
    p2 = ex.export(str(tmp_path / "b.json"))
    assert open(p1, "rb").read() == open(p2, "rb").read()

    data = json.loads(open(p1, encoding="utf-8").read())
    assert data["watermark_ms"] == 2000
    assert [a["series"] for a in data["aggregates"]] == ["metric:cpu", "metric:cpu"]
    sealed = [a for a in data["aggregates"] if a["sealed"]][0]
    assert sealed["count"] == 1 and sealed["max"] == 8.0
    hist = data["alert_history"]
    assert hist and hist[0]["rule_id"] == "high_cpu"
    # 事件默认不导出；显式开启则带上
    assert "events" not in data
    ex.export(str(tmp_path / "c.json"), include_events=True)
    assert "events" in json.loads(open(str(tmp_path / "c.json"), encoding="utf-8").read())
