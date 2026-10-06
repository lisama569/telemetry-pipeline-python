"""批量补报（backfill）入口：运维离线历史数据的整批补录与重放。

设计要点：
- 整批投递、逐条结论：任何单条失败不影响其它条目，最终汇总
  接受/拒绝数量与每种拒绝原因的计数；
- 结论确定：批内按输入顺序逐条判定，判定优先级固定
  （BAD_INPUT > DUPLICATE > WINDOW_CLOSED > CARDINALITY_LIMIT > RATE_LIMIT），
  其中 WINDOW_CLOSED 是事实性结论，优先于瞬时限流，不随线程调度漂移；
- 幂等：同一批重复补报时，已接受条目稳定判为 DUPLICATE，不重复计数。
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from .models import IngestResult, LogEvent, MetricPoint

logger = logging.getLogger("telemetry.backfill")


@dataclass
class BackfillReport:
    """一次批量补报的汇总结论（可解释、可复现）。"""

    batch_id: str
    total: int = 0
    accepted: int = 0
    rejected: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    results: list[IngestResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        """导出为可 JSON 序列化的字典（排序确定，字节级可复现）。"""
        return {
            "batch_id": self.batch_id,
            "total": self.total,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "rejected_by_reason": dict(sorted(self.rejected_by_reason.items())),
            "results": [
                {
                    "idempotency_key": r.idempotency_key,
                    "status": r.status.value,
                    "reason": r.reason.value if r.reason else None,
                    "detail": r.detail,
                    "window_start_ms": r.window_start_ms,
                }
                for r in self.results
            ],
        }


def run_backfill(
    ingest_one,
    events: list[MetricPoint | LogEvent | IngestResult],
    *,
    batch_id: str,
    window_size_ms: int,
) -> BackfillReport:
    """逐条执行补报并汇总。

    - 批内按输入顺序逐条判定，单条失败不影响其它条目；
    - 每条结论（含窗口区间、幂等键、判定依据）写入日志，便于离线复核；
    - 判定本身由采集层补报通道完成（去重/窗口预检/基数，优先级固定，
      不含墙钟限流），同一批重复补报时已接受条目稳定判为 DUPLICATE，
      不重复计数；
    - ``events`` 中允许混入预先判定好的 ``IngestResult``（例如上游
      解析失败的条目），原样计入报告与日志。
    """
    report = BackfillReport(batch_id=batch_id, total=len(events))
    logger.info(
        "补报开始 batch=%s 条数=%d 窗口大小=%dms", batch_id, len(events), window_size_ms
    )
    for seq, event in enumerate(events):
        if isinstance(event, IngestResult):
            # 上游已给出结论的条目（如解析失败），不再过判定链
            result = event
            ts = None
        else:
            result = ingest_one(event)
            ts = getattr(event, "timestamp_ms", None)
        report.results.append(result)
        if isinstance(ts, int):
            ws = (ts // window_size_ms) * window_size_ms
            window = f"[{ws},{ws + window_size_ms})"
        else:
            window = "[未知]"
        if result.accepted:
            report.accepted += 1
            logger.info(
                "补报判定 batch=%s 序号=%d 幂等键=%s 事件时间=%s 窗口=%s 结论=accepted",
                batch_id, seq, result.idempotency_key, ts, window,
            )
        else:
            report.rejected += 1
            reason = result.reason.value if result.reason else "unknown"
            report.rejected_by_reason[reason] = (
                report.rejected_by_reason.get(reason, 0) + 1
            )
            logger.info(
                "补报判定 batch=%s 序号=%d 幂等键=%s 事件时间=%s 窗口=%s "
                "结论=rejected 原因=%s 依据=%s",
                batch_id, seq, result.idempotency_key, ts, window,
                reason, result.detail,
            )
    logger.info(
        "补报汇总 batch=%s 总数=%d 接受=%d 拒绝=%d 拒绝分类=%s",
        batch_id, report.total, report.accepted, report.rejected,
        dict(sorted(report.rejected_by_reason.items())),
    )
    return report
