"""Structured JSON logging and stage telemetry utilities."""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator


class JsonLogFormatter(logging.Formatter):
    """Formats log records as single-line JSON for log aggregators."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra_fields = getattr(record, "structured", None)
        if isinstance(extra_fields, dict):
            payload.update(extra_fields)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "pulseline", json_logs: bool = False) -> logging.Logger:
    """Return a configured logger with optional JSON output."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            JsonLogFormatter()
            if json_logs
            else logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )
        logger.addHandler(handler)
        logger.propagate = False
    elif json_logs and not isinstance(logger.handlers[0].formatter, JsonLogFormatter):
        logger.handlers[0].setFormatter(JsonLogFormatter())
    logger.setLevel(logging.INFO)
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a structured log record carrying key-value telemetry fields."""
    logger.log(level, message, extra={"structured": fields})


@dataclass
class RunMetrics:
    """Structured telemetry collected across a single pipeline execution."""

    run_id: str
    batches_discovered: int = 0
    batches_processed: int = 0
    batches_recovered: int = 0
    batches_skipped_already_done: int = 0
    batches_skipped_duplicate_content: int = 0
    quotes_landed: int = 0
    quote_lines_landed: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    live_rows_total: int = 0
    dq_checks_run: int = 0
    dq_warnings: int = 0
    dq_failures: int = 0
    attributed_rows: int = 0
    stage_timings_ms: dict[str, float] = field(default_factory=dict)

    def record_stage(self, stage: str, elapsed_ms: float) -> None:
        self.stage_timings_ms[stage] = round(
            self.stage_timings_ms.get(stage, 0.0) + elapsed_ms, 3
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "batches_discovered": self.batches_discovered,
            "batches_processed": self.batches_processed,
            "batches_recovered": self.batches_recovered,
            "batches_skipped_already_done": self.batches_skipped_already_done,
            "batches_skipped_duplicate_content": self.batches_skipped_duplicate_content,
            "quotes_landed": self.quotes_landed,
            "quote_lines_landed": self.quote_lines_landed,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "live_rows_total": self.live_rows_total,
            "dq_checks_run": self.dq_checks_run,
            "dq_warnings": self.dq_warnings,
            "dq_failures": self.dq_failures,
            "attributed_rows": self.attributed_rows,
            "stage_timings_ms": self.stage_timings_ms,
        }


@contextmanager
def timed_stage(metrics: RunMetrics, stage: str) -> Iterator[None]:
    """Context manager measuring stage wall-clock latency in milliseconds."""
    start = time.perf_counter()
    try:
        yield
    finally:
        metrics.record_stage(stage, (time.perf_counter() - start) * 1000.0)
