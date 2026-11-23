"""Pipeline orchestrator and fixed assignment entrypoint."""

from __future__ import annotations

import logging
import time
import uuid
from pathlib import Path

from pipeline.catalog import open_catalog
from pipeline.config import PipelineConfig
from pipeline.landing import (
    BATCH_STATUS_COMPLETED,
    BATCH_STATUS_FAILED,
    LandingManager,
    discover_batches,
)
from pipeline.logging_utils import RunMetrics, get_logger, log_event, timed_stage
from pipeline.merge import ObjectMerger
from pipeline.projection import DuckDBProjector
from pipeline.quality import DataQualityValidator
from pipeline.schema import ATTRIBUTED_QUOTE_LINE_TABLE


class PipelineOrchestrator:
    """Coordinates Landing -> Projection -> Object Merge -> DQ -> Campaign Attribution."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self.logger = get_logger("pulseline", json_logs=config.json_logs)
        self.projector = DuckDBProjector()
        self.merger = ObjectMerger()
        self.dq_validator = DataQualityValidator()

    def execute(self) -> RunMetrics:
        run_id = uuid.uuid4().hex[:12]
        metrics = RunMetrics(run_id=run_id)
        overall_start = time.perf_counter()

        catalog = open_catalog(self.config.warehouse_dir)
        try:
            landing_mgr = LandingManager(catalog, strict_schema=self.config.strict_schema)
            with timed_stage(metrics, "bootstrap"):
                landing_quotes, landing_lines, ledger_table, target_table = (
                    landing_mgr.ensure_core_tables()
                )

            with timed_stage(metrics, "discover"):
                candidates = discover_batches(self.config.incoming_dir)
                metrics.batches_discovered = len(candidates)

            with timed_stage(metrics, "landing"):
                outcome = landing_mgr.ingest_new_batches(
                    run_id=run_id,
                    candidates=candidates,
                    landing_quotes=landing_quotes,
                    landing_lines=landing_lines,
                    ledger_table=ledger_table,
                )

            metrics.batches_recovered = len(outcome.recovered_batches)
            metrics.batches_skipped_already_done = len(outcome.skipped_already_done)
            metrics.batches_skipped_duplicate_content = len(outcome.skipped_duplicate_content)
            metrics.quotes_landed = outcome.quotes_landed
            metrics.quote_lines_landed = outcome.quote_lines_landed
            all_dq_results = list(outcome.quality_results)

            needs_projection = bool(outcome.batches_to_project) or self.config.full_refresh
            target_mutated = False

            if needs_projection:
                # If target table is empty while landing has historical rows (e.g. recovery),
                # fall back to full projection so target stays consistent with landing.
                use_full_projection = (
                    self.config.full_refresh or target_table.current_snapshot() is None
                )
                proj_start = time.perf_counter()
                try:
                    with timed_stage(metrics, "projection"):
                        quotes_arrow = landing_quotes.scan().to_arrow()
                        lines_arrow = landing_lines.scan().to_arrow()
                        projection = self.projector.project_quote_lines(
                            landing_quotes=quotes_arrow,
                            landing_quote_lines=lines_arrow,
                            target_batches=outcome.batches_to_project,
                            full_refresh=use_full_projection,
                        )

                    with timed_stage(metrics, "merge"):
                        merge_stats = self.merger.merge_quote_lines(
                            target_table=target_table,
                            live_rows=projection.live_rows,
                            deleted_ids=projection.deleted_ids,
                            full_refresh=self.config.full_refresh,
                        )

                    metrics.rows_inserted = merge_stats.rows_inserted
                    metrics.rows_updated = merge_stats.rows_updated
                    metrics.rows_deleted = merge_stats.rows_deleted
                    target_mutated = merge_stats.snapshot_changed

                    with timed_stage(metrics, "post_merge_dq"):
                        target_arrow = target_table.scan().to_arrow()
                        metrics.live_rows_total = target_arrow.num_rows
                        batch_label = (
                            ",".join(outcome.batches_to_project)
                            if outcome.batches_to_project
                            else "full_refresh"
                        )
                        all_dq_results.extend(
                            self.dq_validator.validate_target_object(
                                run_id=run_id,
                                batch_id=batch_label,
                                target_arrow=target_arrow,
                            )
                        )

                    elapsed_ms = (time.perf_counter() - proj_start) * 1000.0
                    for bid in outcome.batches_to_project:
                        q_cnt, l_cnt = outcome.batch_row_counts.get(bid, (0, 0))
                        note = (
                            "Recovered from LANDED state and merged into crm.quote_line"
                            if bid in outcome.recovered_batches
                            else "Projected and merged into crm.quote_line"
                        )
                        landing_mgr.append_ledger_entry(
                            ledger_table,
                            batch_id=bid,
                            content_sha256=outcome.batch_hashes.get(bid, ""),
                            status=BATCH_STATUS_COMPLETED,
                            quotes_rows=q_cnt,
                            quote_lines_rows=l_cnt,
                            rows_inserted=merge_stats.rows_inserted,
                            rows_updated=merge_stats.rows_updated,
                            rows_deleted=merge_stats.rows_deleted,
                            duration_ms=elapsed_ms,
                            note=note,
                        )
                    metrics.batches_processed = len(outcome.batches_to_project)
                except Exception as exc:
                    for bid in outcome.batches_to_project:
                        q_cnt, l_cnt = outcome.batch_row_counts.get(bid, (0, 0))
                        landing_mgr.append_ledger_entry(
                            ledger_table,
                            batch_id=bid,
                            content_sha256=outcome.batch_hashes.get(bid, ""),
                            status=BATCH_STATUS_FAILED,
                            quotes_rows=q_cnt,
                            quote_lines_rows=l_cnt,
                            note=f"Projection/merge failed: {exc}",
                        )
                    raise
            else:
                metrics.live_rows_total = (
                    target_table.scan(selected_fields=("quote_line_id",)).to_arrow().num_rows
                    if target_table.current_snapshot() is not None
                    else 0
                )

            # Stretch goal: UTM -> Campaign attribution mart
            campaigns_path = self.config.resolved_campaigns_path()
            if campaigns_path is not None and campaigns_path.is_file():
                with timed_stage(metrics, "campaign_attribution"):
                    ref_table, campaigns_changed = self.merger.sync_campaigns_reference(
                        catalog, campaigns_path
                    )
                    attr_exists = catalog.table_exists(ATTRIBUTED_QUOTE_LINE_TABLE)
                    attr_has_snapshot = (
                        attr_exists
                        and catalog.load_table(ATTRIBUTED_QUOTE_LINE_TABLE).current_snapshot()
                        is not None
                    )
                    if (
                        target_mutated
                        or campaigns_changed
                        or self.config.full_refresh
                        or not attr_has_snapshot
                    ):
                        target_arrow = target_table.scan().to_arrow()
                        campaigns_arrow = ref_table.scan().to_arrow()
                        attributed = self.projector.project_campaign_attribution(
                            quote_lines=target_arrow,
                            campaigns=campaigns_arrow,
                        )
                        self.merger.sync_attributed_quote_lines(catalog, attributed)
                        metrics.attributed_rows = attributed.num_rows
                        all_dq_results.extend(
                            self.dq_validator.validate_campaign_attribution(
                                run_id=run_id,
                                attributed_arrow=attributed,
                            )
                        )
                    else:
                        metrics.attributed_rows = (
                            catalog.load_table(ATTRIBUTED_QUOTE_LINE_TABLE)
                            .scan(selected_fields=("quote_line_id",))
                            .to_arrow()
                            .num_rows
                        )

            # Persist DQ report entries
            with timed_stage(metrics, "persist_dq"):
                self.dq_validator.persist_reports(catalog, all_dq_results)

            metrics.dq_checks_run = len(all_dq_results)
            metrics.dq_warnings = sum(1 for r in all_dq_results if r.severity == "WARNING")
            metrics.dq_failures = sum(1 for r in all_dq_results if r.severity == "ERROR")
            metrics.record_stage("total", (time.perf_counter() - overall_start) * 1000.0)

            log_event(
                self.logger,
                logging.INFO,
                "Pipeline run completed",
                **metrics.to_dict(),
            )
            return metrics
        finally:
            catalog.engine.dispose()


def run(
    incoming: Path,
    warehouse: Path,
    *,
    campaigns: Path | None = None,
    full_refresh: bool = False,
    strict_schema: bool = False,
    json_logs: bool = False,
) -> RunMetrics:
    """Contract entrypoint: process every unprocessed batch in incoming into warehouse."""
    config = PipelineConfig(
        incoming_dir=incoming,
        warehouse_dir=warehouse,
        campaigns_path=campaigns,
        full_refresh=full_refresh,
        strict_schema=strict_schema,
        json_logs=json_logs,
    )
    return PipelineOrchestrator(config).execute()
