"""Automated schema drift detection and data quality validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog

from pipeline.schema import (
    DQ_REPORTS_SCHEMA,
    DQ_REPORTS_TABLE,
    ID_FIELD,
    VALID_QUOTE_STATUSES,
)


class SchemaDriftError(ValueError):
    """Raised when an incoming batch violates the required source schema contract."""


class DataQualityError(RuntimeError):
    """Raised when a critical data integrity invariant fails."""


@dataclass(frozen=True)
class QualityCheckResult:
    """Single automated data quality check outcome."""

    run_id: str
    batch_id: str
    stage: str
    check_name: str
    severity: str  # INFO | WARNING | ERROR
    passed: bool
    anomaly_count: int
    details: str
    checked_at: datetime

    def to_record(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "batch_id": self.batch_id,
            "stage": self.stage,
            "check_name": self.check_name,
            "severity": self.severity,
            "passed": self.passed,
            "anomaly_count": self.anomaly_count,
            "details": self.details,
            "checked_at": self.checked_at,
        }


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class SchemaContractValidator:
    """Detects breaking and additive schema drift against expected contracts."""

    def __init__(self, strict: bool = False) -> None:
        self.strict = strict

    def validate(
        self,
        *,
        run_id: str,
        batch_id: str,
        dataset_name: str,
        actual_schema: pa.Schema,
        expected_schema: pa.Schema,
    ) -> list[QualityCheckResult]:
        now = _utc_now_naive()
        results: list[QualityCheckResult] = []

        actual_fields = {f.name: f.type for f in actual_schema}
        expected_fields = {f.name: f.type for f in expected_schema}

        missing = sorted(set(expected_fields) - set(actual_fields))
        if missing:
            detail = f"{dataset_name} missing required columns: {missing}"
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="schema_contract",
                    check_name=f"{dataset_name}_required_columns",
                    severity="ERROR",
                    passed=False,
                    anomaly_count=len(missing),
                    details=detail,
                    checked_at=now,
                )
            )
            raise SchemaDriftError(detail)

        type_mismatches: list[str] = []
        for col, expected_type in expected_fields.items():
            actual_type = actual_fields[col]
            if actual_type != expected_type:
                type_mismatches.append(f"{col}: expected {expected_type}, got {actual_type}")

        if type_mismatches:
            detail = f"{dataset_name} type mismatches: {'; '.join(type_mismatches)}"
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="schema_contract",
                    check_name=f"{dataset_name}_column_types",
                    severity="ERROR",
                    passed=False,
                    anomaly_count=len(type_mismatches),
                    details=detail,
                    checked_at=now,
                )
            )
            raise SchemaDriftError(detail)

        extra = sorted(set(actual_fields) - set(expected_fields))
        if extra:
            detail = f"{dataset_name} additive schema drift — unexpected columns: {extra}"
            severity = "ERROR" if self.strict else "WARNING"
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="schema_contract",
                    check_name=f"{dataset_name}_additive_drift",
                    severity=severity,
                    passed=False,
                    anomaly_count=len(extra),
                    details=detail,
                    checked_at=now,
                )
            )
            if self.strict:
                raise SchemaDriftError(detail)
        else:
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="schema_contract",
                    check_name=f"{dataset_name}_schema_match",
                    severity="INFO",
                    passed=True,
                    anomaly_count=0,
                    details=f"{dataset_name} matches expected contract ({len(expected_fields)} columns)",
                    checked_at=now,
                )
            )
        return results


class DataQualityValidator:
    """Runs pre-landing anomaly detection and post-merge invariant verification."""

    def validate_incoming_batch(
        self,
        *,
        run_id: str,
        batch_id: str,
        quotes: pa.Table,
        quote_lines: pa.Table,
        known_quote_ids: set[str],
        prior_max_quote_ts: datetime | None,
        prior_max_line_ts: datetime | None,
    ) -> list[QualityCheckResult]:
        now = _utc_now_naive()
        results: list[QualityCheckResult] = []
        con = duckdb.connect()
        try:
            con.register("b_quotes", quotes)
            con.register("b_lines", quote_lines)

            # 1. Null primary key / timestamp checks (critical)
            null_q_pk = int(
                con.execute(
                    "SELECT COUNT(*) FROM b_quotes WHERE quote_id IS NULL OR updated_at IS NULL"
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quotes_non_null_key_and_timestamp",
                    severity="ERROR" if null_q_pk else "INFO",
                    passed=null_q_pk == 0,
                    anomaly_count=null_q_pk,
                    details=f"{null_q_pk} quote rows with NULL quote_id or updated_at",
                    checked_at=now,
                )
            )
            if null_q_pk:
                raise DataQualityError(f"Batch {batch_id} has {null_q_pk} quotes with NULL PK/timestamp")

            null_l_pk = int(
                con.execute(
                    "SELECT COUNT(*) FROM b_lines WHERE quote_line_id IS NULL OR quote_id IS NULL OR updated_at IS NULL"
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quote_lines_non_null_key_and_timestamp",
                    severity="ERROR" if null_l_pk else "INFO",
                    passed=null_l_pk == 0,
                    anomaly_count=null_l_pk,
                    details=f"{null_l_pk} line rows with NULL key or updated_at",
                    checked_at=now,
                )
            )
            if null_l_pk:
                raise DataQualityError(
                    f"Batch {batch_id} has {null_l_pk} quote_lines with NULL PK/timestamp"
                )

            # 2. Within-batch duplicate entity versions (e.g., L000025 out-of-order duplicate)
            dup_quotes = con.execute(
                "SELECT quote_id, COUNT(*) FROM b_quotes GROUP BY quote_id HAVING COUNT(*) > 1 ORDER BY quote_id"
            ).fetchall()
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quotes_within_batch_duplicates",
                    severity="WARNING" if dup_quotes else "INFO",
                    passed=len(dup_quotes) == 0,
                    anomaly_count=len(dup_quotes),
                    details=(
                        f"Duplicate quote_ids within batch: {[r[0] for r in dup_quotes[:10]]}"
                        if dup_quotes
                        else "No duplicate quote_ids within batch"
                    ),
                    checked_at=now,
                )
            )

            dup_lines = con.execute(
                "SELECT quote_line_id, COUNT(*) FROM b_lines GROUP BY quote_line_id HAVING COUNT(*) > 1 ORDER BY quote_line_id"
            ).fetchall()
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quote_lines_within_batch_duplicates",
                    severity="WARNING" if dup_lines else "INFO",
                    passed=len(dup_lines) == 0,
                    anomaly_count=len(dup_lines),
                    details=(
                        f"Duplicate quote_line_ids within batch (resolved by updated_at DESC): "
                        f"{[r[0] for r in dup_lines[:10]]}"
                        if dup_lines
                        else "No duplicate quote_line_ids within batch"
                    ),
                    checked_at=now,
                )
            )

            # 3. Domain validation on quote status
            valid_list = ", ".join(f"'{s}'" for s in sorted(VALID_QUOTE_STATUSES))
            invalid_statuses = con.execute(
                f"SELECT DISTINCT status FROM b_quotes WHERE status IS NULL OR status NOT IN ({valid_list})"
            ).fetchall()
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quotes_valid_status_domain",
                    severity="WARNING" if invalid_statuses else "INFO",
                    passed=len(invalid_statuses) == 0,
                    anomaly_count=len(invalid_statuses),
                    details=(
                        f"Unexpected quote statuses: {[r[0] for r in invalid_statuses]}"
                        if invalid_statuses
                        else "All quote statuses within allowed domain"
                    ),
                    checked_at=now,
                )
            )

            # 4. Line arithmetic & positive quantity/price checks
            bad_arith = int(
                con.execute(
                    """
                    SELECT COUNT(*) FROM b_lines
                    WHERE quantity <= 0
                       OR unit_price_vnd < 0
                       OR line_total_vnd != CAST(quantity AS BIGINT) * unit_price_vnd
                    """
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quote_lines_arithmetic_integrity",
                    severity="WARNING" if bad_arith else "INFO",
                    passed=bad_arith == 0,
                    anomaly_count=bad_arith,
                    details=(
                        f"{bad_arith} rows failed quantity*unit_price_vnd == line_total_vnd or positivity"
                        if bad_arith
                        else "All quote lines satisfy quantity * unit_price_vnd == line_total_vnd"
                    ),
                    checked_at=now,
                )
            )

            # 5. Soft delete volume tracking
            soft_deletes = con.execute(
                "SELECT quote_line_id FROM b_lines WHERE is_deleted = TRUE ORDER BY quote_line_id"
            ).fetchall()
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quote_lines_soft_deletes",
                    severity="INFO",
                    passed=True,
                    anomaly_count=len(soft_deletes),
                    details=(
                        f"{len(soft_deletes)} soft-deleted line(s): {[r[0] for r in soft_deletes[:10]]}"
                        if soft_deletes
                        else "No soft-deleted lines in batch"
                    ),
                    checked_at=now,
                )
            )

            # 6. Late-arriving events below previous high-watermark (e.g., L000366)
            if prior_max_line_ts is not None:
                late_lines = con.execute(
                    "SELECT quote_line_id, updated_at FROM b_lines WHERE updated_at < ? ORDER BY updated_at",
                    [prior_max_line_ts],
                ).fetchall()
                results.append(
                    QualityCheckResult(
                        run_id=run_id,
                        batch_id=batch_id,
                        stage="pre_landing",
                        check_name="quote_lines_late_arriving_below_watermark",
                        severity="WARNING" if late_lines else "INFO",
                        passed=len(late_lines) == 0,
                        anomaly_count=len(late_lines),
                        details=(
                            f"{len(late_lines)} late-arriving line(s) older than prior max {prior_max_line_ts}: "
                            f"{[r[0] for r in late_lines[:10]]}"
                            if late_lines
                            else f"No lines older than prior watermark {prior_max_line_ts}"
                        ),
                        checked_at=now,
                    )
                )

            if prior_max_quote_ts is not None:
                late_quotes = con.execute(
                    "SELECT quote_id, updated_at FROM b_quotes WHERE updated_at < ? ORDER BY updated_at",
                    [prior_max_quote_ts],
                ).fetchall()
                results.append(
                    QualityCheckResult(
                        run_id=run_id,
                        batch_id=batch_id,
                        stage="pre_landing",
                        check_name="quotes_late_arriving_below_watermark",
                        severity="WARNING" if late_quotes else "INFO",
                        passed=len(late_quotes) == 0,
                        anomaly_count=len(late_quotes),
                        details=(
                            f"{len(late_quotes)} late-arriving quote(s) older than prior max {prior_max_quote_ts}: "
                            f"{[r[0] for r in late_quotes[:10]]}"
                            if late_quotes
                            else f"No quotes older than prior watermark {prior_max_quote_ts}"
                        ),
                        checked_at=now,
                    )
                )

            # 7. Orphan lines whose parent quote header has not arrived yet (e.g., Q0121 in batch_1)
            batch_quote_ids = {
                r[0] for r in con.execute("SELECT DISTINCT quote_id FROM b_quotes").fetchall()
            }
            all_known_quotes = known_quote_ids | batch_quote_ids
            batch_line_quote_pairs = con.execute(
                "SELECT DISTINCT quote_line_id, quote_id FROM b_lines ORDER BY quote_line_id"
            ).fetchall()
            orphan_lines = [
                f"{lid}->{qid}" for lid, qid in batch_line_quote_pairs if qid not in all_known_quotes
            ]
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="pre_landing",
                    check_name="quote_lines_missing_parent_quote",
                    severity="WARNING" if orphan_lines else "INFO",
                    passed=len(orphan_lines) == 0,
                    anomaly_count=len(orphan_lines),
                    details=(
                        f"{len(orphan_lines)} line(s) arrived before parent quote header: "
                        f"{orphan_lines[:10]}"
                        if orphan_lines
                        else "All quote lines reference a known quote header"
                    ),
                    checked_at=now,
                )
            )
        finally:
            con.close()
        return results

    def validate_target_object(
        self,
        *,
        run_id: str,
        batch_id: str,
        target_arrow: pa.Table,
    ) -> list[QualityCheckResult]:
        now = _utc_now_naive()
        results: list[QualityCheckResult] = []
        con = duckdb.connect()
        try:
            con.register("target_tbl", target_arrow)
            total, distinct_ids, null_ids, missing_headers = con.execute(
                f"""
                SELECT
                    COUNT(*) AS total_rows,
                    COUNT(DISTINCT {ID_FIELD}) AS distinct_ids,
                    COUNT(*) FILTER (WHERE {ID_FIELD} IS NULL) AS null_ids,
                    COUNT(*) FILTER (WHERE quote_number IS NULL) AS missing_headers
                FROM target_tbl
                """
            ).fetchone()

            dup_count = int(total - distinct_ids)
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="post_merge",
                    check_name="target_unique_quote_line_id",
                    severity="ERROR" if dup_count else "INFO",
                    passed=dup_count == 0,
                    anomaly_count=dup_count,
                    details=f"Target has {total} rows and {distinct_ids} distinct {ID_FIELD}s",
                    checked_at=now,
                )
            )
            if dup_count:
                raise DataQualityError(f"crm.quote_line has {dup_count} duplicate {ID_FIELD} rows")

            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="post_merge",
                    check_name="target_non_null_quote_line_id",
                    severity="ERROR" if null_ids else "INFO",
                    passed=null_ids == 0,
                    anomaly_count=int(null_ids),
                    details=f"{null_ids} rows with NULL {ID_FIELD}",
                    checked_at=now,
                )
            )
            if null_ids:
                raise DataQualityError(f"crm.quote_line has {null_ids} NULL {ID_FIELD} rows")

            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id=batch_id,
                    stage="post_merge",
                    check_name="target_unresolved_quote_headers",
                    severity="WARNING" if missing_headers else "INFO",
                    passed=missing_headers == 0,
                    anomaly_count=int(missing_headers),
                    details=(
                        f"{missing_headers} live line(s) awaiting delayed quote header"
                        if missing_headers
                        else "All live quote lines enriched with parent quote header"
                    ),
                    checked_at=now,
                )
            )
        finally:
            con.close()
        return results

    def validate_campaign_attribution(
        self,
        *,
        run_id: str,
        attributed_arrow: pa.Table,
    ) -> list[QualityCheckResult]:
        now = _utc_now_naive()
        results: list[QualityCheckResult] = []
        con = duckdb.connect()
        try:
            con.register("attr_tbl", attributed_arrow)
            pre_launch_quotes = con.execute(
                """
                SELECT DISTINCT quote_id, campaign_id, quote_date, campaign_started_on
                FROM attr_tbl
                WHERE attribution_status = 'attributed_pre_launch'
                ORDER BY quote_id
                """
            ).fetchall()
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id="campaign_attribution",
                    stage="attribution",
                    check_name="attribution_pre_launch_quotes",
                    severity="WARNING" if pre_launch_quotes else "INFO",
                    passed=len(pre_launch_quotes) == 0,
                    anomaly_count=len(pre_launch_quotes),
                    details=(
                        f"{len(pre_launch_quotes)} quote(s) have quote_date < campaign.started_on: "
                        f"{[r[0] for r in pre_launch_quotes[:10]]}"
                        if pre_launch_quotes
                        else "All attributed quotes fall on or after campaign.started_on"
                    ),
                    checked_at=now,
                )
            )

            unknown_utm = int(
                con.execute(
                    "SELECT COUNT(DISTINCT quote_id) FROM attr_tbl WHERE attribution_status IN ('unknown_utm', 'partial_utm')"
                ).fetchone()[0]
            )
            results.append(
                QualityCheckResult(
                    run_id=run_id,
                    batch_id="campaign_attribution",
                    stage="attribution",
                    check_name="attribution_unknown_or_partial_utm",
                    severity="WARNING" if unknown_utm else "INFO",
                    passed=unknown_utm == 0,
                    anomaly_count=unknown_utm,
                    details=f"{unknown_utm} quote(s) carry unrecognized or partial UTM parameters",
                    checked_at=now,
                )
            )
        finally:
            con.close()
        return results

    @staticmethod
    def persist_reports(catalog: Catalog, results: list[QualityCheckResult]) -> None:
        if not results:
            return
        table = catalog.create_table_if_not_exists(DQ_REPORTS_TABLE, schema=DQ_REPORTS_SCHEMA)
        arrow_table = pa.Table.from_pylist([r.to_record() for r in results], schema=DQ_REPORTS_SCHEMA)
        table.append(arrow_table)
