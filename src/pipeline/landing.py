"""Incremental verbatim landing layer with content-addressed batch ledger."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from pipeline.quality import DataQualityValidator, QualityCheckResult, SchemaContractValidator
from pipeline.schema import (
    BATCH_LEDGER_SCHEMA,
    BATCH_LEDGER_TABLE,
    DQ_REPORTS_SCHEMA,
    DQ_REPORTS_TABLE,
    LANDING_QUOTE_LINES_SCHEMA,
    LANDING_QUOTE_LINES_TABLE,
    LANDING_QUOTES_SCHEMA,
    LANDING_QUOTES_TABLE,
    QUOTE_LINE_SCHEMA,
    QUOTE_LINES_SOURCE_SCHEMA,
    QUOTES_SOURCE_SCHEMA,
    TABLE_IDENTIFIER,
)

BATCH_STATUS_LANDED = "LANDED"
BATCH_STATUS_COMPLETED = "COMPLETED"
BATCH_STATUS_SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"
BATCH_STATUS_FAILED = "FAILED"

QUOTES_FILENAME = "quotes.parquet"
QUOTE_LINES_FILENAME = "quote_lines.parquet"


@dataclass(frozen=True)
class BatchCandidate:
    """Incoming batch directory ready for inspection/ingestion."""

    batch_id: str
    directory: Path
    quotes_path: Path
    quote_lines_path: Path
    content_sha256: str


@dataclass(frozen=True)
class BatchLedgerEntry:
    """Latest known state of a batch in meta.batch_ledger."""

    batch_id: str
    content_sha256: str
    status: str
    quotes_rows: int
    quote_lines_rows: int


@dataclass
class LandingOutcome:
    """Summary of landing work performed in one run."""

    batches_to_project: list[str]
    recovered_batches: list[str]
    skipped_already_done: list[str]
    skipped_duplicate_content: list[tuple[str, str]]  # (batch_id, original_batch_id)
    quotes_landed: int
    quote_lines_landed: int
    batch_row_counts: dict[str, tuple[int, int]]
    batch_hashes: dict[str, str]
    quality_results: list[QualityCheckResult]


def compute_batch_fingerprint(quotes_path: Path, quote_lines_path: Path) -> str:
    """Compute a deterministic SHA-256 content digest across both batch Parquet files."""
    hasher = hashlib.sha256()
    for label, file_path in ((QUOTES_FILENAME, quotes_path), (QUOTE_LINES_FILENAME, quote_lines_path)):
        hasher.update(label.encode("utf-8"))
        hasher.update(b"\x00")
        with file_path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                hasher.update(chunk)
        hasher.update(b"\x00")
    return hasher.hexdigest()


def discover_batches(incoming_dir: Path) -> list[BatchCandidate]:
    """Discover incoming batch directories sorted lexicographically (arrival order)."""
    incoming_dir = incoming_dir.resolve()
    if not incoming_dir.exists():
        raise FileNotFoundError(f"Incoming directory does not exist: {incoming_dir}")

    candidates: list[BatchCandidate] = []
    for entry in sorted(incoming_dir.iterdir(), key=lambda p: p.name):
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        quotes_path = entry / QUOTES_FILENAME
        lines_path = entry / QUOTE_LINES_FILENAME
        has_q = quotes_path.is_file()
        has_l = lines_path.is_file()
        if not has_q and not has_l:
            continue
        if has_q != has_l:
            missing = QUOTE_LINES_FILENAME if has_q else QUOTES_FILENAME
            raise FileNotFoundError(f"Incomplete batch '{entry.name}': missing {missing}")
        candidates.append(
            BatchCandidate(
                batch_id=entry.name,
                directory=entry,
                quotes_path=quotes_path,
                quote_lines_path=lines_path,
                content_sha256=compute_batch_fingerprint(quotes_path, lines_path),
            )
        )
    return candidates


def _attach_lineage_columns(table: pa.Table, batch_id: str, ingested_at: datetime) -> pa.Table:
    """Attach append-only ingestion metadata while leaving source columns verbatim."""
    n = table.num_rows
    batch_col = pa.array([batch_id] * n, type=pa.string())
    row_num_col = pa.array(range(1, n + 1), type=pa.int64())
    ingested_col = pa.array([ingested_at] * n, type=pa.timestamp("us"))
    return (
        table.append_column("_batch_id", batch_col)
        .append_column("_row_num", row_num_col)
        .append_column("_ingested_at", ingested_col)
    )


def _append_with_schema_evolution(iceberg_table: Table, arrow_table: pa.Table) -> None:
    """Append Arrow rows, evolving the Iceberg schema safely on additive columns."""
    existing_names = {f.name for f in iceberg_table.schema().fields}
    incoming_names = set(arrow_table.schema.names)
    if incoming_names - existing_names:
        with iceberg_table.update_schema() as update:
            update.union_by_name(arrow_table.schema)
    iceberg_table.append(arrow_table)


class LandingManager:
    """Manages catalog bootstrapping, verbatim landing, and crash-resilient ledger state."""

    def __init__(
        self,
        catalog: Catalog,
        *,
        strict_schema: bool = False,
    ) -> None:
        self.catalog = catalog
        self.schema_validator = SchemaContractValidator(strict=strict_schema)
        self.dq_validator = DataQualityValidator()

    def ensure_core_tables(self) -> tuple[Table, Table, Table, Table]:
        """Ensure required namespaces and core Iceberg tables exist."""
        for ns in ("landing", "crm", "meta", "reference"):
            self.catalog.create_namespace_if_not_exists(ns)

        landing_quotes = self.catalog.create_table_if_not_exists(
            LANDING_QUOTES_TABLE, schema=LANDING_QUOTES_SCHEMA
        )
        landing_lines = self.catalog.create_table_if_not_exists(
            LANDING_QUOTE_LINES_TABLE, schema=LANDING_QUOTE_LINES_SCHEMA
        )
        ledger = self.catalog.create_table_if_not_exists(
            BATCH_LEDGER_TABLE, schema=BATCH_LEDGER_SCHEMA
        )
        self.catalog.create_table_if_not_exists(DQ_REPORTS_TABLE, schema=DQ_REPORTS_SCHEMA)
        target = self.catalog.create_table_if_not_exists(
            TABLE_IDENTIFIER, schema=QUOTE_LINE_SCHEMA
        )
        return landing_quotes, landing_lines, ledger, target

    def read_ledger_state(
        self, ledger_table: Table
    ) -> tuple[dict[str, BatchLedgerEntry], dict[str, str]]:
        """Return latest state per batch_id and map of ingested content_sha256 -> batch_id."""
        status_rank = {
            BATCH_STATUS_LANDED: 1,
            BATCH_STATUS_FAILED: 2,
            BATCH_STATUS_COMPLETED: 3,
            BATCH_STATUS_SKIPPED_DUPLICATE: 3,
        }
        arrow = ledger_table.scan().to_arrow()
        latest_by_batch: dict[str, tuple[datetime, int, BatchLedgerEntry]] = {}
        for row in arrow.to_pylist():
            ts = row["updated_at"]
            entry = BatchLedgerEntry(
                batch_id=row["batch_id"],
                content_sha256=row["content_sha256"],
                status=row["status"],
                quotes_rows=int(row["quotes_rows"] or 0),
                quote_lines_rows=int(row["quote_lines_rows"] or 0),
            )
            rank = status_rank.get(entry.status, 0)
            prev = latest_by_batch.get(entry.batch_id)
            if prev is None or (ts, rank) >= (prev[0], prev[1]):
                latest_by_batch[entry.batch_id] = (ts, rank, entry)

        entries = {bid: item[2] for bid, item in latest_by_batch.items()}
        ingested_hashes: dict[str, str] = {}
        for bid, entry in sorted(entries.items()):
            if entry.status in (BATCH_STATUS_COMPLETED, BATCH_STATUS_LANDED):
                ingested_hashes.setdefault(entry.content_sha256, bid)
        return entries, ingested_hashes

    def append_ledger_entry(
        self,
        ledger_table: Table,
        *,
        batch_id: str,
        content_sha256: str,
        status: str,
        quotes_rows: int = 0,
        quote_lines_rows: int = 0,
        rows_inserted: int = 0,
        rows_updated: int = 0,
        rows_deleted: int = 0,
        duration_ms: float = 0.0,
        note: str = "",
    ) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        record = {
            "batch_id": batch_id,
            "content_sha256": content_sha256,
            "status": status,
            "quotes_rows": quotes_rows,
            "quote_lines_rows": quote_lines_rows,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "rows_deleted": rows_deleted,
            "duration_ms": float(duration_ms),
            "note": note,
            "updated_at": now,
        }
        ledger_table.append(pa.Table.from_pylist([record], schema=BATCH_LEDGER_SCHEMA))

    @staticmethod
    def _landing_baseline_stats(
        landing_quotes: Table, landing_lines: Table
    ) -> tuple[set[str], datetime | None, datetime | None]:
        """Extract known quote_ids and prior watermarks using vectorized PyArrow compute."""
        known_quote_ids: set[str] = set()
        prior_max_quote_ts: datetime | None = None
        if landing_quotes.current_snapshot() is not None:
            q_arrow = landing_quotes.scan(selected_fields=("quote_id", "updated_at")).to_arrow()
            if q_arrow.num_rows > 0:
                known_quote_ids = {
                    v for v in pc.unique(q_arrow["quote_id"]).to_pylist() if v is not None
                }
                prior_max_quote_ts = pc.max(q_arrow["updated_at"]).as_py()

        prior_max_line_ts: datetime | None = None
        if landing_lines.current_snapshot() is not None:
            l_arrow = landing_lines.scan(selected_fields=("updated_at",)).to_arrow()
            if l_arrow.num_rows > 0:
                prior_max_line_ts = pc.max(l_arrow["updated_at"]).as_py()

        return known_quote_ids, prior_max_quote_ts, prior_max_line_ts

    def ingest_new_batches(
        self,
        *,
        run_id: str,
        candidates: list[BatchCandidate],
        landing_quotes: Table,
        landing_lines: Table,
        ledger_table: Table,
    ) -> LandingOutcome:
        """Incrementally land all unseen batches verbatim and detect redeliveries/recoveries."""
        entries, ingested_hashes = self.read_ledger_state(ledger_table)
        baseline_loaded = False
        known_quote_ids: set[str] = set()
        prior_max_quote_ts: datetime | None = None
        prior_max_line_ts: datetime | None = None

        batches_to_project: list[str] = []
        recovered_batches: list[str] = []
        skipped_already_done: list[str] = []
        skipped_duplicate_content: list[tuple[str, str]] = []
        batch_row_counts: dict[str, tuple[int, int]] = {}
        batch_hashes: dict[str, str] = {}
        quality_results: list[QualityCheckResult] = []
        quotes_landed = 0
        quote_lines_landed = 0

        # Also recover any LANDED batch recorded in ledger even if its folder was moved/archived
        discovered_ids = {c.batch_id for c in candidates}
        for bid, entry in sorted(entries.items()):
            if entry.status == BATCH_STATUS_LANDED and bid not in discovered_ids:
                batches_to_project.append(bid)
                recovered_batches.append(bid)
                batch_row_counts[bid] = (entry.quotes_rows, entry.quote_lines_rows)
                batch_hashes[bid] = entry.content_sha256

        for candidate in candidates:
            existing = entries.get(candidate.batch_id)

            # 1. Already completed or already skipped as duplicate
            if existing and existing.status in (
                BATCH_STATUS_COMPLETED,
                BATCH_STATUS_SKIPPED_DUPLICATE,
            ):
                skipped_already_done.append(candidate.batch_id)
                continue

            # 2. Crash recovery: batch was landed into Iceberg before previous run failed
            if existing and existing.status == BATCH_STATUS_LANDED:
                batches_to_project.append(candidate.batch_id)
                recovered_batches.append(candidate.batch_id)
                batch_row_counts[candidate.batch_id] = (
                    existing.quotes_rows,
                    existing.quote_lines_rows,
                )
                batch_hashes[candidate.batch_id] = existing.content_sha256
                continue

            # 3. Redelivered batch under a new name (identical content SHA-256)
            original_batch_id = ingested_hashes.get(candidate.content_sha256)
            if original_batch_id is not None:
                skipped_duplicate_content.append((candidate.batch_id, original_batch_id))
                self.append_ledger_entry(
                    ledger_table,
                    batch_id=candidate.batch_id,
                    content_sha256=candidate.content_sha256,
                    status=BATCH_STATUS_SKIPPED_DUPLICATE,
                    note=f"Content SHA-256 identical to already-ingested batch '{original_batch_id}'",
                )
                quality_results.append(
                    QualityCheckResult(
                        run_id=run_id,
                        batch_id=candidate.batch_id,
                        stage="landing",
                        check_name="batch_content_deduplication",
                        severity="WARNING",
                        passed=False,
                        anomaly_count=1,
                        details=(
                            f"Skipped redelivered batch '{candidate.batch_id}' "
                            f"(identical SHA-256 to '{original_batch_id}')"
                        ),
                        checked_at=datetime.now(timezone.utc).replace(tzinfo=None),
                    )
                )
                continue

            # Lazy-load landing baseline stats only when a new batch is actually ingested
            if not baseline_loaded:
                known_quote_ids, prior_max_quote_ts, prior_max_line_ts = (
                    self._landing_baseline_stats(landing_quotes, landing_lines)
                )
                baseline_loaded = True

            # 4. Read raw Parquet files verbatim
            raw_quotes = pq.read_table(candidate.quotes_path)
            raw_lines = pq.read_table(candidate.quote_lines_path)

            # 5. Schema contract & drift checks
            quality_results.extend(
                self.schema_validator.validate(
                    run_id=run_id,
                    batch_id=candidate.batch_id,
                    dataset_name="quotes",
                    actual_schema=raw_quotes.schema,
                    expected_schema=QUOTES_SOURCE_SCHEMA,
                )
            )
            quality_results.extend(
                self.schema_validator.validate(
                    run_id=run_id,
                    batch_id=candidate.batch_id,
                    dataset_name="quote_lines",
                    actual_schema=raw_lines.schema,
                    expected_schema=QUOTE_LINES_SOURCE_SCHEMA,
                )
            )

            # 6. Pre-landing anomaly & DQ checks
            quality_results.extend(
                self.dq_validator.validate_incoming_batch(
                    run_id=run_id,
                    batch_id=candidate.batch_id,
                    quotes=raw_quotes,
                    quote_lines=raw_lines,
                    known_quote_ids=known_quote_ids,
                    prior_max_quote_ts=prior_max_quote_ts,
                    prior_max_line_ts=prior_max_line_ts,
                )
            )

            # 7. Append verbatim rows + lineage metadata into Iceberg landing tables
            ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
            if raw_quotes.num_rows > 0:
                _append_with_schema_evolution(
                    landing_quotes,
                    _attach_lineage_columns(raw_quotes, candidate.batch_id, ingested_at),
                )
            if raw_lines.num_rows > 0:
                _append_with_schema_evolution(
                    landing_lines,
                    _attach_lineage_columns(raw_lines, candidate.batch_id, ingested_at),
                )

            # 8. Write-Ahead Ledger entry (LANDED) for crash recovery
            self.append_ledger_entry(
                ledger_table,
                batch_id=candidate.batch_id,
                content_sha256=candidate.content_sha256,
                status=BATCH_STATUS_LANDED,
                quotes_rows=raw_quotes.num_rows,
                quote_lines_rows=raw_lines.num_rows,
                note="Landed verbatim into Iceberg bronze tables",
            )

            # Update running baseline for subsequent batches in the same run
            ingested_hashes[candidate.content_sha256] = candidate.batch_id
            batches_to_project.append(candidate.batch_id)
            batch_row_counts[candidate.batch_id] = (raw_quotes.num_rows, raw_lines.num_rows)
            batch_hashes[candidate.batch_id] = candidate.content_sha256
            quotes_landed += raw_quotes.num_rows
            quote_lines_landed += raw_lines.num_rows

            if raw_quotes.num_rows > 0:
                known_quote_ids.update(
                    v for v in pc.unique(raw_quotes["quote_id"]).to_pylist() if v is not None
                )
                batch_q_max = pc.max(raw_quotes["updated_at"]).as_py()
                if batch_q_max is not None:
                    prior_max_quote_ts = (
                        batch_q_max
                        if prior_max_quote_ts is None
                        else max(prior_max_quote_ts, batch_q_max)
                    )
            if raw_lines.num_rows > 0:
                batch_l_max = pc.max(raw_lines["updated_at"]).as_py()
                if batch_l_max is not None:
                    prior_max_line_ts = (
                        batch_l_max
                        if prior_max_line_ts is None
                        else max(prior_max_line_ts, batch_l_max)
                    )

        return LandingOutcome(
            batches_to_project=batches_to_project,
            recovered_batches=recovered_batches,
            skipped_already_done=skipped_already_done,
            skipped_duplicate_content=skipped_duplicate_content,
            quotes_landed=quotes_landed,
            quote_lines_landed=quote_lines_landed,
            batch_row_counts=batch_row_counts,
            batch_hashes=batch_hashes,
            quality_results=quality_results,
        )
