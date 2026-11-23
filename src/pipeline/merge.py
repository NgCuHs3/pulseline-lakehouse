"""Atomic PyIceberg object merge and stretch-goal campaign attribution writer."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import In
from pyiceberg.table import Table

from pipeline.schema import (
    ATTRIBUTED_QUOTE_LINE_SCHEMA,
    ATTRIBUTED_QUOTE_LINE_TABLE,
    ID_FIELD,
    REFERENCE_CAMPAIGNS_SCHEMA,
    REFERENCE_CAMPAIGNS_TABLE,
)

DELETE_CHUNK_SIZE = 500


@dataclass(frozen=True)
class MergeStats:
    """Exact mutation counts produced by merging into crm.quote_line."""

    rows_inserted: int
    rows_updated: int
    rows_deleted: int
    snapshot_changed: bool


def _file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


class ObjectMerger:
    """Applies atomic Iceberg upserts/deletes on crm.quote_line and auxiliary marts."""

    def merge_quote_lines(
        self,
        *,
        target_table: Table,
        live_rows: pa.Table,
        deleted_ids: list[str],
        full_refresh: bool = False,
    ) -> MergeStats:
        before_snapshot = target_table.current_snapshot()
        before_snap_id = before_snapshot.snapshot_id if before_snapshot else None

        if full_refresh:
            existing_count = (
                target_table.scan(selected_fields=(ID_FIELD,)).to_arrow().num_rows
                if target_table.current_snapshot() is not None
                else 0
            )
            if target_table.current_snapshot() is None:
                if live_rows.num_rows > 0:
                    target_table.append(live_rows)
            else:
                target_table.overwrite(live_rows)
            after_snapshot = target_table.current_snapshot()
            after_snap_id = after_snapshot.snapshot_id if after_snapshot else None
            return MergeStats(
                rows_inserted=max(live_rows.num_rows - existing_count, 0),
                rows_updated=min(live_rows.num_rows, existing_count),
                rows_deleted=max(existing_count - live_rows.num_rows, 0),
                snapshot_changed=after_snap_id != before_snap_id,
            )

        existing_deleted_ids = self._filter_existing_ids(target_table, deleted_ids)
        if live_rows.num_rows == 0 and not existing_deleted_ids:
            return MergeStats(
                rows_inserted=0,
                rows_updated=0,
                rows_deleted=0,
                snapshot_changed=False,
            )

        rows_inserted = 0
        rows_updated = 0
        with target_table.transaction() as tx:
            if live_rows.num_rows > 0:
                upsert_res = tx.upsert(live_rows, join_cols=[ID_FIELD])
                rows_inserted = upsert_res.rows_inserted
                rows_updated = upsert_res.rows_updated
            for i in range(0, len(existing_deleted_ids), DELETE_CHUNK_SIZE):
                chunk = existing_deleted_ids[i : i + DELETE_CHUNK_SIZE]
                tx.delete(In(ID_FIELD, chunk))

        after_snapshot = target_table.current_snapshot()
        after_snap_id = after_snapshot.snapshot_id if after_snapshot else None
        return MergeStats(
            rows_inserted=rows_inserted,
            rows_updated=rows_updated,
            rows_deleted=len(existing_deleted_ids),
            snapshot_changed=after_snap_id != before_snap_id,
        )

    @staticmethod
    def _filter_existing_ids(target_table: Table, candidate_ids: list[str]) -> list[str]:
        """Return only candidate_ids currently present in target_table."""
        if not candidate_ids or target_table.current_snapshot() is None:
            return []
        unique_candidates = sorted(set(candidate_ids))
        present: list[str] = []
        for i in range(0, len(unique_candidates), DELETE_CHUNK_SIZE):
            chunk = unique_candidates[i : i + DELETE_CHUNK_SIZE]
            matched = target_table.scan(
                row_filter=In(ID_FIELD, chunk),
                selected_fields=(ID_FIELD,),
            ).to_arrow()
            if matched.num_rows > 0:
                present.extend(matched[ID_FIELD].to_pylist())
        return sorted(set(present))

    def sync_campaigns_reference(
        self, catalog: Catalog, campaigns_path: Path
    ) -> tuple[Table, bool]:
        """Load data/reference/campaigns.parquet into reference.campaigns when changed."""
        ref_table = catalog.create_table_if_not_exists(
            REFERENCE_CAMPAIGNS_TABLE, schema=REFERENCE_CAMPAIGNS_SCHEMA
        )
        digest = _file_sha256(campaigns_path)
        existing = ref_table.scan(selected_fields=("_content_sha256",)).to_arrow()
        if existing.num_rows > 0 and existing["_content_sha256"][0].as_py() == digest:
            return ref_table, False

        raw = pq.read_table(campaigns_path)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        enriched = raw.append_column(
            "_content_sha256", pa.array([digest] * raw.num_rows, type=pa.string())
        ).append_column(
            "_ingested_at", pa.array([now] * raw.num_rows, type=pa.timestamp("us"))
        )
        if ref_table.current_snapshot() is None:
            ref_table.append(enriched.cast(REFERENCE_CAMPAIGNS_SCHEMA))
        else:
            ref_table.overwrite(enriched.cast(REFERENCE_CAMPAIGNS_SCHEMA))
        return ref_table, True

    def sync_attributed_quote_lines(
        self, catalog: Catalog, attributed_arrow: pa.Table
    ) -> tuple[Table, bool]:
        """Materialize crm.quote_line_attributed idempotently."""
        attr_table = catalog.create_table_if_not_exists(
            ATTRIBUTED_QUOTE_LINE_TABLE, schema=ATTRIBUTED_QUOTE_LINE_SCHEMA
        )
        normalized_new = attributed_arrow.cast(ATTRIBUTED_QUOTE_LINE_SCHEMA).sort_by(
            [(ID_FIELD, "ascending")]
        ).combine_chunks()
        if attr_table.current_snapshot() is not None:
            existing = (
                attr_table.scan()
                .to_arrow()
                .cast(ATTRIBUTED_QUOTE_LINE_SCHEMA)
                .sort_by([(ID_FIELD, "ascending")])
                .combine_chunks()
            )
            if existing.equals(normalized_new):
                return attr_table, False
            attr_table.overwrite(normalized_new)
        else:
            attr_table.append(normalized_new)
        return attr_table, True
