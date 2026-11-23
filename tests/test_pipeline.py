"""Comprehensive test suite covering assignment invariants, anomalies, and platform features."""

from __future__ import annotations

import json
import shutil
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline.catalog import open_catalog
from pipeline.cli import main
from pipeline.landing import BATCH_STATUS_COMPLETED, BATCH_STATUS_LANDED, BATCH_STATUS_SKIPPED_DUPLICATE, LandingManager, compute_batch_fingerprint
from pipeline.quality import SchemaDriftError
from pipeline.run import run
from pipeline.schema import (
    ATTRIBUTED_QUOTE_LINE_TABLE,
    BATCH_LEDGER_TABLE,
    DQ_REPORTS_TABLE,
    ID_FIELD,
    LANDING_QUOTE_LINES_TABLE,
    LANDING_QUOTES_TABLE,
    QUOTE_LINE_SCHEMA,
    QUOTE_LINES_SOURCE_SCHEMA,
    QUOTES_SOURCE_SCHEMA,
    REFERENCE_CAMPAIGNS_TABLE,
    TABLE_IDENTIFIER,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
INCOMING_DATA = REPO_ROOT / "data" / "incoming"
CAMPAIGNS_DATA = REPO_ROOT / "data" / "reference" / "campaigns.parquet"


def _read_table_sorted(warehouse: Path, identifier: str = TABLE_IDENTIFIER) -> pa.Table:
    cat = open_catalog(warehouse)
    try:
        tbl = cat.load_table(identifier).scan().to_arrow()
        if ID_FIELD in tbl.schema.names:
            tbl = tbl.sort_by([(ID_FIELD, "ascending")])
        return tbl.combine_chunks()
    finally:
        cat.engine.dispose()


def _snapshot_count(warehouse: Path, identifier: str) -> int:
    cat = open_catalog(warehouse)
    try:
        return len(cat.load_table(identifier).snapshots())
    finally:
        cat.engine.dispose()


def _write_batch(
    incoming: Path,
    batch_name: str,
    quotes: list[dict[str, object]],
    lines: list[dict[str, object]],
    *,
    quotes_schema: pa.Schema = QUOTES_SOURCE_SCHEMA,
    lines_schema: pa.Schema = QUOTE_LINES_SOURCE_SCHEMA,
) -> Path:
    bdir = incoming / batch_name
    bdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(quotes, schema=quotes_schema), bdir / "quotes.parquet")
    pq.write_table(pa.Table.from_pylist(lines, schema=lines_schema), bdir / "quote_lines.parquet")
    return bdir


# ---------------------------------------------------------------------------
# 1. Real dataset: incremental arrival, planted anomalies, and equivalence
# ---------------------------------------------------------------------------


def test_incremental_batches_resolve_all_planted_anomalies(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    warehouse = tmp_path / "warehouse"
    incoming.mkdir()

    # Step 1: Only batch_1 arrives
    shutil.copytree(INCOMING_DATA / "batch_1", incoming / "batch_1")
    m1 = run(incoming, warehouse, campaigns=CAMPAIGNS_DATA)
    assert m1.batches_processed == 1
    assert m1.live_rows_total == 298

    t1 = _read_table_sorted(warehouse)
    assert t1.schema == QUOTE_LINE_SCHEMA
    by_id_1 = {r["quote_line_id"]: r for r in t1.to_pylist()}

    # Anomaly 1: L000025 appears twice in batch_1 (row 25 newer qty=8, row 299 stale qty=5)
    assert by_id_1["L000025"]["quantity"] == 8
    assert by_id_1["L000025"]["line_total_vnd"] == 200_000_000
    assert by_id_1["L000025"]["line_updated_at"] == datetime(2026, 9, 4, 9, 9, 2)

    # Anomaly 2: L000297 & L000298 arrive in batch_1 before parent quote Q0121 (in batch_2)
    assert "L000297" in by_id_1 and "L000298" in by_id_1
    assert by_id_1["L000297"]["quote_id"] == "Q0121"
    assert by_id_1["L000297"]["quote_number"] is None
    assert by_id_1["L000298"]["quote_status"] is None

    # L000047 is live in batch_1
    assert "L000047" in by_id_1

    # Q0007 starts as draft in batch_1
    q0007_lines_b1 = [r for r in by_id_1.values() if r["quote_id"] == "Q0007"]
    assert len(q0007_lines_b1) == 4
    assert all(r["quote_status"] == "draft" for r in q0007_lines_b1)

    # Step 2: batch_2 arrives
    shutil.copytree(INCOMING_DATA / "batch_2", incoming / "batch_2")
    m2 = run(incoming, warehouse, campaigns=CAMPAIGNS_DATA)
    assert m2.batches_processed == 1
    assert m2.rows_deleted == 1
    assert m2.live_rows_total == 365

    t2 = _read_table_sorted(warehouse)
    assert t2.schema == QUOTE_LINE_SCHEMA
    by_id_2 = {r["quote_line_id"]: r for r in t2.to_pylist()}
    assert len(by_id_2) == 365

    # Anomaly 2 resolved: Q0121 header backfilled onto L000297 & L000298
    for lid in ("L000297", "L000298"):
        assert by_id_2[lid]["quote_number"] == "BG-2026-00121"
        assert by_id_2[lid]["customer_id"] == "C012"
        assert by_id_2[lid]["quote_status"] == "draft"
        assert by_id_2[lid]["quote_date"] == date(2026, 9, 12)
        assert by_id_2[lid]["utm_source"] == "google"
        assert by_id_2[lid]["utm_campaign"] == "brand_search"
        assert by_id_2[lid]["quote_updated_at"] == datetime(2026, 9, 13, 3, 19, 59)

    # Anomaly 3: L000366 late-arriving row (updated_at 2026-09-05 < batch_1 max) joined with batch_1 Q0005
    assert "L000366" in by_id_2
    assert by_id_2["L000366"]["quote_id"] == "Q0005"
    assert by_id_2["L000366"]["quote_number"] == "BG-2026-00005"
    assert by_id_2["L000366"]["line_updated_at"] == datetime(2026, 9, 5, 14, 30, 0)
    assert by_id_2["L000366"]["line_total_vnd"] == 180_000_000

    # Anomaly 4: L000047 soft-deleted in batch_2 must be removed from crm.quote_line
    assert "L000047" not in by_id_2

    # Anomaly 5: Header-only updates propagate to ALL untouched lines from batch_1
    q0007_lines_b2 = [r for r in by_id_2.values() if r["quote_id"] == "Q0007"]
    assert len(q0007_lines_b2) == 4
    assert all(r["quote_status"] == "rejected" for r in q0007_lines_b2)
    assert all(r["quote_updated_at"] == datetime(2026, 9, 12, 20, 25, 31) for r in q0007_lines_b2)

    # Timestamp-only quote update (Q0053 stayed 'sent') and line update (L000151 stayed qty=5)
    q0053_lines = [r for r in by_id_2.values() if r["quote_id"] == "Q0053"]
    assert all(r["quote_updated_at"] == datetime(2026, 9, 13, 1, 21, 43) for r in q0053_lines)
    assert by_id_2["L000151"]["line_updated_at"] == datetime(2026, 9, 10, 8, 59, 59)

    # Line-only update on untouched batch_1 quote (L000034 on Q0013)
    assert by_id_2["L000034"]["quantity"] == 12
    assert by_id_2["L000034"]["line_total_vnd"] == 10_800_000
    assert by_id_2["L000034"]["quote_number"] == "BG-2026-00013"

    # Zero unresolved quote headers remain after batch_2
    assert all(r["quote_number"] is not None for r in by_id_2.values())

    # Step 3: batch_2_redelivered (identical bytes, new folder name) changes NOTHING
    snaps_target_before = _snapshot_count(warehouse, TABLE_IDENTIFIER)
    snaps_land_q_before = _snapshot_count(warehouse, LANDING_QUOTES_TABLE)
    snaps_land_l_before = _snapshot_count(warehouse, LANDING_QUOTE_LINES_TABLE)

    shutil.copytree(INCOMING_DATA / "batch_2_redelivered", incoming / "batch_2_redelivered")
    m3 = run(incoming, warehouse, campaigns=CAMPAIGNS_DATA)
    assert m3.batches_processed == 0
    assert m3.batches_skipped_duplicate_content == 1
    assert m3.rows_inserted == 0 and m3.rows_updated == 0 and m3.rows_deleted == 0

    assert _snapshot_count(warehouse, TABLE_IDENTIFIER) == snaps_target_before
    assert _snapshot_count(warehouse, LANDING_QUOTES_TABLE) == snaps_land_q_before
    assert _snapshot_count(warehouse, LANDING_QUOTE_LINES_TABLE) == snaps_land_l_before
    assert _read_table_sorted(warehouse).equals(t2)

    # Step 4: Re-running pipeline again is a complete no-op
    m4 = run(incoming, warehouse, campaigns=CAMPAIGNS_DATA)
    assert m4.batches_processed == 0
    assert m4.batches_skipped_already_done == 3
    assert _snapshot_count(warehouse, TABLE_IDENTIFIER) == snaps_target_before

    # Verify landing tables preserve verbatim counts (120+46=166 quotes, 299+89=388 lines)
    assert _read_table_sorted(warehouse, LANDING_QUOTES_TABLE).num_rows == 166
    assert _read_table_sorted(warehouse, LANDING_QUOTE_LINES_TABLE).num_rows == 388

    # Verify ledger statuses
    ledger_rows = _read_table_sorted(warehouse, BATCH_LEDGER_TABLE).to_pylist()
    statuses_by_batch: dict[str, list[str]] = {}
    for row in ledger_rows:
        statuses_by_batch.setdefault(row["batch_id"], []).append(row["status"])
    assert BATCH_STATUS_COMPLETED in statuses_by_batch["batch_1"]
    assert BATCH_STATUS_COMPLETED in statuses_by_batch["batch_2"]
    assert statuses_by_batch["batch_2_redelivered"] == [BATCH_STATUS_SKIPPED_DUPLICATE]


def test_incremental_equals_single_shot_and_full_refresh(tmp_path: Path) -> None:
    # Warehouse A: batch_1 -> batch_2 -> batch_2_redelivered incrementally
    inc_a = tmp_path / "inc_a"
    wh_a = tmp_path / "wh_a"
    inc_a.mkdir()
    shutil.copytree(INCOMING_DATA / "batch_1", inc_a / "batch_1")
    run(inc_a, wh_a)
    shutil.copytree(INCOMING_DATA / "batch_2", inc_a / "batch_2")
    run(inc_a, wh_a)
    shutil.copytree(INCOMING_DATA / "batch_2_redelivered", inc_a / "batch_2_redelivered")
    run(inc_a, wh_a)

    # Warehouse B: single-shot over full data/incoming
    wh_b = tmp_path / "wh_b"
    run(INCOMING_DATA, wh_b)

    # Warehouse C: full refresh rebuild
    wh_c = tmp_path / "wh_c"
    run(INCOMING_DATA, wh_c, full_refresh=True)

    tbl_a = _read_table_sorted(wh_a)
    tbl_b = _read_table_sorted(wh_b)
    tbl_c = _read_table_sorted(wh_c)

    assert tbl_a.equals(tbl_b)
    assert tbl_a.equals(tbl_c)
    assert tbl_a.num_rows == 365


# ---------------------------------------------------------------------------
# 2. Out-of-order stale updates, delete/resurrection semantics, and ties
# ---------------------------------------------------------------------------


def test_out_of_order_stale_updates_and_delete_resurrection(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    warehouse = tmp_path / "warehouse"

    # Batch 1: Q1 (sent at T=10), L1 (live at T=10), L2 (deleted at T=20)
    _write_batch(
        incoming,
        "batch_01",
        quotes=[
            {
                "quote_id": "Q1",
                "quote_number": "BG-2026-00001",
                "customer_id": "C001",
                "status": "sent",
                "quote_date": date(2026, 9, 1),
                "utm_source": "google",
                "utm_campaign": "brand_search",
                "updated_at": datetime(2026, 9, 10, 12, 0, 0),
            }
        ],
        lines=[
            {
                "quote_line_id": "L1",
                "quote_id": "Q1",
                "product_sku": "SKU-A",
                "quantity": 10,
                "unit_price_vnd": 1000,
                "line_total_vnd": 10_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 10, 12, 0, 0),
            },
            {
                "quote_line_id": "L2",
                "quote_id": "Q1",
                "product_sku": "SKU-B",
                "quantity": 5,
                "unit_price_vnd": 2000,
                "line_total_vnd": 10_000,
                "is_deleted": True,
                "updated_at": datetime(2026, 9, 20, 12, 0, 0),
            },
        ],
    )
    run(incoming, warehouse)
    r1 = {r["quote_line_id"]: r for r in _read_table_sorted(warehouse).to_pylist()}
    assert set(r1.keys()) == {"L1"}
    assert r1["L1"]["quote_status"] == "sent"
    assert r1["L1"]["quantity"] == 10

    # Batch 2: STALE Q1 (draft at T=5), STALE L1 (qty=2 at T=5), STALE live L2 (T=15 < delete T=20)
    _write_batch(
        incoming,
        "batch_02",
        quotes=[
            {
                "quote_id": "Q1",
                "quote_number": "BG-2026-00001",
                "customer_id": "C001",
                "status": "draft",
                "quote_date": date(2026, 9, 1),
                "utm_source": "google",
                "utm_campaign": "brand_search",
                "updated_at": datetime(2026, 9, 5, 12, 0, 0),
            }
        ],
        lines=[
            {
                "quote_line_id": "L1",
                "quote_id": "Q1",
                "product_sku": "SKU-A",
                "quantity": 2,
                "unit_price_vnd": 1000,
                "line_total_vnd": 2_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 5, 12, 0, 0),
            },
            {
                "quote_line_id": "L2",
                "quote_id": "Q1",
                "product_sku": "SKU-B",
                "quantity": 5,
                "unit_price_vnd": 2000,
                "line_total_vnd": 10_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 15, 12, 0, 0),
            },
        ],
    )
    run(incoming, warehouse)
    r2 = {r["quote_line_id"]: r for r in _read_table_sorted(warehouse).to_pylist()}
    # Stale records must NOT overwrite Q1/L1 and must NOT resurrect L2
    assert set(r2.keys()) == {"L1"}
    assert r2["L1"]["quote_status"] == "sent"
    assert r2["L1"]["quantity"] == 10

    # Batch 3: NEWER L2 (is_deleted=False at T=25 > delete T=20) resurrects L2;
    # exact timestamp tie on L1 (T=10) deterministically prefers later batch_03
    _write_batch(
        incoming,
        "batch_03",
        quotes=[],
        lines=[
            {
                "quote_line_id": "L2",
                "quote_id": "Q1",
                "product_sku": "SKU-B",
                "quantity": 7,
                "unit_price_vnd": 2000,
                "line_total_vnd": 14_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 25, 12, 0, 0),
            },
            {
                "quote_line_id": "L1",
                "quote_id": "Q1",
                "product_sku": "SKU-A",
                "quantity": 11,
                "unit_price_vnd": 1000,
                "line_total_vnd": 11_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 10, 12, 0, 0),
            },
        ],
    )
    run(incoming, warehouse)
    r3 = {r["quote_line_id"]: r for r in _read_table_sorted(warehouse).to_pylist()}
    assert set(r3.keys()) == {"L1", "L2"}
    assert r3["L2"]["quantity"] == 7
    assert r3["L1"]["quantity"] == 11


# ---------------------------------------------------------------------------
# 3. Crash / failure recovery without duplicating landing rows
# ---------------------------------------------------------------------------


def test_crash_recovery_from_landed_state_without_duplicate_landing(tmp_path: Path) -> None:
    incoming = tmp_path / "incoming"
    warehouse = tmp_path / "warehouse"
    incoming.mkdir()
    shutil.copytree(INCOMING_DATA / "batch_1", incoming / "batch_1")

    # Simulate a crash right after landing batch_1 (before projection/merge commits)
    cat = open_catalog(warehouse)
    try:
        mgr = LandingManager(cat)
        lq, ll, ledger, _ = mgr.ensure_core_tables()
        bdir = incoming / "batch_1"
        raw_q = pq.read_table(bdir / "quotes.parquet")
        raw_l = pq.read_table(bdir / "quote_lines.parquet")
        now = datetime(2026, 9, 15, 0, 0, 0)
        lq.append(
            raw_q.append_column("_batch_id", pa.array(["batch_1"] * raw_q.num_rows))
            .append_column("_row_num", pa.array(range(1, raw_q.num_rows + 1), type=pa.int64()))
            .append_column("_ingested_at", pa.array([now] * raw_q.num_rows, type=pa.timestamp("us")))
        )
        ll.append(
            raw_l.append_column("_batch_id", pa.array(["batch_1"] * raw_l.num_rows))
            .append_column("_row_num", pa.array(range(1, raw_l.num_rows + 1), type=pa.int64()))
            .append_column("_ingested_at", pa.array([now] * raw_l.num_rows, type=pa.timestamp("us")))
        )
        mgr.append_ledger_entry(
            ledger,
            batch_id="batch_1",
            content_sha256=compute_batch_fingerprint(bdir / "quotes.parquet", bdir / "quote_lines.parquet"),
            status=BATCH_STATUS_LANDED,
            quotes_rows=raw_q.num_rows,
            quote_lines_rows=raw_l.num_rows,
            note="Simulated crash after landing",
        )
    finally:
        cat.engine.dispose()

    # Re-run pipeline: should recover batch_1, populate crm.quote_line, and NOT double-land rows
    metrics = run(incoming, warehouse)
    assert metrics.batches_recovered == 1
    assert metrics.quotes_landed == 0
    assert metrics.quote_lines_landed == 0
    assert metrics.live_rows_total == 298
    assert _read_table_sorted(warehouse, LANDING_QUOTES_TABLE).num_rows == 120
    assert _read_table_sorted(warehouse, LANDING_QUOTE_LINES_TABLE).num_rows == 299


# ---------------------------------------------------------------------------
# 4. Schema drift detection (breaking vs additive) & DQ reporting
# ---------------------------------------------------------------------------


def test_schema_drift_breaking_and_additive(tmp_path: Path) -> None:
    incoming_bad = tmp_path / "incoming_bad"
    warehouse_bad = tmp_path / "warehouse_bad"
    bad_dir = incoming_bad / "batch_01"
    bad_dir.mkdir(parents=True)

    # Breaking drift: missing 'status' column
    bad_q_schema = pa.schema([f for f in QUOTES_SOURCE_SCHEMA if f.name != "status"])
    pq.write_table(pa.Table.from_pylist([], schema=bad_q_schema), bad_dir / "quotes.parquet")
    pq.write_table(
        pa.Table.from_pylist([], schema=QUOTE_LINES_SOURCE_SCHEMA),
        bad_dir / "quote_lines.parquet",
    )
    with pytest.raises(SchemaDriftError, match="missing required columns"):
        run(incoming_bad, warehouse_bad)

    # Additive drift: extra column 'sales_rep' absorbed in landing, ignored in target contract
    incoming_add = tmp_path / "incoming_add"
    warehouse_add = tmp_path / "warehouse_add"
    add_q_schema = pa.schema(list(QUOTES_SOURCE_SCHEMA) + [pa.field("sales_rep", pa.string())])
    _write_batch(
        incoming_add,
        "batch_01",
        quotes=[
            {
                "quote_id": "Q1",
                "quote_number": "BG-2026-00001",
                "customer_id": "C001",
                "status": "draft",
                "quote_date": date(2026, 9, 1),
                "utm_source": None,
                "utm_campaign": None,
                "updated_at": datetime(2026, 9, 1, 10, 0, 0),
                "sales_rep": "alice",
            }
        ],
        lines=[
            {
                "quote_line_id": "L1",
                "quote_id": "Q1",
                "product_sku": "SKU-A",
                "quantity": 2,
                "unit_price_vnd": 500_000,
                "line_total_vnd": 1_000_000,
                "is_deleted": False,
                "updated_at": datetime(2026, 9, 1, 10, 0, 0),
            }
        ],
        quotes_schema=add_q_schema,
    )
    metrics = run(incoming_add, warehouse_add)
    assert metrics.live_rows_total == 1
    assert metrics.dq_warnings >= 1
    assert _read_table_sorted(warehouse_add).schema == QUOTE_LINE_SCHEMA
    assert "sales_rep" in _read_table_sorted(warehouse_add, LANDING_QUOTES_TABLE).schema.names

    # Same additive drift fails when strict_schema=True
    with pytest.raises(SchemaDriftError, match="additive schema drift"):
        run(incoming_add, tmp_path / "warehouse_strict", strict_schema=True)


def test_dq_reports_capture_all_real_anomalies(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    run(INCOMING_DATA, warehouse)

    dq_rows = _read_table_sorted(warehouse, DQ_REPORTS_TABLE).to_pylist()
    warn_names = {r["check_name"] for r in dq_rows if r["severity"] == "WARNING"}
    assert "quote_lines_within_batch_duplicates" in warn_names  # L000025
    assert "quote_lines_missing_parent_quote" in warn_names     # L000297, L000298
    assert "quote_lines_late_arriving_below_watermark" in warn_names  # L000366
    assert "batch_content_deduplication" in warn_names          # batch_2_redelivered
    assert "attribution_pre_launch_quotes" in warn_names        # 6 CMP-04 quotes


# ---------------------------------------------------------------------------
# 5. Stretch goal: Campaign attribution & CLI subcommands
# ---------------------------------------------------------------------------


def test_stretch_goal_campaign_attribution(tmp_path: Path) -> None:
    warehouse = tmp_path / "warehouse"
    metrics = run(INCOMING_DATA, warehouse)
    assert metrics.attributed_rows == 365

    campaigns_tbl = _read_table_sorted(warehouse, REFERENCE_CAMPAIGNS_TABLE)
    assert campaigns_tbl.num_rows == 4

    attr_tbl = _read_table_sorted(warehouse, ATTRIBUTED_QUOTE_LINE_TABLE)
    assert attr_tbl.num_rows == 365
    rows = attr_tbl.to_pylist()

    # Check the 6 pre-launch CMP-04 quotes (quote_date < 2026-09-02)
    pre_launch_quotes = {
        r["quote_id"] for r in rows if r["attribution_status"] == "attributed_pre_launch"
    }
    assert pre_launch_quotes == {"Q0026", "Q0032", "Q0033", "Q0039", "Q0067", "Q0073"}

    # Check organic/unattributed quotes
    unattributed_quotes = {
        r["quote_id"] for r in rows if r["attribution_status"] == "unattributed"
    }
    assert len(unattributed_quotes) == 55

    # Check campaign IDs assigned properly
    campaigns_seen = {r["campaign_id"] for r in rows if r["campaign_id"] is not None}
    assert campaigns_seen == {"CMP-01", "CMP-02", "CMP-03", "CMP-04"}


def test_empty_incoming_initializes_valid_target(tmp_path: Path) -> None:
    empty_in = tmp_path / "empty_in"
    empty_in.mkdir()
    warehouse = tmp_path / "warehouse"
    metrics = run(empty_in, warehouse)
    assert metrics.live_rows_total == 0
    tbl = _read_table_sorted(warehouse)
    assert tbl.num_rows == 0
    assert tbl.schema.names == QUOTE_LINE_SCHEMA.names
    assert tbl.cast(QUOTE_LINE_SCHEMA).schema == QUOTE_LINE_SCHEMA


def test_cli_inspect_and_benchmark(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    warehouse = tmp_path / "warehouse"
    assert main(["run", "--incoming", str(INCOMING_DATA), "--warehouse", str(warehouse)]) == 0
    assert main(["inspect", "--warehouse", str(warehouse), "--json"]) == 0
    inspect_out = json.loads(capsys.readouterr().out)
    assert inspect_out["tables"][TABLE_IDENTIFIER]["rows"] == 365
    assert inspect_out["tables"][ATTRIBUTED_QUOTE_LINE_TABLE]["rows"] == 365

    assert main(["benchmark", "--quotes", "40", "--lines", "120", "--delta-lines", "20", "--json"]) == 0
    bench_out = json.loads(capsys.readouterr().out)
    assert bench_out["quotes_count"] == 40
    assert len(bench_out["stages"]) == 4
