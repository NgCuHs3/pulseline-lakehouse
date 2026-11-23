"""Synthetic scale-up benchmark harness for latency, throughput, and memory profiling."""

from __future__ import annotations

import random
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.run import run
from pipeline.schema import QUOTE_LINES_SOURCE_SCHEMA, QUOTES_SOURCE_SCHEMA

_SKUS = [
    ("SKU-CRM-SEAT", 450_000),
    ("SKU-DMS-SEAT", 350_000),
    ("SKU-ERP-BASIC", 12_000_000),
    ("SKU-ERP-PRO", 25_000_000),
    ("SKU-API-CALLS", 900_000),
    ("SKU-STORAGE-TB", 2_200_000),
    ("SKU-SUPPORT-GOLD", 18_000_000),
]
_UTMS = [
    ("tiktok", "autumn_sale_2026"),
    ("facebook", "sme_retarget"),
    ("google", "brand_search"),
    ("zalo", "partner_webinar"),
    (None, None),
]
_STATUSES = ["draft", "sent", "accepted", "rejected"]


@dataclass(frozen=True)
class BenchmarkStage:
    name: str
    input_rows: int
    live_rows: int
    rows_mutated: int
    elapsed_ms: float
    projection_ms: float
    merge_ms: float
    rows_per_sec: float


@dataclass(frozen=True)
class BenchmarkReport:
    quotes_count: int
    lines_count: int
    delta_lines_per_batch: int
    stages: list[BenchmarkStage]

    def to_dict(self) -> dict[str, object]:
        return {
            "quotes_count": self.quotes_count,
            "lines_count": self.lines_count,
            "delta_lines_per_batch": self.delta_lines_per_batch,
            "stages": [asdict(s) for s in self.stages],
        }


def _generate_base_batch(
    batch_dir: Path,
    *,
    num_quotes: int,
    num_lines: int,
    rng: random.Random,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    batch_dir.mkdir(parents=True, exist_ok=True)
    base_dt = datetime(2026, 9, 1, 0, 0, 0)
    quotes: list[dict[str, object]] = []
    for i in range(1, num_quotes + 1):
        utm_src, utm_cmp = rng.choice(_UTMS)
        q_date = date(2026, 8, 29) + timedelta(days=rng.randint(0, 10))
        quotes.append(
            {
                "quote_id": f"Q{i:06d}",
                "quote_number": f"BG-2026-{i:06d}",
                "customer_id": f"C{rng.randint(1, 500):04d}",
                "status": rng.choice(["draft", "sent"]),
                "quote_date": q_date,
                "utm_source": utm_src,
                "utm_campaign": utm_cmp,
                "updated_at": base_dt + timedelta(seconds=rng.randint(0, 8 * 86400)),
            }
        )

    lines: list[dict[str, object]] = []
    for j in range(1, num_lines + 1):
        sku, price = rng.choice(_SKUS)
        qty = rng.randint(1, 20)
        qid = f"Q{rng.randint(1, num_quotes):06d}"
        lines.append(
            {
                "quote_line_id": f"L{j:07d}",
                "quote_id": qid,
                "product_sku": sku,
                "quantity": qty,
                "unit_price_vnd": price,
                "line_total_vnd": qty * price,
                "is_deleted": False,
                "updated_at": base_dt + timedelta(seconds=rng.randint(0, 8 * 86400)),
            }
        )
    # Plant an out-of-order within-batch duplicate
    if lines:
        dup = dict(lines[0])
        dup["quantity"] = 1
        dup["line_total_vnd"] = dup["unit_price_vnd"]
        dup["updated_at"] = dup["updated_at"] - timedelta(hours=2)
        lines.append(dup)

    pq.write_table(pa.Table.from_pylist(quotes, schema=QUOTES_SOURCE_SCHEMA), batch_dir / "quotes.parquet")
    pq.write_table(pa.Table.from_pylist(lines, schema=QUOTE_LINES_SOURCE_SCHEMA), batch_dir / "quote_lines.parquet")
    return quotes, lines


def _generate_delta_batch(
    batch_dir: Path,
    *,
    base_quotes: list[dict[str, object]],
    base_lines: list[dict[str, object]],
    next_quote_idx: int,
    next_line_idx: int,
    delta_quotes: int,
    delta_lines: int,
    batch_num: int,
    rng: random.Random,
) -> tuple[int, int]:
    batch_dir.mkdir(parents=True, exist_ok=True)
    delta_dt = datetime(2026, 9, 10 + batch_num, 0, 0, 0)

    quotes_out: list[dict[str, object]] = []
    # 50% header updates on existing quotes, 50% new quotes
    upd_q_cnt = min(delta_quotes // 2, len(base_quotes))
    for q in rng.sample(base_quotes, upd_q_cnt):
        updated = dict(q)
        updated["status"] = rng.choice(["accepted", "rejected"])
        updated["updated_at"] = delta_dt + timedelta(seconds=rng.randint(0, 86400))
        quotes_out.append(updated)

    new_q_ids: list[str] = []
    for k in range(delta_quotes - upd_q_cnt):
        idx = next_quote_idx + k
        qid = f"Q{idx:06d}"
        new_q_ids.append(qid)
        utm_src, utm_cmp = rng.choice(_UTMS)
        quotes_out.append(
            {
                "quote_id": qid,
                "quote_number": f"BG-2026-{idx:06d}",
                "customer_id": f"C{rng.randint(1, 500):04d}",
                "status": "draft",
                "quote_date": date(2026, 9, 10 + batch_num),
                "utm_source": utm_src,
                "utm_campaign": utm_cmp,
                "updated_at": delta_dt + timedelta(seconds=rng.randint(0, 86400)),
            }
        )

    lines_out: list[dict[str, object]] = []
    upd_l_cnt = min(delta_lines // 3, len(base_lines))
    del_l_cnt = max(1, delta_lines // 20)
    sampled_lines = rng.sample(base_lines, min(upd_l_cnt + del_l_cnt, len(base_lines)))
    for idx_l, row in enumerate(sampled_lines):
        mut = dict(row)
        mut["updated_at"] = delta_dt + timedelta(seconds=rng.randint(0, 86400))
        if idx_l < del_l_cnt:
            mut["is_deleted"] = True
        else:
            qty = rng.randint(1, 20)
            mut["quantity"] = qty
            mut["line_total_vnd"] = qty * int(mut["unit_price_vnd"])
        lines_out.append(mut)

    remaining = max(0, delta_lines - len(lines_out))
    for k in range(remaining):
        idx = next_line_idx + k
        sku, price = rng.choice(_SKUS)
        qty = rng.randint(1, 20)
        qid = rng.choice(new_q_ids) if new_q_ids else str(base_quotes[0]["quote_id"])
        # Make 1 late-arriving row below watermark
        ts = (
            datetime(2026, 9, 4, 12, 0, 0)
            if k == 0
            else delta_dt + timedelta(seconds=rng.randint(0, 86400))
        )
        lines_out.append(
            {
                "quote_line_id": f"L{idx:07d}",
                "quote_id": qid,
                "product_sku": sku,
                "quantity": qty,
                "unit_price_vnd": price,
                "line_total_vnd": qty * price,
                "is_deleted": False,
                "updated_at": ts,
            }
        )

    pq.write_table(pa.Table.from_pylist(quotes_out, schema=QUOTES_SOURCE_SCHEMA), batch_dir / "quotes.parquet")
    pq.write_table(pa.Table.from_pylist(lines_out, schema=QUOTE_LINES_SOURCE_SCHEMA), batch_dir / "quote_lines.parquet")
    return len(quotes_out), len(lines_out)


def run_benchmark(
    *,
    num_quotes: int = 5_000,
    num_lines: int = 20_000,
    delta_lines: int = 1_000,
    seed: int = 42,
) -> BenchmarkReport:
    """Measure initial load, incremental delta, redelivery no-op, and full-refresh latency."""
    rng = random.Random(seed)
    stages: list[BenchmarkStage] = []

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        incoming = root / "incoming"
        warehouse = root / "warehouse"
        warehouse_full = root / "warehouse_full"

        # Stage 1: Initial bulk load
        b1_dir = incoming / "batch_01"
        base_quotes, base_lines = _generate_base_batch(
            b1_dir, num_quotes=num_quotes, num_lines=num_lines, rng=rng
        )
        t0 = time.perf_counter()
        m1 = run(incoming, warehouse)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        in_rows = m1.quotes_landed + m1.quote_lines_landed
        stages.append(
            BenchmarkStage(
                name="initial_load_incremental",
                input_rows=in_rows,
                live_rows=m1.live_rows_total,
                rows_mutated=m1.rows_inserted + m1.rows_updated + m1.rows_deleted,
                elapsed_ms=round(elapsed_ms, 2),
                projection_ms=round(m1.stage_timings_ms.get("projection", 0.0), 2),
                merge_ms=round(m1.stage_timings_ms.get("merge", 0.0), 2),
                rows_per_sec=round(in_rows / (elapsed_ms / 1000.0), 1) if elapsed_ms > 0 else 0.0,
            )
        )

        # Stage 2: Incremental delta batch (header updates + line updates + deletes + late arrival)
        b2_dir = incoming / "batch_02"
        dq, dl = _generate_delta_batch(
            b2_dir,
            base_quotes=base_quotes,
            base_lines=base_lines,
            next_quote_idx=num_quotes + 1,
            next_line_idx=num_lines + 1,
            delta_quotes=max(20, num_quotes // 20),
            delta_lines=delta_lines,
            batch_num=1,
            rng=rng,
        )
        t0 = time.perf_counter()
        m2 = run(incoming, warehouse)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        stages.append(
            BenchmarkStage(
                name="delta_batch_incremental_merge",
                input_rows=dq + dl,
                live_rows=m2.live_rows_total,
                rows_mutated=m2.rows_inserted + m2.rows_updated + m2.rows_deleted,
                elapsed_ms=round(elapsed_ms, 2),
                projection_ms=round(m2.stage_timings_ms.get("projection", 0.0), 2),
                merge_ms=round(m2.stage_timings_ms.get("merge", 0.0), 2),
                rows_per_sec=round((dq + dl) / (elapsed_ms / 1000.0), 1) if elapsed_ms > 0 else 0.0,
            )
        )

        # Stage 3: Redelivered batch under new directory name (content-hash skip)
        shutil.copytree(b2_dir, incoming / "batch_02_redelivered")
        t0 = time.perf_counter()
        m3 = run(incoming, warehouse)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        stages.append(
            BenchmarkStage(
                name="redelivered_batch_noop",
                input_rows=dq + dl,
                live_rows=m3.live_rows_total,
                rows_mutated=m3.rows_inserted + m3.rows_updated + m3.rows_deleted,
                elapsed_ms=round(elapsed_ms, 2),
                projection_ms=round(m3.stage_timings_ms.get("projection", 0.0), 2),
                merge_ms=round(m3.stage_timings_ms.get("merge", 0.0), 2),
                rows_per_sec=round((dq + dl) / (elapsed_ms / 1000.0), 1) if elapsed_ms > 0 else 0.0,
            )
        )

        # Stage 4: Full-refresh baseline on same two batches for comparison
        run(incoming, warehouse_full)
        t0 = time.perf_counter()
        m4 = run(incoming, warehouse_full, full_refresh=True)
        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        total_rows = num_quotes + num_lines + dq + dl
        stages.append(
            BenchmarkStage(
                name="full_refresh_rebuild",
                input_rows=total_rows,
                live_rows=m4.live_rows_total,
                rows_mutated=m4.live_rows_total,
                elapsed_ms=round(elapsed_ms, 2),
                projection_ms=round(m4.stage_timings_ms.get("projection", 0.0), 2),
                merge_ms=round(m4.stage_timings_ms.get("merge", 0.0), 2),
                rows_per_sec=round(total_rows / (elapsed_ms / 1000.0), 1) if elapsed_ms > 0 else 0.0,
            )
        )

    return BenchmarkReport(
        quotes_count=num_quotes,
        lines_count=num_lines,
        delta_lines_per_batch=delta_lines,
        stages=stages,
    )
