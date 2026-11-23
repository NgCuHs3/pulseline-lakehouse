"""CLI entrypoint for PulseLine Lakehouse."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.benchmark import run_benchmark
from pipeline.catalog import open_catalog
from pipeline.run import run
from pipeline.schema import (
    ATTRIBUTED_QUOTE_LINE_TABLE,
    BATCH_LEDGER_TABLE,
    DQ_REPORTS_TABLE,
    LANDING_QUOTE_LINES_TABLE,
    LANDING_QUOTES_TABLE,
    TABLE_IDENTIFIER,
)


def _cmd_inspect(warehouse: Path, as_json: bool = False) -> int:
    catalog = open_catalog(warehouse)
    try:
        summary: dict[str, object] = {"warehouse": str(warehouse.resolve()), "tables": {}}
        for identifier in (
            LANDING_QUOTES_TABLE,
            LANDING_QUOTE_LINES_TABLE,
            TABLE_IDENTIFIER,
            ATTRIBUTED_QUOTE_LINE_TABLE,
            BATCH_LEDGER_TABLE,
            DQ_REPORTS_TABLE,
        ):
            if catalog.table_exists(identifier):
                tbl = catalog.load_table(identifier)
                rows = tbl.scan().to_arrow().num_rows
                summary["tables"][identifier] = {
                    "rows": rows,
                    "snapshots": len(tbl.snapshots()),
                }

        if catalog.table_exists(BATCH_LEDGER_TABLE):
            ledger = catalog.load_table(BATCH_LEDGER_TABLE).scan().to_arrow().to_pylist()
            summary["batch_ledger"] = [
                {
                    "batch_id": r["batch_id"],
                    "status": r["status"],
                    "quotes_rows": r["quotes_rows"],
                    "quote_lines_rows": r["quote_lines_rows"],
                    "note": r["note"],
                }
                for r in ledger
            ]

        if catalog.table_exists(DQ_REPORTS_TABLE):
            dq = catalog.load_table(DQ_REPORTS_TABLE).scan().to_arrow().to_pylist()
            summary["dq_warnings"] = [
                {
                    "batch_id": r["batch_id"],
                    "check_name": r["check_name"],
                    "anomaly_count": r["anomaly_count"],
                    "details": r["details"],
                }
                for r in dq
                if r["severity"] in ("WARNING", "ERROR")
            ]

        if as_json:
            print(json.dumps(summary, indent=2, default=str))
        else:
            print(f"Warehouse: {summary['warehouse']}")
            print("\nIceberg Tables:")
            for name, meta in summary["tables"].items():
                print(f"  - {name:28s} rows={meta['rows']:<6d} snapshots={meta['snapshots']}")
            if "batch_ledger" in summary:
                print("\nBatch Ledger:")
                for item in summary["batch_ledger"]:
                    print(
                        f"  - {item['batch_id']:22s} {item['status']:18s} "
                        f"q={item['quotes_rows']:<4d} l={item['quote_lines_rows']:<4d} | {item['note']}"
                    )
            if "dq_warnings" in summary:
                print(f"\nData Quality Warnings/Errors ({len(summary['dq_warnings'])}):")
                for w in summary["dq_warnings"]:
                    print(f"  - [{w['batch_id']}] {w['check_name']} ({w['anomaly_count']}): {w['details']}")
        return 0
    finally:
        catalog.engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser(
        "run", help="process every batch in --incoming not yet processed"
    )
    run_parser.add_argument("--incoming", type=Path, required=True)
    run_parser.add_argument("--warehouse", type=Path, required=True)
    run_parser.add_argument(
        "--campaigns",
        type=Path,
        default=None,
        help="optional path to campaigns.parquet (auto-discovered by default)",
    )
    run_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="rebuild crm.quote_line from all landed rows",
    )
    run_parser.add_argument(
        "--strict-schema",
        action="store_true",
        help="fail on additive (extra) columns in incoming batches",
    )
    run_parser.add_argument(
        "--json-logs",
        action="store_true",
        help="emit structured JSON log records",
    )

    inspect_parser = commands.add_parser(
        "inspect", help="inspect Iceberg table counts, batch ledger, and DQ anomalies"
    )
    inspect_parser.add_argument("--warehouse", type=Path, required=True)
    inspect_parser.add_argument("--json", action="store_true", dest="as_json")

    bench_parser = commands.add_parser(
        "benchmark", help="run synthetic scale-up latency, throughput, and memory benchmark"
    )
    bench_parser.add_argument("--quotes", type=int, default=5_000)
    bench_parser.add_argument("--lines", type=int, default=20_000)
    bench_parser.add_argument("--delta-lines", type=int, default=1_000)
    bench_parser.add_argument("--json", action="store_true", dest="as_json")

    args = parser.parse_args(argv)
    if args.command == "run":
        run(
            args.incoming,
            args.warehouse,
            campaigns=args.campaigns,
            full_refresh=args.full_refresh,
            strict_schema=args.strict_schema,
            json_logs=args.json_logs,
        )
        return 0
    if args.command == "inspect":
        return _cmd_inspect(args.warehouse, as_json=args.as_json)
    if args.command == "benchmark":
        report = run_benchmark(
            num_quotes=args.quotes,
            num_lines=args.lines,
            delta_lines=args.delta_lines,
        )
        if args.as_json:
            print(json.dumps(report.to_dict(), indent=2))
        else:
            print(
                f"Benchmark ({report.quotes_count:,} quotes, {report.lines_count:,} base lines, "
                f"{report.delta_lines_per_batch:,} delta lines):"
            )
            print(
                f"{'Stage':32s} {'Input':>8s} {'Live':>8s} {'Mutated':>8s} "
                f"{'Total(ms)':>10s} {'Proj(ms)':>10s} {'Merge(ms)':>10s} {'Rows/sec':>10s}"
            )
            for s in report.stages:
                print(
                    f"{s.name:32s} {s.input_rows:8d} {s.live_rows:8d} {s.rows_mutated:8d} "
                    f"{s.elapsed_ms:10.2f} {s.projection_ms:10.2f} {s.merge_ms:10.2f} {s.rows_per_sec:10.1f}"
                )
        return 0
    return 1
