#!/usr/bin/env bash
set -euo pipefail

INCOMING_DIR="${1:-data/incoming}"
WAREHOUSE_DIR="${2:-warehouse}"

echo "[PulseLine] Syncing pinned Python 3.12 environment via uv..."
uv sync --frozen

echo "[PulseLine] Running incremental lakehouse pipeline..."
uv run pipeline run --incoming "${INCOMING_DIR}" --warehouse "${WAREHOUSE_DIR}" --json-logs

echo "[PulseLine] Inspecting warehouse tables, ledger, and data quality reports..."
uv run pipeline inspect --warehouse "${WAREHOUSE_DIR}"
