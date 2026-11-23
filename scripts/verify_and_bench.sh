#!/usr/bin/env bash
set -euo pipefail

echo "[PulseLine] Syncing pinned environment..."
uv sync --frozen

echo "[PulseLine] Running full pytest suite..."
uv run pytest -v

echo "[PulseLine] Running 100k-line scale-up benchmark..."
uv run pipeline benchmark --quotes 20000 --lines 100000 --delta-lines 2000
