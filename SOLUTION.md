# PulseLine Lakehouse — Solution Notes

## 1. Data Problems Found & How Each Was Handled

Profiling `data/incoming/` (`batch_1`, `batch_2`, `batch_2_redelivered`) and `data/reference/campaigns.parquet` uncovered seven distinct real-world ingestion hazards:

| # | Anomaly Found in Batches | Exact Evidence | How It Is Handled & Why |
|---|---|---|---|
| 1 | **Within-batch out-of-order duplicate** | `L000025` appears twice in `batch_1/quote_lines.parquet`: row 25 (`qty=8`, `200M VND`, `2026-09-04 09:09:02`) and row 299 (`qty=5`, `125M VND`, `2026-09-04 06:09:02` — 3h older at EOF). | Landing stores both rows verbatim. DuckDB projection deduplicates via `ROW_NUMBER() OVER (PARTITION BY quote_line_id ORDER BY updated_at DESC, _batch_id DESC, _row_num DESC) = 1`, so `qty=8` wins instead of file-order last-row-wins. Flagged in `meta.data_quality_reports`. |
| 2 | **Child lines arrive in an earlier batch than parent quote** | `L000297` & `L000298` arrive in `batch_1` referencing `Q0121`, whose header only arrives in `batch_2` (`BG-2026-00121`, `C012`). | Projection uses `LEFT JOIN` over full landing tables, preserving both lines immediately after `batch_1` (nullable header fields, flagged as `quote_lines_missing_parent_quote`). When `batch_2` lands `Q0121`, affected-key scoping re-projects `Q0121`'s lines and backfills all header columns. |
| 3 | **Late-arriving row below high-watermark** | `L000366` (`Q0005`, `180M VND`) arrives in `batch_2` with `updated_at = 2026-09-05 14:30:00` — earlier than `batch_1`'s max timestamp (`2026-09-09 21:44:34`). | Ingestion tracks processed batches by batch ledger + content hash rather than an event-time watermark filter (`updated_at > max_ts`). `L000366` lands verbatim, joins `Q0005` from `batch_1`, and is surfaced as a DQ warning (`quote_lines_late_arriving_below_watermark`). |
| 4 | **Header-only & line-only cross-batch updates** | 15 quotes update in `batch_2` (10 have zero lines in `batch_2`, e.g. `Q0007` `draft -> rejected`; `Q0053` updates only `updated_at`). 16 updated lines in `batch_2` belong to untouched `batch_1` quotes (including `L000151`, timestamp-only). | The single DuckDB SQL query computes the closed affected key set: lines in newly landed batches **UNION** historical lines whose `quote_id` appeared in newly landed quotes. Every untouched sibling line receives the latest quote header & timestamps. |
| 5 | **Soft deletes** | `L000047` (`Q0020`) is live in `batch_1` (`is_deleted=False`) and soft-deleted in `batch_2` (`is_deleted=True`, `2026-09-12 18:10:24`). | Deduplication runs *before* filtering `is_deleted`. Live rows upsert and deleted keys delete atomically inside one PyIceberg `table.transaction()`. Because latest version wins first, a later stale `is_deleted=False` cannot resurrect a deleted line, while a newer `is_deleted=False` can. |
| 6 | **Redelivered batch under a new directory name** | `batch_2_redelivered` is byte-for-byte identical to `batch_2` (same SHA-256 for both Parquet files) under a different folder name. | `meta.batch_ledger` tracks both `batch_id` and streaming SHA-256 content digest. `batch_2_redelivered` is recorded as `SKIPPED_DUPLICATE`, creating zero duplicate landing rows and zero new snapshots on `crm.quote_line`. |
| 7 | **Pre-launch campaign attribution (Stretch goal)** | 6 quotes (`Q0026`, `Q0032`, `Q0033`, `Q0039`, `Q0067`, `Q0073`) carry `zalo / partner_webinar` (`CMP-04`) with `quote_date` (`2026-08-30..09-01`) earlier than `started_on` (`2026-09-02`); 55 quotes have NULL UTMs. | `crm.quote_line_attributed` attributes via normalized `(utm_source, utm_campaign)` while preserving `attribution_status` (`attributed`, `attributed_pre_launch`, `unattributed`, `partial_utm`, `unknown_utm`) and logging a DQ warning so analysts can include or filter pre-launch traffic explicitly. |

*(Observation also noted: 111 of 365 live lines have `line_updated_at::date < quote_date`; treated as valid CRM quote re-dating/drafting behavior rather than rejecting lines.)*

---

## 2. Key Architectural Decisions & Trade-Offs

- **Three-stage Iceberg lakehouse + Write-Ahead Ledger (`meta.batch_ledger`):**
  1. **Landing (`landing.quotes`, `landing.quote_lines`):** Append-only, immutable bronze tables storing source columns verbatim plus `_batch_id`, `_row_num`, and `_ingested_at`. Additive upstream columns evolve safely via Iceberg `union_by_name` (unless `--strict-schema` is set); breaking drift (missing columns or type changes) fails fast with `SchemaDriftError`.
  2. **Projection (`src/pipeline/projection.py`):** One vectorized DuckDB SQL query (`QUOTE_LINE_PROJECTION_SQL`) over Arrow-scanned landing tables resolves deduplication, cross-batch joins, header propagation, and soft-delete classification in a single pass.
  3. **Object Merge (`src/pipeline/merge.py`):** Atomic PyIceberg `table.transaction()` combining `tx.upsert(..., join_cols=["quote_line_id"])` and `tx.delete(In("quote_line_id", ...))`. Writing `LANDED` to `meta.batch_ledger` before projection allows crash recovery without double-appending bronze data.
- **`LEFT JOIN` vs `INNER JOIN` for early-arriving child lines (`Q0121`):** Chose `LEFT JOIN` so `crm.quote_line` always holds *one row per live quote line* (298 rows after `batch_1`, 365 after `batch_2`) with zero silently dropped revenue (`20.7M VND` for `L000297`/`L000298`). Downstream consumers needing complete headers filter `quote_number IS NOT NULL`, while `meta.data_quality_reports` alerts on unresolved orphans.
- **Keep `crm.quote_line` contract pristine for Stretch Goal:** Rather than mutating `QUOTE_LINE_SCHEMA`, campaign attribution materializes into `reference.campaigns` and `crm.quote_line_attributed`, preserving 100% compatibility with grading tests.
- **Windows cross-platform fix in `open_catalog`:** PyIceberg 0.12's `PyArrowFileIO.parse_location` mishandles `file:///C:/...` on Windows (`WinError 123`). `open_catalog` keeps `warehouse.as_uri()` unchanged on Linux/macOS and passes the resolved path on Windows (`os.name == "nt"`), plus disposes the SQLAlchemy engine cleanly after runs.
- **Measured Performance & Copy-on-Write Trade-Off (`uv run pipeline benchmark --quotes 20000 --lines 100000 --delta-lines 2000`):**

| Benchmark Stage (20k quotes / 100k lines) | Input Rows | Mutated | Total (ms) | DuckDB Proj (ms) | Iceberg Merge (ms) | Throughput |
|---|---:|---:|---:|---:|---:|---:|
| `initial_load_incremental` | 120,001 | 100,000 | 4,523.2 | 527.7 | 2,402.9 | 26,530 rows/s |
| `delta_batch_incremental_merge` | 3,000 | 4,534 | 2,743.9 | 232.2 | 1,730.3 | 1,093 rows/s |
| `redelivered_batch_noop` | 3,000 | 0 | 439.5 | 0.0 | 0.0 | 6,825 rows/s |
| `full_refresh_rebuild` | 123,000 | 101,134 | 926.8 | 253.0 | 186.6 | 132,720 rows/s |

  **Engineering takeaway:** DuckDB projection is sub-quarter-second at 123k rows (`232 ms`). On an *unpartitioned* local Copy-on-Write Iceberg table, PyIceberg `upsert` + `delete` rewrites the single data file twice (`1,730 ms`), making single-pass `overwrite` faster below ~1M rows. At production scale (billions of monthly rows), bucket/date partitioning + Merge-on-Read delete files (or ClickHouse `ReplacingMergeTree`) eliminates that write amplification.

---

## 3. What I Would Do Next With More Time

1. **Table Partitioning & Compaction:** Partition `landing.*` by `_ingested_at` day and `crm.quote_line` by `bucket(32, quote_line_id)` (or `quote_date` month) with periodic Iceberg `rewrite_data_files` + `expire_snapshots` maintenance jobs to prevent small-file explosion.
2. **Event-Driven Ingestion & Multi-Engine Serving (Production Scale-Out):** Replace folder polling with **RabbitMQ** quorum queues (publisher confirms, prefetch backpressure, dead-letter exchanges), stream enriched records into **ClickHouse** (`ReplacingMergeTree(updated_at, is_deleted)`) for sub-second analytics, store operational metadata/ledgers in **MySQL** & raw payloads in **MongoDB**, and index searchable product/quote text in **Apache Solr** (detailed in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)).
3. **Quarantine & Alerting:** Route critical DQ violations (null PKs, breaking schema drift) to a `quarantine.*` Iceberg table + Prometheus/Slack alerting rather than halting the entire batch when partial isolation is preferred.

---

## 4. Time Spent & AI Tools Used

- **Total time:** ~2.5 hours (data profiling & edge-case discovery ~25m, core lakehouse & DQ architecture ~60m, testing & Windows/PyIceberg debugging ~35m, benchmarking & documentation ~30m).
- **AI tools used:** Used an AI coding assistant (Antigravity / Gemini) to accelerate exploratory Parquet profiling scripts, boilerplate PyArrow/PyIceberg scaffolding, synthetic benchmark generation, and documentation structuring. All architectural judgment calls — content-addressed redelivery dedup, `LEFT JOIN` vs `INNER JOIN` orphan semantics, closed affected-key SQL scoping, atomic `upsert`+`delete` transactions, pre-launch campaign tagging, and CoW write-amplification analysis — were verified directly against the data, unit tests (`11/11` passing), and live benchmarks.
