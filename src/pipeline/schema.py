"""Table identifiers, Iceberg/PyArrow schemas, and source contracts."""

import pyarrow as pa

# ---------------------------------------------------------------------------
# Fixed assignment contract (do not modify names or types)
# ---------------------------------------------------------------------------
TABLE_IDENTIFIER = "crm.quote_line"
ID_FIELD = "quote_line_id"

QUOTE_LINE_SCHEMA = pa.schema(
    [
        pa.field("quote_line_id", pa.string()),
        pa.field("quote_id", pa.string()),
        pa.field("quote_number", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("quote_status", pa.string()),
        pa.field("quote_date", pa.date32()),
        pa.field("utm_source", pa.string()),
        pa.field("utm_campaign", pa.string()),
        pa.field("product_sku", pa.string()),
        pa.field("quantity", pa.int32()),
        pa.field("unit_price_vnd", pa.int64()),
        pa.field("line_total_vnd", pa.int64()),
        pa.field("line_updated_at", pa.timestamp("us")),
        pa.field("quote_updated_at", pa.timestamp("us")),
    ]
)

# ---------------------------------------------------------------------------
# Internal lakehouse table identifiers
# ---------------------------------------------------------------------------
LANDING_QUOTES_TABLE = "landing.quotes"
LANDING_QUOTE_LINES_TABLE = "landing.quote_lines"
BATCH_LEDGER_TABLE = "meta.batch_ledger"
DQ_REPORTS_TABLE = "meta.data_quality_reports"
REFERENCE_CAMPAIGNS_TABLE = "reference.campaigns"
ATTRIBUTED_QUOTE_LINE_TABLE = "crm.quote_line_attributed"

# ---------------------------------------------------------------------------
# Source contracts (expected incoming Parquet schemas)
# ---------------------------------------------------------------------------
QUOTES_SOURCE_SCHEMA = pa.schema(
    [
        pa.field("quote_id", pa.string()),
        pa.field("quote_number", pa.string()),
        pa.field("customer_id", pa.string()),
        pa.field("status", pa.string()),
        pa.field("quote_date", pa.date32()),
        pa.field("utm_source", pa.string()),
        pa.field("utm_campaign", pa.string()),
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

QUOTE_LINES_SOURCE_SCHEMA = pa.schema(
    [
        pa.field("quote_line_id", pa.string()),
        pa.field("quote_id", pa.string()),
        pa.field("product_sku", pa.string()),
        pa.field("quantity", pa.int32()),
        pa.field("unit_price_vnd", pa.int64()),
        pa.field("line_total_vnd", pa.int64()),
        pa.field("is_deleted", pa.bool_()),
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

CAMPAIGNS_SOURCE_SCHEMA = pa.schema(
    [
        pa.field("campaign_id", pa.string()),
        pa.field("utm_source", pa.string()),
        pa.field("utm_campaign", pa.string()),
        pa.field("campaign_name", pa.string()),
        pa.field("channel", pa.string()),
        pa.field("started_on", pa.date32()),
    ]
)

VALID_QUOTE_STATUSES = frozenset({"draft", "sent", "accepted", "rejected"})

# ---------------------------------------------------------------------------
# Landing schemas (verbatim source columns + ingestion lineage metadata)
# ---------------------------------------------------------------------------
_LINEAGE_FIELDS = [
    pa.field("_batch_id", pa.string()),
    pa.field("_row_num", pa.int64()),
    pa.field("_ingested_at", pa.timestamp("us")),
]

LANDING_QUOTES_SCHEMA = pa.schema(list(QUOTES_SOURCE_SCHEMA) + _LINEAGE_FIELDS)
LANDING_QUOTE_LINES_SCHEMA = pa.schema(list(QUOTE_LINES_SOURCE_SCHEMA) + _LINEAGE_FIELDS)

# ---------------------------------------------------------------------------
# Metadata & observability schemas
# ---------------------------------------------------------------------------
BATCH_LEDGER_SCHEMA = pa.schema(
    [
        pa.field("batch_id", pa.string()),
        pa.field("content_sha256", pa.string()),
        pa.field("status", pa.string()),
        pa.field("quotes_rows", pa.int64()),
        pa.field("quote_lines_rows", pa.int64()),
        pa.field("rows_inserted", pa.int64()),
        pa.field("rows_updated", pa.int64()),
        pa.field("rows_deleted", pa.int64()),
        pa.field("duration_ms", pa.float64()),
        pa.field("note", pa.string()),
        pa.field("updated_at", pa.timestamp("us")),
    ]
)

DQ_REPORTS_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string()),
        pa.field("batch_id", pa.string()),
        pa.field("stage", pa.string()),
        pa.field("check_name", pa.string()),
        pa.field("severity", pa.string()),
        pa.field("passed", pa.bool_()),
        pa.field("anomaly_count", pa.int64()),
        pa.field("details", pa.string()),
        pa.field("checked_at", pa.timestamp("us")),
    ]
)

# ---------------------------------------------------------------------------
# Stretch goal: Campaign attribution schemas
# ---------------------------------------------------------------------------
REFERENCE_CAMPAIGNS_SCHEMA = pa.schema(
    list(CAMPAIGNS_SOURCE_SCHEMA)
    + [
        pa.field("_content_sha256", pa.string()),
        pa.field("_ingested_at", pa.timestamp("us")),
    ]
)

ATTRIBUTED_QUOTE_LINE_SCHEMA = pa.schema(
    list(QUOTE_LINE_SCHEMA)
    + [
        pa.field("campaign_id", pa.string()),
        pa.field("campaign_name", pa.string()),
        pa.field("campaign_channel", pa.string()),
        pa.field("campaign_started_on", pa.date32()),
        pa.field("attribution_status", pa.string()),
    ]
)
