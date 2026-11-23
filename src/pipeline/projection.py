"""Single-query DuckDB projection engine for crm.quote_line and campaign attribution."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from pipeline.schema import ATTRIBUTED_QUOTE_LINE_SCHEMA, QUOTE_LINE_SCHEMA

# Single DuckDB SQL query shaping verbatim landed quotes + quote_lines into crm.quote_line.
# - Resolves within-batch and cross-batch out-of-order versions via updated_at DESC,
#   breaking exact timestamp ties deterministically by arrival order (_batch_id DESC, _row_num DESC).
# - In incremental mode ($full_refresh = FALSE), computes the exact closed set of affected
#   quote_line_ids: lines in target_batches PLUS any historical lines whose parent quote_id
#   was inserted/updated in target_batches (handles header-only updates and late parent arrival).
# - Preserves lines whose parent quote has not arrived yet via LEFT JOIN so zero rows are lost;
#   subsequent batches backfill header columns automatically when the quote arrives.
QUOTE_LINE_PROJECTION_SQL = """
WITH affected_quote_ids AS (
    SELECT DISTINCT quote_id
    FROM landing_quotes
    WHERE _batch_id IN (SELECT batch_id FROM target_batches)
),
affected_line_ids AS (
    SELECT DISTINCT quote_line_id
    FROM landing_quote_lines
    WHERE _batch_id IN (SELECT batch_id FROM target_batches)
    UNION
    SELECT DISTINCT quote_line_id
    FROM landing_quote_lines
    WHERE quote_id IN (SELECT quote_id FROM affected_quote_ids)
),
scoped_lines AS (
    SELECT
        quote_line_id,
        quote_id,
        product_sku,
        quantity,
        unit_price_vnd,
        line_total_vnd,
        is_deleted,
        updated_at,
        _batch_id,
        _row_num
    FROM landing_quote_lines
    WHERE $full_refresh
       OR quote_line_id IN (SELECT quote_line_id FROM affected_line_ids)
),
latest_lines AS (
    SELECT
        quote_line_id,
        quote_id,
        product_sku,
        quantity,
        unit_price_vnd,
        line_total_vnd,
        is_deleted,
        updated_at AS line_updated_at
    FROM scoped_lines
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY quote_line_id
        ORDER BY updated_at DESC, _batch_id DESC, _row_num DESC
    ) = 1
),
latest_quotes AS (
    SELECT
        quote_id,
        quote_number,
        customer_id,
        status AS quote_status,
        quote_date,
        utm_source,
        utm_campaign,
        updated_at AS quote_updated_at
    FROM landing_quotes
    WHERE quote_id IN (SELECT DISTINCT quote_id FROM latest_lines)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY quote_id
        ORDER BY updated_at DESC, _batch_id DESC, _row_num DESC
    ) = 1
)
SELECT
    l.quote_line_id,
    l.quote_id,
    q.quote_number,
    q.customer_id,
    q.quote_status,
    q.quote_date,
    q.utm_source,
    q.utm_campaign,
    l.product_sku,
    l.quantity,
    l.unit_price_vnd,
    l.line_total_vnd,
    l.line_updated_at,
    q.quote_updated_at,
    l.is_deleted AS _is_deleted
FROM latest_lines l
LEFT JOIN latest_quotes q
    ON l.quote_id = q.quote_id
ORDER BY l.quote_line_id
"""

# Stretch-goal DuckDB SQL query attributing live crm.quote_line rows to reference.campaigns.
# Preserves attribution even when quote_date precedes campaign.started_on while tagging
# such rows as 'attributed_pre_launch' for downstream filtering and data-quality alerting.
CAMPAIGN_ATTRIBUTION_SQL = """
WITH normalized_campaigns AS (
    SELECT
        campaign_id,
        LOWER(TRIM(utm_source)) AS norm_utm_source,
        LOWER(TRIM(utm_campaign)) AS norm_utm_campaign,
        campaign_name,
        channel,
        started_on
    FROM ref_campaigns
)
SELECT
    ql.quote_line_id,
    ql.quote_id,
    ql.quote_number,
    ql.customer_id,
    ql.quote_status,
    ql.quote_date,
    ql.utm_source,
    ql.utm_campaign,
    ql.product_sku,
    ql.quantity,
    ql.unit_price_vnd,
    ql.line_total_vnd,
    ql.line_updated_at,
    ql.quote_updated_at,
    c.campaign_id,
    c.campaign_name,
    c.channel AS campaign_channel,
    c.started_on AS campaign_started_on,
    CASE
        WHEN ql.utm_source IS NULL AND ql.utm_campaign IS NULL THEN 'unattributed'
        WHEN ql.utm_source IS NULL OR ql.utm_campaign IS NULL THEN 'partial_utm'
        WHEN c.campaign_id IS NULL THEN 'unknown_utm'
        WHEN ql.quote_date IS NOT NULL AND ql.quote_date < c.started_on THEN 'attributed_pre_launch'
        ELSE 'attributed'
    END AS attribution_status
FROM crm_quote_line ql
LEFT JOIN normalized_campaigns c
    ON LOWER(TRIM(ql.utm_source)) = c.norm_utm_source
   AND LOWER(TRIM(ql.utm_campaign)) = c.norm_utm_campaign
ORDER BY ql.quote_line_id
"""


@dataclass(frozen=True)
class ProjectionResult:
    """Output of the DuckDB projection step."""

    live_rows: pa.Table
    deleted_ids: list[str]


class DuckDBProjector:
    """Executes vectorized SQL projections over Iceberg-backed Arrow tables."""

    def project_quote_lines(
        self,
        *,
        landing_quotes: pa.Table,
        landing_quote_lines: pa.Table,
        target_batches: list[str],
        full_refresh: bool = False,
    ) -> ProjectionResult:
        con = duckdb.connect()
        try:
            con.register("landing_quotes", landing_quotes)
            con.register("landing_quote_lines", landing_quote_lines)
            batch_table = pa.table({"batch_id": pa.array(target_batches, type=pa.string())})
            con.register("target_batches", batch_table)

            projected = con.execute(
                QUOTE_LINE_PROJECTION_SQL,
                {"full_refresh": full_refresh},
            ).to_arrow_table()
        finally:
            con.close()

        if projected.num_rows == 0:
            return ProjectionResult(
                live_rows=pa.Table.from_pylist([], schema=QUOTE_LINE_SCHEMA),
                deleted_ids=[],
            )

        is_deleted_col = pc.fill_null(projected["_is_deleted"], False)
        live_table = (
            projected.filter(pc.invert(is_deleted_col))
            .drop(["_is_deleted"])
            .cast(QUOTE_LINE_SCHEMA)
        )
        deleted_table = projected.filter(is_deleted_col)
        deleted_ids = deleted_table["quote_line_id"].to_pylist()
        return ProjectionResult(live_rows=live_table, deleted_ids=deleted_ids)

    def project_campaign_attribution(
        self,
        *,
        quote_lines: pa.Table,
        campaigns: pa.Table,
    ) -> pa.Table:
        con = duckdb.connect()
        try:
            con.register("crm_quote_line", quote_lines)
            con.register("ref_campaigns", campaigns)
            result = con.execute(CAMPAIGN_ATTRIBUTION_SQL).to_arrow_table()
        finally:
            con.close()
        return result.cast(ATTRIBUTED_QUOTE_LINE_SCHEMA)
