import pyarrow as pa

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
