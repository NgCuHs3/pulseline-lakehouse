"""PulseLine Lakehouse: incremental CRM quote-line Iceberg pipeline."""

from pipeline.catalog import open_catalog
from pipeline.run import run
from pipeline.schema import ID_FIELD, QUOTE_LINE_SCHEMA, TABLE_IDENTIFIER

__all__ = [
    "ID_FIELD",
    "QUOTE_LINE_SCHEMA",
    "TABLE_IDENTIFIER",
    "open_catalog",
    "run",
]
