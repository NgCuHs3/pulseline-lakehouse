from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog


def open_catalog(warehouse: Path) -> SqlCatalog:
    warehouse = warehouse.resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{warehouse / 'catalog.db'}",
        warehouse=warehouse.as_uri(),
    )
