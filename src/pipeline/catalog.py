import os
from pathlib import Path

from pyiceberg.catalog.sql import SqlCatalog


def open_catalog(warehouse: Path) -> SqlCatalog:
    warehouse = warehouse.resolve()
    warehouse.mkdir(parents=True, exist_ok=True)
    # PyIceberg's PyArrowFileIO leaves a leading slash on Windows file:///C:/... URIs;
    # passing the resolved local path on Windows avoids WinError 123 while keeping
    # POSIX behavior identical to warehouse.as_uri().
    warehouse_location = str(warehouse) if os.name == "nt" else warehouse.as_uri()
    return SqlCatalog(
        "local",
        uri=f"sqlite:///{warehouse / 'catalog.db'}",
        warehouse=warehouse_location,
    )
