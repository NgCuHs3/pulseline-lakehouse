from pathlib import Path

from pipeline.catalog import open_catalog


def run(incoming: Path, warehouse: Path) -> None:
    open_catalog(warehouse)
