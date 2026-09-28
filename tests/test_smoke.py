from pathlib import Path

from pipeline.cli import main
from pipeline.schema import ID_FIELD, QUOTE_LINE_SCHEMA

INCOMING = Path(__file__).resolve().parents[1] / "data" / "incoming"


def test_run_exits_cleanly(tmp_path: Path) -> None:
    assert main(["run", "--incoming", str(INCOMING), "--warehouse", str(tmp_path / "warehouse")]) == 0


def test_target_schema_contains_id_field() -> None:
    assert ID_FIELD in QUOTE_LINE_SCHEMA.names
