"""Pipeline configuration models."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration for an incremental lakehouse run."""

    incoming_dir: Path
    warehouse_dir: Path
    campaigns_path: Path | None = None
    full_refresh: bool = False
    strict_schema: bool = False
    json_logs: bool = False

    def resolved_campaigns_path(self) -> Path | None:
        """Return explicit campaigns path or auto-discover adjacent reference file."""
        if self.campaigns_path is not None:
            return self.campaigns_path
        candidate = self.incoming_dir.resolve().parent / "reference" / "campaigns.parquet"
        return candidate if candidate.is_file() else None
