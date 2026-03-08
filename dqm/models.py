"""Shared data models used across dqm modules."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    null_pct: float
    unique_count: int
    min_value: object = None
    max_value: object = None


@dataclass
class ColumnDiff:
    column: str
    dtype: str
    null_pct_before: float
    null_pct_after: float
    unique_before: int
    unique_after: int

    @property
    def null_pct_delta(self) -> float:
        return self.null_pct_after - self.null_pct_before


@dataclass
class Anomaly:
    column: str
    metric: str
    value_before: float
    value_after: float
    severity: str  # "ALERT" | "WARNING" | "OK"
    delta_pp: float = 0.0


@dataclass
class TableDiff:
    table: str
    snapshot_before: str
    snapshot_after: str
    date_before: datetime
    date_after: datetime
    columns: list[ColumnDiff] = field(default_factory=list)
