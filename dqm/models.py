"""Shared data models used across dqm modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class ColumnProfile:
    name: str
    dtype: str
    row_count: int
    null_count: int
    null_pct: float
    unique_count: int
    min_val: Any
    max_val: Any
    mean: float | None        # numeric only
    p25: float | None         # numeric only
    p75: float | None         # numeric only
    top_values: list[tuple]   # top 5 most frequent (value, count) pairs


@dataclass
class TableProfile:
    table: str
    db_path: str
    profiled_at: datetime
    columns: list[ColumnProfile] = field(default_factory=list)


@dataclass
class AnomalyContext:
    """Extra context passed to the LLM explainer."""
    table: str
    top_values_before: list[str] = field(default_factory=list)
    top_values_after: list[str] = field(default_factory=list)


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
