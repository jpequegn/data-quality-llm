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
    """Per-column diff between two TableProfile snapshots.

    Attributes
    ----------
    column:
        Column name.
    dtype:
        DuckDB data type.
    null_pct_before / null_pct_after:
        Null percentage in snapshot A and B.
    unique_before / unique_after:
        Distinct-value count in snapshot A and B.
    min_before / min_after:
        Minimum value in snapshot A and B (None when non-comparable).
    max_before / max_after:
        Maximum value in snapshot A and B.
    top_values_before / top_values_after:
        Top-5 (value, count) pairs for each snapshot.
    severity:
        ``"ok"`` | ``"warn"`` | ``"alert"`` — driven by null_pct change.
    """

    column: str
    dtype: str

    # Null stats
    null_pct_before: float
    null_pct_after: float

    # Cardinality
    unique_before: int
    unique_after: int

    # Range drift (kept as raw values — could be numeric, date, str …)
    min_before: Any = None
    min_after: Any = None
    max_before: Any = None
    max_after: Any = None

    # Top values
    top_values_before: list[tuple] = field(default_factory=list)
    top_values_after: list[tuple] = field(default_factory=list)

    # Severity assessment
    severity: str = "ok"   # "ok" | "warn" | "alert"

    # ------------------------------------------------------------------
    # Convenience properties
    # ------------------------------------------------------------------

    @property
    def null_pct_delta(self) -> float:
        """Change in null % (percentage-points)."""
        return self.null_pct_after - self.null_pct_before

    @property
    def unique_delta(self) -> int:
        """Change in unique count (absolute)."""
        return self.unique_after - self.unique_before

    @property
    def new_top_values(self) -> list[Any]:
        """Values that appear in top_values_after but not in top_values_before."""
        before_vals = {v for v, _ in self.top_values_before}
        return [v for v, _ in self.top_values_after if v not in before_vals]


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
    """Result of comparing two TableProfile snapshots for the same table.

    Attributes
    ----------
    table:
        Table name.
    snapshot_before / snapshot_after:
        Human-readable labels for the two snapshots (e.g. ``"id=3"``).
    date_before / date_after:
        ``profiled_at`` timestamps for snapshot A and B.
    columns:
        One :class:`ColumnDiff` per shared column.
    """

    table: str
    snapshot_before: str
    snapshot_after: str
    date_before: datetime
    date_after: datetime
    columns: list[ColumnDiff] = field(default_factory=list)
