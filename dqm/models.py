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
    """Per-column diff between two TableProfile snapshots."""

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

    # Row counts
    row_count_before: int = 0
    row_count_after: int = 0

    # Top values
    top_values_before: list[tuple] = field(default_factory=list)
    top_values_after: list[tuple] = field(default_factory=list)

    # Severity from diff engine
    severity: str = "ok"   # "ok" | "warn" | "alert"

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


class Anomaly:
    """A single anomaly detected by the rule-based AnomalyDetector.

    Supports two calling conventions:

    **New** (issue #8):
        ``Anomaly(column=..., rule_triggered=..., old_val=..., new_val=..., severity=...)``

    **Legacy** (issues #2/#3):
        ``Anomaly(column=..., metric=..., value_before=..., value_after=..., severity=..., delta_pp=...)``

    Both sets of names are available as attributes after construction.
    """

    def __init__(
        self,
        column: str,
        severity: str,
        # New-style fields
        rule_triggered: str | None = None,
        old_val: float | None = None,
        new_val: float | None = None,
        # Legacy fields (kept for backward compatibility)
        metric: str | None = None,
        value_before: float | None = None,
        value_after: float | None = None,
        delta_pp: float | None = None,
    ) -> None:
        self.column = column
        self.severity = severity

        # Unify: new-style takes precedence; fall back to legacy names
        self.rule_triggered: str = rule_triggered or metric or ""
        self.old_val: float = old_val if old_val is not None else (value_before if value_before is not None else 0.0)
        self.new_val: float = new_val if new_val is not None else (value_after if value_after is not None else 0.0)

        # Explicit delta_pp can override computed value (legacy callers may pass it)
        if delta_pp is not None:
            self._delta_pp: float | None = delta_pp
        else:
            self._delta_pp = None

    # --- Aliases ---
    @property
    def metric(self) -> str:
        return self.rule_triggered

    @property
    def value_before(self) -> float:
        return self.old_val

    @property
    def value_after(self) -> float:
        return self.new_val

    @property
    def delta_pp(self) -> float:
        if self._delta_pp is not None:
            return self._delta_pp
        return self.new_val - self.old_val

    def __repr__(self) -> str:
        return (
            f"Anomaly(column={self.column!r}, rule_triggered={self.rule_triggered!r}, "
            f"old_val={self.old_val}, new_val={self.new_val}, severity={self.severity!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Anomaly):
            return NotImplemented
        return (
            self.column == other.column
            and self.rule_triggered == other.rule_triggered
            and self.old_val == other.old_val
            and self.new_val == other.new_val
            and self.severity == other.severity
        )


@dataclass
class TableDiff:
    """Result of comparing two TableProfile snapshots for the same table."""

    table: str
    snapshot_before: str
    snapshot_after: str
    date_before: datetime
    date_after: datetime
    columns: list[ColumnDiff] = field(default_factory=list)
