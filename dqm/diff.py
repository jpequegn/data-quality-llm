"""Diff engine: compare two TableProfile snapshots and surface what changed.

Usage
-----
>>> from dqm.diff import DiffEngine
>>> engine = DiffEngine()
>>> table_diff = engine.diff(snapshot_a, snapshot_b)

Severity thresholds
-------------------
The null-% change drives the severity label assigned to each :class:`ColumnDiff`:

* **alert** — null % rose by ≥ 10 percentage-points
* **warn**  — null % rose by ≥ 2 percentage-points
* **ok**    — everything else

These thresholds are intentionally conservative: false-positives here are much
cheaper than missed regressions.
"""

from __future__ import annotations

from datetime import datetime, timezone

from .models import ColumnDiff, ColumnProfile, TableDiff, TableProfile

# Severity thresholds (percentage-point change in null %)
_ALERT_PP = 0.10   # ≥ 10 pp  →  alert
_WARN_PP = 0.02    # ≥  2 pp  →  warn


def _severity(null_pct_delta: float) -> str:
    """Map a null-% change to a severity label."""
    # Use a tiny epsilon to absorb floating-point imprecision
    # (e.g. 0.12 - 0.02 == 0.09999… in IEEE 754 arithmetic).
    _EPS = 1e-9
    if null_pct_delta >= _ALERT_PP - _EPS:
        return "alert"
    if null_pct_delta >= _WARN_PP - _EPS:
        return "warn"
    return "ok"


class DiffEngine:
    """Compare two :class:`~dqm.models.TableProfile` snapshots.

    Parameters
    ----------
    alert_pp:
        Null-% change threshold for **alert** severity (default 10 pp).
    warn_pp:
        Null-% change threshold for **warn** severity (default 2 pp).
    """

    def __init__(
        self,
        alert_pp: float = _ALERT_PP,
        warn_pp: float = _WARN_PP,
    ) -> None:
        self._alert_pp = alert_pp
        self._warn_pp = warn_pp

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def diff(self, snapshot_a: TableProfile, snapshot_b: TableProfile) -> TableDiff:
        """Compare *snapshot_a* (before) against *snapshot_b* (after).

        Columns that exist in both snapshots are compared field-by-field.
        Columns present in only one snapshot are silently skipped (they may
        be tracked in a future "schema drift" feature).

        Parameters
        ----------
        snapshot_a:
            The *older* snapshot (the baseline).
        snapshot_b:
            The *newer* snapshot (the current state).

        Returns
        -------
        TableDiff
            Aggregated diff with one :class:`ColumnDiff` per shared column.
        """
        cols_a: dict[str, ColumnProfile] = {c.name: c for c in snapshot_a.columns}
        cols_b: dict[str, ColumnProfile] = {c.name: c for c in snapshot_b.columns}

        shared_names = [n for n in cols_b if n in cols_a]  # preserve b's column order

        column_diffs: list[ColumnDiff] = []
        for name in shared_names:
            col_diff = self._diff_column(cols_a[name], cols_b[name])
            column_diffs.append(col_diff)

        return TableDiff(
            table=snapshot_b.table,
            snapshot_before=f"id={getattr(snapshot_a, '_snapshot_id', snapshot_a.profiled_at.isoformat())}",
            snapshot_after=f"id={getattr(snapshot_b, '_snapshot_id', snapshot_b.profiled_at.isoformat())}",
            date_before=snapshot_a.profiled_at,
            date_after=snapshot_b.profiled_at,
            columns=column_diffs,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _diff_column(self, before: ColumnProfile, after: ColumnProfile) -> ColumnDiff:
        """Build a :class:`ColumnDiff` for a single column."""
        null_pct_delta = after.null_pct - before.null_pct
        sev = self._assess_severity(null_pct_delta)

        return ColumnDiff(
            column=before.name,
            dtype=after.dtype,
            null_pct_before=before.null_pct,
            null_pct_after=after.null_pct,
            unique_before=before.unique_count,
            unique_after=after.unique_count,
            min_before=before.min_val,
            min_after=after.min_val,
            max_before=before.max_val,
            max_after=after.max_val,
            top_values_before=list(before.top_values),
            top_values_after=list(after.top_values),
            severity=sev,
        )

    def _assess_severity(self, null_pct_delta: float) -> str:
        """Return severity string based on the null-% change."""
        _EPS = 1e-9
        if null_pct_delta >= self._alert_pp - _EPS:
            return "alert"
        if null_pct_delta >= self._warn_pp - _EPS:
            return "warn"
        return "ok"
