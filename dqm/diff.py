"""Diff engine: compare two TableProfile snapshots and surface what changed."""

from __future__ import annotations

from .models import ColumnDiff, ColumnProfile, TableDiff, TableProfile


class DiffEngine:
    """Compare two :class:`~dqm.models.TableProfile` snapshots.

    Parameters
    ----------
    alert_pp:
        Null-% change threshold for **alert** severity (default 10 pp).
    warn_pp:
        Null-% change threshold for **warn** severity (default 2 pp).
    """

    def __init__(self, alert_pp: float = 0.10, warn_pp: float = 0.02) -> None:
        self._alert_pp = alert_pp
        self._warn_pp = warn_pp

    def diff(self, snapshot_a: TableProfile, snapshot_b: TableProfile) -> TableDiff:
        """Compare *snapshot_a* (before) against *snapshot_b* (after).

        Columns that exist in both snapshots are compared field-by-field.
        Columns present in only one snapshot are silently skipped.
        """
        cols_a: dict[str, ColumnProfile] = {c.name: c for c in snapshot_a.columns}
        cols_b: dict[str, ColumnProfile] = {c.name: c for c in snapshot_b.columns}

        shared_names = [n for n in cols_b if n in cols_a]

        column_diffs: list[ColumnDiff] = []
        for name in shared_names:
            col_diff = self._diff_column(cols_a[name], cols_b[name])
            column_diffs.append(col_diff)

        return TableDiff(
            table=snapshot_b.table,
            snapshot_before=snapshot_a.profiled_at.isoformat(),
            snapshot_after=snapshot_b.profiled_at.isoformat(),
            date_before=snapshot_a.profiled_at,
            date_after=snapshot_b.profiled_at,
            columns=column_diffs,
        )

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
            row_count_before=before.row_count,
            row_count_after=after.row_count,
            top_values_before=list(before.top_values),
            top_values_after=list(after.top_values),
            severity=sev,
        )

    def _assess_severity(self, null_pct_delta: float) -> str:
        _EPS = 1e-9
        if null_pct_delta >= self._alert_pp - _EPS:
            return "alert"
        if null_pct_delta >= self._warn_pp - _EPS:
            return "warn"
        return "ok"
