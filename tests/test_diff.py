"""Tests for dqm.diff — DiffEngine compares two TableProfile snapshots.

Acceptance criteria (from issue #7):
  Manually corrupt 10% of a column's values, run diff, see the change flagged.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dqm.diff import DiffEngine
from dqm.models import ColumnDiff, ColumnProfile, TableDiff, TableProfile


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _col(
    name: str = "title",
    dtype: str = "VARCHAR",
    row_count: int = 100,
    null_count: int = 0,
    null_pct: float = 0.0,
    unique_count: int = 100,
    min_val=None,
    max_val=None,
    mean=None,
    p25=None,
    p75=None,
    top_values=None,
) -> ColumnProfile:
    return ColumnProfile(
        name=name,
        dtype=dtype,
        row_count=row_count,
        null_count=null_count,
        null_pct=null_pct,
        unique_count=unique_count,
        min_val=min_val,
        max_val=max_val,
        mean=mean,
        p25=p25,
        p75=p75,
        top_values=top_values or [],
    )


def _profile(
    table: str = "episodes",
    profiled_at: datetime | None = None,
    columns: list[ColumnProfile] | None = None,
) -> TableProfile:
    return TableProfile(
        table=table,
        db_path="/data/test.duckdb",
        profiled_at=profiled_at or datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc),
        columns=columns or [],
    )


@pytest.fixture
def engine() -> DiffEngine:
    return DiffEngine()


# ---------------------------------------------------------------------------
# TableDiff structure
# ---------------------------------------------------------------------------

class TestDiffStructure:
    def test_returns_table_diff(self, engine):
        a = _profile(columns=[_col("id")])
        b = _profile(columns=[_col("id")])
        result = engine.diff(a, b)
        assert isinstance(result, TableDiff)

    def test_table_name_taken_from_snapshot_b(self, engine):
        a = _profile(table="episodes")
        b = _profile(table="episodes")
        result = engine.diff(a, b)
        assert result.table == "episodes"

    def test_dates_set_correctly(self, engine):
        dt_a = datetime(2026, 3, 1, tzinfo=timezone.utc)
        dt_b = datetime(2026, 3, 2, tzinfo=timezone.utc)
        a = _profile(profiled_at=dt_a)
        b = _profile(profiled_at=dt_b)
        result = engine.diff(a, b)
        assert result.date_before == dt_a
        assert result.date_after == dt_b

    def test_column_count_matches_shared_columns(self, engine):
        a = _profile(columns=[_col("id"), _col("title"), _col("score")])
        b = _profile(columns=[_col("id"), _col("title"), _col("score")])
        result = engine.diff(a, b)
        assert len(result.columns) == 3

    def test_only_shared_columns_included(self, engine):
        """Columns present in only one snapshot are excluded."""
        a = _profile(columns=[_col("id"), _col("old_col")])
        b = _profile(columns=[_col("id"), _col("new_col")])
        result = engine.diff(a, b)
        # Only "id" is shared
        assert len(result.columns) == 1
        assert result.columns[0].column == "id"

    def test_column_order_follows_snapshot_b(self, engine):
        a = _profile(columns=[_col("id"), _col("title"), _col("score")])
        b = _profile(columns=[_col("score"), _col("id"), _col("title")])
        result = engine.diff(a, b)
        assert [c.column for c in result.columns] == ["score", "id", "title"]


# ---------------------------------------------------------------------------
# ColumnDiff field values
# ---------------------------------------------------------------------------

class TestColumnDiffFields:
    def test_null_pct_before_after(self, engine):
        a = _profile(columns=[_col("title", null_pct=0.02)])
        b = _profile(columns=[_col("title", null_pct=0.15)])
        diff = engine.diff(a, b).columns[0]
        assert diff.null_pct_before == pytest.approx(0.02)
        assert diff.null_pct_after == pytest.approx(0.15)

    def test_null_pct_delta(self, engine):
        a = _profile(columns=[_col("title", null_pct=0.02)])
        b = _profile(columns=[_col("title", null_pct=0.15)])
        diff = engine.diff(a, b).columns[0]
        assert diff.null_pct_delta == pytest.approx(0.13)

    def test_unique_before_after(self, engine):
        a = _profile(columns=[_col("title", unique_count=100)])
        b = _profile(columns=[_col("title", unique_count=200)])
        diff = engine.diff(a, b).columns[0]
        assert diff.unique_before == 100
        assert diff.unique_after == 200

    def test_unique_delta(self, engine):
        a = _profile(columns=[_col("title", unique_count=100)])
        b = _profile(columns=[_col("title", unique_count=200)])
        diff = engine.diff(a, b).columns[0]
        assert diff.unique_delta == 100

    def test_unique_delta_negative(self, engine):
        a = _profile(columns=[_col("title", unique_count=200)])
        b = _profile(columns=[_col("title", unique_count=150)])
        diff = engine.diff(a, b).columns[0]
        assert diff.unique_delta == -50

    def test_min_max_before_after(self, engine):
        a = _profile(columns=[_col("score", min_val=1.0, max_val=9.5)])
        b = _profile(columns=[_col("score", min_val=0.5, max_val=10.0)])
        diff = engine.diff(a, b).columns[0]
        assert diff.min_before == 1.0
        assert diff.min_after == 0.5
        assert diff.max_before == 9.5
        assert diff.max_after == 10.0

    def test_top_values_before_after(self, engine):
        top_a = [("A", 10), ("B", 5)]
        top_b = [("B", 8), ("C", 4)]
        a = _profile(columns=[_col("cat", top_values=top_a)])
        b = _profile(columns=[_col("cat", top_values=top_b)])
        diff = engine.diff(a, b).columns[0]
        assert diff.top_values_before == top_a
        assert diff.top_values_after == top_b

    def test_new_top_values(self, engine):
        top_a = [("A", 10), ("B", 5)]
        top_b = [("B", 8), ("C", 4)]
        a = _profile(columns=[_col("cat", top_values=top_a)])
        b = _profile(columns=[_col("cat", top_values=top_b)])
        diff = engine.diff(a, b).columns[0]
        # "C" is new; "A" was removed; "B" stayed
        assert diff.new_top_values == ["C"]

    def test_new_top_values_empty_when_unchanged(self, engine):
        top = [("A", 10), ("B", 5)]
        a = _profile(columns=[_col("cat", top_values=top)])
        b = _profile(columns=[_col("cat", top_values=top)])
        diff = engine.diff(a, b).columns[0]
        assert diff.new_top_values == []

    def test_dtype_taken_from_snapshot_b(self, engine):
        a = _profile(columns=[_col("id", dtype="INTEGER")])
        b = _profile(columns=[_col("id", dtype="BIGINT")])
        diff = engine.diff(a, b).columns[0]
        assert diff.dtype == "BIGINT"


# ---------------------------------------------------------------------------
# Severity thresholds — acceptance criteria for issue #7
# ---------------------------------------------------------------------------

class TestSeverity:
    """10% null corruption should be flagged as 'alert'."""

    def test_ok_when_no_change(self, engine):
        a = _profile(columns=[_col("title", null_pct=0.01)])
        b = _profile(columns=[_col("title", null_pct=0.01)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "ok"

    def test_ok_when_change_below_warn_threshold(self, engine):
        # 1 pp change — below the 2 pp warn threshold
        a = _profile(columns=[_col("title", null_pct=0.00)])
        b = _profile(columns=[_col("title", null_pct=0.01)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "ok"

    def test_warn_at_2pp_increase(self, engine):
        a = _profile(columns=[_col("title", null_pct=0.00)])
        b = _profile(columns=[_col("title", null_pct=0.02)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "warn"

    def test_warn_below_alert_threshold(self, engine):
        a = _profile(columns=[_col("title", null_pct=0.00)])
        b = _profile(columns=[_col("title", null_pct=0.09)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "warn"

    def test_alert_at_10pp_increase(self, engine):
        """Core acceptance criterion: 10% corruption → alert."""
        # Before: 2% nulls; after: 12% nulls (+10 pp)
        a = _profile(columns=[_col("title", null_pct=0.02)])
        b = _profile(columns=[_col("title", null_pct=0.12)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "alert"

    def test_alert_when_column_goes_from_0_to_10pct(self, engine):
        """Typical corruption scenario: 0% → 10% null."""
        a = _profile(columns=[_col("body", null_pct=0.00)])
        b = _profile(columns=[_col("body", null_pct=0.10)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "alert"

    def test_ok_when_null_pct_decreases(self, engine):
        """Null % going down is not an alert."""
        a = _profile(columns=[_col("title", null_pct=0.20)])
        b = _profile(columns=[_col("title", null_pct=0.01)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "ok"

    def test_custom_thresholds(self):
        """Custom alert/warn thresholds are respected."""
        engine = DiffEngine(alert_pp=0.05, warn_pp=0.01)
        a = _profile(columns=[_col("col", null_pct=0.00)])
        b = _profile(columns=[_col("col", null_pct=0.05)])
        diff = engine.diff(a, b).columns[0]
        assert diff.severity == "alert"


# ---------------------------------------------------------------------------
# End-to-end: simulate 10% corruption
# ---------------------------------------------------------------------------

class TestCorruptionAcceptanceCriteria:
    """Simulate the acceptance scenario: corrupt 10% of a column's values."""

    def test_10pct_null_corruption_flagged(self, engine):
        """
        Simulate a column with 0 nulls before and 10% nulls after corruption.
        The diff should detect this and raise 'alert' severity.
        """
        before = _profile(
            table="episodes",
            profiled_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
            columns=[
                _col("title", null_pct=0.00, unique_count=1000, null_count=0, row_count=1000),
                _col("score", null_pct=0.00, unique_count=50, null_count=0, row_count=1000),
            ],
        )

        # After corruption: 10% of `title` values set to NULL
        after = _profile(
            table="episodes",
            profiled_at=datetime(2026, 3, 2, tzinfo=timezone.utc),
            columns=[
                _col("title", null_pct=0.10, unique_count=900, null_count=100, row_count=1000),
                _col("score", null_pct=0.00, unique_count=50, null_count=0, row_count=1000),
            ],
        )

        result = engine.diff(before, after)

        title_diff = next(c for c in result.columns if c.column == "title")
        score_diff = next(c for c in result.columns if c.column == "score")

        # Title should be flagged
        assert title_diff.severity == "alert", (
            f"Expected 'alert' for 10% null increase, got '{title_diff.severity}'"
        )
        assert title_diff.null_pct_delta == pytest.approx(0.10)

        # Score should be fine
        assert score_diff.severity == "ok"

    def test_multiple_columns_with_mixed_severity(self, engine):
        """Diff handles multiple columns with different severities."""
        before = _profile(columns=[
            _col("a", null_pct=0.00),
            _col("b", null_pct=0.00),
            _col("c", null_pct=0.00),
        ])
        after = _profile(columns=[
            _col("a", null_pct=0.00),   # ok
            _col("b", null_pct=0.05),   # warn
            _col("c", null_pct=0.15),   # alert
        ])
        result = engine.diff(before, after)

        by_col = {d.column: d for d in result.columns}
        assert by_col["a"].severity == "ok"
        assert by_col["b"].severity == "warn"
        assert by_col["c"].severity == "alert"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_snapshots(self, engine):
        a = _profile(columns=[])
        b = _profile(columns=[])
        result = engine.diff(a, b)
        assert result.columns == []

    def test_single_column(self, engine):
        a = _profile(columns=[_col("id", null_pct=0.0)])
        b = _profile(columns=[_col("id", null_pct=0.0)])
        result = engine.diff(a, b)
        assert len(result.columns) == 1

    def test_snapshot_labels_include_profiled_at_when_no_id(self, engine):
        a = _profile(columns=[_col("id")])
        b = _profile(columns=[_col("id")])
        result = engine.diff(a, b)
        assert result.snapshot_before != ""
        assert result.snapshot_after != ""
