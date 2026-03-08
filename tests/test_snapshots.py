"""Tests for dqm.snapshots — snapshot store using a temporary SQLite database."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from dqm.models import ColumnProfile, TableProfile
from dqm.snapshots import (
    _json_to_profile,
    _open,
    _profile_to_json,
    get_snapshot,
    list_snapshots,
    save_snapshot,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def snap_db(tmp_path: Path) -> Path:
    """Return a fresh path for a temporary snapshot SQLite database."""
    return tmp_path / "test_snapshots.db"


@pytest.fixture
def sample_profile() -> TableProfile:
    """A minimal but complete TableProfile fixture."""
    return TableProfile(
        table="episodes",
        db_path="/data/p3.duckdb",
        profiled_at=datetime(2026, 3, 8, 12, 0, 0, tzinfo=timezone.utc),
        columns=[
            ColumnProfile(
                name="id",
                dtype="INTEGER",
                row_count=100,
                null_count=0,
                null_pct=0.0,
                unique_count=100,
                min_val=1,
                max_val=100,
                mean=50.5,
                p25=25.75,
                p75=75.25,
                top_values=[(1, 1), (2, 1)],
            ),
            ColumnProfile(
                name="title",
                dtype="VARCHAR",
                row_count=100,
                null_count=5,
                null_pct=0.05,
                unique_count=95,
                min_val="A title",
                max_val="Z title",
                mean=None,
                p25=None,
                p75=None,
                top_values=[("A title", 2)],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# _open — schema creation
# ---------------------------------------------------------------------------

class TestOpen:
    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        db_path = tmp_path / "nested" / "dir" / "snaps.db"
        conn = _open(db_path)
        conn.close()
        assert db_path.exists()

    def test_creates_snapshots_table(self, snap_db: Path) -> None:
        conn = _open(snap_db)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshots'"
        ).fetchall()
        conn.close()
        assert rows, "snapshots table should be created"

    def test_idempotent_schema(self, snap_db: Path) -> None:
        """Calling _open twice must not raise (CREATE TABLE IF NOT EXISTS)."""
        _open(snap_db).close()
        _open(snap_db).close()


# ---------------------------------------------------------------------------
# Serialisation round-trip
# ---------------------------------------------------------------------------

class TestSerialisation:
    def test_json_round_trip(self, sample_profile: TableProfile) -> None:
        raw = _profile_to_json(sample_profile)
        recovered = _json_to_profile(raw)

        assert recovered.table == sample_profile.table
        assert recovered.db_path == sample_profile.db_path
        assert len(recovered.columns) == len(sample_profile.columns)

    def test_column_fields_preserved(self, sample_profile: TableProfile) -> None:
        raw = _profile_to_json(sample_profile)
        recovered = _json_to_profile(raw)

        id_col = next(c for c in recovered.columns if c.name == "id")
        assert id_col.row_count == 100
        assert id_col.null_count == 0
        assert id_col.unique_count == 100
        assert id_col.mean == pytest.approx(50.5)
        assert id_col.p25 == pytest.approx(25.75)
        assert id_col.p75 == pytest.approx(75.25)

    def test_none_numerics_preserved(self, sample_profile: TableProfile) -> None:
        raw = _profile_to_json(sample_profile)
        recovered = _json_to_profile(raw)

        title_col = next(c for c in recovered.columns if c.name == "title")
        assert title_col.mean is None
        assert title_col.p25 is None
        assert title_col.p75 is None

    def test_top_values_are_tuples(self, sample_profile: TableProfile) -> None:
        raw = _profile_to_json(sample_profile)
        recovered = _json_to_profile(raw)

        id_col = next(c for c in recovered.columns if c.name == "id")
        for item in id_col.top_values:
            assert isinstance(item, tuple)

    def test_datetime_preserved(self, sample_profile: TableProfile) -> None:
        raw = _profile_to_json(sample_profile)
        recovered = _json_to_profile(raw)
        assert recovered.profiled_at == sample_profile.profiled_at


# ---------------------------------------------------------------------------
# save_snapshot
# ---------------------------------------------------------------------------

class TestSaveSnapshot:
    def test_returns_integer_id(self, sample_profile: TableProfile, snap_db: Path) -> None:
        snap_id = save_snapshot(sample_profile, snap_db)
        assert isinstance(snap_id, int)
        assert snap_id > 0

    def test_ids_are_sequential(self, sample_profile: TableProfile, snap_db: Path) -> None:
        id1 = save_snapshot(sample_profile, snap_db)
        id2 = save_snapshot(sample_profile, snap_db)
        assert id2 > id1

    def test_row_appears_in_db(self, sample_profile: TableProfile, snap_db: Path) -> None:
        snap_id = save_snapshot(sample_profile, snap_db)
        conn = _open(snap_db)
        row = conn.execute(
            "SELECT source_db, table_name FROM snapshots WHERE id = ?", (snap_id,)
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["source_db"] == sample_profile.db_path
        assert row["table_name"] == sample_profile.table

    def test_creates_db_if_missing(self, tmp_path: Path, sample_profile: TableProfile) -> None:
        db_path = tmp_path / "brand_new" / "snaps.db"
        assert not db_path.exists()
        save_snapshot(sample_profile, db_path)
        assert db_path.exists()


# ---------------------------------------------------------------------------
# list_snapshots
# ---------------------------------------------------------------------------

class TestListSnapshots:
    def test_empty_list_for_unknown_table(self, snap_db: Path) -> None:
        rows = list_snapshots("no_such_table", snap_db)
        assert rows == []

    def test_returns_correct_count(self, sample_profile: TableProfile, snap_db: Path) -> None:
        save_snapshot(sample_profile, snap_db)
        save_snapshot(sample_profile, snap_db)
        save_snapshot(sample_profile, snap_db)
        rows = list_snapshots(sample_profile.table, snap_db)
        assert len(rows) == 3

    def test_ordered_newest_first(self, snap_db: Path) -> None:
        earlier = TableProfile(
            table="episodes",
            db_path="/data/p3.duckdb",
            profiled_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
            columns=[],
        )
        later = TableProfile(
            table="episodes",
            db_path="/data/p3.duckdb",
            profiled_at=datetime(2026, 3, 8, tzinfo=timezone.utc),
            columns=[],
        )
        save_snapshot(earlier, snap_db)
        save_snapshot(later, snap_db)

        rows = list_snapshots("episodes", snap_db)
        # Newest first → later timestamp should appear first
        assert rows[0]["profiled_at"] > rows[1]["profiled_at"]

    def test_filters_by_table(self, snap_db: Path) -> None:
        ep_profile = TableProfile(
            table="episodes", db_path="/db", profiled_at=datetime.now(tz=timezone.utc), columns=[]
        )
        sum_profile = TableProfile(
            table="summaries", db_path="/db", profiled_at=datetime.now(tz=timezone.utc), columns=[]
        )
        save_snapshot(ep_profile, snap_db)
        save_snapshot(sum_profile, snap_db)

        ep_rows = list_snapshots("episodes", snap_db)
        sum_rows = list_snapshots("summaries", snap_db)
        assert len(ep_rows) == 1
        assert len(sum_rows) == 1

    def test_row_has_expected_keys(self, sample_profile: TableProfile, snap_db: Path) -> None:
        save_snapshot(sample_profile, snap_db)
        rows = list_snapshots(sample_profile.table, snap_db)
        assert set(rows[0].keys()) == {"id", "source_db", "table_name", "profiled_at"}


# ---------------------------------------------------------------------------
# get_snapshot
# ---------------------------------------------------------------------------

class TestGetSnapshot:
    def test_returns_none_for_missing_id(self, snap_db: Path) -> None:
        result = get_snapshot(9999, snap_db)
        assert result is None

    def test_returns_table_profile(self, sample_profile: TableProfile, snap_db: Path) -> None:
        snap_id = save_snapshot(sample_profile, snap_db)
        recovered = get_snapshot(snap_id, snap_db)
        assert isinstance(recovered, TableProfile)

    def test_table_name_matches(self, sample_profile: TableProfile, snap_db: Path) -> None:
        snap_id = save_snapshot(sample_profile, snap_db)
        recovered = get_snapshot(snap_id, snap_db)
        assert recovered.table == sample_profile.table

    def test_column_count_matches(self, sample_profile: TableProfile, snap_db: Path) -> None:
        snap_id = save_snapshot(sample_profile, snap_db)
        recovered = get_snapshot(snap_id, snap_db)
        assert len(recovered.columns) == len(sample_profile.columns)

    def test_different_ids_return_different_profiles(self, snap_db: Path) -> None:
        prof_a = TableProfile(
            table="t_a", db_path="/db", profiled_at=datetime.now(tz=timezone.utc), columns=[]
        )
        prof_b = TableProfile(
            table="t_b", db_path="/db", profiled_at=datetime.now(tz=timezone.utc), columns=[]
        )
        id_a = save_snapshot(prof_a, snap_db)
        id_b = save_snapshot(prof_b, snap_db)

        assert get_snapshot(id_a, snap_db).table == "t_a"
        assert get_snapshot(id_b, snap_db).table == "t_b"
