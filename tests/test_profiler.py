"""Tests for dqm.profiler — uses real in-memory DuckDB connections."""

from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from dqm.models import ColumnProfile, TableProfile
from dqm.profiler import Profiler, _quote


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    """In-memory DuckDB connection with a rich test table."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE episodes (
            id        INTEGER,
            title     VARCHAR,
            score     DOUBLE,
            published TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO episodes VALUES
            (1,  'Ep One',   9.5,  '2026-01-01'),
            (2,  'Ep Two',   8.0,  '2026-01-02'),
            (3,  'Ep Three', 7.5,  '2026-01-03'),
            (4,  'Ep Four',  NULL, '2026-01-04'),
            (5,  NULL,       6.0,  NULL),
            (6,  'Ep Four',  6.0,  '2026-01-06')
        """
    )
    yield con
    con.close()


@pytest.fixture
def profiler():
    return Profiler()


# ---------------------------------------------------------------------------
# TableProfile shape
# ---------------------------------------------------------------------------


def test_profile_table_returns_table_profile(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    assert isinstance(result, TableProfile)
    assert result.table == "episodes"
    assert isinstance(result.profiled_at, datetime)


def test_profile_table_has_correct_column_count(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    assert len(result.columns) == 4


def test_profile_table_column_names(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    names = [c.name for c in result.columns]
    assert "id" in names
    assert "title" in names
    assert "score" in names
    assert "published" in names


# ---------------------------------------------------------------------------
# Row counts
# ---------------------------------------------------------------------------


def test_row_count_is_correct(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    for col in result.columns:
        assert col.row_count == 6


# ---------------------------------------------------------------------------
# Null stats
# ---------------------------------------------------------------------------


def test_null_count_varchar(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    assert title.null_count == 1


def test_null_pct_varchar(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    assert pytest.approx(title.null_pct, rel=1e-3) == 1 / 6


def test_null_count_numeric(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    score = next(c for c in result.columns if c.name == "score")
    assert score.null_count == 1


def test_null_pct_no_nulls(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    id_col = next(c for c in result.columns if c.name == "id")
    assert id_col.null_count == 0
    assert id_col.null_pct == 0.0


# ---------------------------------------------------------------------------
# Cardinality
# ---------------------------------------------------------------------------


def test_unique_count_id(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    id_col = next(c for c in result.columns if c.name == "id")
    assert id_col.unique_count == 6


def test_unique_count_title_with_duplicate(profiler, conn):
    # 'Ep Four' appears twice; title has 1 NULL → 4 distinct non-null values
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    # DISTINCT includes nulls in DuckDB COUNT(DISTINCT): nulls are excluded
    assert title.unique_count == 4


# ---------------------------------------------------------------------------
# Min / Max
# ---------------------------------------------------------------------------


def test_min_max_integer(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    id_col = next(c for c in result.columns if c.name == "id")
    assert id_col.min_val == 1
    assert id_col.max_val == 6


def test_min_max_double(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    score = next(c for c in result.columns if c.name == "score")
    assert score.min_val == pytest.approx(6.0)
    assert score.max_val == pytest.approx(9.5)


def test_min_max_varchar(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    assert title.min_val is not None
    assert title.max_val is not None


# ---------------------------------------------------------------------------
# Numeric stats (mean, p25, p75)
# ---------------------------------------------------------------------------


def test_mean_numeric(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    score = next(c for c in result.columns if c.name == "score")
    # Values: 9.5, 8.0, 7.5, NULL, 6.0, 6.0 → mean of non-null = 37.0/5 = 7.4
    assert score.mean == pytest.approx(7.4, rel=1e-3)


def test_p25_p75_numeric(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    score = next(c for c in result.columns if c.name == "score")
    assert score.p25 is not None
    assert score.p75 is not None
    assert score.p25 <= score.p75


def test_no_numeric_stats_for_varchar(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    assert title.mean is None
    assert title.p25 is None
    assert title.p75 is None


def test_no_numeric_stats_for_timestamp(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    published = next(c for c in result.columns if c.name == "published")
    assert published.mean is None


# ---------------------------------------------------------------------------
# Top values
# ---------------------------------------------------------------------------


def test_top_values_is_list_of_tuples(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    assert isinstance(title.top_values, list)
    for item in title.top_values:
        assert isinstance(item, tuple)
        assert len(item) == 2


def test_top_values_most_frequent_first(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    # 'Ep Four' appears twice, should be first
    assert title.top_values[0][0] == "Ep Four"
    assert title.top_values[0][1] == 2


def test_top_values_excludes_nulls(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    title = next(c for c in result.columns if c.name == "title")
    values = [v for v, _ in title.top_values]
    assert None not in values


def test_top_values_at_most_five(profiler, conn):
    result = profiler.profile_table(conn, "episodes")
    for col in result.columns:
        assert len(col.top_values) <= 5


# ---------------------------------------------------------------------------
# Empty table edge case
# ---------------------------------------------------------------------------


def test_empty_table(profiler):
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE empty_t (x INTEGER, y VARCHAR)")
    result = profiler.profile_table(con, "empty_t")
    assert result.table == "empty_t"
    assert len(result.columns) == 2
    for col in result.columns:
        assert col.row_count == 0
        assert col.null_count == 0
        assert col.null_pct == 0.0
    con.close()


# ---------------------------------------------------------------------------
# _quote helper
# ---------------------------------------------------------------------------


def test_quote_simple():
    assert _quote("my_table") == '"my_table"'


def test_quote_with_embedded_double_quote():
    assert _quote('my"table') == '"my""table"'
