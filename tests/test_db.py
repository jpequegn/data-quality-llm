"""Tests for dqm.db — uses a real temporary DuckDB file."""

import pytest
import duckdb
from pathlib import Path

from dqm.db import connect, list_tables, resolve_default_db


@pytest.fixture
def tmp_db(tmp_path):
    db_file = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE episodes (id INTEGER, title VARCHAR)")
    con.execute("CREATE TABLE summaries (id INTEGER, body VARCHAR)")
    con.close()
    return str(db_file)


def test_connect_returns_connection(tmp_db):
    con = connect(tmp_db)
    assert con is not None
    con.close()


def test_connect_missing_file_raises():
    with pytest.raises(FileNotFoundError, match="Database not found"):
        connect("/nonexistent/path/db.duckdb")


def test_list_tables_returns_sorted(tmp_db):
    tables = list_tables(tmp_db)
    assert tables == ["episodes", "summaries"]


def test_list_tables_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        list_tables("/nonexistent/path/db.duckdb")


def test_resolve_default_db_returns_string():
    result = resolve_default_db()
    assert isinstance(result, str)
    assert result.endswith(".duckdb")


def test_resolve_default_db_prefers_existing(tmp_path, monkeypatch):
    """If one candidate exists, it should be returned first."""
    db_file = tmp_path / "p3.duckdb"
    db_file.touch()

    import dqm.db as db_module
    monkeypatch.setattr(db_module, "_P3_CANDIDATES", [db_file, Path("/nonexistent/p3.duckdb")])
    assert resolve_default_db() == str(db_file)


def test_resolve_default_db_fallback_to_first_candidate(monkeypatch):
    """If no candidate exists, return the first candidate path."""
    import dqm.db as db_module
    candidates = [Path("/missing/a.duckdb"), Path("/missing/b.duckdb")]
    monkeypatch.setattr(db_module, "_P3_CANDIDATES", candidates)
    assert resolve_default_db() == str(candidates[0])
