"""DuckDB connection helper."""

from pathlib import Path

import duckdb

# Candidate paths for the P³ podcast-processor database, in priority order.
_P3_CANDIDATES = [
    Path.home() / ".p3" / "p3.duckdb",
    Path.home() / "Code" / "parakeet-podcast-processor" / "data" / "p3.duckdb",
]


def resolve_default_db() -> str:
    """Return the first existing P³ DB path, or the primary candidate as a default."""
    for path in _P3_CANDIDATES:
        if path.exists():
            return str(path)
    return str(_P3_CANDIDATES[0])


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection, raising a clear error if the file is missing."""
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Pass --db <path> or set up the P³ DB at ~/.p3/p3.duckdb"
        )
    return duckdb.connect(str(path), read_only=True)


def list_tables(db_path: str) -> list[str]:
    """Return sorted table names in the database."""
    con = connect(db_path)
    rows = con.execute("SHOW TABLES").fetchall()
    con.close()
    return sorted(row[0] for row in rows)
