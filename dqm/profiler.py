"""Column profiler: nulls, cardinality, min/max, distribution for any DuckDB table."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import duckdb

from .db import connect
from .models import ColumnProfile, TableProfile

# DuckDB type families that support numeric aggregates (mean, percentiles)
_NUMERIC_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "FLOAT",
        "DOUBLE",
        "DECIMAL",
        "REAL",
    }
)

_TOP_N = 5


def _is_numeric(dtype: str) -> bool:
    """Return True if dtype belongs to the numeric family."""
    return dtype.upper().split("(")[0].strip() in _NUMERIC_TYPES


class Profiler:
    """Produce a statistical profile of every column in a DuckDB table."""

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def profile_table(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> TableProfile:
        """Profile every column in *table_name* and return a :class:`TableProfile`.

        Parameters
        ----------
        conn:
            An open DuckDB connection (read-only is fine).
        table_name:
            Name of the table to profile.

        Returns
        -------
        TableProfile
            Statistical profile with one :class:`ColumnProfile` per column.
        """
        # 1. Get column metadata from SUMMARIZE
        summarize_rows = self._summarize(conn, table_name)

        # 2. Get total row count once (cheaper than per-column)
        row_count = self._row_count(conn, table_name)

        columns: list[ColumnProfile] = []
        for row in summarize_rows:
            col_name, dtype = row["column_name"], row["column_type"]
            null_count = self._null_count(conn, table_name, col_name, row_count)
            null_pct = (null_count / row_count) if row_count > 0 else 0.0
            unique_count = self._unique_count(conn, table_name, col_name)
            min_val, max_val = self._min_max(conn, table_name, col_name)
            mean, p25, p75 = self._numeric_stats(conn, table_name, col_name, dtype)
            top_values = self._top_values(conn, table_name, col_name)

            columns.append(
                ColumnProfile(
                    name=col_name,
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
                    top_values=top_values,
                )
            )

        return TableProfile(
            table=table_name,
            db_path="",  # filled in by the caller if desired
            profiled_at=datetime.now(tz=timezone.utc),
            columns=columns,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _summarize(
        self, conn: duckdb.DuckDBPyConnection, table_name: str
    ) -> list[dict[str, Any]]:
        """Run DuckDB's SUMMARIZE and return rows as dicts."""
        safe_name = _quote(table_name)
        rows = conn.execute(f"SUMMARIZE {safe_name}").fetchall()
        desc = conn.execute(f"SUMMARIZE {safe_name}").description
        col_names = [d[0] for d in desc]
        return [dict(zip(col_names, row)) for row in rows]

    def _row_count(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
        safe_name = _quote(table_name)
        result = conn.execute(f"SELECT COUNT(*) FROM {safe_name}").fetchone()
        return int(result[0]) if result else 0

    def _null_count(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        col_name: str,
        row_count: int,
    ) -> int:
        safe_table = _quote(table_name)
        safe_col = _quote(col_name)
        result = conn.execute(
            f"SELECT COUNT(*) FROM {safe_table} WHERE {safe_col} IS NULL"
        ).fetchone()
        return int(result[0]) if result else 0

    def _unique_count(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, col_name: str
    ) -> int:
        safe_table = _quote(table_name)
        safe_col = _quote(col_name)
        result = conn.execute(
            f"SELECT COUNT(DISTINCT {safe_col}) FROM {safe_table}"
        ).fetchone()
        return int(result[0]) if result else 0

    def _min_max(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, col_name: str
    ) -> tuple[Any, Any]:
        safe_table = _quote(table_name)
        safe_col = _quote(col_name)
        result = conn.execute(
            f"SELECT MIN({safe_col}), MAX({safe_col}) FROM {safe_table}"
        ).fetchone()
        if result:
            return result[0], result[1]
        return None, None

    def _numeric_stats(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        col_name: str,
        dtype: str,
    ) -> tuple[float | None, float | None, float | None]:
        """Return (mean, p25, p75) for numeric columns; (None, None, None) otherwise."""
        if not _is_numeric(dtype):
            return None, None, None
        safe_table = _quote(table_name)
        safe_col = _quote(col_name)
        try:
            result = conn.execute(
                f"""
                SELECT
                    AVG({safe_col}::DOUBLE),
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {safe_col}::DOUBLE),
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {safe_col}::DOUBLE)
                FROM {safe_table}
                """
            ).fetchone()
            if result:
                mean = float(result[0]) if result[0] is not None else None
                p25 = float(result[1]) if result[1] is not None else None
                p75 = float(result[2]) if result[2] is not None else None
                return mean, p25, p75
        except Exception:
            pass
        return None, None, None

    def _top_values(
        self, conn: duckdb.DuckDBPyConnection, table_name: str, col_name: str
    ) -> list[tuple]:
        """Return the top-N most frequent (value, count) pairs, excluding NULLs."""
        safe_table = _quote(table_name)
        safe_col = _quote(col_name)
        try:
            rows = conn.execute(
                f"""
                SELECT {safe_col}, COUNT(*) AS cnt
                FROM {safe_table}
                WHERE {safe_col} IS NOT NULL
                GROUP BY {safe_col}
                ORDER BY cnt DESC
                LIMIT {_TOP_N}
                """
            ).fetchall()
            return [(row[0], int(row[1])) for row in rows]
        except Exception:
            return []


def _quote(name: str) -> str:
    """Double-quote an identifier, escaping any embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# Convenience function for the CLI
# ---------------------------------------------------------------------------


def profile_table(db_path: str, table_name: str) -> TableProfile:
    """Open *db_path*, profile *table_name*, close connection, return profile."""
    conn = connect(db_path)
    try:
        profiler = Profiler()
        profile = profiler.profile_table(conn, table_name)
        profile.db_path = db_path
        return profile
    finally:
        conn.close()
