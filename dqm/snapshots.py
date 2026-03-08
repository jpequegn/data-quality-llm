"""Snapshot store: persist TableProfile snapshots in a local SQLite database.

The database lives at ``~/.dqm/snapshots.db`` and grows every time a profile
is taken, enabling historical comparison via the diff engine.

Schema
------
.. code-block:: sql

    CREATE TABLE snapshots (
        id           INTEGER PRIMARY KEY,
        source_db    TEXT,
        table_name   TEXT,
        profiled_at  TIMESTAMP,
        profile_json TEXT   -- full TableProfile as JSON
    );
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import ColumnProfile, TableProfile

# Default location for the snapshot database
_DEFAULT_DB = Path.home() / ".dqm" / "snapshots.db"

_DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_db    TEXT        NOT NULL,
    table_name   TEXT        NOT NULL,
    profiled_at  TIMESTAMP   NOT NULL,
    profile_json TEXT        NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _open(db_path: Path = _DEFAULT_DB) -> sqlite3.Connection:
    """Open (and initialise if needed) the SQLite snapshot database."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(_DDL)
    conn.commit()
    return conn


def _profile_to_json(profile: TableProfile) -> str:
    """Serialise a TableProfile to a JSON string."""

    def _default(obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    return json.dumps(asdict(profile), default=_default)


def _json_to_profile(raw: str) -> TableProfile:
    """Deserialise a JSON string back into a TableProfile."""
    data = json.loads(raw)

    columns = [
        ColumnProfile(
            name=col["name"],
            dtype=col["dtype"],
            row_count=col["row_count"],
            null_count=col["null_count"],
            null_pct=col["null_pct"],
            unique_count=col["unique_count"],
            min_val=col["min_val"],
            max_val=col["max_val"],
            mean=col["mean"],
            p25=col["p25"],
            p75=col["p75"],
            top_values=[tuple(v) for v in col["top_values"]],
        )
        for col in data.get("columns", [])
    ]

    profiled_at_raw = data.get("profiled_at", "")
    try:
        profiled_at = datetime.fromisoformat(profiled_at_raw)
    except (ValueError, TypeError):
        profiled_at = datetime.now(tz=timezone.utc)

    return TableProfile(
        table=data["table"],
        db_path=data.get("db_path", ""),
        profiled_at=profiled_at,
        columns=columns,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def save_snapshot(profile: TableProfile, snapshots_db: Path = _DEFAULT_DB) -> int:
    """Persist *profile* and return the new snapshot id.

    Parameters
    ----------
    profile:
        The :class:`~dqm.models.TableProfile` returned by the profiler.
    snapshots_db:
        Path to the SQLite snapshot store (defaults to ``~/.dqm/snapshots.db``).

    Returns
    -------
    int
        The ``id`` of the newly inserted row.
    """
    conn = _open(snapshots_db)
    try:
        cursor = conn.execute(
            "INSERT INTO snapshots (source_db, table_name, profiled_at, profile_json) "
            "VALUES (?, ?, ?, ?)",
            (
                profile.db_path,
                profile.table,
                profile.profiled_at.isoformat(),
                _profile_to_json(profile),
            ),
        )
        conn.commit()
        return cursor.lastrowid  # type: ignore[return-value]
    finally:
        conn.close()


def list_snapshots(
    table_name: str,
    snapshots_db: Path = _DEFAULT_DB,
) -> list[dict[str, Any]]:
    """Return snapshot history for *table_name*, newest first.

    Each entry is a dict with keys: ``id``, ``source_db``, ``table_name``,
    ``profiled_at`` (ISO string).
    """
    conn = _open(snapshots_db)
    try:
        rows = conn.execute(
            "SELECT id, source_db, table_name, profiled_at "
            "FROM snapshots "
            "WHERE table_name = ? "
            "ORDER BY profiled_at DESC",
            (table_name,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_snapshot(snapshot_id: int, snapshots_db: Path = _DEFAULT_DB) -> TableProfile | None:
    """Fetch a single snapshot by *id* and deserialise it.

    Returns ``None`` if no snapshot with that id exists.
    """
    conn = _open(snapshots_db)
    try:
        row = conn.execute(
            "SELECT profile_json FROM snapshots WHERE id = ?",
            (snapshot_id,),
        ).fetchone()
        if row is None:
            return None
        return _json_to_profile(row["profile_json"])
    finally:
        conn.close()


def get_latest_two_snapshots(
    table_name: str,
    snapshots_db: Path = _DEFAULT_DB,
) -> tuple[TableProfile, TableProfile] | None:
    """Return (before, after) — the two most recent snapshots for *table_name*.

    Returns ``None`` if fewer than two snapshots exist.
    """
    conn = _open(snapshots_db)
    try:
        rows = conn.execute(
            "SELECT profile_json FROM snapshots "
            "WHERE table_name = ? "
            "ORDER BY profiled_at DESC "
            "LIMIT 2",
            (table_name,),
        ).fetchall()
        if len(rows) < 2:
            return None
        # rows[0] = newest (after), rows[1] = second newest (before)
        after = _json_to_profile(rows[0]["profile_json"])
        before = _json_to_profile(rows[1]["profile_json"])
        return before, after
    finally:
        conn.close()
