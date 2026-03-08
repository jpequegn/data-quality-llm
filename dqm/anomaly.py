"""Rule-based anomaly detector.

Compares two :class:`~dqm.models.TableProfile` snapshots using a set of
configurable threshold rules and returns a list of :class:`~dqm.models.Anomaly`
objects.

Default rules
-------------
1. **null_pct_increase** — null % rose by > 10 pp                  → ALERT
2. **unique_count_drop** — unique count fell by > 20 %              → WARN
3. **row_count_decrease** — row count decreased at all              → ALERT
4. **row_count_spike**   — row count increased by > 500 %           → WARN
5. **max_val_decrease**  — max_val fell for a monotonic column      → ALERT

Configuration
-------------
Thresholds can be customised via a YAML file.  The detector looks for
``~/.dqm/anomaly_config.yaml`` first; if that is absent it falls back to the
built-in ``anomaly_config.yaml`` bundled with the package.  You can also pass
``config_path`` explicitly.

Usage
-----
>>> from dqm.anomaly import AnomalyDetector
>>> detector = AnomalyDetector()
>>> anomalies = detector.detect(profile_before, profile_after)
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .models import Anomaly, ColumnProfile, TableProfile

# ---------------------------------------------------------------------------
# YAML loading (stdlib only — no extra dep required)
# ---------------------------------------------------------------------------

try:
    import yaml as _yaml

    def _load_yaml(path: Path) -> dict[str, Any]:
        with path.open() as fh:
            return _yaml.safe_load(fh) or {}

except ImportError:
    # PyYAML not installed — fall back to a minimal hand-rolled parser that
    # covers only the simple key: value pairs used by our config.
    import re as _re

    def _load_yaml(path: Path) -> dict[str, Any]:  # type: ignore[misc]
        """Very minimal YAML subset parser (no PyYAML dependency)."""
        result: dict[str, Any] = {"thresholds": {}, "severity": {}}
        section: dict[str, Any] = {}

        with path.open() as fh:
            for line in fh:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue

                # Top-level section header (no leading spaces, ends with colon)
                if not line.startswith(" ") and stripped.endswith(":"):
                    key = stripped[:-1]
                    section = {}
                    result[key] = section
                    continue

                # Key: value pair (with optional inline comment)
                m = _re.match(r"\s+([\w_]+):\s*(.+)", line)
                if m:
                    k, v = m.group(1), m.group(2).split("#")[0].strip()
                    if v in ("true", "True"):
                        section[k] = True
                    elif v in ("false", "False"):
                        section[k] = False
                    elif v.startswith("[") and v.endswith("]"):
                        section[k] = []
                    else:
                        try:
                            section[k] = float(v)
                        except ValueError:
                            section[k] = v

        return result


# ---------------------------------------------------------------------------
# Default config path (bundled alongside this file)
# ---------------------------------------------------------------------------

_BUNDLED_CONFIG = Path(__file__).parent / "anomaly_config.yaml"
_USER_CONFIG = Path.home() / ".dqm" / "anomaly_config.yaml"


def _load_config(config_path: Path | None) -> dict[str, Any]:
    """Load thresholds from YAML, merging user overrides over bundled defaults."""
    # 1. Start with bundled defaults
    cfg: dict[str, Any] = {}
    if _BUNDLED_CONFIG.exists():
        cfg = _load_yaml(_BUNDLED_CONFIG)

    # 2. Overlay user config if present
    user = config_path or _USER_CONFIG
    if user.exists():
        user_cfg = _load_yaml(user)
        for section_key, section_val in user_cfg.items():
            if isinstance(section_val, dict):
                cfg.setdefault(section_key, {}).update(section_val)
            else:
                cfg[section_key] = section_val

    return cfg


# ---------------------------------------------------------------------------
# AnomalyDetector
# ---------------------------------------------------------------------------

class AnomalyDetector:
    """Detect data quality anomalies using configurable threshold rules.

    Parameters
    ----------
    config_path:
        Path to a YAML config file.  Defaults to ``~/.dqm/anomaly_config.yaml``
        (falls back to the bundled defaults if the file does not exist).
    """

    def __init__(self, config_path: Path | str | None = None) -> None:
        cp = Path(config_path) if config_path else None
        cfg = _load_config(cp)
        t = cfg.get("thresholds", {})
        s = cfg.get("severity", {})

        self._null_pct_increase_pp: float = float(t.get("null_pct_increase_pp", 10.0))
        self._unique_count_drop_pct: float = float(t.get("unique_count_drop_pct", 0.20))
        self._row_count_decrease: bool = bool(t.get("row_count_decrease", True))
        self._row_count_increase_pct: float = float(t.get("row_count_increase_pct", 5.00))
        self._monotonic_columns: list[str] = list(t.get("monotonic_columns") or [])

        self._label_alert: str = str(s.get("alert", "ALERT"))
        self._label_warn: str = str(s.get("warn", "WARN"))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(
        self,
        before: TableProfile,
        after: TableProfile,
    ) -> list[Anomaly]:
        """Run all rules and return every triggered anomaly.

        Parameters
        ----------
        before:
            The older / baseline snapshot.
        after:
            The newer / current snapshot.

        Returns
        -------
        list[Anomaly]
            One entry per triggered rule per column (or table-level metric).
        """
        anomalies: list[Anomaly] = []

        cols_before = {c.name: c for c in before.columns}
        cols_after = {c.name: c for c in after.columns}

        # --- Table-level row count rules (use row_count from first column) ---
        rc_before = before.columns[0].row_count if before.columns else 0
        rc_after = after.columns[0].row_count if after.columns else 0
        anomalies.extend(self._check_row_count(rc_before, rc_after))

        # --- Column-level rules ---
        for col_name, col_after in cols_after.items():
            col_before = cols_before.get(col_name)
            if col_before is None:
                continue  # new column — skip for now

            anomalies.extend(self._check_null_pct(col_before, col_after))
            anomalies.extend(self._check_unique_count(col_before, col_after))

            if col_name in self._monotonic_columns:
                anomalies.extend(self._check_max_val_decrease(col_before, col_after))

        return anomalies

    # ------------------------------------------------------------------
    # Individual rule checks
    # ------------------------------------------------------------------

    def _check_null_pct(
        self, before: ColumnProfile, after: ColumnProfile
    ) -> list[Anomaly]:
        """Rule 1: null_pct increased by > threshold pp → ALERT."""
        delta_pp = (after.null_pct - before.null_pct) * 100.0
        if delta_pp > self._null_pct_increase_pp:
            return [
                Anomaly(
                    column=after.name,
                    rule_triggered="null_pct_increase",
                    old_val=before.null_pct * 100.0,
                    new_val=after.null_pct * 100.0,
                    severity=self._label_alert,
                )
            ]
        return []

    def _check_unique_count(
        self, before: ColumnProfile, after: ColumnProfile
    ) -> list[Anomaly]:
        """Rule 2: unique_count dropped by > threshold % → WARN."""
        if before.unique_count == 0:
            return []
        drop_pct = (before.unique_count - after.unique_count) / before.unique_count
        if drop_pct > self._unique_count_drop_pct:
            return [
                Anomaly(
                    column=after.name,
                    rule_triggered="unique_count_drop",
                    old_val=float(before.unique_count),
                    new_val=float(after.unique_count),
                    severity=self._label_warn,
                )
            ]
        return []

    def _check_row_count(self, rc_before: int, rc_after: int) -> list[Anomaly]:
        """Rules 3 & 4: row count decreased → ALERT; increased > threshold → WARN."""
        results: list[Anomaly] = []

        if self._row_count_decrease and rc_after < rc_before:
            results.append(
                Anomaly(
                    column="__table__",
                    rule_triggered="row_count_decrease",
                    old_val=float(rc_before),
                    new_val=float(rc_after),
                    severity=self._label_alert,
                )
            )

        if rc_before > 0:
            increase_factor = rc_after / rc_before
            if increase_factor > 1 + self._row_count_increase_pct:
                results.append(
                    Anomaly(
                        column="__table__",
                        rule_triggered="row_count_spike",
                        old_val=float(rc_before),
                        new_val=float(rc_after),
                        severity=self._label_warn,
                    )
                )

        return results

    def _check_max_val_decrease(
        self, before: ColumnProfile, after: ColumnProfile
    ) -> list[Anomaly]:
        """Rule 5: max_val decreased for a monotonically increasing column → ALERT."""
        try:
            max_before = float(before.max_val)  # type: ignore[arg-type]
            max_after = float(after.max_val)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return []  # non-numeric / None — skip

        if not math.isnan(max_before) and not math.isnan(max_after):
            if max_after < max_before:
                return [
                    Anomaly(
                        column=after.name,
                        rule_triggered="max_val_decrease",
                        old_val=max_before,
                        new_val=max_after,
                        severity=self._label_alert,
                    )
                ]
        return []
