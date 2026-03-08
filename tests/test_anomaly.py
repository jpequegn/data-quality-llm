"""Tests for dqm.anomaly — AnomalyDetector rule-based checks."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from dqm.anomaly import AnomalyDetector
from dqm.models import Anomaly, ColumnProfile, TableProfile


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 8, tzinfo=timezone.utc)


def _make_col(
    name: str = "col",
    row_count: int = 1000,
    null_pct: float = 0.01,
    unique_count: int = 500,
    min_val: float | None = 1.0,
    max_val: float | None = 100.0,
    dtype: str = "DOUBLE",
) -> ColumnProfile:
    null_count = int(null_pct * row_count)
    return ColumnProfile(
        name=name,
        dtype=dtype,
        row_count=row_count,
        null_count=null_count,
        null_pct=null_pct,
        unique_count=unique_count,
        min_val=min_val,
        max_val=max_val,
        mean=None,
        p25=None,
        p75=None,
        top_values=[],
    )


def _make_profile(
    table: str = "episodes",
    columns: list[ColumnProfile] | None = None,
) -> TableProfile:
    if columns is None:
        columns = [_make_col()]
    return TableProfile(table=table, db_path=":memory:", profiled_at=_NOW, columns=columns)


@pytest.fixture
def detector() -> AnomalyDetector:
    return AnomalyDetector()


# ---------------------------------------------------------------------------
# Rule 1: null_pct_increase — ALERT when delta > 10 pp
# ---------------------------------------------------------------------------


def test_null_pct_increase_triggers_alert(detector):
    before = _make_profile(columns=[_make_col("title", null_pct=0.01)])
    after = _make_profile(columns=[_make_col("title", null_pct=0.15)])  # +14 pp
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "null_pct_increase" and a.severity == "ALERT" for a in anomalies)


def test_null_pct_increase_column_name(detector):
    before = _make_profile(columns=[_make_col("body", null_pct=0.00)])
    after = _make_profile(columns=[_make_col("body", null_pct=0.50)])
    anomalies = detector.detect(before, after)
    alert = next(a for a in anomalies if a.rule_triggered == "null_pct_increase")
    assert alert.column == "body"


def test_null_pct_increase_values(detector):
    before = _make_profile(columns=[_make_col("score", null_pct=0.05)])
    after = _make_profile(columns=[_make_col("score", null_pct=0.30)])
    anomalies = detector.detect(before, after)
    alert = next(a for a in anomalies if a.rule_triggered == "null_pct_increase")
    assert alert.old_val == pytest.approx(5.0)
    assert alert.new_val == pytest.approx(30.0)


def test_null_pct_increase_no_alert_below_threshold(detector):
    before = _make_profile(columns=[_make_col("score", null_pct=0.01)])
    after = _make_profile(columns=[_make_col("score", null_pct=0.05)])  # +4 pp — below 10 pp
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "null_pct_increase" for a in anomalies)


def test_null_pct_decrease_does_not_trigger(detector):
    before = _make_profile(columns=[_make_col("score", null_pct=0.30)])
    after = _make_profile(columns=[_make_col("score", null_pct=0.01)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "null_pct_increase" for a in anomalies)


def test_null_pct_exactly_at_threshold_no_alert(detector):
    """Exactly 10 pp — rule uses strictly-greater-than."""
    before = _make_profile(columns=[_make_col("x", null_pct=0.00)])
    after = _make_profile(columns=[_make_col("x", null_pct=0.10)])  # exactly 10 pp
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "null_pct_increase" for a in anomalies)


# ---------------------------------------------------------------------------
# Rule 2: unique_count_drop — WARN when drop > 20 %
# ---------------------------------------------------------------------------


def test_unique_count_drop_triggers_warn(detector):
    before = _make_profile(columns=[_make_col("model", unique_count=100)])
    after = _make_profile(columns=[_make_col("model", unique_count=50)])  # -50 %
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "unique_count_drop" and a.severity == "WARN" for a in anomalies)


def test_unique_count_drop_values(detector):
    before = _make_profile(columns=[_make_col("model", unique_count=200)])
    after = _make_profile(columns=[_make_col("model", unique_count=100)])
    anomalies = detector.detect(before, after)
    warn = next(a for a in anomalies if a.rule_triggered == "unique_count_drop")
    assert warn.old_val == pytest.approx(200.0)
    assert warn.new_val == pytest.approx(100.0)


def test_unique_count_drop_no_warn_below_threshold(detector):
    before = _make_profile(columns=[_make_col("model", unique_count=100)])
    after = _make_profile(columns=[_make_col("model", unique_count=90)])  # -10 % — fine
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "unique_count_drop" for a in anomalies)


def test_unique_count_drop_no_trigger_when_zero_before(detector):
    before = _make_profile(columns=[_make_col("model", unique_count=0)])
    after = _make_profile(columns=[_make_col("model", unique_count=0)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "unique_count_drop" for a in anomalies)


def test_unique_count_increase_does_not_trigger(detector):
    before = _make_profile(columns=[_make_col("model", unique_count=100)])
    after = _make_profile(columns=[_make_col("model", unique_count=200)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "unique_count_drop" for a in anomalies)


# ---------------------------------------------------------------------------
# Rule 3: row_count_decrease — ALERT when rows decreased
# ---------------------------------------------------------------------------


def test_row_count_decrease_triggers_alert(detector):
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=800)])
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "row_count_decrease" and a.severity == "ALERT" for a in anomalies)


def test_row_count_decrease_values(detector):
    before = _make_profile(columns=[_make_col(row_count=5000)])
    after = _make_profile(columns=[_make_col(row_count=1000)])
    anomalies = detector.detect(before, after)
    a = next(x for x in anomalies if x.rule_triggered == "row_count_decrease")
    assert a.old_val == 5000.0
    assert a.new_val == 1000.0
    assert a.column == "__table__"


def test_row_count_stable_does_not_trigger(detector):
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=1000)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "row_count_decrease" for a in anomalies)


def test_row_count_increase_does_not_trigger_decrease_rule(detector):
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=1100)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "row_count_decrease" for a in anomalies)


# ---------------------------------------------------------------------------
# Rule 4: row_count_spike — WARN when rows increased > 500 %
# ---------------------------------------------------------------------------


def test_row_count_spike_triggers_warn(detector):
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=7000)])  # +600 %
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "row_count_spike" and a.severity == "WARN" for a in anomalies)


def test_row_count_spike_exactly_at_threshold_no_warn(detector):
    """Exactly 6× (500 % increase) sits right at the boundary (not over)."""
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=6000)])  # exactly 6× = 500 % more
    anomalies = detector.detect(before, after)
    # At exactly 6× (1 + 5.00) there is no spike — must be strictly greater
    assert not any(a.rule_triggered == "row_count_spike" for a in anomalies)


def test_row_count_modest_increase_no_warn(detector):
    before = _make_profile(columns=[_make_col(row_count=1000)])
    after = _make_profile(columns=[_make_col(row_count=1500)])  # +50 %
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "row_count_spike" for a in anomalies)


# ---------------------------------------------------------------------------
# Rule 5: max_val_decrease — ALERT for monotonic columns
# ---------------------------------------------------------------------------


def test_max_val_decrease_triggers_alert():
    detector = AnomalyDetector()
    # Manually set the monotonic columns list
    detector._monotonic_columns = ["id"]
    before = _make_profile(columns=[_make_col("id", max_val=1000.0)])
    after = _make_profile(columns=[_make_col("id", max_val=900.0)])
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "max_val_decrease" and a.severity == "ALERT" for a in anomalies)


def test_max_val_decrease_values():
    detector = AnomalyDetector()
    detector._monotonic_columns = ["id"]
    before = _make_profile(columns=[_make_col("id", max_val=500.0)])
    after = _make_profile(columns=[_make_col("id", max_val=100.0)])
    anomalies = detector.detect(before, after)
    a = next(x for x in anomalies if x.rule_triggered == "max_val_decrease")
    assert a.old_val == pytest.approx(500.0)
    assert a.new_val == pytest.approx(100.0)


def test_max_val_increase_does_not_trigger():
    detector = AnomalyDetector()
    detector._monotonic_columns = ["id"]
    before = _make_profile(columns=[_make_col("id", max_val=100.0)])
    after = _make_profile(columns=[_make_col("id", max_val=200.0)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "max_val_decrease" for a in anomalies)


def test_max_val_not_checked_for_non_monotonic_column(detector):
    before = _make_profile(columns=[_make_col("score", max_val=100.0)])
    after = _make_profile(columns=[_make_col("score", max_val=50.0)])
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "max_val_decrease" for a in anomalies)


def test_max_val_decrease_non_numeric_skipped():
    detector = AnomalyDetector()
    detector._monotonic_columns = ["name"]
    before = _make_profile(columns=[_make_col("name", max_val="zoo", min_val="aardvark", dtype="VARCHAR")])
    after = _make_profile(columns=[_make_col("name", max_val="alpha", min_val="aardvark", dtype="VARCHAR")])
    # Strings are non-numeric → should not raise, just skip
    anomalies = detector.detect(before, after)
    # may or may not fire depending on float() cast — the point is no exception
    assert isinstance(anomalies, list)


# ---------------------------------------------------------------------------
# Anomaly model / backward-compat properties
# ---------------------------------------------------------------------------


def test_anomaly_metric_alias():
    a = Anomaly(column="x", rule_triggered="null_pct_increase", old_val=5.0, new_val=20.0, severity="ALERT")
    assert a.metric == "null_pct_increase"


def test_anomaly_value_aliases():
    a = Anomaly(column="x", rule_triggered="row_count_decrease", old_val=1000.0, new_val=500.0, severity="ALERT")
    assert a.value_before == 1000.0
    assert a.value_after == 500.0


def test_anomaly_delta_pp():
    a = Anomaly(column="x", rule_triggered="null_pct_increase", old_val=5.0, new_val=20.0, severity="ALERT")
    assert a.delta_pp == pytest.approx(15.0)


# ---------------------------------------------------------------------------
# Configurable thresholds
# ---------------------------------------------------------------------------


def test_custom_threshold_via_config(tmp_path):
    """Lower the null_pct_increase threshold to 5 pp via a YAML config file."""
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("thresholds:\n  null_pct_increase_pp: 5.0\n")
    detector = AnomalyDetector(config_path=cfg)

    before = _make_profile(columns=[_make_col("x", null_pct=0.00)])
    after = _make_profile(columns=[_make_col("x", null_pct=0.07)])  # +7 pp > 5 pp
    anomalies = detector.detect(before, after)
    assert any(a.rule_triggered == "null_pct_increase" for a in anomalies)


def test_custom_threshold_no_alert_with_strict_threshold(tmp_path):
    """Raise the null_pct_increase threshold to 20 pp — 12 pp should not fire."""
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("thresholds:\n  null_pct_increase_pp: 20.0\n")
    detector = AnomalyDetector(config_path=cfg)

    before = _make_profile(columns=[_make_col("x", null_pct=0.00)])
    after = _make_profile(columns=[_make_col("x", null_pct=0.12)])  # +12 pp < 20 pp
    anomalies = detector.detect(before, after)
    assert not any(a.rule_triggered == "null_pct_increase" for a in anomalies)


def test_monotonic_columns_from_yaml_config(tmp_path):
    """Pass monotonic_columns list via YAML config."""
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("thresholds:\n  monotonic_columns:\n    - episode_id\n")
    detector = AnomalyDetector(config_path=cfg)
    assert "episode_id" in detector._monotonic_columns


# ---------------------------------------------------------------------------
# Multiple anomalies in one detect() call
# ---------------------------------------------------------------------------


def test_multiple_anomalies_same_table():
    detector = AnomalyDetector()
    before = _make_profile(
        columns=[
            _make_col("title", null_pct=0.01, unique_count=900, row_count=1000),
            _make_col("score", null_pct=0.00, unique_count=500, row_count=1000),
        ]
    )
    after = _make_profile(
        columns=[
            _make_col("title", null_pct=0.50, unique_count=50, row_count=500),  # both null + unique fire
            _make_col("score", null_pct=0.00, unique_count=500, row_count=500),
        ]
    )
    anomalies = detector.detect(before, after)
    rules = [a.rule_triggered for a in anomalies]
    assert "null_pct_increase" in rules
    assert "unique_count_drop" in rules
    assert "row_count_decrease" in rules


def test_no_anomalies_clean_data(detector):
    before = _make_profile(columns=[_make_col(row_count=1000, null_pct=0.01, unique_count=500)])
    after = _make_profile(columns=[_make_col(row_count=1010, null_pct=0.01, unique_count=505)])
    anomalies = detector.detect(before, after)
    assert anomalies == []
