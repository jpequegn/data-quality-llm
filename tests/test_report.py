"""Tests for the Markdown report generator."""

from datetime import datetime, timezone

import pytest

from dqm.models import Anomaly, ColumnDiff, TableDiff
from dqm.report import ReportGenerator


@pytest.fixture
def sample_diff():
    return TableDiff(
        table="episodes",
        snapshot_before="snap_20260306",
        snapshot_after="snap_20260307",
        date_before=datetime(2026, 3, 6, tzinfo=timezone.utc),
        date_after=datetime(2026, 3, 7, tzinfo=timezone.utc),
        columns=[
            ColumnDiff(
                column="title",
                dtype="VARCHAR",
                null_pct_before=0.01,
                null_pct_after=0.24,
                unique_before=980,
                unique_after=760,
            ),
            ColumnDiff(
                column="duration",
                dtype="DOUBLE",
                null_pct_before=0.00,
                null_pct_after=0.00,
                unique_before=500,
                unique_after=502,
            ),
        ],
    )


@pytest.fixture
def sample_anomalies():
    return [
        Anomaly(
            column="title",
            metric="null_pct",
            value_before=0.01,
            value_after=0.24,
            severity="ALERT",
            delta_pp=23.0,
        )
    ]


def test_report_contains_table_name(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "episodes" in md


def test_report_contains_date(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "2026-03-07" in md


def test_report_contains_summary_counts(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "2 columns checked" in md
    assert "1 anomaly detected" in md


def test_report_severity(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "ALERT" in md


def test_report_anomaly_column(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "title" in md
    assert "+23pp" in md


def test_report_explanation_included(sample_diff, sample_anomalies):
    explanations = {"title": "Ingestion pipeline dropped null checks after a schema change."}
    md = ReportGenerator().generate(sample_diff, sample_anomalies, explanations)
    assert "Ingestion pipeline" in md


def test_report_snapshot_ids(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert "snap_20260306" in md
    assert "snap_20260307" in md


def test_report_db_path(sample_diff):
    md = ReportGenerator().generate(sample_diff, [], {}, db_path="/data/p3.duckdb")
    assert "/data/p3.duckdb" in md


def test_report_no_anomalies(sample_diff):
    md = ReportGenerator().generate(sample_diff, [], {})
    assert "0 anomalies detected" in md
    assert "## Anomalies" not in md


def test_column_diff_table_present(sample_diff):
    md = ReportGenerator().generate(sample_diff, [], {})
    assert "| Column |" in md
    assert "`title`" in md
    assert "`duration`" in md


def test_report_output_is_string(sample_diff, sample_anomalies):
    md = ReportGenerator().generate(sample_diff, sample_anomalies, {})
    assert isinstance(md, str)
    assert len(md) > 0
