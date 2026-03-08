"""Markdown report generator for data quality results."""

from datetime import datetime, timezone
from pathlib import Path

from .models import Anomaly, TableDiff


class ReportGenerator:
    def generate(
        self,
        table_diff: TableDiff,
        anomalies: list[Anomaly],
        explanations: dict[str, str],
        db_path: str = "",
    ) -> str:
        """Generate a Markdown data quality report."""
        date_str = table_diff.date_after.strftime("%Y-%m-%d")
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        top_severity = self._top_severity(anomalies)

        lines: list[str] = []
        lines.append(f"# Data Quality Report — {table_diff.table} — {date_str}\n")
        lines.append(f"_Generated: {now}_  ")
        if db_path:
            lines.append(f"_Source DB: `{db_path}`_  ")
        lines.append(
            f"_Snapshots: `{table_diff.snapshot_before}` → `{table_diff.snapshot_after}`_\n"
        )

        # Summary
        n_cols = len(table_diff.columns)
        n_anomalies = len(anomalies)
        lines.append("## Summary\n")
        lines.append(f"- {n_cols} column{'s' if n_cols != 1 else ''} checked, "
                     f"{n_anomalies} anomal{'ies' if n_anomalies != 1 else 'y'} detected")
        if top_severity:
            lines.append(f"- Severity: **{top_severity}**")
        lines.append("")

        # Anomalies
        if anomalies:
            lines.append("## Anomalies\n")
            for anomaly in anomalies:
                sign = "+" if anomaly.delta_pp >= 0 else ""
                lines.append(f"### {anomaly.metric} spike in `{anomaly.column}` "
                              f"({sign}{anomaly.delta_pp:.0f}pp)\n")
                lines.append(f"- Before: `{anomaly.value_before:.2%}`  "
                              f"After: `{anomaly.value_after:.2%}`")
                lines.append(f"- Severity: **{anomaly.severity}**")
                explanation = explanations.get(anomaly.column, "")
                if explanation:
                    lines.append(f"\n**Explanation:** {explanation}")
                lines.append("")

        # Column profiles diff table
        lines.append("## Column Profiles (diff from "
                     f"{table_diff.date_before.strftime('%Y-%m-%d')})\n")
        lines.append("| Column | Type | Nulls (before→after) | Unique (before→after) | Change |")
        lines.append("|--------|------|----------------------|----------------------|--------|")
        for col in table_diff.columns:
            delta = col.null_pct_delta
            sign = "+" if delta >= 0 else ""
            change = f"{sign}{delta:.1%}" if delta != 0 else "—"
            lines.append(
                f"| `{col.column}` | {col.dtype} "
                f"| {col.null_pct_before:.1%} → {col.null_pct_after:.1%} "
                f"| {col.unique_before:,} → {col.unique_after:,} "
                f"| {change} |"
            )
        lines.append("")

        return "\n".join(lines)

    def _top_severity(self, anomalies: list[Anomaly]) -> str:
        order = {"ALERT": 2, "WARNING": 1, "OK": 0}
        if not anomalies:
            return ""
        return max(anomalies, key=lambda a: order.get(a.severity, -1)).severity
