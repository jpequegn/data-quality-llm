"""CLI entrypoint for dqm."""

import click
import sys
from pathlib import Path

DEFAULT_DB = Path.home() / ".p3" / "p3.duckdb"


@click.group()
@click.option(
    "--db",
    default=str(DEFAULT_DB),
    show_default=True,
    help="Path to DuckDB database file.",
)
@click.pass_context
def cli(ctx: click.Context, db: str) -> None:
    """Data quality checks for DuckDB databases, powered by Claude."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db


@cli.command()
@click.argument("table")
@click.option("--output", "-o", default=None, help="Write report to this file (default: stdout).")
@click.pass_context
def report(ctx: click.Context, table: str, output: str | None) -> None:
    """Generate a Markdown data quality report for TABLE."""
    from datetime import datetime, timezone
    from .models import Anomaly, ColumnDiff, TableDiff
    from .report import ReportGenerator

    # Placeholder diff — real data comes from snapshot store (issue #6) and diff engine (issue #7)
    now = datetime.now(tz=timezone.utc)
    diff = TableDiff(
        table=table,
        snapshot_before="<no snapshot>",
        snapshot_after="<no snapshot>",
        date_before=now,
        date_after=now,
        columns=[],
    )
    markdown = ReportGenerator().generate(
        table_diff=diff,
        anomalies=[],
        explanations={},
        db_path=ctx.obj["db"],
    )

    if output:
        Path(output).write_text(markdown)
        click.echo(f"Report written to {output}")
    else:
        click.echo(markdown, nl=False)


def main() -> None:
    cli()
