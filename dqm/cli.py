"""CLI entrypoint for dqm."""

import click
from pathlib import Path

from .db import resolve_default_db


@click.group()
@click.option(
    "--db",
    default=None,
    show_default=False,
    help="Path to DuckDB database file. Defaults to ~/.p3/p3.duckdb.",
)
@click.pass_context
def cli(ctx: click.Context, db: str | None) -> None:
    """Data quality checks for DuckDB databases, powered by Claude."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db or resolve_default_db()


@cli.command("tables")
@click.pass_context
def tables_cmd(ctx: click.Context) -> None:
    """List all tables in the connected database."""
    from rich.console import Console
    from rich.table import Table
    from .db import list_tables

    db_path = ctx.obj["db"]
    console = Console()

    try:
        names = list_tables(db_path)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)

    table = Table(title=f"Tables in {db_path}", show_header=True)
    table.add_column("Table", style="cyan")
    for name in names:
        table.add_row(name)
    console.print(table)


@cli.command()
@click.argument("table")
@click.option("--output", "-o", default=None, help="Write report to this file (default: stdout).")
@click.pass_context
def report(ctx: click.Context, table: str, output: str | None) -> None:
    """Generate a Markdown data quality report for TABLE."""
    from datetime import datetime, timezone
    from .models import ColumnDiff, TableDiff
    from .report import ReportGenerator

    # Placeholder diff — real data comes from snapshot store (#6) and diff engine (#7)
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
