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


@cli.command("profile")
@click.argument("table")
@click.pass_context
def profile_cmd(ctx: click.Context, table: str) -> None:
    """Profile every column in TABLE, pretty-print statistics, and save a snapshot."""
    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text
    from .profiler import profile_table
    from .snapshots import save_snapshot

    db_path = ctx.obj["db"]
    console = Console()

    try:
        prof = profile_table(db_path, table)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)
    except Exception as e:
        console.print(f"[red]Error profiling table '{table}':[/red] {e}")
        raise SystemExit(1)

    # ── Pretty-print ──────────────────────────────────────────────────
    console.print(
        f"\n[bold cyan]Column profile[/bold cyan] — "
        f"[bold]{prof.table}[/bold]  "
        f"[dim]{db_path}[/dim]  "
        f"[dim]{prof.profiled_at.strftime('%Y-%m-%d %H:%M UTC')}[/dim]\n"
    )

    rt = RichTable(show_header=True, header_style="bold magenta", show_lines=True)
    rt.add_column("Column", style="cyan", no_wrap=True)
    rt.add_column("Type", style="green")
    rt.add_column("Rows", justify="right")
    rt.add_column("Nulls", justify="right")
    rt.add_column("Null %", justify="right")
    rt.add_column("Unique", justify="right")
    rt.add_column("Min", overflow="fold")
    rt.add_column("Max", overflow="fold")
    rt.add_column("Mean", justify="right")
    rt.add_column("P25", justify="right")
    rt.add_column("P75", justify="right")
    rt.add_column("Top values (value: count)", overflow="fold")

    for col in prof.columns:
        # Colour null% red when it's significant
        null_pct_str = f"{col.null_pct:.1%}"
        null_text = Text(null_pct_str)
        if col.null_pct >= 0.20:
            null_text.stylize("bold red")
        elif col.null_pct >= 0.05:
            null_text.stylize("yellow")

        top_str = "  ".join(f"{v}: {c:,}" for v, c in col.top_values) if col.top_values else "—"

        rt.add_row(
            col.name,
            col.dtype,
            f"{col.row_count:,}",
            f"{col.null_count:,}",
            null_text,
            f"{col.unique_count:,}",
            _fmt(col.min_val),
            _fmt(col.max_val),
            _fmt_float(col.mean),
            _fmt_float(col.p25),
            _fmt_float(col.p75),
            top_str,
        )

    console.print(rt)
    if prof.columns:
        console.print(
            f"\n[dim]{len(prof.columns)} column(s) profiled — "
            f"{prof.columns[0].row_count:,} rows[/dim]\n"
        )

    # ── Auto-save snapshot ────────────────────────────────────────────
    try:
        snap_id = save_snapshot(prof)
        console.print(
            f"[green]✓[/green] Snapshot saved "
            f"[dim](id={snap_id})[/dim] — "
            f"view history with [bold]dqm snapshots list {table}[/bold]\n"
        )
    except Exception as e:
        console.print(f"[yellow]Warning:[/yellow] Could not save snapshot: {e}")


# ---------------------------------------------------------------------------
# snapshots command group
# ---------------------------------------------------------------------------

@cli.group("snapshots")
def snapshots_group() -> None:
    """Manage profile snapshots stored in ~/.dqm/snapshots.db."""


@snapshots_group.command("list")
@click.argument("table")
def snapshots_list(table: str) -> None:
    """Show snapshot history for TABLE (newest first)."""
    from rich.console import Console
    from rich.table import Table as RichTable
    from .snapshots import list_snapshots

    console = Console()
    rows = list_snapshots(table)

    if not rows:
        console.print(
            f"[yellow]No snapshots found for table '{table}'.[/yellow]\n"
            f"Run [bold]dqm profile {table}[/bold] to create one."
        )
        return

    rt = RichTable(
        title=f"Snapshot history — {table}",
        show_header=True,
        header_style="bold magenta",
    )
    rt.add_column("ID", justify="right", style="cyan")
    rt.add_column("Source DB", style="dim")
    rt.add_column("Table", style="green")
    rt.add_column("Profiled at", style="yellow")

    for row in rows:
        rt.add_row(
            str(row["id"]),
            row["source_db"],
            row["table_name"],
            row["profiled_at"],
        )

    console.print(rt)
    console.print(
        f"\n[dim]{len(rows)} snapshot(s) — "
        f"use [bold]dqm snapshots get <id>[/bold] to inspect one[/dim]\n"
    )


@snapshots_group.command("get")
@click.argument("snapshot_id", type=int)
def snapshots_get(snapshot_id: int) -> None:
    """Print the full profile stored in snapshot SNAPSHOT_ID."""
    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text
    from .snapshots import get_snapshot

    console = Console()
    prof = get_snapshot(snapshot_id)

    if prof is None:
        console.print(f"[red]Error:[/red] No snapshot found with id={snapshot_id}.")
        raise SystemExit(1)

    console.print(
        f"\n[bold cyan]Snapshot #{snapshot_id}[/bold cyan] — "
        f"[bold]{prof.table}[/bold]  "
        f"[dim]{prof.db_path}[/dim]  "
        f"[dim]{prof.profiled_at.strftime('%Y-%m-%d %H:%M UTC')}[/dim]\n"
    )

    rt = RichTable(show_header=True, header_style="bold magenta", show_lines=True)
    rt.add_column("Column", style="cyan", no_wrap=True)
    rt.add_column("Type", style="green")
    rt.add_column("Rows", justify="right")
    rt.add_column("Nulls", justify="right")
    rt.add_column("Null %", justify="right")
    rt.add_column("Unique", justify="right")
    rt.add_column("Min", overflow="fold")
    rt.add_column("Max", overflow="fold")
    rt.add_column("Mean", justify="right")
    rt.add_column("P25", justify="right")
    rt.add_column("P75", justify="right")
    rt.add_column("Top values (value: count)", overflow="fold")

    for col in prof.columns:
        null_pct_str = f"{col.null_pct:.1%}"
        null_text = Text(null_pct_str)
        if col.null_pct >= 0.20:
            null_text.stylize("bold red")
        elif col.null_pct >= 0.05:
            null_text.stylize("yellow")

        top_str = "  ".join(f"{v}: {c:,}" for v, c in col.top_values) if col.top_values else "—"

        rt.add_row(
            col.name,
            col.dtype,
            f"{col.row_count:,}",
            f"{col.null_count:,}",
            null_text,
            f"{col.unique_count:,}",
            _fmt(col.min_val),
            _fmt(col.max_val),
            _fmt_float(col.mean),
            _fmt_float(col.p25),
            _fmt_float(col.p75),
            top_str,
        )

    console.print(rt)
    if prof.columns:
        console.print(
            f"\n[dim]{len(prof.columns)} column(s) — "
            f"{prof.columns[0].row_count:,} rows[/dim]\n"
        )


# ---------------------------------------------------------------------------
# Formatting helpers (shared by profile_cmd and snapshots_get)
# ---------------------------------------------------------------------------

def _fmt(val: object) -> str:
    if val is None:
        return "[dim]—[/dim]"
    s = str(val)
    return s[:40] + "…" if len(s) > 40 else s


def _fmt_float(val: float | None) -> str:
    if val is None:
        return "[dim]—[/dim]"
    return f"{val:,.2f}"


def main() -> None:
    cli()
