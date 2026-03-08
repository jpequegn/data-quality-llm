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
    from .models import TableDiff
    from .report import ReportGenerator

    # Placeholder diff — real data comes from snapshot store and diff engine
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
    """Profile every column in TABLE and pretty-print statistics."""
    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text
    from .profiler import profile_table

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

    # Header
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
    console.print(
        f"\n[dim]{len(prof.columns)} column(s) profiled — "
        f"{prof.columns[0].row_count:,} rows[/dim]\n" if prof.columns else ""
    )


@cli.command("check")
@click.argument("table")
@click.option(
    "--snapshots-db",
    default=None,
    help="Path to snapshot SQLite DB. Defaults to ~/.dqm/snapshots.db.",
)
@click.option(
    "--anomaly-config",
    default=None,
    help="Path to anomaly detector YAML config. Defaults to ~/.dqm/anomaly_config.yaml.",
)
@click.option(
    "--save/--no-save",
    default=True,
    show_default=True,
    help="Save the new profile snapshot after checking.",
)
@click.pass_context
def check_cmd(
    ctx: click.Context,
    table: str,
    snapshots_db: str | None,
    anomaly_config: str | None,
    save: bool,
) -> None:
    """Profile TABLE, diff against last snapshot, and flag anomalies.

    On first run (no prior snapshot) the profile is saved and you are told to
    run again after a data change.  On subsequent runs the current profile is
    compared against the previous one and any rule violations are printed.
    """
    from pathlib import Path as _Path

    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text

    from .anomaly import AnomalyDetector
    from .diff import DiffEngine
    from .models import Anomaly
    from .profiler import profile_table
    from .snapshots import (
        _DEFAULT_DB as _SNAP_DEFAULT,
        get_latest_two_snapshots,
        save_snapshot,
    )

    db_path = ctx.obj["db"]
    snap_db = _Path(snapshots_db) if snapshots_db else _SNAP_DEFAULT
    cfg_path = _Path(anomaly_config) if anomaly_config else None
    console = Console()

    # ------------------------------------------------------------------ #
    # 1. Profile current state
    # ------------------------------------------------------------------ #
    console.print(f"\n[bold cyan]dqm check[/bold cyan] — [bold]{table}[/bold]  [dim]{db_path}[/dim]\n")
    console.print("[dim]Step 1/3  Profiling table…[/dim]")

    try:
        current = profile_table(db_path, table)
    except FileNotFoundError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise SystemExit(1)
    except Exception as e:
        console.print(f"[red]Error profiling '{table}':[/red] {e}")
        raise SystemExit(1)

    row_count = current.columns[0].row_count if current.columns else 0
    console.print(
        f"  Profiled [bold]{len(current.columns)}[/bold] columns, "
        f"[bold]{row_count:,}[/bold] rows.\n"
    )

    # ------------------------------------------------------------------ #
    # 2. Save snapshot (optionally)
    # ------------------------------------------------------------------ #
    if save:
        snap_id = save_snapshot(current, snap_db)
        console.print(f"[dim]  Snapshot saved → id={snap_id} ({snap_db})[/dim]\n")

    # ------------------------------------------------------------------ #
    # 3. Retrieve previous snapshot for diffing
    # ------------------------------------------------------------------ #
    console.print("[dim]Step 2/3  Loading previous snapshot…[/dim]")
    pair = get_latest_two_snapshots(table, snap_db)

    if pair is None:
        console.print(
            "  [yellow]No prior snapshot found — this is the baseline.[/yellow]\n"
            "  Run [bold]dqm check[/bold] again after your next data load to detect anomalies.\n"
        )
        raise SystemExit(0)

    before, after = pair
    console.print(
        f"  Comparing snapshot from [bold]{before.profiled_at.strftime('%Y-%m-%d %H:%M UTC')}[/bold] "
        f"→ [bold]{after.profiled_at.strftime('%Y-%m-%d %H:%M UTC')}[/bold]\n"
    )

    # ------------------------------------------------------------------ #
    # 4. Diff + anomaly detection
    # ------------------------------------------------------------------ #
    console.print("[dim]Step 3/3  Running anomaly detection…[/dim]\n")
    diff = DiffEngine().diff(before, after)
    detector = AnomalyDetector(config_path=cfg_path)
    anomalies = detector.detect(before, after)

    # ------------------------------------------------------------------ #
    # 5. Print results
    # ------------------------------------------------------------------ #
    _print_diff_table(console, diff)
    _print_anomalies(console, anomalies)


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _print_diff_table(console: object, diff: object) -> None:  # type: ignore[type-arg]
    """Print a Rich table showing per-column diffs."""
    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text
    from .models import TableDiff

    console = console  # type: Console  # noqa: F841
    diff = diff  # type: TableDiff  # noqa: F841

    rt = RichTable(
        title="Column Diff",
        show_header=True,
        header_style="bold magenta",
        show_lines=True,
    )
    rt.add_column("Column", style="cyan", no_wrap=True)
    rt.add_column("Type", style="green")
    rt.add_column("Rows (before→after)", justify="right")
    rt.add_column("Null % (before→after)", justify="right")
    rt.add_column("Null Δ", justify="right")
    rt.add_column("Unique (before→after)", justify="right")

    for col in diff.columns:
        delta = col.null_pct_delta * 100
        sign = "+" if delta >= 0 else ""
        delta_text = Text(f"{sign}{delta:.1f}pp")
        if delta > 10:
            delta_text.stylize("bold red")
        elif delta > 2:
            delta_text.stylize("yellow")

        rc_before = getattr(col, "row_count_before", 0)
        rc_after = getattr(col, "row_count_after", 0)

        rt.add_row(
            col.column,
            col.dtype,
            f"{rc_before:,} → {rc_after:,}",
            f"{col.null_pct_before:.1%} → {col.null_pct_after:.1%}",
            delta_text,
            f"{col.unique_before:,} → {col.unique_after:,}",
        )

    console.print(rt)
    console.print()


def _print_anomalies(console: object, anomalies: list) -> None:  # type: ignore[type-arg]
    """Print a summary of detected anomalies."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table as RichTable
    from rich.text import Text

    console = console  # type: Console  # noqa: F841

    if not anomalies:
        console.print(Panel("[bold green]✓ No anomalies detected[/bold green]", expand=False))
        console.print()
        return

    n_alert = sum(1 for a in anomalies if a.severity == "ALERT")
    n_warn = sum(1 for a in anomalies if a.severity == "WARN")

    title_color = "red" if n_alert else "yellow"
    title = f"[bold {title_color}]{len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'} detected"
    if n_alert:
        title += f" — {n_alert} ALERT"
    if n_warn:
        title += f" — {n_warn} WARN"
    title += "[/bold " + title_color + "]"

    rt = RichTable(
        title=title,
        show_header=True,
        header_style="bold white",
        show_lines=True,
    )
    rt.add_column("Severity", justify="center", no_wrap=True)
    rt.add_column("Column", style="cyan", no_wrap=True)
    rt.add_column("Rule", style="magenta")
    rt.add_column("Old value", justify="right")
    rt.add_column("New value", justify="right")
    rt.add_column("Change", justify="right")

    for anomaly in anomalies:
        sev_text = Text(anomaly.severity)
        if anomaly.severity == "ALERT":
            sev_text.stylize("bold red")
        else:
            sev_text.stylize("bold yellow")

        old_v = anomaly.old_val
        new_v = anomaly.new_val

        # Format based on rule type
        rule = anomaly.rule_triggered
        if rule == "null_pct_increase":
            old_s = f"{old_v:.1f}%"
            new_s = f"{new_v:.1f}%"
            change = f"+{new_v - old_v:.1f}pp"
        elif rule in ("row_count_decrease", "row_count_spike"):
            old_s = f"{old_v:,.0f}"
            new_s = f"{new_v:,.0f}"
            pct = ((new_v - old_v) / old_v * 100) if old_v else 0
            sign = "+" if pct >= 0 else ""
            change = f"{sign}{pct:.0f}%"
        elif rule == "unique_count_drop":
            old_s = f"{old_v:,.0f}"
            new_s = f"{new_v:,.0f}"
            pct = ((new_v - old_v) / old_v * 100) if old_v else 0
            change = f"{pct:.0f}%"
        elif rule == "max_val_decrease":
            old_s = f"{old_v:g}"
            new_s = f"{new_v:g}"
            change = f"{new_v - old_v:+g}"
        else:
            old_s = str(old_v)
            new_s = str(new_v)
            change = "—"

        rt.add_row(sev_text, anomaly.column, rule, old_s, new_s, change)

    console.print(rt)
    console.print()


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
