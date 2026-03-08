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


# ---------------------------------------------------------------------------
# dqm tables
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# dqm report
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# dqm profile
# ---------------------------------------------------------------------------

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
# dqm snapshots command group
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
# dqm diff
# ---------------------------------------------------------------------------

@cli.command("diff")
@click.argument("table")
@click.option(
    "--since",
    default=None,
    metavar="DATE",
    help=(
        "Compare the latest snapshot against the snapshot nearest to DATE "
        "(format: YYYY-MM-DD).  Defaults to the previous snapshot."
    ),
)
@click.pass_context
def diff_cmd(ctx: click.Context, table: str, since: str | None) -> None:
    """Show what changed between two snapshots of TABLE.

    By default, diffs the latest snapshot against the one immediately before it.

    \b
    Examples
    --------
    dqm diff episodes
    dqm diff episodes --since 2026-03-01
    """
    from datetime import datetime, timezone
    from rich.console import Console
    from rich.table import Table as RichTable
    from rich.text import Text

    from .snapshots import list_snapshots, get_snapshot
    from .diff import DiffEngine

    console = Console()

    # ── Fetch snapshot history ────────────────────────────────────────
    rows = list_snapshots(table)  # newest first

    if len(rows) < 2:
        console.print(
            f"[yellow]Not enough snapshots for '{table}'.[/yellow]\n"
            f"At least 2 are needed. Run [bold]dqm profile {table}[/bold] again."
        )
        raise SystemExit(1)

    # ── Resolve "after" snapshot: always the latest ───────────────────
    after_row = rows[0]

    # ── Resolve "before" snapshot ─────────────────────────────────────
    if since:
        try:
            since_dt = datetime.strptime(since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            console.print(
                f"[red]Error:[/red] --since must be in YYYY-MM-DD format, got: {since!r}"
            )
            raise SystemExit(1)

        # Find the snapshot whose profiled_at is closest to (and ≤) since_dt
        candidates = [
            r for r in rows[1:]   # skip the "after" snapshot
            if _parse_ts(r["profiled_at"]) <= since_dt
        ]
        if not candidates:
            # Fall back: pick the oldest available snapshot before "after"
            candidates = rows[1:]
        before_row = candidates[0]  # rows are newest-first; first is closest to since_dt
    else:
        before_row = rows[1]

    # ── Load full profiles ────────────────────────────────────────────
    snap_before = get_snapshot(before_row["id"])
    snap_after = get_snapshot(after_row["id"])

    if snap_before is None or snap_after is None:
        console.print("[red]Error:[/red] Could not load snapshots from store.")
        raise SystemExit(1)

    # ── Run diff engine ───────────────────────────────────────────────
    engine = DiffEngine()
    table_diff = engine.diff(snap_before, snap_after)

    # ── Render results ────────────────────────────────────────────────
    date_fmt = "%Y-%m-%d %H:%M UTC"
    console.print(
        f"\n[bold cyan]Diff[/bold cyan] — "
        f"[bold]{table_diff.table}[/bold]\n"
        f"  [dim]before:[/dim] snapshot #{before_row['id']}  "
        f"[dim]({table_diff.date_before.strftime(date_fmt)})[/dim]\n"
        f"  [dim]after: [/dim] snapshot #{after_row['id']}  "
        f"[dim]({table_diff.date_after.strftime(date_fmt)})[/dim]\n"
    )

    rt = RichTable(show_header=True, header_style="bold magenta", show_lines=True)
    rt.add_column("Column", style="cyan", no_wrap=True)
    rt.add_column("Type", style="green")
    rt.add_column("Null % (before→after)", justify="right")
    rt.add_column("Δ Null pp", justify="right")
    rt.add_column("Unique (before→after)", justify="right")
    rt.add_column("Δ Unique", justify="right")
    rt.add_column("Min drift", overflow="fold")
    rt.add_column("Max drift", overflow="fold")
    rt.add_column("New top values", overflow="fold")
    rt.add_column("Severity", justify="center")

    for col in table_diff.columns:
        delta_null = col.null_pct_delta
        sign_null = "+" if delta_null >= 0 else ""
        delta_null_str = f"{sign_null}{delta_null:.1%}"

        delta_uniq = col.unique_delta
        sign_uniq = "+" if delta_uniq >= 0 else ""
        delta_uniq_str = f"{sign_uniq}{delta_uniq:,}"

        min_drift = _fmt_drift(col.min_before, col.min_after)
        max_drift = _fmt_drift(col.max_before, col.max_after)

        new_top = ", ".join(str(v) for v in col.new_top_values) if col.new_top_values else "—"

        # Severity styling
        sev_text = Text(col.severity.upper())
        if col.severity == "alert":
            sev_text.stylize("bold red")
        elif col.severity == "warn":
            sev_text.stylize("bold yellow")
        else:
            sev_text.stylize("green")

        # Highlight null delta in red/yellow when notable
        null_delta_text = Text(delta_null_str)
        if col.severity == "alert":
            null_delta_text.stylize("bold red")
        elif col.severity == "warn":
            null_delta_text.stylize("bold yellow")

        rt.add_row(
            col.column,
            col.dtype,
            f"{col.null_pct_before:.1%} → {col.null_pct_after:.1%}",
            null_delta_text,
            f"{col.unique_before:,} → {col.unique_after:,}",
            delta_uniq_str,
            min_drift,
            max_drift,
            new_top,
            sev_text,
        )

    console.print(rt)

    # ── Summary line ──────────────────────────────────────────────────
    alerts = sum(1 for c in table_diff.columns if c.severity == "alert")
    warns = sum(1 for c in table_diff.columns if c.severity == "warn")

    if alerts:
        console.print(
            f"\n[bold red]⚠  {alerts} alert(s)[/bold red]"
            + (f", [yellow]{warns} warning(s)[/yellow]" if warns else "")
            + " detected.\n"
        )
    elif warns:
        console.print(f"\n[yellow]△  {warns} warning(s)[/yellow] detected.\n")
    else:
        console.print("\n[green]✓  No significant changes detected.[/green]\n")


# ---------------------------------------------------------------------------
# dqm check
# ---------------------------------------------------------------------------

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

    from .anomaly import AnomalyDetector
    from .diff import DiffEngine
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
# Output helpers for dqm check
# ---------------------------------------------------------------------------

def _print_diff_table(console: object, diff: object) -> None:  # type: ignore[type-arg]
    """Print a Rich table showing per-column diffs."""
    from rich.table import Table as RichTable
    from rich.text import Text

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

    for col in diff.columns:  # type: ignore[union-attr]
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

    console.print(rt)  # type: ignore[union-attr]
    console.print()  # type: ignore[union-attr]


def _print_anomalies(console: object, anomalies: list) -> None:  # type: ignore[type-arg]
    """Print a summary of detected anomalies."""
    from rich.panel import Panel
    from rich.table import Table as RichTable
    from rich.text import Text

    if not anomalies:
        console.print(Panel("[bold green]✓ No anomalies detected[/bold green]", expand=False))  # type: ignore[union-attr]
        console.print()  # type: ignore[union-attr]
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

    console.print(rt)  # type: ignore[union-attr]
    console.print()  # type: ignore[union-attr]


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str):
    """Parse an ISO timestamp string produced by the snapshot store."""
    from datetime import datetime, timezone
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return datetime.now(tz=timezone.utc)


def _fmt_drift(before, after) -> str:
    """Show 'before → after' only when the values actually differ."""
    if before == after:
        return "—"
    return f"{_fmt(before)} → {_fmt(after)}"


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
