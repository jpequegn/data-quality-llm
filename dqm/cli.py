"""CLI entrypoint for dqm."""

import click
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


def main() -> None:
    cli()
