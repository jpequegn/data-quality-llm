# data-quality-llm

Data quality checks for DuckDB databases, powered by Claude.

## Installation

```bash
uv sync
```

## Usage

```bash
# List tables in the default P³ database
uv run dqm tables

# Use a custom database
uv run dqm --db /path/to/other.duckdb tables

# Profile all columns in a table (nulls, cardinality, min/max, distribution)
# Auto-saves a snapshot to ~/.dqm/snapshots.db
uv run dqm profile episodes
uv run dqm --db /path/to/other.duckdb profile my_table

# Generate a Markdown report for a table
uv run dqm report episodes
uv run dqm report episodes --output report.md

# Diff: show what changed between the two most recent snapshots
uv run dqm diff episodes

# Diff against a specific date (picks the nearest snapshot on or before that date)
uv run dqm diff episodes --since 2026-03-01
```

## Column Profiler

`dqm profile <table>` computes per-column statistics and renders them in a rich table:

| Stat | Description |
|------|-------------|
| **Rows** | Total row count |
| **Nulls** | Absolute null count |
| **Null %** | Null percentage (highlighted red ≥ 20%, yellow ≥ 5%) |
| **Unique** | Count of distinct non-null values |
| **Min / Max** | Minimum and maximum value |
| **Mean** | Average (numeric columns only) |
| **P25 / P75** | 25th and 75th percentiles (numeric only) |
| **Top values** | Top 5 most-frequent values with counts |

Supported DuckDB types: `VARCHAR`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `TIMESTAMP`, `JSON`, and more.

Each `dqm profile` run auto-saves a snapshot to `~/.dqm/snapshots.db`.
View history with `dqm snapshots list <table>` and inspect any snapshot with `dqm snapshots get <id>`.

## Diff Engine

`dqm diff <table>` compares the two most-recent snapshots and surfaces what changed:

| Column | Meaning |
|--------|---------|
| **Null % (before→after)** | Null rate in each snapshot |
| **Δ Null pp** | Change in null % (percentage-points); red ≥ +10 pp, yellow ≥ +2 pp |
| **Unique (before→after)** | Distinct-value count |
| **Δ Unique** | Absolute change in cardinality |
| **Min / Max drift** | Shows `before → after` only when the boundary value changed |
| **New top values** | Values that appear in the top-5 now but didn't before |
| **Severity** | `OK` / `WARN` / `ALERT` driven by null-% change |

### Severity thresholds

| Label | Condition |
|-------|-----------|
| `ALERT` | Null % rose by ≥ 10 percentage-points |
| `WARN` | Null % rose by ≥ 2 percentage-points |
| `OK` | Everything else |

## Snapshot Store

Snapshots are persisted in a local SQLite database at `~/.dqm/snapshots.db`.

```bash
# List snapshot history for a table (newest first)
uv run dqm snapshots list episodes

# Inspect a specific snapshot by id
uv run dqm snapshots get 42
```

## Default data source

The tool defaults to the P³ (parakeet-podcast-processor) DuckDB, checking these paths in order:

1. `~/.p3/p3.duckdb`
2. `~/Code/parakeet-podcast-processor/data/p3.duckdb`

Override with `--db <path>`.

## P³ columns worth monitoring

| Table | Column | Why |
|-------|--------|-----|
| `episodes` | `title` | Null spike → feed ingestion broken |
| `episodes` | `published_at` | Null or future date → parser regression |
| `summaries` | `body` | Null spike → LLM summariser failing |
| `summaries` | `model` | Cardinality drop → model switch / rollback |
| `transcripts` | `text` | Null or very short → Whisper pipeline error |
| `transcripts` | `duration_s` | Outliers → bad audio or wrong episode matched |
| `errors` | `error_type` | Cardinality spike → new failure mode |
| `errors` | `created_at` | Volume spike → systemic processing failure |
