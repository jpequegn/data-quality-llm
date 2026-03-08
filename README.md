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
uv run dqm profile episodes
uv run dqm --db /path/to/other.duckdb profile my_table

# Run full anomaly check: profile → snapshot → diff → flag rule violations
uv run dqm check episodes
uv run dqm --db /path/to/other.duckdb check my_table

# Generate a Markdown report for a table
uv run dqm report episodes
uv run dqm report episodes --output report.md
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

## Default data source

The tool defaults to the P³ (parakeet-podcast-processor) DuckDB, checking these paths in order:

1. `~/.p3/p3.duckdb`
2. `~/Code/parakeet-podcast-processor/data/p3.duckdb`

Override with `--db <path>`.

## Anomaly Detector

`dqm check <table>` profiles the table, saves a snapshot, diffs it against the
previous snapshot, and runs all threshold rules.  Each anomaly shows the
triggering rule, the old and new values, and a severity badge.

### Default rules

| Rule | Condition | Severity |
|------|-----------|----------|
| `null_pct_increase` | null % rose by > 10 pp | **ALERT** |
| `unique_count_drop` | unique count fell by > 20 % | **WARN** |
| `row_count_decrease` | row count decreased at all | **ALERT** |
| `row_count_spike` | row count increased by > 500 % | **WARN** |
| `max_val_decrease` | max value fell for a monotonic column | **ALERT** |

### Configuration

Thresholds are read from `~/.dqm/anomaly_config.yaml` (user overrides) falling
back to the bundled `dqm/anomaly_config.yaml`.  Example override:

```yaml
thresholds:
  null_pct_increase_pp: 5.0        # fire at 5 pp instead of 10 pp
  unique_count_drop_pct: 0.10      # fire at 10 % drop instead of 20 %
  row_count_increase_pct: 2.00     # fire at 200 % increase instead of 500 %
  monotonic_columns:               # check max_val for these columns
    - episode_id
    - published_at
```

Pass a custom config path with `--anomaly-config <path>`.

Snapshots are stored in `~/.dqm/snapshots.db` (SQLite).  Override with
`--snapshots-db <path>`.

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
