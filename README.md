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
