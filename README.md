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

# Generate a Markdown report for a table
uv run dqm report episodes
uv run dqm report episodes --output report.md
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
