# readql

High-throughput read-only SQLite HTTP query server written in Rust. `readql` exposes an HTTP interface to query SQLite databases concurrently and stream query results formatted as RFC 4180-compliant CSV.

This project was inspired by the wonderful [Datasette](https://datasette.io/) project. `Datasette` is a full featured tool for exploring and publishing data. `readql` is none of those things. Instead, `readql` attempts to be an even faster and even lighter weight version of the `datasette serve` command.

## Features

- **High Throughput & Low Latency:** Optimized SQLite read configuration with `mmap`, `cache_size`, and in-memory temporary storage.
- **Zero-Copy CSV Streaming:** Uses `ValueRef` column inspection to eliminate intermediate allocations while formatting results directly to RFC 4180 CSV.
- **Apache Parquet Streaming:** High-performance binary columnar format with Zstandard compression for data science tools like Pandas, Polars, and DuckDB.
- **Built-in Web UI Studio:** Interactive browser query UI running on port `8001` with schema explorer, query history, and data export.
- **Asynchronous Architecture:** Built on Axum and Tokio for high concurrency.

## Prerequisites

- [Rust toolchain](https://rustup.rs/) (edition 2024 / stable 1.85+)
- SQLite 3

## Installation & Building

```bash
# Clone the repository
git clone https://github.com/your-username/readql.git
cd readql

# Build optimized release binary
cargo build --release

# The compiled binary will be in target/release/readql
```

## CLI Usage

```bash
# Start server (API on port 8002, Web UI on port 8001)
readql <path-to-sqlite-db>

# Custom host, API port, and UI port
readql <path-to-sqlite-db> --listen 127.0.0.1 --port 9000 --ui-port 9001

# Disable Web UI (run API server only)
readql <path-to-sqlite-db> --no-ui

# Display help
readql --help
```

### Options

| Option | Short | Description | Default |
| :--- | :--- | :--- | :--- |
| `<DB_PATH>` | | Path to the SQLite database file | *(Required)* |
| `--listen` | `-l` | IP address to listen on | `0.0.0.0` |
| `--port` | `-p` | TCP port to listen on for the API | `8002` |
| `--ui-port` | | TCP port to listen on for the Web UI | `8001` |
| `--no-ui` | | Disable the Web UI server | `false` |
| `--connections` | `-c` | Number of pooled SQLite connections | *(CPU Cores)* |
| `--help` | `-h` | Print help information | |
| `--version` | `-V` | Print version information | |

## HTTP API

### `GET /`

Executes a SQL read query provided in the `sql` query parameter and returns RFC 4180 CSV or Apache Parquet data.

#### Query Parameters

| Parameter | Type | Description | Default |
| :--- | :--- | :--- | :--- |
| `sql` | `string` | The SQL query to execute *(Required)* | |
| `format` | `string` | Output format: `csv` or `parquet` (or `pq`) | `csv` (or inferred from `Accept` header) |

#### CSV Example Request

```bash
curl -G "http://localhost:8002/" --data-urlencode "sql=SELECT * FROM temperatures LIMIT 10"
```

#### CSV Response

```csv
id,timestamp,reference,t1_single,t1_average,t2_single,t2_average,t3_single,t3_average
1,1700000000,21.5,21.4,21.45,21.6,21.55,21.3,21.35
2,1700000001,21.5,21.4,21.45,21.6,21.55,21.3,21.35
```

#### Parquet Example Request (Pandas / Polars)

```python
import io, requests, pandas as pd

# Query as Parquet via ?format=parquet
resp = requests.get("http://localhost:8002/?sql=SELECT+*+FROM+temperatures&format=parquet")
df = pd.read_parquet(io.BytesIO(resp.content))
```

```python
import io, requests, polars as pl

# Query as Parquet in Polars
resp = requests.get("http://localhost:8002/?sql=SELECT+*+FROM+temperatures&format=parquet")
df = pl.read_parquet(io.BytesIO(resp.content))
```

### `GET /api/metrics`

Returns a real-time JSON metrics snapshot including:
- Cumulative requests (total, successful, failed, active in-flight)
- Data transfer volume (CSV vs. Parquet) and total rows streamed
- Request throughput (`current_rps`), transfer rate (`current_bytes_per_sec`), and row throughput (`current_rows_per_sec`)
- 60-second rolling time-series history
- Per-client host breakdown (IP, total requests, rows, bytes, average latency, last active)
- Recent 30 queries log

```bash
curl http://localhost:8002/api/metrics
```

## Web UI Studio & Live Dashboard

`readql` ships with an embedded single-page application on port `8001` (`http://localhost:8001`) with zero external dependencies:
- **Query Studio:** Interactive table explorer, SQL query editor (`Ctrl+Enter`), query history in `localStorage`, data grid, and 1-click export (CSV, Parquet, Python Pandas/Polars snippets).
- **Live Dashboard:** Real-time KPI summary cards, hardware-accelerated 60s Canvas graphs (Req/s throughput and MB/s transfer rate), active client host statistics table, and recent query execution log.

## Development & Testing

```bash
# Run unit tests
cargo test

# Check code formatting
cargo fmt --check

# Run linter
cargo clippy -- -D warnings

# Generate demo database
python3 scripts/generate_demo_database.py demo.db --count 1000000

# Run CSV vs Parquet import benchmark
python3 scripts/benchmark_csv_vs_parquet.py --host localhost --limit 1000000

# Run concurrent throughput benchmark
python3 scripts/benchmark_throughput.py --host localhost --threads 10 --requests 100
```

## License

This project is licensed under the **GNU General Public License v2.0** (GPL-2.0-only). See [LICENSE.md](LICENSE.md) for details.
