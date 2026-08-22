# readql

High-throughput read-only SQLite HTTP query server written in Rust. `readql` exposes an HTTP interface to query SQLite databases concurrently and stream query results formatted as RFC 4180-compliant CSV.

This project was inspired by the wonderful [Datasette](https://datasette.io/) project. `Datasette` is a full featured tool for exploring and publishing data. `readql` is none of those things. Instead, `readql` attempts to be an even faster and even lighter weight version of the `datasette serve` command.

## Features

- **High Throughput & Low Latency:** Optimized SQLite read configuration with `mmap`, `cache_size`, and in-memory temporary storage.
- **Zero-Copy CSV Streaming:** Uses `ValueRef` column inspection to eliminate intermediate allocations while formatting results directly to RFC 4180 CSV.
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
# Start server with default host (0.0.0.0) and port (8002)
readql <path-to-sqlite-db>

# Custom host and port
readql <path-to-sqlite-db> --listen 127.0.0.1 --port 9000

# Short flag syntax
readql <path-to-sqlite-db> -l 127.0.0.1 -p 9000

# Display help
readql --help
```

### Options

| Option | Short | Description | Default |
| :--- | :--- | :--- | :--- |
| `<DB_PATH>` | | Path to the SQLite database file | *(Required)* |
| `--listen` | `-l` | IP address to listen on | `0.0.0.0` |
| `--port` | `-p` | TCP port to listen on | `8002` |
| `--help` | `-h` | Print help information | |
| `--version` | `-V` | Print version information | |

## HTTP API

### `GET /`

Executes a SQL read query provided in the `sql` query parameter and returns RFC 4180 CSV data.

#### Example Request

```bash
curl -G "http://localhost:8002/" --data-urlencode "sql=SELECT * FROM temperatures LIMIT 10"
```

#### Example Response

```csv
id,timestamp,reference,t1_single,t1_average,t2_single,t2_average,t3_single,t3_average
1,1700000000,21.5,21.4,21.45,21.6,21.55,21.3,21.35
2,1700000001,21.5,21.4,21.45,21.6,21.55,21.3,21.35
```

## Development & Testing

```bash
# Run unit tests
cargo test

# Check code formatting
cargo fmt --check

# Run linter
cargo clippy -- -D warnings

# Run benchmark suite
python3 benchmark.py --url "http://localhost:8002?sql=SELECT+*+FROM+temperatures+LIMIT+1000"
```

## License

This project is licensed under the **GNU General Public License v2.0** (GPL-2.0-only). See [LICENSE.md](LICENSE.md) for details.
