#!/usr/bin/env python3
"""
benchmark_csv_vs_parquet.py

Benchmark comparing Pandas CSV import vs Pandas Parquet import from readql.
Measures payload sizes, network transfer latency, Pandas parsing time, and end-to-end throughput.

Usage:
    # Run against localhost with default 1,000,000 row query
    python3 scripts/benchmark_csv_vs_parquet.py

    # Run against a remote host with custom limit and iteration count
    python3 scripts/benchmark_csv_vs_parquet.py --host toaster --limit 5000000 -n 3

    # Run with a full overriding URL
    python3 scripts/benchmark_csv_vs_parquet.py --url "http://toaster:8002/?sql=SELECT+*+FROM+temperatures+LIMIT+100000"
"""

from __future__ import annotations

import argparse
import io
import time
import urllib.parse
import pandas as pd
import requests

DEFAULT_LIMIT: int = 1_000_000
DEFAULT_RUNS: int = 5


def format_bytes(num_bytes: float) -> str:
    """Formats a byte count into a human-readable string (e.g. KB, MB, GB)."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}" if unit != "B" else f"{int(num_bytes)} B"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


def fmt_ms(val_s: float) -> str:
    """Formats seconds into milliseconds with clean decimal precision."""
    val_ms = val_s * 1000
    if val_ms >= 10000:
        return f"{val_ms:,.0f} ms"
    elif val_ms >= 1000:
        return f"{val_ms:,.1f} ms"
    else:
        return f"{val_ms:.2f} ms"


def fmt_rows_s(rows_per_sec: float) -> str:
    """Formats throughput as rows per second."""
    return f"{rows_per_sec:,.0f} rows/s"


def benchmark_csv_split(
    base_url: str,
    sql: str,
) -> tuple[pd.DataFrame, float, float, float, int]:
    """
    Measures network fetch and parsing separately for CSV output.

    Args:
        base_url: The target readql base URL.
        sql: The SQL query string to execute.

    Returns:
        A tuple of (DataFrame, total_time, fetch_time, parse_time, payload_size_bytes).
    """
    params = {"sql": sql, "format": "csv"}
    t0 = time.perf_counter()
    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    t1 = time.perf_counter()
    fetch_time = t1 - t0
    payload_size = len(resp.content)

    t2 = time.perf_counter()
    df = pd.read_csv(io.BytesIO(resp.content))
    t3 = time.perf_counter()
    parse_time = t3 - t2
    total_time = t3 - t0

    return df, total_time, fetch_time, parse_time, payload_size


def benchmark_parquet_split(
    base_url: str,
    sql: str,
) -> tuple[pd.DataFrame, float, float, float, int]:
    """
    Measures network fetch and parsing separately for Parquet output using Pandas.

    Args:
        base_url: The target readql base URL.
        sql: The SQL query string to execute.

    Returns:
        A tuple of (DataFrame, total_time, fetch_time, parse_time, payload_size_bytes).
    """
    params = {"sql": sql, "format": "parquet"}
    t0 = time.perf_counter()
    resp = requests.get(base_url, params=params)
    resp.raise_for_status()
    t1 = time.perf_counter()
    fetch_time = t1 - t0
    payload_size = len(resp.content)

    t2 = time.perf_counter()
    df = pd.read_parquet(io.BytesIO(resp.content))
    t3 = time.perf_counter()
    parse_time = t3 - t2
    total_time = t3 - t0

    return df, total_time, fetch_time, parse_time, payload_size


def run_benchmark(
    base_url: str,
    sql: str,
    runs: int,
) -> None:
    """
    Executes multiple benchmark iterations comparing CSV vs Parquet in Pandas.

    Args:
        base_url: Target readql base URL.
        sql: SQL query string.
        runs: Number of benchmark runs.
    """
    print("=" * 75)
    print("        READQL BENCHMARK: PANDAS CSV vs PANDAS PARQUET")
    print("=" * 75)
    print(f"Target:      {base_url}")
    print(f"Query:       {sql}")
    print(f"Iterations:  {runs}")
    print("-" * 75)

    # Warmup
    print("Warming up server and connection pool...")
    try:
        requests.get(base_url, params={"sql": "SELECT 1"}).raise_for_status()
    except Exception as e:
        print(f"Warning: Warmup failed ({e})")
    print("Running benchmarks...\n")

    csv_totals, csv_fetches, csv_parses = [], [], []
    pq_totals, pq_fetches, pq_parses = [], [], []
    csv_bytes, pq_bytes = 0, 0
    row_count = 0
    col_count = 0

    for i in range(1, runs + 1):
        print(f"Run {i}/{runs}:")

        # 1. Benchmark CSV
        df_csv, total_csv, fetch_csv, parse_csv, size_csv = benchmark_csv_split(base_url, sql)
        csv_totals.append(total_csv)
        csv_fetches.append(fetch_csv)
        csv_parses.append(parse_csv)
        csv_bytes = size_csv
        row_count = len(df_csv)
        col_count = len(df_csv.columns)

        print(
            f"  [CSV]     Total: {fmt_ms(total_csv):>10}  (Fetch: {fmt_ms(fetch_csv):>9} | Parse: {fmt_ms(parse_csv):>9}) | Size: {format_bytes(size_csv):>9}"
        )

        # 2. Benchmark Parquet
        df_pq, total_pq, fetch_pq, parse_pq, size_pq = benchmark_parquet_split(base_url, sql)
        pq_totals.append(total_pq)
        pq_fetches.append(fetch_pq)
        pq_parses.append(parse_pq)
        pq_bytes = size_pq

        speedup = total_csv / total_pq if total_pq > 0 else 0
        print(
            f"  [Parquet] Total: {fmt_ms(total_pq):>10}  (Fetch: {fmt_ms(fetch_pq):>9} | Parse: {fmt_ms(parse_pq):>9}) | Size: {format_bytes(size_pq):>9} | {speedup:.2f}x faster"
        )

    # Calculate statistics
    avg_csv_total = sum(csv_totals) / len(csv_totals)
    avg_csv_fetch = sum(csv_fetches) / len(csv_fetches)
    avg_csv_parse = sum(csv_parses) / len(csv_parses)

    avg_pq_total = sum(pq_totals) / len(pq_totals)
    avg_pq_fetch = sum(pq_fetches) / len(pq_fetches)
    avg_pq_parse = sum(pq_parses) / len(pq_parses)

    size_reduction = (1 - (pq_bytes / csv_bytes)) * 100 if csv_bytes > 0 else 0
    total_speedup = avg_csv_total / avg_pq_total if avg_pq_total > 0 else 0
    parse_speedup = avg_csv_parse / avg_pq_parse if avg_pq_parse > 0 else 0
    fetch_speedup = avg_csv_fetch / avg_pq_fetch if avg_pq_fetch > 0 else 0

    csv_throughput = row_count / avg_csv_total if avg_csv_total > 0 else 0
    pq_throughput = row_count / avg_pq_total if avg_pq_total > 0 else 0

    print("\n" + "=" * 75)
    print("                         SUMMARY RESULTS")
    print("=" * 75)
    print(f"Rows Loaded:        {row_count:,} rows")
    print(f"Columns:            {col_count} columns")
    print("-" * 75)
    print(f"{'Metric':<24} | {'Pandas (CSV)':<18} | {'Pandas (Parquet)':<18} | {'Improvement'}")
    print("-" * 75)
    print(
        f"{'Payload Size':<24} | {format_bytes(csv_bytes):<18} | {format_bytes(pq_bytes):<18} | {size_reduction:.1f}% smaller"
    )
    print(
        f"{'Network Fetch Time':<24} | {fmt_ms(avg_csv_fetch):<18} | {fmt_ms(avg_pq_fetch):<18} | {fetch_speedup:.2f}x faster"
    )
    print(
        f"{'Pandas Parse Time':<24} | {fmt_ms(avg_csv_parse):<18} | {fmt_ms(avg_pq_parse):<18} | {parse_speedup:.2f}x faster"
    )
    print(
        f"{'Total End-to-End Time':<24} | {fmt_ms(avg_csv_total):<18} | {fmt_ms(avg_pq_total):<18} | {total_speedup:.2f}x faster"
    )
    print(
        f"{'Ingestion Throughput':<24} | {fmt_rows_s(csv_throughput):<18} | {fmt_rows_s(pq_throughput):<18} | {pq_throughput/csv_throughput if csv_throughput else 0:.2f}x"
    )
    print("=" * 75)


def main() -> None:
    """Parses command-line arguments and runs the CSV vs Parquet benchmark."""
    parser = argparse.ArgumentParser(
        description="Benchmark Pandas CSV import vs Pandas Parquet import from readql."
    )
    parser.add_argument(
        "--url",
        default=None,
        help="Full overriding URL (e.g. 'http://localhost:8002/?sql=SELECT+*+FROM+temperatures+LIMIT+1000000')",
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Target readql host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8002,
        help="Target readql port (default: 8002)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Number of rows to query by default (default: {DEFAULT_LIMIT:,})",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="Custom SQL query (default: 'SELECT * FROM temperatures LIMIT <limit>')",
    )
    parser.add_argument(
        "-n",
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help=f"Number of benchmark iterations (default: {DEFAULT_RUNS})",
    )

    args = parser.parse_args()

    if args.url:
        parsed = urllib.parse.urlparse(args.url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"
        query_params = urllib.parse.parse_qs(parsed.query)
        sql = query_params.get(
            "sql", [args.sql or f"SELECT * FROM temperatures LIMIT {args.limit}"]
        )[0]
    else:
        base_url = f"http://{args.host}:{args.port}/"
        sql = args.sql if args.sql else f"SELECT * FROM temperatures LIMIT {args.limit}"

    run_benchmark(base_url, sql, args.runs)


if __name__ == "__main__":
    main()
