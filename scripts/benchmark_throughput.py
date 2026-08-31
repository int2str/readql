#!/usr/bin/env python3
"""
benchmark_throughput.py

Benchmarks concurrent HTTP throughput and latency against readql using multi-threading.
Supports benchmarking CSV and Parquet endpoints across customizable concurrency levels.

Usage:
    # Run against localhost with default parameters (10 threads, 100 requests)
    python3 scripts/benchmark_throughput.py

    # Run against a remote host with custom concurrency and Parquet format
    python3 scripts/benchmark_throughput.py --host toaster --format parquet -t 20 -n 200

    # Run against a full overriding URL
    python3 scripts/benchmark_throughput.py --url "http://toaster:8002/?sql=SELECT+*+FROM+temperatures+LIMIT+100000"
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_LIMIT: int = 1_000_000
DEFAULT_THREADS: int = 10
DEFAULT_TOTAL_REQUESTS: int = 100
DEFAULT_TIMEOUT: float = 30.0


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


def fetch(url: str, timeout: float = 30.0) -> tuple[bool, float, int, int, str]:
    """
    Performs a single GET request and measures execution metrics.

    Args:
        url: Complete target HTTP URL.
        timeout: Maximum duration in seconds to wait for a response.

    Returns:
        A tuple of (success, latency_seconds, response_status_or_code, bytes_count, error_message).
    """
    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "readql-benchmark/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read()
            latency = time.perf_counter() - start
            return True, latency, response.status, len(body), ""
    except urllib.error.HTTPError as e:
        latency = time.perf_counter() - start
        body_len = 0
        try:
            body_len = len(e.read())
        except Exception:
            pass
        return False, latency, e.code, body_len, str(e)
    except Exception as e:
        latency = time.perf_counter() - start
        return False, latency, 0, 0, str(e)


def percentile(data: list[float], pct: float) -> float:
    """Computes the requested percentile value from a sorted list of floats."""
    if not data:
        return 0.0
    idx = int(len(data) * pct / 100)
    return data[min(idx, len(data) - 1)]


def run_benchmark(
    target_url: str,
    query_display: str,
    output_format: str,
    num_threads: int,
    total_requests: int,
    timeout: float,
) -> None:
    """
    Executes a concurrent multi-threaded throughput benchmark against readql.

    Args:
        target_url: Full target URL with query parameters.
        query_display: Display string for the SQL query being run.
        output_format: Requested output format ('csv' or 'parquet').
        num_threads: Number of concurrent worker threads.
        total_requests: Total number of HTTP requests to execute.
        timeout: Per-request timeout in seconds.
    """
    print("=" * 75)
    print("                READQL BENCHMARK: CONCURRENT THROUGHPUT")
    print("=" * 75)
    print(f"Target URL:     {target_url}")
    print(f"Query:          {query_display}")
    print(f"Format:         {output_format}")
    print(f"Concurrency:    {num_threads} threads")
    print(f"Total Requests: {total_requests}")
    print(f"Timeout:        {timeout:.1f} s")
    print("-" * 75)

    print("Warming up server and connection pool...")
    warmup_parsed = urllib.parse.urlparse(target_url)
    warmup_url = f"{warmup_parsed.scheme}://{warmup_parsed.netloc}/?sql=SELECT+1"
    try:
        urllib.request.urlopen(warmup_url, timeout=5.0)
    except Exception:
        pass
    print("Running benchmark...\n")

    latencies: list[float] = []
    errors: list[str] = []
    status_codes: dict[int, int] = {}
    total_bytes = 0

    bench_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(fetch, target_url, timeout) for _ in range(total_requests)]

        for future in as_completed(futures):
            success, latency, status, bytes_count, err = future.result()
            latencies.append(latency)
            total_bytes += bytes_count
            status_codes[status] = status_codes.get(status, 0) + 1
            if not success:
                errors.append(f"Status {status}: {err}" if status else err)

    bench_duration = time.perf_counter() - bench_start

    successful_reqs = total_requests - len(errors)
    rps = total_requests / bench_duration if bench_duration > 0 else 0
    bytes_per_sec = total_bytes / bench_duration if bench_duration > 0 else 0

    latencies.sort()

    print("=" * 75)
    print("                         SUMMARY RESULTS")
    print("=" * 75)
    print(f"{'Metric':<24} | {'Value'}")
    print("-" * 75)
    print(f"{'Total Duration':<24} | {bench_duration:.3f} s")
    print(f"{'Throughput':<24} | {rps:,.2f} req/s")
    print(f"{'Data Transferred':<24} | {format_bytes(total_bytes)} ({total_bytes:,} bytes)")
    print(f"{'Transfer Rate':<24} | {format_bytes(bytes_per_sec)}/s ({bytes_per_sec:,.2f} B/s)")
    print(f"{'Total Requests':<24} | {total_requests}")
    print(f"{'Successful Requests':<24} | {successful_reqs} ({successful_reqs/total_requests * 100:.1f}%)")
    print(f"{'Failed Requests':<24} | {len(errors)} ({len(errors)/total_requests * 100:.1f}%)")

    if status_codes:
        print("-" * 75)
        print(f"{'Status Code':<24} | {'Count'}")
        print("-" * 75)
        for code, count in sorted(status_codes.items()):
            print(f"{'HTTP ' + str(code or 'Error'):<24} | {count}")

    if latencies:
        print("-" * 75)
        print(f"{'Latency Metric':<24} | {'Latency'}")
        print("-" * 75)
        print(f"{'Min':<24} | {fmt_ms(min(latencies))}")
        print(f"{'Mean':<24} | {fmt_ms(statistics.mean(latencies))}")
        print(f"{'Median (P50)':<24} | {fmt_ms(statistics.median(latencies))}")
        print(f"{'P90':<24} | {fmt_ms(percentile(latencies, 90))}")
        print(f"{'P95':<24} | {fmt_ms(percentile(latencies, 95))}")
        print(f"{'P99':<24} | {fmt_ms(percentile(latencies, 99))}")
        print(f"{'Max':<24} | {fmt_ms(max(latencies))}")

    if errors:
        print("-" * 75)
        print("First Errors:")
        for err in errors[:5]:
            print(f"  - {err}")
    print("=" * 75)


def main() -> None:
    """Parses command-line arguments and runs the throughput benchmark."""
    parser = argparse.ArgumentParser(
        description="Benchmark readql HTTP endpoint throughput and concurrency."
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
        "--format",
        default="csv",
        choices=["csv", "parquet"],
        help="Response format to query (default: csv)",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=DEFAULT_THREADS,
        help=f"Number of concurrent threads (default: {DEFAULT_THREADS})",
    )
    parser.add_argument(
        "-n",
        "--requests",
        type=int,
        default=DEFAULT_TOTAL_REQUESTS,
        help=f"Total requests to make (default: {DEFAULT_TOTAL_REQUESTS})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"Per-request timeout in seconds (default: {DEFAULT_TIMEOUT:.1f})",
    )

    args = parser.parse_args()

    if args.url:
        target_url = args.url
        parsed = urllib.parse.urlparse(args.url)
        query_params = urllib.parse.parse_qs(parsed.query)
        query_display = query_params.get("sql", ["<custom>"])[0]
        output_format = query_params.get("format", [args.format])[0]
    else:
        sql = args.sql if args.sql else f"SELECT * FROM temperatures LIMIT {args.limit}"
        query_params = urllib.parse.urlencode({"sql": sql, "format": args.format})
        target_url = f"http://{args.host}:{args.port}/?{query_params}"
        query_display = sql
        output_format = args.format

    run_benchmark(
        target_url=target_url,
        query_display=query_display,
        output_format=output_format,
        num_threads=args.threads,
        total_requests=args.requests,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
