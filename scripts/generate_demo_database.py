#!/usr/bin/env python3
"""
generate_demo_database.py

Fast SQLite demo database generator for readql benchmarks.
Populates a 'temperatures' table with realistic simulated sensor data
using high-performance bulk insertion techniques.

Schema:
    CREATE TABLE temperatures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER NOT NULL,
        reference REAL NOT NULL,
        t1 REAL,
        t2 REAL,
        t3 REAL
    );

Usage:
    # Generate default 1,000,000 records to demo.db
    python3 scripts/generate_demo_database.py demo.db

    # Generate custom count
    python3 scripts/generate_demo_database.py demo.db --count 5000000
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import random
import sqlite3
import time

DEFAULT_COUNT: int = 1_000_000
BATCH_SIZE: int = 50_000


def format_bytes(num_bytes: float) -> str:
    """Formats a byte count into a human-readable string (e.g. KB, MB, GB)."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}" if unit != "B" else f"{int(num_bytes)} B"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


def generate_database(db_path: Path | str, count: int = DEFAULT_COUNT) -> None:
    """
    Creates and populates the SQLite database with simulated temperature data.

    Args:
        db_path: Path to the SQLite database file to create or overwrite.
        count: Total number of temperature records to insert.
    """
    path = Path(db_path)
    if path.parent and not path.parent.exists():
        path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 75)
    print("                 READQL DEMO DATABASE GENERATOR")
    print("=" * 75)
    print(f"Database Path:  {path}")
    print(f"Target Records: {count:,}")
    print("-" * 75)

    start_time = time.perf_counter()

    conn = sqlite3.connect(path)
    # High-performance bulk load pragmas
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA journal_mode = MEMORY;")
    conn.execute("PRAGMA cache_size = -64000;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS temperatures (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            reference REAL NOT NULL,
            t1 REAL,
            t2 REAL,
            t3 REAL
        )
        """
    )

    base_time_ms = int(time.time() * 1000)
    inserted = 0

    print(f"Generating and inserting {count:,} records in batches of {BATCH_SIZE:,}...")

    while inserted < count:
        current_batch_size = min(BATCH_SIZE, count - inserted)
        batch = [
            (
                base_time_ms + (inserted + i) * 100,
                round(random.uniform(36.0, 38.0), 3),
                round(random.uniform(36.0, 38.0), 3),
                round(random.uniform(36.0, 38.0), 3),
                round(random.uniform(36.0, 38.0), 3),
            )
            for i in range(current_batch_size)
        ]
        conn.executemany(
            "INSERT INTO temperatures (timestamp, reference, t1, t2, t3) VALUES (?, ?, ?, ?, ?)",
            batch,
        )
        inserted += current_batch_size
        percent = (inserted / count) * 100
        print(f"\r  Progress: {inserted:,} / {count:,} records ({percent:5.1f}%)", end="", flush=True)

    conn.commit()

    # Reconfigure for production readql read/write access (WAL mode)
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.close()

    duration = time.perf_counter() - start_time
    file_size = os.path.getsize(path) if os.path.exists(path) else 0
    throughput = count / duration if duration > 0 else 0

    print("\n" + "-" * 75)
    print(f"Generated {count:,} records in {duration:.2f} s ({throughput:,.0f} records/s)")
    print(f"Database file size: {format_bytes(file_size)}")
    print("=" * 75)


def main() -> None:
    """Parses command-line arguments and initiates database generation."""
    parser = argparse.ArgumentParser(
        description="Fast SQLite demo database generator for readql benchmarks."
    )
    parser.add_argument(
        "database",
        type=Path,
        nargs="?",
        default=Path("demo.db"),
        help="Path to SQLite database file (default: demo.db)",
    )
    parser.add_argument(
        "-n",
        "--count",
        type=int,
        default=DEFAULT_COUNT,
        help=f"Number of records to generate (default: {DEFAULT_COUNT:,})",
    )

    args = parser.parse_args()
    generate_database(args.database, args.count)


if __name__ == "__main__":
    main()
