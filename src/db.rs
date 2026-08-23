// Copyright (C) 2026 readql contributors
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

//!
//! SQLite connection lifecycle management and query execution.
//!

use std::path::Path;

use axum::body::Body;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_rusqlite::rusqlite::Statement;
use tokio_rusqlite::{Connection, OpenFlags};
use tokio_stream::wrappers::ReceiverStream;

use crate::csv::{CsvFormatter, write_header};

const CHUNK_SIZE: usize = 64 * 1024; // 64 KB per CSV chunk
const CHANNEL_CAPACITY: usize = 16; // Max 16 chunks buffered (~1 MB max in-memory)

/// Opens a SQLite database in read-only mode and configures read-optimized PRAGMAs.
pub async fn open_file(path: &Path) -> Result<Connection, tokio_rusqlite::Error> {
    let db = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .await?;

    db.call(|conn| {
        conn.execute_batch(
            "PRAGMA query_only = ON;
             PRAGMA cache_size = -64000;
             PRAGMA mmap_size = 30000000000;
             PRAGMA temp_store = MEMORY;",
        )
    })
    .await?;

    Ok(db)
}

/// Flushes the current buffer as a byte chunk over the channel.
/// Returns `false` if the receiver has disconnected.
fn flush_chunk(buffer: &mut String, tx: &mpsc::Sender<Result<Bytes, std::io::Error>>) -> bool {
    let chunk = Bytes::from(std::mem::take(buffer));
    buffer.reserve(CHUNK_SIZE + 4096);
    tx.blocking_send(Ok(chunk)).is_ok()
}

/// Iterates rows of a prepared query statement, formatting and streaming chunks.
fn stream_rows(stmt: &mut Statement, tx: mpsc::Sender<Result<Bytes, std::io::Error>>) {
    let column_names = stmt.column_names();
    let column_count = column_names.len();

    let mut formatter = CsvFormatter::new();
    let mut buffer = String::with_capacity(CHUNK_SIZE + 4096);

    if !column_names.is_empty() {
        write_header(&mut buffer, column_names.iter().copied());
    }

    let mut rows = match stmt.query([]) {
        Ok(r) => r,
        Err(err) => {
            tracing::error!("Failed to execute query: {err}");
            let _ = tx.blocking_send(Err(std::io::Error::other(err.to_string())));
            return;
        }
    };

    loop {
        match rows.next() {
            Ok(Some(row)) => {
                if let Err(err) = formatter.write_row(&mut buffer, row, column_count) {
                    tracing::error!("Failed to format CSV row: {err}");
                    let _ = tx.blocking_send(Err(std::io::Error::other(err.to_string())));
                    return;
                }

                if buffer.len() >= CHUNK_SIZE && !flush_chunk(&mut buffer, &tx) {
                    return; // Client disconnected early
                }
            }
            Ok(None) => break,
            Err(err) => {
                tracing::error!("Failed to fetch next row: {err}");
                let _ = tx.blocking_send(Err(std::io::Error::other(err.to_string())));
                return;
            }
        }
    }

    if !buffer.is_empty() {
        flush_chunk(&mut buffer, &tx);
    }
}

/// Executes a prepared query on the SQLite connection and streams the CSV result.
fn execute_stream(
    conn: &mut tokio_rusqlite::rusqlite::Connection,
    sql: &str,
    tx: mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut stmt = match conn.prepare(sql) {
        Ok(s) => s,
        Err(err) => {
            tracing::error!("Failed to prepare SQL statement: {err}");
            let _ = tx.blocking_send(Err(std::io::Error::other(err.to_string())));
            return;
        }
    };

    stream_rows(&mut stmt, tx);
}

/// Executes a SQL query against the database and streams the RFC 4180 CSV result
/// in chunks as an Axum `Body`.
pub fn query_as_csv_stream(db: &Connection, sql: String) -> Body {
    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let db_clone = db.clone();

    tokio::spawn(async move {
        let res = db_clone
            .call(move |conn| {
                execute_stream(conn, &sql, tx);
                Ok::<(), tokio_rusqlite::rusqlite::Error>(())
            })
            .await;

        if let Err(err) = res {
            tracing::error!("tokio_rusqlite query stream error: {err}");
        }
    });

    Body::from_stream(ReceiverStream::new(rx))
}

/// Executes a SQL query against the database and returns the result formatted
/// as RFC 4180 CSV in a single `String`.
pub async fn query_as_csv(db: &Connection, sql: String) -> Result<String, tokio_rusqlite::Error> {
    db.call(move |conn| {
        let mut prepared_statement = conn.prepare(&sql)?;
        let column_names = prepared_statement.column_names();
        let column_count = column_names.len();

        let mut csv_out = String::with_capacity(CHUNK_SIZE);
        let mut formatter = CsvFormatter::new();

        if !column_names.is_empty() {
            write_header(&mut csv_out, column_names.iter().copied());
        }

        let mut rows = prepared_statement.query([])?;
        while let Some(row) = rows.next()? {
            formatter.write_row(&mut csv_out, row, column_count)?;
        }

        Ok(csv_out)
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_as_csv_and_stream() {
        let db = Connection::open_in_memory().await.unwrap();
        db.call(|conn| {
            conn.execute_batch(
                "CREATE TABLE items (id INTEGER, name TEXT, price REAL);
                 INSERT INTO items VALUES (1, 'apple', 1.25);
                 INSERT INTO items VALUES (2, 'banana', 0.75);",
            )
        })
        .await
        .unwrap();

        let csv = query_as_csv(&db, "SELECT * FROM items ORDER BY id".to_string())
            .await
            .unwrap();
        assert_eq!(csv, "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n");

        let body = query_as_csv_stream(&db, "SELECT * FROM items ORDER BY id".to_string());
        let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(text, "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n");
    }
}
