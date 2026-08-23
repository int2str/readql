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
//! SQLite connection lifecycle management, connection pooling, and query execution.
//!

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::body::Body;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_rusqlite::rusqlite::Statement;
use tokio_rusqlite::{Connection, OpenFlags};
use tokio_stream::wrappers::ReceiverStream;

use crate::csv::{CsvFormatter, write_header};

const CHUNK_SIZE: usize = 64 * 1024; // 64 KB per CSV chunk
const CHANNEL_CAPACITY: usize = 16; // Max 16 chunks buffered (~1 MB max in-memory)

/// A pool of read-only SQLite database connections for concurrent query execution.
#[derive(Clone)]
pub struct ConnectionPool {
    connections: Arc<[Connection]>,
    next_index: Arc<AtomicUsize>,
}

impl ConnectionPool {
    /// Creates a new connection pool with the specified database connections.
    pub fn new(connections: Vec<Connection>) -> Self {
        Self {
            connections: connections.into(),
            next_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Returns the next database connection from the pool in round-robin order.
    pub fn get_connection(&self) -> Connection {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        self.connections[index % self.connections.len()].clone()
    }

    /// Returns the total number of connections in the pool.
    pub fn size(&self) -> usize {
        self.connections.len()
    }

    /// Returns `true` if the connection pool contains no connections.
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }
}

/// Opens a single SQLite database connection in read-only mode and configures read-optimized PRAGMAs.
pub async fn open_connection(database_path: &Path) -> Result<Connection, tokio_rusqlite::Error> {
    let connection = Connection::open_with_flags(
        database_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .await?;

    connection
        .call(|raw_connection| {
            raw_connection.execute_batch(
                "PRAGMA query_only = ON;
                 PRAGMA cache_size = -64000;
                 PRAGMA mmap_size = 30000000000;
                 PRAGMA temp_store = MEMORY;",
            )
        })
        .await?;

    Ok(connection)
}

/// Opens a pool of SQLite database connections in read-only mode and configures read-optimized PRAGMAs.
pub async fn open_pool(
    database_path: &Path,
    pool_size: usize,
) -> Result<ConnectionPool, tokio_rusqlite::Error> {
    let connection_count = if pool_size == 0 {
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
    } else {
        pool_size
    };

    let mut connections = Vec::with_capacity(connection_count);
    for _ in 0..connection_count {
        let connection = open_connection(database_path).await?;
        connections.push(connection);
    }

    Ok(ConnectionPool::new(connections))
}

/// Opens a SQLite database in read-only mode and configures read-optimized PRAGMAs.
pub async fn open_file(database_path: &Path) -> Result<Connection, tokio_rusqlite::Error> {
    open_connection(database_path).await
}

/// Flushes the current buffer as a byte chunk over the channel.
/// Returns `false` if the receiver has disconnected.
fn flush_chunk(buffer: &mut String, sender: &mpsc::Sender<Result<Bytes, std::io::Error>>) -> bool {
    let chunk = Bytes::from(std::mem::take(buffer));
    buffer.reserve(CHUNK_SIZE + 4096);
    sender.blocking_send(Ok(chunk)).is_ok()
}

/// Iterates rows of a prepared query statement, formatting and streaming chunks.
fn stream_rows(statement: &mut Statement, sender: mpsc::Sender<Result<Bytes, std::io::Error>>) {
    let column_names = statement.column_names();
    let column_count = column_names.len();

    let mut formatter = CsvFormatter::new();
    let mut buffer = String::with_capacity(CHUNK_SIZE + 4096);

    if !column_names.is_empty() {
        write_header(&mut buffer, column_names.iter().copied());
    }

    let mut rows = match statement.query([]) {
        Ok(query_rows) => query_rows,
        Err(error) => {
            tracing::error!("Failed to execute query: {error}");
            let _ = sender.blocking_send(Err(std::io::Error::other(error.to_string())));
            return;
        }
    };

    loop {
        match rows.next() {
            Ok(Some(row)) => {
                if let Err(error) = formatter.write_row(&mut buffer, row, column_count) {
                    tracing::error!("Failed to format CSV row: {error}");
                    let _ = sender.blocking_send(Err(std::io::Error::other(error.to_string())));
                    return;
                }

                if buffer.len() >= CHUNK_SIZE && !flush_chunk(&mut buffer, &sender) {
                    return; // Client disconnected early
                }
            }
            Ok(None) => break,
            Err(error) => {
                tracing::error!("Failed to fetch next row: {error}");
                let _ = sender.blocking_send(Err(std::io::Error::other(error.to_string())));
                return;
            }
        }
    }

    if !buffer.is_empty() {
        flush_chunk(&mut buffer, &sender);
    }
}

/// Executes a prepared query on the SQLite connection and streams the CSV result.
fn execute_stream(
    connection: &mut tokio_rusqlite::rusqlite::Connection,
    sql_query: &str,
    sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let mut statement = match connection.prepare(sql_query) {
        Ok(prepared_statement) => prepared_statement,
        Err(error) => {
            tracing::error!("Failed to prepare SQL statement: {error}");
            let _ = sender.blocking_send(Err(std::io::Error::other(error.to_string())));
            return;
        }
    };

    stream_rows(&mut statement, sender);
}

/// Executes a SQL query against the database and streams the RFC 4180 CSV result
/// in chunks as an Axum `Body`.
pub fn query_as_csv_stream(connection: &Connection, sql_query: String) -> Body {
    let (sender, receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let connection_clone = connection.clone();

    tokio::spawn(async move {
        let result = connection_clone
            .call(move |raw_connection| {
                execute_stream(raw_connection, &sql_query, sender);
                Ok::<(), tokio_rusqlite::rusqlite::Error>(())
            })
            .await;

        if let Err(error) = result {
            tracing::error!("tokio_rusqlite query stream error: {error}");
        }
    });

    Body::from_stream(ReceiverStream::new(receiver))
}

/// Executes a SQL query against the database and returns the result formatted
/// as RFC 4180 CSV in a single `String`.
pub async fn query_as_csv(
    connection: &Connection,
    sql_query: String,
) -> Result<String, tokio_rusqlite::Error> {
    connection
        .call(move |raw_connection| {
            let mut prepared_statement = raw_connection.prepare(&sql_query)?;
            let column_names = prepared_statement.column_names();
            let column_count = column_names.len();

            let mut csv_output = String::with_capacity(CHUNK_SIZE);
            let mut formatter = CsvFormatter::new();

            if !column_names.is_empty() {
                write_header(&mut csv_output, column_names.iter().copied());
            }

            let mut rows = prepared_statement.query([])?;
            while let Some(row) = rows.next()? {
                formatter.write_row(&mut csv_output, row, column_count)?;
            }

            Ok(csv_output)
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_query_as_csv_and_stream() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE items (id INTEGER, name TEXT, price REAL);
                     INSERT INTO items VALUES (1, 'apple', 1.25);
                     INSERT INTO items VALUES (2, 'banana', 0.75);",
                )
            })
            .await
            .unwrap();

        let csv_output = query_as_csv(&connection, "SELECT * FROM items ORDER BY id".to_string())
            .await
            .unwrap();
        assert_eq!(
            csv_output,
            "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n"
        );

        let body = query_as_csv_stream(&connection, "SELECT * FROM items ORDER BY id".to_string());
        let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(text, "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n");
    }

    #[tokio::test]
    async fn test_connection_pool_round_robin() {
        let first_connection = Connection::open_in_memory().await.unwrap();
        let second_connection = Connection::open_in_memory().await.unwrap();

        let connection_pool = ConnectionPool::new(vec![first_connection, second_connection]);
        assert_eq!(connection_pool.size(), 2);
        assert!(!connection_pool.is_empty());

        let _first_acquired = connection_pool.get_connection();
        let _second_acquired = connection_pool.get_connection();
        let _third_acquired = connection_pool.get_connection();
    }
}
