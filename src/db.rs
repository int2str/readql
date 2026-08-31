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
use tokio_rusqlite::{Connection, OpenFlags};
use tokio_stream::wrappers::ReceiverStream;

use crate::AppError;
use crate::csv::CsvWriter;
use crate::parquet_format::{
    ChunkWriter, RecordBatchAccumulator, create_parquet_writer, extract_column_metadata,
    infer_schema_from_metadata,
};

const CHUNK_SIZE: usize = 64 * 1024; // 64 KB per CSV/Parquet chunk
const CHANNEL_CAPACITY: usize = 16; // Max 16 chunks buffered (~1 MB max in-memory)
const ROW_BATCH_SIZE: usize = 8192; // Max rows per Arrow RecordBatch

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

/// Executes a SQL query against the database and streams the RFC 4180 CSV result
/// in chunks as an Axum `Body`. Fails early if the SQL statement cannot be prepared.
pub async fn query_as_csv_stream(
    connection: &Connection,
    sql_query: String,
) -> Result<Body, AppError> {
    let (chunk_sender, chunk_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let (prepared_sender, prepared_receiver) = tokio::sync::oneshot::channel();
    let connection_clone = connection.clone();

    tokio::spawn(async move {
        let result = connection_clone
            .call(move |raw_connection| {
                let mut prepared_statement = match raw_connection.prepare(&sql_query) {
                    Ok(statement) => {
                        let _ = prepared_sender.send(Ok(()));
                        statement
                    }
                    Err(error) => {
                        let _ = prepared_sender.send(Err(error));
                        return Ok::<(), tokio_rusqlite::rusqlite::Error>(());
                    }
                };

                let column_names: Vec<String> = prepared_statement
                    .column_names()
                    .into_iter()
                    .map(String::from)
                    .collect();
                let column_count = column_names.len();

                let mut output_buffer = Vec::with_capacity(CHUNK_SIZE + 4096);
                let mut csv_writer = CsvWriter::new(&mut output_buffer);

                if !column_names.is_empty() {
                    let header_names: Vec<&str> = column_names.iter().map(String::as_str).collect();
                    if let Err(error) = csv_writer.write_header(header_names) {
                        tracing::error!("Failed to write CSV header: {error}");
                        let _ = chunk_sender
                            .blocking_send(Err(std::io::Error::other(error.to_string())));
                        return Ok(());
                    }
                }

                let mut query_rows = match prepared_statement.query([]) {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::error!("Failed to execute query: {error}");
                        let _ = chunk_sender
                            .blocking_send(Err(std::io::Error::other(error.to_string())));
                        return Ok(());
                    }
                };

                loop {
                    match query_rows.next() {
                        Ok(Some(row)) => {
                            if let Err(error) = csv_writer.write_row(row, column_count) {
                                tracing::error!("Failed to write CSV row: {error}");
                                let _ = chunk_sender
                                    .blocking_send(Err(std::io::Error::other(error.to_string())));
                                return Ok(());
                            }

                            if csv_writer.get_ref().len() >= CHUNK_SIZE {
                                let chunk_bytes = Bytes::copy_from_slice(csv_writer.get_ref());
                                csv_writer.get_mut().clear();
                                if chunk_sender.blocking_send(Ok(chunk_bytes)).is_err() {
                                    return Ok(()); // Client disconnected early
                                }
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            tracing::error!("Failed to fetch next row: {error}");
                            let _ = chunk_sender
                                .blocking_send(Err(std::io::Error::other(error.to_string())));
                            return Ok(());
                        }
                    }
                }

                if !csv_writer.get_ref().is_empty() {
                    let chunk_bytes = Bytes::copy_from_slice(csv_writer.get_ref());
                    csv_writer.get_mut().clear();
                    let _ = chunk_sender.blocking_send(Ok(chunk_bytes));
                }

                Ok(())
            })
            .await;

        if let Err(error) = result {
            tracing::error!("tokio_rusqlite query stream error: {error}");
        }
    });

    match prepared_receiver.await {
        Ok(Ok(())) => Ok(Body::from_stream(ReceiverStream::new(chunk_receiver))),
        Ok(Err(error)) => Err(AppError::BadRequest(format!(
            "SQL query error: {error}\r\n"
        ))),
        Err(_) => Err(AppError::BadRequest(
            "Failed to initialize query stream\r\n".to_string(),
        )),
    }
}

/// Executes a SQL query against the database and streams the Parquet result
/// in chunks as an Axum `Body`. Fails early if the SQL statement cannot be prepared.
pub async fn query_as_parquet_stream(
    connection: &Connection,
    sql_query: String,
) -> Result<Body, AppError> {
    let (chunk_sender, chunk_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let (prepared_sender, prepared_receiver) = tokio::sync::oneshot::channel();
    let connection_clone = connection.clone();

    tokio::spawn(async move {
        let result = connection_clone
            .call(move |raw_connection| {
                let mut prepared_statement = match raw_connection.prepare(&sql_query) {
                    Ok(statement) => {
                        let _ = prepared_sender.send(Ok(()));
                        statement
                    }
                    Err(error) => {
                        let _ = prepared_sender.send(Err(error));
                        return Ok::<(), tokio_rusqlite::rusqlite::Error>(());
                    }
                };

                let column_metadata = extract_column_metadata(&prepared_statement);

                let mut query_rows = match prepared_statement.query([]) {
                    Ok(rows) => rows,
                    Err(error) => {
                        tracing::error!("Failed to execute query: {error}");
                        let _ = chunk_sender
                            .blocking_send(Err(std::io::Error::other(error.to_string())));
                        return Ok(());
                    }
                };

                let first_row = match query_rows.next() {
                    Ok(row) => row,
                    Err(error) => {
                        tracing::error!("Failed to fetch initial row: {error}");
                        let _ = chunk_sender
                            .blocking_send(Err(std::io::Error::other(error.to_string())));
                        return Ok(());
                    }
                };

                let schema = infer_schema_from_metadata(&column_metadata, first_row);
                let chunk_writer = ChunkWriter::new(chunk_sender.clone(), CHUNK_SIZE);
                let mut parquet_writer = match create_parquet_writer(chunk_writer, schema.clone()) {
                    Ok(writer) => writer,
                    Err(error) => {
                        tracing::error!("Failed to initialize Parquet writer: {error}");
                        let _ = chunk_sender
                            .blocking_send(Err(std::io::Error::other(error.to_string())));
                        return Ok(());
                    }
                };

                let mut accumulator = RecordBatchAccumulator::new(schema, ROW_BATCH_SIZE);
                let has_first_row = first_row.is_some();

                if let Some(row) = first_row
                    && let Err(error) = accumulator.append_row(row)
                {
                    tracing::error!("Failed to append first row: {error}");
                    let _ =
                        chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
                    return Ok(());
                }

                if has_first_row {
                    loop {
                        match query_rows.next() {
                            Ok(Some(row)) => {
                                if let Err(error) = accumulator.append_row(row) {
                                    tracing::error!("Failed to append Parquet row: {error}");
                                    let _ = chunk_sender.blocking_send(Err(std::io::Error::other(
                                        error.to_string(),
                                    )));
                                    return Ok(());
                                }

                                if accumulator.is_full() {
                                    match accumulator.finish_batch() {
                                        Ok(batch) => {
                                            if let Err(error) = parquet_writer.write(&batch) {
                                                tracing::error!(
                                                    "Failed to write Parquet batch: {error}"
                                                );
                                                let _ = chunk_sender.blocking_send(Err(
                                                    std::io::Error::other(error.to_string()),
                                                ));
                                                return Ok(());
                                            }
                                        }
                                        Err(error) => {
                                            tracing::error!(
                                                "Failed to create RecordBatch: {error}"
                                            );
                                            let _ = chunk_sender.blocking_send(Err(
                                                std::io::Error::other(error.to_string()),
                                            ));
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                tracing::error!("Failed to fetch next row: {error}");
                                let _ = chunk_sender
                                    .blocking_send(Err(std::io::Error::other(error.to_string())));
                                return Ok(());
                            }
                        }
                    }
                }

                if !accumulator.is_empty() || !has_first_row {
                    match accumulator.finish_batch() {
                        Ok(batch) => {
                            if let Err(error) = parquet_writer.write(&batch) {
                                tracing::error!("Failed to write final Parquet batch: {error}");
                                let _ = chunk_sender
                                    .blocking_send(Err(std::io::Error::other(error.to_string())));
                                return Ok(());
                            }
                        }
                        Err(error) => {
                            tracing::error!("Failed to finish final RecordBatch: {error}");
                            let _ = chunk_sender
                                .blocking_send(Err(std::io::Error::other(error.to_string())));
                            return Ok(());
                        }
                    }
                }

                if let Err(error) = parquet_writer.close() {
                    tracing::error!("Failed to close Parquet writer: {error}");
                    let _ =
                        chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
                    return Ok(());
                }

                Ok(())
            })
            .await;

        if let Err(error) = result {
            tracing::error!("tokio_rusqlite Parquet query stream error: {error}");
        }
    });

    match prepared_receiver.await {
        Ok(Ok(())) => Ok(Body::from_stream(ReceiverStream::new(chunk_receiver))),
        Ok(Err(error)) => Err(AppError::BadRequest(format!(
            "SQL query error: {error}\r\n"
        ))),
        Err(_) => Err(AppError::BadRequest(
            "Failed to initialize Parquet query stream\r\n".to_string(),
        )),
    }
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
            let column_names: Vec<String> = prepared_statement
                .column_names()
                .into_iter()
                .map(String::from)
                .collect();
            let column_count = column_names.len();

            let mut output_buffer = Vec::with_capacity(CHUNK_SIZE);
            let mut csv_writer = CsvWriter::new(&mut output_buffer);

            if !column_names.is_empty() {
                let header_names: Vec<&str> = column_names.iter().map(String::as_str).collect();
                csv_writer.write_header(header_names).map_err(|error| {
                    tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                })?;
            }

            let mut query_rows = prepared_statement.query([])?;
            while let Some(row) = query_rows.next()? {
                csv_writer.write_row(row, column_count)?;
            }

            let csv_string = String::from_utf8(output_buffer).map_err(|error| {
                tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
            })?;

            Ok(csv_string)
        })
        .await
}

/// Executes a SQL query against the database and returns the result formatted
/// as Parquet bytes in a single `Vec<u8>`.
pub async fn query_as_parquet(
    connection: &Connection,
    sql_query: String,
) -> Result<Vec<u8>, AppError> {
    connection
        .call(move |raw_connection| {
            let mut prepared_statement = raw_connection.prepare(&sql_query)?;
            let column_metadata = extract_column_metadata(&prepared_statement);
            let mut query_rows = prepared_statement.query([])?;
            let first_row = query_rows.next()?;

            let schema = infer_schema_from_metadata(&column_metadata, first_row);
            let mut buffer = Vec::new();
            let mut parquet_writer =
                create_parquet_writer(&mut buffer, schema.clone()).map_err(|error| {
                    tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                })?;

            let mut accumulator = RecordBatchAccumulator::new(schema, ROW_BATCH_SIZE);
            let has_first_row = first_row.is_some();

            if let Some(row) = first_row {
                accumulator.append_row(row)?;
            }

            if has_first_row {
                while let Some(row) = query_rows.next()? {
                    accumulator.append_row(row)?;
                    if accumulator.is_full() {
                        let batch = accumulator.finish_batch().map_err(|error| {
                            tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?;
                        parquet_writer.write(&batch).map_err(|error| {
                            tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                        })?;
                    }
                }
            }

            if !accumulator.is_empty() || !has_first_row {
                let batch = accumulator.finish_batch().map_err(|error| {
                    tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                })?;
                parquet_writer.write(&batch).map_err(|error| {
                    tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
                })?;
            }

            parquet_writer.close().map_err(|error| {
                tokio_rusqlite::rusqlite::Error::ToSqlConversionFailure(Box::new(error))
            })?;

            Ok(buffer)
        })
        .await
        .map_err(AppError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::RecordBatchReader;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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

        let body = query_as_csv_stream(&connection, "SELECT * FROM items ORDER BY id".to_string())
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(text, "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n");
    }

    #[tokio::test]
    async fn test_query_as_parquet_and_stream() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE products (id INTEGER, name TEXT, price REAL, in_stock BOOLEAN);
                     INSERT INTO products VALUES (1, 'widget', 19.99, 1);
                     INSERT INTO products VALUES (2, 'gadget', 49.95, 0);",
                )
            })
            .await
            .unwrap();

        let parquet_bytes = query_as_parquet(
            &connection,
            "SELECT * FROM products ORDER BY id".to_string(),
        )
        .await
        .unwrap();
        assert_eq!(&parquet_bytes[0..4], b"PAR1");

        let reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes)).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4);

        let body = query_as_parquet_stream(
            &connection,
            "SELECT * FROM products ORDER BY id".to_string(),
        )
        .await
        .unwrap();
        let stream_bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        assert_eq!(&stream_bytes[0..4], b"PAR1");

        let stream_reader_builder = ParquetRecordBatchReaderBuilder::try_new(stream_bytes).unwrap();
        let mut stream_reader = stream_reader_builder.build().unwrap();
        let stream_batch = stream_reader.next().unwrap().unwrap();
        assert_eq!(stream_batch.num_rows(), 2);
        assert_eq!(stream_batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_query_as_parquet_empty_result() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch("CREATE TABLE empty_table (id INTEGER, label TEXT);")
            })
            .await
            .unwrap();

        let body = query_as_parquet_stream(&connection, "SELECT * FROM empty_table".to_string())
            .await
            .unwrap();
        let stream_bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        assert_eq!(&stream_bytes[0..4], b"PAR1");

        let stream_reader_builder = ParquetRecordBatchReaderBuilder::try_new(stream_bytes).unwrap();
        assert_eq!(
            stream_reader_builder.metadata().file_metadata().num_rows(),
            0
        );
        let mut stream_reader = stream_reader_builder.build().unwrap();
        assert_eq!(stream_reader.schema().fields().len(), 2);
        if let Some(batch_res) = stream_reader.next() {
            let stream_batch = batch_res.unwrap();
            assert_eq!(stream_batch.num_rows(), 0);
            assert_eq!(stream_batch.num_columns(), 2);
        }
    }

    #[tokio::test]
    async fn test_query_as_csv_stream_invalid_query() {
        let connection = Connection::open_in_memory().await.unwrap();
        let result =
            query_as_csv_stream(&connection, "SELECT * FROM nonexistent_table".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_query_as_parquet_stream_invalid_query() {
        let connection = Connection::open_in_memory().await.unwrap();
        let result =
            query_as_parquet_stream(&connection, "SELECT * FROM nonexistent_table".to_string())
                .await;
        assert!(result.is_err());
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
