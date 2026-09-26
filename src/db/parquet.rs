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
//! Parquet query execution, multi-threaded streaming pipeline, and batch encoding.
//!

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_rusqlite::Connection;
use tokio_rusqlite::rusqlite::{Error as RusqliteError, Rows};
use tokio_stream::wrappers::ReceiverStream;

use super::chunk_writer::ChunkWriter;
use super::{CHANNEL_CAPACITY, CHUNK_SIZE};
use crate::AppError;
use crate::parquet_format::{
    RecordBatchAccumulator, create_parquet_writer, extract_column_metadata,
    infer_schema_from_metadata,
};

const ROW_BATCH_SIZE: usize = 65_356; // Max rows per Arrow RecordBatch

const RECORD_BATCH_CHANNEL_CAPACITY: usize = 8; // Max 8 RecordBatches queued (~2.5 MB)

/// Reads SQLite rows, accumulates them into Arrow [`RecordBatch`]es, and sends them to the channel.
fn produce_parquet_batches(
    rows: &mut Rows<'_>,
    mut accumulator: RecordBatchAccumulator,
    has_first_row: bool,
    batch_sender: &mpsc::Sender<RecordBatch>,
    row_counter: Option<&AtomicU64>,
) -> Result<(), RusqliteError> {
    if has_first_row {
        while let Some(row) = rows.next()? {
            if let Some(counter) = row_counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            accumulator.append_row(row)?;

            if accumulator.is_full() {
                let batch = accumulator
                    .finish_batch()
                    .map_err(|error| RusqliteError::ToSqlConversionFailure(Box::new(error)))?;
                if batch_sender.blocking_send(batch).is_err() {
                    return Ok(()); // Consumer disconnected
                }
            }
        }
    }

    if !accumulator.is_empty() || !has_first_row {
        let batch = accumulator
            .finish_batch()
            .map_err(|error| RusqliteError::ToSqlConversionFailure(Box::new(error)))?;
        let _ = batch_sender.blocking_send(batch);
    }

    Ok(())
}

/// Consumes Arrow [`RecordBatch`]es from a channel, encodes them into Parquet format,
/// and streams the chunks out.
fn consume_parquet_batches(
    schema: SchemaRef,
    mut batch_receiver: mpsc::Receiver<RecordBatch>,
    chunk_sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
) {
    let chunk_writer = ChunkWriter::new(chunk_sender.clone(), CHUNK_SIZE);
    let mut parquet_writer = match create_parquet_writer(chunk_writer, schema) {
        Ok(writer) => writer,
        Err(error) => {
            tracing::error!("Failed to initialize Parquet writer: {error}");
            let _ = chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
            return;
        }
    };

    let mut stream_cancelled = false;
    while let Some(batch) = batch_receiver.blocking_recv() {
        if let Err(error) = parquet_writer.write(&batch) {
            if !chunk_sender.is_closed() {
                tracing::error!("Failed to write Parquet batch: {error}");
                let _ = chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
            }
            stream_cancelled = true;
            break;
        }
    }

    if !stream_cancelled
        && let Err(error) = parquet_writer.close()
        && !chunk_sender.is_closed()
    {
        tracing::error!("Failed to close Parquet writer: {error}");
        let _ = chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
    }
}

/// Prepares the statement, negotiates schema, and streams batches from the raw SQLite connection.
fn execute_parquet_producer(
    raw_connection: &tokio_rusqlite::rusqlite::Connection,
    sql_query: &str,
    schema_sender: tokio::sync::oneshot::Sender<Result<SchemaRef, RusqliteError>>,
    batch_sender: &mpsc::Sender<RecordBatch>,
    chunk_sender: &mpsc::Sender<Result<Bytes, std::io::Error>>,
    row_counter: Option<&AtomicU64>,
) -> Result<(), RusqliteError> {
    let mut prepared_statement = match raw_connection.prepare(sql_query) {
        Ok(statement) => statement,
        Err(error) => {
            let _ = schema_sender.send(Err(error));
            return Ok(());
        }
    };

    let column_metadata = extract_column_metadata(&prepared_statement);

    let mut query_rows = match prepared_statement.query([]) {
        Ok(rows) => rows,
        Err(error) => {
            tracing::error!("Failed to execute query: {error}");
            let _ = schema_sender.send(Err(error));
            return Ok(());
        }
    };

    let first_row = match query_rows.next() {
        Ok(row) => row,
        Err(error) => {
            tracing::error!("Failed to fetch initial row: {error}");
            let _ = schema_sender.send(Err(error));
            return Ok(());
        }
    };

    let schema = infer_schema_from_metadata(&column_metadata, first_row);
    let has_first_row = first_row.is_some();

    if schema_sender.send(Ok(schema.clone())).is_err() {
        return Ok(());
    }

    let mut accumulator = RecordBatchAccumulator::new(schema, ROW_BATCH_SIZE);

    if let Some(row) = first_row {
        if let Some(counter) = row_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(error) = accumulator.append_row(row) {
            tracing::error!("Failed to append first row: {error}");
            let _ = chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
            return Ok(());
        }
    }

    if let Err(error) = produce_parquet_batches(
        &mut query_rows,
        accumulator,
        has_first_row,
        batch_sender,
        row_counter,
    ) && !chunk_sender.is_closed()
    {
        tracing::error!("Failed to produce Parquet batches: {error}");
        let _ = chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
    }

    Ok(())
}

/// Executes a SQL query against the database and streams the Parquet result
/// in chunks as a `ReceiverStream`. RecordBatch generation and Parquet encoding
/// run concurrently across threads for optimal throughput.
pub async fn query_as_parquet_stream(
    connection: &Connection,
    sql_query: String,
    row_counter: Option<Arc<AtomicU64>>,
) -> Result<ReceiverStream<Result<Bytes, std::io::Error>>, AppError> {
    let (chunk_sender, chunk_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let (schema_sender, schema_receiver) = tokio::sync::oneshot::channel();
    let (batch_sender, batch_receiver) = mpsc::channel(RECORD_BATCH_CHANNEL_CAPACITY);
    let connection_clone = connection.clone();
    let chunk_sender_clone = chunk_sender.clone();

    let producer_task = tokio::spawn(async move {
        let result = connection_clone
            .call(move |raw_connection| {
                execute_parquet_producer(
                    raw_connection,
                    &sql_query,
                    schema_sender,
                    &batch_sender,
                    &chunk_sender_clone,
                    row_counter.as_deref(),
                )
            })
            .await;

        if let Err(error) = result {
            tracing::error!("tokio_rusqlite Parquet query stream error: {error}");
        }
    });

    let schema = match schema_receiver.await {
        Ok(Ok(schema)) => schema,
        Ok(Err(error)) => {
            return Err(AppError::BadRequest(format!(
                "SQL query error: {error}\r\n"
            )));
        }
        Err(_) => {
            return Err(AppError::BadRequest(
                "Failed to initialize Parquet query stream\r\n".to_string(),
            ));
        }
    };

    let consumer_handle = tokio::task::spawn_blocking(move || {
        consume_parquet_batches(schema, batch_receiver, chunk_sender);
    });

    tokio::spawn(async move {
        let _ = tokio::join!(producer_task, consumer_handle);
    });

    Ok(ReceiverStream::new(chunk_receiver))
}
