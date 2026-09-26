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
//! CSV query execution and streaming.
//!

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_rusqlite::Connection;
use tokio_rusqlite::rusqlite::{Error as RusqliteError, Statement};
use tokio_stream::wrappers::ReceiverStream;

use super::chunk_writer::ChunkWriter;
use super::{CHANNEL_CAPACITY, CHUNK_SIZE};
use crate::AppError;
use crate::csv::CsvWriter;

/// Serializes query results into a CSV writer, outputting headers and all rows.
fn write_csv_results<Writer: Write>(
    statement: &mut Statement<'_>,
    csv_writer: &mut CsvWriter<Writer>,
    row_counter: Option<&AtomicU64>,
) -> Result<(), RusqliteError> {
    let column_names: Vec<String> = statement
        .column_names()
        .into_iter()
        .map(String::from)
        .collect();
    let column_count = column_names.len();

    if !column_names.is_empty() {
        let header_names: Vec<&str> = column_names.iter().map(String::as_str).collect();
        csv_writer
            .write_header(header_names)
            .map_err(|error| RusqliteError::ToSqlConversionFailure(Box::new(error)))?;
    }

    let mut query_rows = statement.query([])?;
    while let Some(row) = query_rows.next()? {
        if let Some(counter) = row_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        csv_writer.write_row(row, column_count)?;
    }

    csv_writer
        .flush()
        .map_err(|error| RusqliteError::ToSqlConversionFailure(Box::new(error)))?;

    Ok(())
}

/// Executes a SQL query against the database and streams the RFC 4180 CSV result
/// in chunks as a `ReceiverStream`. Fails early if the SQL statement cannot be prepared.
pub async fn query_as_csv_stream(
    connection: &Connection,
    sql_query: String,
    row_counter: Option<Arc<AtomicU64>>,
) -> Result<ReceiverStream<Result<Bytes, std::io::Error>>, AppError> {
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
                        return Ok::<(), RusqliteError>(());
                    }
                };

                let chunk_writer = ChunkWriter::new(chunk_sender.clone(), CHUNK_SIZE);
                let mut csv_writer = CsvWriter::new(chunk_writer);

                if let Err(error) = write_csv_results(
                    &mut prepared_statement,
                    &mut csv_writer,
                    row_counter.as_deref(),
                ) && !chunk_sender.is_closed()
                {
                    tracing::error!("CSV stream error: {error}");
                    let _ =
                        chunk_sender.blocking_send(Err(std::io::Error::other(error.to_string())));
                }

                Ok(())
            })
            .await;

        if let Err(error) = result {
            tracing::error!("tokio_rusqlite CSV query stream error: {error}");
        }
    });

    match prepared_receiver.await {
        Ok(Ok(())) => Ok(ReceiverStream::new(chunk_receiver)),
        Ok(Err(error)) => Err(AppError::BadRequest(format!(
            "SQL query error: {error}\r\n"
        ))),
        Err(_) => Err(AppError::BadRequest(
            "Failed to initialize query stream\r\n".to_string(),
        )),
    }
}
