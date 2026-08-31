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
//! Parquet serialization, Arrow schema inference, and RecordBatch building for SQLite rows.
//!

use std::io::Write;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::sync::mpsc;
use tokio_rusqlite::rusqlite::types::ValueRef;
use tokio_rusqlite::rusqlite::{Row, Statement};

/// Adapter implementing [`std::io::Write`] that buffers bytes and sends chunks
/// over a Tokio [`mpsc::Sender`] channel for streaming HTTP responses.
pub struct ChunkWriter {
    sender: mpsc::Sender<Result<Bytes, std::io::Error>>,
    buffer: Vec<u8>,
    chunk_size: usize,
}

impl ChunkWriter {
    /// Creates a new `ChunkWriter` with the given channel sender and chunk buffer size.
    pub fn new(sender: mpsc::Sender<Result<Bytes, std::io::Error>>, chunk_size: usize) -> Self {
        Self {
            sender,
            buffer: Vec::with_capacity(chunk_size + 4096),
            chunk_size,
        }
    }
}

impl Write for ChunkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(buf);
        if self.buffer.len() >= self.chunk_size {
            let chunk = Bytes::copy_from_slice(&self.buffer);
            self.buffer.clear();
            if self.sender.blocking_send(Ok(chunk)).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Client disconnected",
                ));
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if !self.buffer.is_empty() {
            let chunk = Bytes::copy_from_slice(&self.buffer);
            self.buffer.clear();
            if self.sender.blocking_send(Ok(chunk)).is_err() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "Client disconnected",
                ));
            }
        }
        Ok(())
    }
}

/// Infers an Arrow [`DataType`] from a column's declared SQLite type and/or sample value.
pub fn infer_arrow_type(decl_type: Option<&str>, sample_value: Option<&ValueRef>) -> DataType {
    if let Some(decl) = decl_type {
        let upper = decl.to_uppercase();
        if upper.contains("INT") {
            return DataType::Int64;
        } else if upper.contains("REAL")
            || upper.contains("FLOA")
            || upper.contains("DOUB")
            || upper.contains("NUM")
            || upper.contains("DEC")
        {
            return DataType::Float64;
        } else if upper.contains("BOOL") {
            return DataType::Boolean;
        } else if upper.contains("BLOB") || upper.contains("BINARY") {
            return DataType::Binary;
        } else {
            return DataType::Utf8;
        }
    }

    if let Some(val) = sample_value {
        match val {
            ValueRef::Integer(_) => DataType::Int64,
            ValueRef::Real(_) => DataType::Float64,
            ValueRef::Blob(_) => DataType::Binary,
            ValueRef::Text(_) => DataType::Utf8,
            ValueRef::Null => DataType::Utf8,
        }
    } else {
        DataType::Utf8
    }
}

/// Column metadata consisting of column name and optional declared type.
pub type ColumnMetadata = (String, Option<String>);

/// Extracts column metadata (names and declared types) from a prepared SQLite statement.
pub fn extract_column_metadata(stmt: &Statement) -> Vec<ColumnMetadata> {
    stmt.columns()
        .into_iter()
        .map(|col| (col.name().to_string(), col.decl_type().map(String::from)))
        .collect()
}

/// Infers the Arrow [`Schema`] from extracted column metadata and an optional sample row.
pub fn infer_schema_from_metadata(
    columns: &[ColumnMetadata],
    sample_row: Option<&Row>,
) -> SchemaRef {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(idx, (name, decl))| {
            let sample_val = sample_row.and_then(|r| r.get_ref(idx).ok());
            let data_type = infer_arrow_type(decl.as_deref(), sample_val.as_ref());
            Field::new(name, data_type, true)
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new(fields))
}

/// Infers the Arrow [`Schema`] for a prepared SQLite statement.
pub fn infer_schema(stmt: &Statement, sample_row: Option<&Row>) -> SchemaRef {
    let columns = extract_column_metadata(stmt);
    infer_schema_from_metadata(&columns, sample_row)
}

enum ColumnAppender {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Binary(BinaryBuilder),
    Utf8(StringBuilder),
}

impl ColumnAppender {
    fn with_capacity(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64)),
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    fn append_value(&mut self, value: ValueRef) {
        match (self, value) {
            (Self::Int64(builder), ValueRef::Integer(i)) => builder.append_value(i),
            (Self::Int64(builder), ValueRef::Real(f)) => builder.append_value(f as i64),
            (Self::Int64(builder), ValueRef::Text(t)) => {
                if let Ok(s) = std::str::from_utf8(t)
                    && let Ok(i) = s.parse::<i64>()
                {
                    builder.append_value(i);
                    return;
                }
                builder.append_null();
            }
            (Self::Int64(builder), _) => builder.append_null(),

            (Self::Float64(builder), ValueRef::Real(f)) => builder.append_value(f),
            (Self::Float64(builder), ValueRef::Integer(i)) => builder.append_value(i as f64),
            (Self::Float64(builder), ValueRef::Text(t)) => {
                if let Ok(s) = std::str::from_utf8(t)
                    && let Ok(f) = s.parse::<f64>()
                {
                    builder.append_value(f);
                    return;
                }
                builder.append_null();
            }
            (Self::Float64(builder), _) => builder.append_null(),

            (Self::Boolean(builder), ValueRef::Integer(i)) => builder.append_value(i != 0),
            (Self::Boolean(builder), ValueRef::Real(f)) => builder.append_value(f != 0.0),
            (Self::Boolean(builder), ValueRef::Text(t)) => {
                if let Ok(s) = std::str::from_utf8(t) {
                    match s.trim().to_lowercase().as_str() {
                        "true" | "1" | "t" | "yes" | "y" => builder.append_value(true),
                        "false" | "0" | "f" | "no" | "n" => builder.append_value(false),
                        _ => builder.append_null(),
                    }
                } else {
                    builder.append_null();
                }
            }
            (Self::Boolean(builder), _) => builder.append_null(),

            (Self::Binary(builder), ValueRef::Blob(b)) => builder.append_value(b),
            (Self::Binary(builder), ValueRef::Text(t)) => builder.append_value(t),
            (Self::Binary(builder), _) => builder.append_null(),

            (Self::Utf8(builder), ValueRef::Text(t)) => {
                let s = std::str::from_utf8(t).unwrap_or("");
                builder.append_value(s);
            }
            (Self::Utf8(builder), ValueRef::Integer(i)) => {
                let mut itoa_buffer = itoa::Buffer::new();
                builder.append_value(itoa_buffer.format(i));
            }
            (Self::Utf8(builder), ValueRef::Real(f)) => {
                let mut ryu_buffer = ryu::Buffer::new();
                builder.append_value(ryu_buffer.format(f));
            }
            (Self::Utf8(builder), ValueRef::Blob(b)) => {
                if let Ok(s) = std::str::from_utf8(b) {
                    builder.append_value(s);
                } else {
                    builder.append_value(format!("<blob {} bytes>", b.len()));
                }
            }
            (Self::Utf8(builder), ValueRef::Null) => builder.append_null(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
        }
    }
}

/// Helper for accumulating SQLite rows into Arrow [`RecordBatch`] instances.
pub struct RecordBatchAccumulator {
    schema: SchemaRef,
    appenders: Vec<ColumnAppender>,
    capacity: usize,
    count: usize,
}

impl RecordBatchAccumulator {
    /// Creates a new accumulator with the given schema and batch capacity.
    pub fn new(schema: SchemaRef, capacity: usize) -> Self {
        let appenders = schema
            .fields()
            .iter()
            .map(|field| ColumnAppender::with_capacity(field.data_type(), capacity))
            .collect();

        Self {
            schema,
            appenders,
            capacity,
            count: 0,
        }
    }

    /// Appends a SQLite row to the columnar appenders.
    pub fn append_row(&mut self, row: &Row<'_>) -> Result<(), tokio_rusqlite::rusqlite::Error> {
        for (col_idx, appender) in self.appenders.iter_mut().enumerate() {
            let val_ref = row.get_ref(col_idx)?;
            appender.append_value(val_ref);
        }
        self.count += 1;
        Ok(())
    }

    /// Returns `true` if the accumulator has reached its batch capacity.
    pub fn is_full(&self) -> bool {
        self.count >= self.capacity
    }

    /// Returns `true` if no rows have been appended to the accumulator.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the number of accumulated rows.
    pub fn len(&self) -> usize {
        self.count
    }

    /// Finishes the current batch into an Arrow [`RecordBatch`] and resets the appenders.
    pub fn finish_batch(&mut self) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let columns = self.appenders.iter_mut().map(|a| a.finish()).collect();
        self.count = 0;
        self.appenders = self
            .schema
            .fields()
            .iter()
            .map(|field| ColumnAppender::with_capacity(field.data_type(), self.capacity))
            .collect();

        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

/// Creates an [`ArrowWriter`] with Zstandard compression for writing Parquet data.
pub fn create_parquet_writer<W: Write + Send>(
    sink: W,
    schema: SchemaRef,
) -> Result<ArrowWriter<W>, parquet::errors::ParquetError> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    ArrowWriter::try_new(sink, schema, Some(properties))
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tokio_rusqlite::rusqlite::Connection;

    #[test]
    fn test_infer_arrow_types() {
        assert_eq!(infer_arrow_type(Some("INTEGER"), None), DataType::Int64);
        assert_eq!(infer_arrow_type(Some("BIGINT"), None), DataType::Int64);
        assert_eq!(infer_arrow_type(Some("REAL"), None), DataType::Float64);
        assert_eq!(infer_arrow_type(Some("FLOAT"), None), DataType::Float64);
        assert_eq!(infer_arrow_type(Some("BOOLEAN"), None), DataType::Boolean);
        assert_eq!(infer_arrow_type(Some("BLOB"), None), DataType::Binary);
        assert_eq!(infer_arrow_type(Some("TEXT"), None), DataType::Utf8);
        assert_eq!(infer_arrow_type(Some("VARCHAR(255)"), None), DataType::Utf8);
        assert_eq!(
            infer_arrow_type(None, Some(&ValueRef::Integer(42))),
            DataType::Int64
        );
        assert_eq!(
            infer_arrow_type(None, Some(&ValueRef::Real(3.14))),
            DataType::Float64
        );
        assert_eq!(
            infer_arrow_type(None, Some(&ValueRef::Text(b"hello"))),
            DataType::Utf8
        );
    }

    #[test]
    fn test_write_and_read_parquet_bytes() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE users (id INTEGER, name TEXT, score REAL, active BOOLEAN, avatar BLOB);
             INSERT INTO users VALUES (1, 'Alice', 95.5, 1, X'010203');
             INSERT INTO users VALUES (2, 'Bob', 82.0, 0, NULL);
             INSERT INTO users VALUES (3, NULL, NULL, NULL, NULL);",
        )
        .unwrap();

        let mut stmt = conn.prepare("SELECT * FROM users ORDER BY id").unwrap();
        let schema = infer_schema(&stmt, None);
        assert_eq!(schema.fields().len(), 5);

        let mut buffer = Vec::new();
        let mut writer = create_parquet_writer(&mut buffer, schema.clone()).unwrap();

        let mut accumulator = RecordBatchAccumulator::new(schema.clone(), 1024);
        let mut rows = stmt.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            accumulator.append_row(row).unwrap();
        }

        let batch = accumulator.finish_batch().unwrap();
        assert_eq!(batch.num_rows(), 3);

        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(!buffer.is_empty());
        assert_eq!(&buffer[0..4], b"PAR1");

        // Verify Parquet file can be read back
        let bytes = Bytes::from(buffer);
        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let read_batch = reader.next().unwrap().unwrap();

        assert_eq!(read_batch.num_rows(), 3);
        assert_eq!(read_batch.num_columns(), 5);
    }
}
