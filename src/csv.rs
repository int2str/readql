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
//! CSV formatting utilities conforming to RFC 4180 for escaping and streaming
//! values and records directly to byte writers.
//!

use std::io::{self, Write};

use tokio_rusqlite::rusqlite::types::ValueRef;
use tokio_rusqlite::rusqlite::{Error as RusqliteError, Row};

/// A streaming CSV writer conforming to RFC 4180.
pub struct CsvWriter<Writer> {
    writer: Writer,
    itoa_buffer: itoa::Buffer,
    ryu_buffer: ryu::Buffer,
}

impl<Writer: Write> CsvWriter<Writer> {
    /// Creates a new `CsvWriter` wrapping the provided writer.
    #[inline]
    pub fn new(writer: Writer) -> Self {
        Self {
            writer,
            itoa_buffer: itoa::Buffer::new(),
            ryu_buffer: ryu::Buffer::new(),
        }
    }

    /// Writes a single field slice, escaping inner quotes and wrapping in quotes if needed.
    #[inline]
    pub fn write_escaped_field(&mut self, field_bytes: &[u8]) -> io::Result<()> {
        let requires_quotes = field_bytes
            .iter()
            .any(|&byte| matches!(byte, b',' | b'"' | b'\r' | b'\n'));

        if !requires_quotes {
            return self.writer.write_all(field_bytes);
        }

        self.writer.write_all(b"\"")?;
        let mut slice_start_index = 0;
        for (current_index, &byte) in field_bytes.iter().enumerate() {
            if byte == b'"' {
                self.writer
                    .write_all(&field_bytes[slice_start_index..=current_index])?;
                self.writer.write_all(b"\"")?;
                slice_start_index = current_index + 1;
            }
        }
        if slice_start_index < field_bytes.len() {
            self.writer.write_all(&field_bytes[slice_start_index..])?;
        }
        self.writer.write_all(b"\"")
    }

    /// Formats and writes a SQLite column value into the CSV output.
    #[inline]
    pub fn write_value(&mut self, value: ValueRef<'_>) -> io::Result<()> {
        match value {
            ValueRef::Null => Ok(()),
            ValueRef::Integer(integer_value) => {
                let formatted_slice = self.itoa_buffer.format(integer_value);
                self.writer.write_all(formatted_slice.as_bytes())
            }
            ValueRef::Real(floating_point_value) => {
                let formatted_slice = self.ryu_buffer.format(floating_point_value);
                self.writer.write_all(formatted_slice.as_bytes())
            }
            ValueRef::Text(bytes) | ValueRef::Blob(bytes) => self.write_escaped_field(bytes),
        }
    }

    /// Writes an RFC 4180 CSV header record from an iterator yielding column name string slices.
    #[inline]
    pub fn write_header<'a, FieldIterator>(&mut self, column_names: FieldIterator) -> io::Result<()>
    where
        FieldIterator: IntoIterator<Item = &'a str>,
    {
        let mut is_first_column = true;
        for column_name in column_names {
            if !is_first_column {
                self.writer.write_all(b",")?;
            }
            is_first_column = false;
            self.write_escaped_field(column_name.as_bytes())?;
        }
        self.writer.write_all(b"\r\n")
    }

    /// Formats and writes a full SQLite row into CSV.
    #[inline]
    pub fn write_row(&mut self, row: &Row<'_>, column_count: usize) -> Result<(), RusqliteError> {
        for column_index in 0..column_count {
            if column_index > 0 {
                self.writer.write_all(b",").map_err(|io_error| {
                    RusqliteError::ToSqlConversionFailure(Box::new(io_error))
                })?;
            }
            let column_value = row.get_ref(column_index)?;
            self.write_value(column_value)
                .map_err(|io_error| RusqliteError::ToSqlConversionFailure(Box::new(io_error)))?;
        }
        self.writer
            .write_all(b"\r\n")
            .map_err(|io_error| RusqliteError::ToSqlConversionFailure(Box::new(io_error)))?;
        Ok(())
    }

    /// Returns an immutable reference to the underlying writer.
    #[inline]
    pub fn get_ref(&self) -> &Writer {
        &self.writer
    }

    /// Returns a mutable reference to the underlying writer.
    #[inline]
    pub fn get_mut(&mut self) -> &mut Writer {
        &mut self.writer
    }

    /// Consumes the `CsvWriter`, returning the underlying writer.
    #[inline]
    pub fn into_inner(self) -> Writer {
        self.writer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_escaped_field() {
        let mut csv_writer = CsvWriter::new(Vec::new());

        csv_writer.write_escaped_field(b"plain").unwrap();
        assert_eq!(csv_writer.get_ref(), b"plain");

        csv_writer.get_mut().clear();
        csv_writer.write_escaped_field(b"hello, world").unwrap();
        assert_eq!(csv_writer.get_ref(), b"\"hello, world\"");

        csv_writer.get_mut().clear();
        csv_writer.write_escaped_field(b"with \"quotes\"").unwrap();
        assert_eq!(csv_writer.get_ref(), b"\"with \"\"quotes\"\"\"");

        csv_writer.get_mut().clear();
        csv_writer.write_escaped_field(b"line1\nline2").unwrap();
        assert_eq!(csv_writer.get_ref(), b"\"line1\nline2\"");

        csv_writer.get_mut().clear();
        csv_writer.write_escaped_field(b"line1\r\nline2").unwrap();
        assert_eq!(csv_writer.get_ref(), b"\"line1\r\nline2\"");
    }

    #[test]
    fn test_write_header() {
        let mut csv_writer = CsvWriter::new(Vec::new());

        csv_writer
            .write_header(["id", "name", "desc with, comma"])
            .unwrap();
        assert_eq!(
            String::from_utf8(csv_writer.into_inner()).unwrap(),
            "id,name,\"desc with, comma\"\r\n"
        );
    }

    #[test]
    fn test_write_value_formatting() {
        let mut csv_writer = CsvWriter::new(Vec::new());

        csv_writer.write_value(ValueRef::Integer(42)).unwrap();
        assert_eq!(csv_writer.get_ref(), b"42");

        csv_writer.get_mut().clear();
        csv_writer.write_value(ValueRef::Real(123.456)).unwrap();
        assert_eq!(csv_writer.get_ref(), b"123.456");

        csv_writer.get_mut().clear();
        csv_writer.write_value(ValueRef::Null).unwrap();
        assert_eq!(csv_writer.get_ref(), b"");

        csv_writer.get_mut().clear();
        csv_writer
            .write_value(ValueRef::Text(b"hello, world"))
            .unwrap();
        assert_eq!(csv_writer.get_ref(), b"\"hello, world\"");
    }

    #[test]
    fn test_csv_writer_reuse() {
        let mut csv_writer = CsvWriter::new(Vec::new());

        csv_writer.write_value(ValueRef::Integer(100)).unwrap();
        csv_writer.get_mut().extend_from_slice(b",");
        csv_writer.write_value(ValueRef::Real(456.789)).unwrap();
        csv_writer.get_mut().extend_from_slice(b",");
        csv_writer.write_value(ValueRef::Integer(-50)).unwrap();
        csv_writer.get_mut().extend_from_slice(b",");
        csv_writer.write_value(ValueRef::Null).unwrap();
        csv_writer.get_mut().extend_from_slice(b",");
        csv_writer
            .write_value(ValueRef::Text(b"sample,text"))
            .unwrap();

        assert_eq!(
            String::from_utf8(csv_writer.into_inner()).unwrap(),
            "100,456.789,-50,,\"sample,text\""
        );
    }
}
