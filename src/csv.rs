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
//! values and records.
//!

use tokio_rusqlite::rusqlite::types::ValueRef;
use tokio_rusqlite::rusqlite::{Error, Row};

/// Escapes and writes a string field into a CSV buffer according to RFC 4180.
///
/// Fields containing commas, double quotes, or newlines are enclosed in double quotes.
#[inline]
pub fn write_escaped_field(output: &mut String, field: &str) {
    if field.contains([',', '"', '\r', '\n']) {
        output.push('"');
        for character in field.chars() {
            if character == '"' {
                output.push('"');
            }
            output.push(character);
        }
        output.push('"');
    } else {
        output.push_str(field);
    }
}

/// Reusable formatting buffers for CSV serialization across rows and columns.
#[derive(Default)]
pub struct CsvFormatter {
    itoa_buffer: itoa::Buffer,
    ryu_buffer: ryu::Buffer,
}

impl CsvFormatter {
    /// Creates a new `CsvFormatter` with initialized `itoa` and `ryu` buffers.
    #[inline]
    pub fn new() -> Self {
        Self {
            itoa_buffer: itoa::Buffer::new(),
            ryu_buffer: ryu::Buffer::new(),
        }
    }

    /// Formats and writes a SQLite column value into a CSV buffer reusing internal buffers.
    #[inline]
    pub fn write_value(&mut self, output: &mut String, value: ValueRef<'_>) {
        match value {
            ValueRef::Null => {}
            ValueRef::Integer(integer_value) => {
                output.push_str(self.itoa_buffer.format(integer_value));
            }
            ValueRef::Real(floating_point_value) => {
                output.push_str(self.ryu_buffer.format(floating_point_value));
            }
            ValueRef::Text(bytes) | ValueRef::Blob(bytes) => match std::str::from_utf8(bytes) {
                Ok(text) => write_escaped_field(output, text),
                Err(_) => {
                    let text = String::from_utf8_lossy(bytes);
                    write_escaped_field(output, &text);
                }
            },
        }
    }

    /// Formats a full SQLite row into CSV with zero per-column closure overhead.
    #[inline]
    pub fn write_row(
        &mut self,
        output: &mut String,
        row: &Row<'_>,
        column_count: usize,
    ) -> Result<(), Error> {
        for column_index in 0..column_count {
            if column_index > 0 {
                output.push(',');
            }
            let value = row.get_ref(column_index)?;
            self.write_value(output, value);
        }
        output.push_str("\r\n");
        Ok(())
    }
}

/// Formats and writes a SQLite column value into a CSV buffer without heap allocations.
#[inline]
pub fn write_value(output: &mut String, value: ValueRef<'_>) {
    let mut formatter = CsvFormatter::new();
    formatter.write_value(output, value);
}

/// Writes a CSV header record from an iterator yielding column name string slices.
#[inline]
pub fn write_header<'a, FieldIterator>(output: &mut String, fields: FieldIterator)
where
    FieldIterator: IntoIterator<Item = &'a str>,
{
    let mut is_first_column = true;
    for field in fields {
        if !is_first_column {
            output.push(',');
        }
        is_first_column = false;
        write_escaped_field(output, field);
    }
    output.push_str("\r\n");
}

/// Formats a full SQLite row into CSV with zero per-column closure overhead.
#[inline]
pub fn write_row(output: &mut String, row: &Row<'_>, column_count: usize) -> Result<(), Error> {
    let mut formatter = CsvFormatter::new();
    formatter.write_row(output, row, column_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_escaped_field() {
        let mut output = String::new();
        write_escaped_field(&mut output, "plain");
        assert_eq!(output, "plain");

        output.clear();
        write_escaped_field(&mut output, "hello, world");
        assert_eq!(output, "\"hello, world\"");

        output.clear();
        write_escaped_field(&mut output, "with \"quotes\"");
        assert_eq!(output, "\"with \"\"quotes\"\"\"");

        output.clear();
        write_escaped_field(&mut output, "line1\nline2");
        assert_eq!(output, "\"line1\nline2\"");

        output.clear();
        write_escaped_field(&mut output, "line1\r\nline2");
        assert_eq!(output, "\"line1\r\nline2\"");
    }

    #[test]
    fn test_write_header() {
        let mut output = String::new();
        write_header(&mut output, ["id", "name", "desc with, comma"]);
        assert_eq!(output, "id,name,\"desc with, comma\"\r\n");
    }

    #[test]
    fn test_write_value_formatting() {
        let mut output = String::new();
        write_value(&mut output, ValueRef::Integer(42));
        assert_eq!(output, "42");

        output.clear();
        write_value(&mut output, ValueRef::Real(123.456));
        assert_eq!(output, "123.456");

        output.clear();
        write_value(&mut output, ValueRef::Null);
        assert_eq!(output, "");

        output.clear();
        write_value(&mut output, ValueRef::Text(b"hello, world"));
        assert_eq!(output, "\"hello, world\"");
    }

    #[test]
    fn test_csv_formatter_reuse() {
        let mut formatter = CsvFormatter::new();
        let mut output = String::new();

        formatter.write_value(&mut output, ValueRef::Integer(100));
        assert_eq!(output, "100");

        output.push(',');
        formatter.write_value(&mut output, ValueRef::Real(456.789));
        assert_eq!(output, "100,456.789");

        output.push(',');
        formatter.write_value(&mut output, ValueRef::Integer(-50));
        assert_eq!(output, "100,456.789,-50");

        output.push(',');
        formatter.write_value(&mut output, ValueRef::Null);
        assert_eq!(output, "100,456.789,-50,");

        output.push(',');
        formatter.write_value(&mut output, ValueRef::Text(b"sample,text"));
        assert_eq!(output, "100,456.789,-50,,\"sample,text\"");
    }
}
