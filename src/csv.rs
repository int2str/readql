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
pub fn write_escaped_field(out: &mut String, field: &str) {
    if field.contains([',', '"', '\r', '\n']) {
        out.push('"');
        for ch in field.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    } else {
        out.push_str(field);
    }
}

/// Formats and writes a SQLite column value into a CSV buffer without heap allocations.
#[inline]
pub fn write_value(out: &mut String, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => {}
        ValueRef::Integer(i) => {
            let mut buffer = itoa::Buffer::new();
            out.push_str(buffer.format(i));
        }
        ValueRef::Real(f) => {
            let mut buffer = ryu::Buffer::new();
            out.push_str(buffer.format(f));
        }
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => {
            let text = String::from_utf8_lossy(bytes);
            write_escaped_field(out, &text);
        }
    }
}

/// Writes a CSV header record from an iterator yielding column name string slices.
#[inline]
pub fn write_header<'a, I>(out: &mut String, fields: I)
where
    I: IntoIterator<Item = &'a str>,
{
    let mut first = true;
    for field in fields {
        if !first {
            out.push(',');
        }
        first = false;
        write_escaped_field(out, field);
    }
    out.push_str("\r\n");
}

/// Formats a full SQLite row into CSV with zero per-column closure overhead.
#[inline]
pub fn write_row(out: &mut String, row: &Row<'_>, column_count: usize) -> Result<(), Error> {
    for i in 0..column_count {
        if i > 0 {
            out.push(',');
        }
        let value = row.get_ref(i)?;
        write_value(out, value);
    }
    out.push_str("\r\n");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_escaped_field() {
        let mut out = String::new();
        write_escaped_field(&mut out, "plain");
        assert_eq!(out, "plain");

        out.clear();
        write_escaped_field(&mut out, "hello, world");
        assert_eq!(out, "\"hello, world\"");

        out.clear();
        write_escaped_field(&mut out, "with \"quotes\"");
        assert_eq!(out, "\"with \"\"quotes\"\"\"");

        out.clear();
        write_escaped_field(&mut out, "line1\nline2");
        assert_eq!(out, "\"line1\nline2\"");

        out.clear();
        write_escaped_field(&mut out, "line1\r\nline2");
        assert_eq!(out, "\"line1\r\nline2\"");
    }

    #[test]
    fn test_write_header() {
        let mut out = String::new();
        write_header(&mut out, ["id", "name", "desc with, comma"]);
        assert_eq!(out, "id,name,\"desc with, comma\"\r\n");
    }

    #[test]
    fn test_write_value_formatting() {
        let mut out = String::new();
        write_value(&mut out, ValueRef::Integer(42));
        assert_eq!(out, "42");

        out.clear();
        write_value(&mut out, ValueRef::Real(3.14159));
        assert_eq!(out, "3.14159");

        out.clear();
        write_value(&mut out, ValueRef::Null);
        assert_eq!(out, "");

        out.clear();
        write_value(&mut out, ValueRef::Text(b"hello, world"));
        assert_eq!(out, "\"hello, world\"");
    }
}
