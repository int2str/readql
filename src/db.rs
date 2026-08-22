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

use tokio_rusqlite::{Connection, OpenFlags};

use crate::csv::{write_header, write_row};

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

/// Executes a SQL query against the database and returns the result formatted
/// as RFC 4180 CSV.
pub async fn query_as_csv(db: &Connection, sql: String) -> Result<String, tokio_rusqlite::Error> {
    db.call(move |conn| {
        let mut prepared_statement = conn.prepare(&sql)?;
        let column_names = prepared_statement.column_names();
        let column_count = column_names.len();

        let mut csv_out = String::with_capacity(4096);

        if !column_names.is_empty() {
            write_header(&mut csv_out, column_names.iter().copied());
        }

        let mut rows = prepared_statement.query([])?;
        while let Some(row) = rows.next()? {
            write_row(&mut csv_out, row, column_count)?;
        }

        Ok(csv_out)
    })
    .await
}
