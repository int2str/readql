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
//! SQLite database connection pool and connection lifecycle management.
//!

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio_rusqlite::{Connection, OpenFlags};

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
                 PRAGMA cache_size = -65536;
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
