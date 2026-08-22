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
//! Core library for `readql`, providing SQLite database access, RFC 4180 CSV
//! serialization, and Axum HTTP routing to stream query results.
//!

/// CSV formatting, escaping, and record serialization.
pub mod csv;

/// SQLite database connection setup and query execution.
pub mod db;

/// Application error types and HTTP response conversions.
pub mod error;
pub use error::AppError;

/// Axum HTTP request handlers and routing.
pub mod handlers;
