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
//! Axum HTTP routes and request handlers for executing queries.
//!

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{Router, get};
use serde::Deserialize;
use tokio_rusqlite::Connection;

use crate::AppError;
use crate::db;

/// Query parameters extracted from the HTTP request URL.
#[derive(Deserialize)]
pub struct SqlParams {
    /// The SQL query string to execute.
    pub sql: Option<String>,
}

/// Strips unnecessary line-breaks, tabs, duplicate spaces, and surrounding whitespace from a SQL query.
pub fn clean_query(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Creates the Axum router configured with the shared SQLite connection state.
pub fn create_router(db: Connection) -> Router {
    Router::new().route("/", get(root)).with_state(db)
}

/// Handles HTTP `GET /` requests to execute SQL queries and stream CSV results.
pub async fn root(
    ConnectInfo(client_address): ConnectInfo<SocketAddr>,
    State(db): State<Connection>,
    Query(params): Query<SqlParams>,
) -> Result<impl IntoResponse, AppError> {
    let sql = clean_query(&params.sql.unwrap_or_default());
    let client_ip = client_address.ip();
    tracing::info!("{client_ip} | {sql}");

    if sql.is_empty() {
        return Err(AppError::BadRequest("Error: No SQL query provided\r\n"));
    }

    let csv_data = db::query_as_csv(&db, sql).await?;

    Ok((
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        csv_data,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clean_query_basic() {
        assert_eq!(clean_query("SELECT * FROM users"), "SELECT * FROM users");
    }

    #[test]
    fn test_clean_query_line_breaks_and_tabs() {
        let input = "SELECT\n    id,\n    name\nFROM\n    users\nWHERE\n    age > 20";
        assert_eq!(
            clean_query(input),
            "SELECT id, name FROM users WHERE age > 20"
        );

        let input_crlf = "SELECT\r\n\tid,\r\n\tname\r\nFROM\r\n\tusers";
        assert_eq!(clean_query(input_crlf), "SELECT id, name FROM users");
    }

    #[test]
    fn test_clean_query_extra_whitespace() {
        assert_eq!(
            clean_query("   SELECT    *    FROM    users   "),
            "SELECT * FROM users"
        );
    }

    #[test]
    fn test_clean_query_empty_and_whitespace_only() {
        assert_eq!(clean_query(""), "");
        assert_eq!(clean_query("   \n\t\r\n   "), "");
    }
}
