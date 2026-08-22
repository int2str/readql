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
    let sql = params.sql.unwrap_or_default();
    tracing::info!("{} | SQL: [{:?}]", client_address, sql);

    if sql.is_empty() {
        return Err(AppError::BadRequest("Error: No SQL query provided\r\n"));
    }

    let csv_data = db::query_as_csv(&db, sql).await?;

    Ok((
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        csv_data,
    ))
}
