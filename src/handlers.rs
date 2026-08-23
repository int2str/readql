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

    let body = db::query_as_csv_stream(&db, sql);

    Ok(([(header::CONTENT_TYPE, "text/csv; charset=utf-8")], body))
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

    #[tokio::test]
    async fn test_root_handler_stream() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let db = Connection::open_in_memory().await.unwrap();
        db.call(|conn| {
            conn.execute_batch(
                "CREATE TABLE products (id INTEGER, name TEXT);
                 INSERT INTO products VALUES (10, 'widget');",
            )
        })
        .await
        .unwrap();

        let app = create_router(db);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+*+FROM+products")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/csv; charset=utf-8"
        );

        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_str = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(body_str, "id,name\r\n10,widget\r\n");
    }

    #[tokio::test]
    async fn test_root_handler_empty_query() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let db = Connection::open_in_memory().await.unwrap();
        let app = create_router(db);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
