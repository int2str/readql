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

use crate::AppError;
use crate::db::{self, ConnectionPool};

/// Query parameters extracted from the HTTP request URL.
#[derive(Deserialize)]
pub struct SqlParameters {
    /// The SQL query string to execute.
    pub sql: Option<String>,
}

/// Formats a SQL query as a single line for structured logging without mutating the underlying SQL query.
pub fn format_query_for_logging(sql_query: &str) -> String {
    sql_query.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Creates the Axum router configured with the shared SQLite connection pool state.
pub fn create_router(connection_pool: ConnectionPool) -> Router {
    Router::new()
        .route("/", get(root))
        .with_state(connection_pool)
}

/// Handles HTTP `GET /` requests to execute SQL queries and stream CSV results.
pub async fn root(
    ConnectInfo(client_address): ConnectInfo<SocketAddr>,
    State(connection_pool): State<ConnectionPool>,
    Query(parameters): Query<SqlParameters>,
) -> Result<impl IntoResponse, AppError> {
    let raw_sql_query = parameters.sql.unwrap_or_default();
    let sql_query = raw_sql_query.trim();

    if sql_query.is_empty() {
        return Err(AppError::BadRequest(
            "Error: No SQL query provided\r\n".to_string(),
        ));
    }

    let client_ip_address = client_address.ip();
    let single_line_query_log = format_query_for_logging(sql_query);
    tracing::info!("{client_ip_address} | {single_line_query_log}");

    let connection = connection_pool.get_connection();
    let response_body = db::query_as_csv_stream(&connection, sql_query.to_string()).await?;

    Ok((
        [(header::CONTENT_TYPE, "text/csv; charset=utf-8")],
        response_body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_rusqlite::Connection;

    #[test]
    fn test_format_query_for_logging_basic() {
        assert_eq!(
            format_query_for_logging("SELECT * FROM users"),
            "SELECT * FROM users"
        );
    }

    #[test]
    fn test_format_query_for_logging_line_breaks_and_tabs() {
        let input = "SELECT\n    id,\n    name\nFROM\n    users\nWHERE\n    age > 20";
        assert_eq!(
            format_query_for_logging(input),
            "SELECT id, name FROM users WHERE age > 20"
        );

        let input_crlf = "SELECT\r\n\tid,\r\n\tname\r\nFROM\r\n\tusers";
        assert_eq!(
            format_query_for_logging(input_crlf),
            "SELECT id, name FROM users"
        );
    }

    #[test]
    fn test_format_query_for_logging_extra_whitespace() {
        assert_eq!(
            format_query_for_logging("   SELECT    *    FROM    users   "),
            "SELECT * FROM users"
        );
    }

    #[test]
    fn test_format_query_for_logging_empty_and_whitespace_only() {
        assert_eq!(format_query_for_logging(""), "");
        assert_eq!(format_query_for_logging("   \n\t\r\n   "), "");
    }

    #[tokio::test]
    async fn test_root_handler_stream() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE products (id INTEGER, name TEXT);
                     INSERT INTO products VALUES (10, 'widget');",
                )
            })
            .await
            .unwrap();

        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
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
        let body_string = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(body_string, "id,name\r\n10,widget\r\n");
    }

    #[tokio::test]
    async fn test_root_handler_preserves_string_literal_whitespace() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+'hello+++world'+AS+message")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let body_string = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(body_string, "message\r\nhello   world\r\n");
    }

    #[tokio::test]
    async fn test_root_handler_empty_query() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
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

    #[tokio::test]
    async fn test_root_handler_invalid_query() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+*+FROM+nonexistent_table")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
