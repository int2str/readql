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
use axum::http::{HeaderMap, header};
use axum::response::IntoResponse;
use axum::routing::{Router, get};
use serde::Deserialize;

use crate::AppError;
use crate::db::{self, ConnectionPool};

/// Supported query output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// RFC 4180 CSV text format.
    #[default]
    Csv,
    /// Apache Parquet binary columnar format.
    Parquet,
}

impl OutputFormat {
    /// Returns the HTTP `Content-Type` corresponding to the output format.
    pub fn content_type(&self) -> &'static str {
        match self {
            OutputFormat::Csv => "text/csv; charset=utf-8",
            OutputFormat::Parquet => "application/vnd.apache.parquet",
        }
    }

    /// Returns the single-character indicator for structured query logging.
    pub fn indicator(&self) -> &'static str {
        match self {
            OutputFormat::Csv => "C",
            OutputFormat::Parquet => "P",
        }
    }
}

/// Query parameters extracted from the HTTP request URL.
#[derive(Deserialize)]
pub struct SqlParameters {
    /// The SQL query string to execute.
    pub sql: Option<String>,
    /// Optional response output format (`csv` or `parquet`).
    pub format: Option<String>,
}

/// Formats a SQL query as a single line for structured logging without mutating the underlying SQL query.
pub fn format_query_for_logging(sql_query: &str) -> String {
    sql_query.split_whitespace().collect::<Vec<_>>().join(" ")
}

use tower_http::cors::CorsLayer;

/// Creates the Axum router configured with the shared SQLite connection pool state.
pub fn create_router(connection_pool: ConnectionPool) -> Router {
    Router::new()
        .route("/", get(root))
        .layer(CorsLayer::permissive())
        .with_state(connection_pool)
}

/// Handles HTTP `GET /` requests to execute SQL queries and stream CSV or Parquet results.
pub async fn root(
    ConnectInfo(client_address): ConnectInfo<SocketAddr>,
    State(connection_pool): State<ConnectionPool>,
    headers: HeaderMap,
    Query(parameters): Query<SqlParameters>,
) -> Result<impl IntoResponse, AppError> {
    let raw_sql_query = parameters.sql.unwrap_or_default();
    let sql_query = raw_sql_query.trim();

    if sql_query.is_empty() {
        return Err(AppError::BadRequest(
            "Error: No SQL query provided\r\n".to_string(),
        ));
    }

    let output_format = match parameters.format.as_deref() {
        None => {
            if let Some(accept) = headers.get(header::ACCEPT).and_then(|h| h.to_str().ok()) {
                if accept.contains("application/vnd.apache.parquet") || accept.contains("parquet") {
                    OutputFormat::Parquet
                } else {
                    OutputFormat::Csv
                }
            } else {
                OutputFormat::Csv
            }
        }
        Some(fmt) if fmt.eq_ignore_ascii_case("csv") => OutputFormat::Csv,
        Some(fmt) if fmt.eq_ignore_ascii_case("parquet") || fmt.eq_ignore_ascii_case("pq") => {
            OutputFormat::Parquet
        }
        Some(invalid) => {
            return Err(AppError::BadRequest(format!(
                "Error: Unsupported format '{invalid}'. Supported formats are 'csv' and 'parquet'.\r\n"
            )));
        }
    };

    let client_ip_address = client_address.ip();
    let format_indicator = output_format.indicator();
    let single_line_query_log = format_query_for_logging(sql_query);
    tracing::info!("{client_ip_address} | {format_indicator} | {single_line_query_log}");

    let connection = connection_pool.get_connection();
    let response_body = match output_format {
        OutputFormat::Csv => db::query_as_csv_stream(&connection, sql_query.to_string()).await?,
        OutputFormat::Parquet => {
            db::query_as_parquet_stream(&connection, sql_query.to_string()).await?
        }
    };

    Ok((
        [(header::CONTENT_TYPE, output_format.content_type())],
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
    fn test_output_format_indicator() {
        assert_eq!(OutputFormat::Csv.indicator(), "C");
        assert_eq!(OutputFormat::Parquet.indicator(), "P");
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
    async fn test_root_handler_parquet_format() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use bytes::Bytes;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE items (id INTEGER, name TEXT, price REAL);
                     INSERT INTO items VALUES (1, 'item1', 10.5);",
                )
            })
            .await
            .unwrap();

        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+*+FROM+items&format=parquet")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/vnd.apache.parquet"
        );

        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(&bytes[0..4], b"PAR1");

        let reader_builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        let mut reader = reader_builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_root_handler_parquet_accept_header() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE items (id INTEGER, name TEXT);
                     INSERT INTO items VALUES (1, 'item1');",
                )
            })
            .await
            .unwrap();

        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+*+FROM+items")
                    .header("Accept", "application/vnd.apache.parquet")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "application/vnd.apache.parquet"
        );

        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(&bytes[0..4], b"PAR1");
    }

    #[tokio::test]
    async fn test_root_handler_unsupported_format() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        let connection_pool = ConnectionPool::new(vec![connection]);
        let application = create_router(connection_pool);

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+1&format=json")
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
