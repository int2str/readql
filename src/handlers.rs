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

use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use axum::Json;
use axum::extract::{ConnectInfo, Query, State};
use axum::http::{HeaderMap, header};
use axum::response::IntoResponse;
use axum::routing::{Router, get};
use bytes::Bytes;
use serde::Deserialize;
use tokio_stream::Stream;
use tower_http::cors::CorsLayer;

use crate::AppError;
use crate::db::{self, ConnectionPool};
use crate::metrics::{RequestEvent, ServerMetrics};

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

/// Shared application state across HTTP routes.
#[derive(Clone)]
pub struct AppState {
    pub connection_pool: ConnectionPool,
    pub metrics: Arc<ServerMetrics>,
}

/// Stream wrapper that tracks bytes transferred and records request metrics on finish or drop.
pub struct MetricTrackingStream<S> {
    inner: S,
    metrics: Arc<ServerMetrics>,
    client_ip: IpAddr,
    format_indicator: &'static str,
    sql_query: String,
    start_time: Instant,
    row_counter: Arc<AtomicU64>,
    bytes_sent: u64,
    status: u16,
    finished: bool,
}

impl<S, E> Stream for MetricTrackingStream<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                self.bytes_sent += chunk.len() as u64;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(err))) => {
                self.status = 500;
                Poll::Ready(Some(Err(err)))
            }
            Poll::Ready(None) => {
                self.finish_tracking();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> MetricTrackingStream<S> {
    fn finish_tracking(&mut self) {
        if !self.finished {
            self.finished = true;
            let rows = self.row_counter.load(Ordering::Relaxed);
            self.metrics.record_request(RequestEvent {
                client_ip: self.client_ip,
                format_indicator: self.format_indicator,
                duration: self.start_time.elapsed(),
                status: self.status,
                bytes_sent: self.bytes_sent,
                rows_streamed: rows,
                sql_query: &self.sql_query,
            });
        }
    }
}

impl<S> Drop for MetricTrackingStream<S> {
    fn drop(&mut self) {
        self.finish_tracking();
    }
}

/// Creates the Axum router configured with the shared SQLite connection pool and metrics state.
pub fn create_router(connection_pool: ConnectionPool, metrics: Arc<ServerMetrics>) -> Router {
    let state = AppState {
        connection_pool,
        metrics,
    };

    Router::new()
        .route("/", get(root))
        .route("/api/metrics", get(get_metrics))
        .route("/metrics", get(get_metrics))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Handles `GET /api/metrics` to return the real-time metrics snapshot.
pub async fn get_metrics(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.metrics.snapshot())
}

/// Handles HTTP `GET /` requests to execute SQL queries and stream CSV or Parquet results.
pub async fn root(
    ConnectInfo(client_address): ConnectInfo<SocketAddr>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(parameters): Query<SqlParameters>,
) -> Result<impl IntoResponse, AppError> {
    let start_time = Instant::now();
    state.metrics.request_started();

    let raw_sql_query = parameters.sql.unwrap_or_default();
    let sql_query = raw_sql_query.trim();
    let client_ip_address = client_address.ip();

    if sql_query.is_empty() {
        state.metrics.record_request(RequestEvent {
            client_ip: client_ip_address,
            format_indicator: "C",
            duration: start_time.elapsed(),
            status: 400,
            bytes_sent: 0,
            rows_streamed: 0,
            sql_query: "",
        });
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
            state.metrics.record_request(RequestEvent {
                client_ip: client_ip_address,
                format_indicator: "C",
                duration: start_time.elapsed(),
                status: 400,
                bytes_sent: 0,
                rows_streamed: 0,
                sql_query,
            });
            return Err(AppError::BadRequest(format!(
                "Error: Unsupported format '{invalid}'. Supported formats are 'csv' and 'parquet'.\r\n"
            )));
        }
    };

    let format_indicator = output_format.indicator();
    let single_line_query_log = format_query_for_logging(sql_query);
    tracing::info!("{client_ip_address} | {format_indicator} | {single_line_query_log}");

    let connection = state.connection_pool.get_connection();
    let row_counter = Arc::new(AtomicU64::new(0));

    let stream_result = match output_format {
        OutputFormat::Csv => {
            db::query_as_csv_stream(
                &connection,
                sql_query.to_string(),
                Some(row_counter.clone()),
            )
            .await
        }
        OutputFormat::Parquet => {
            db::query_as_parquet_stream(
                &connection,
                sql_query.to_string(),
                Some(row_counter.clone()),
            )
            .await
        }
    };

    let raw_stream = match stream_result {
        Ok(s) => s,
        Err(err) => {
            state.metrics.record_request(RequestEvent {
                client_ip: client_ip_address,
                format_indicator,
                duration: start_time.elapsed(),
                status: 500,
                bytes_sent: 0,
                rows_streamed: 0,
                sql_query,
            });
            return Err(err);
        }
    };

    let tracking_stream = MetricTrackingStream {
        inner: raw_stream,
        metrics: state.metrics.clone(),
        client_ip: client_ip_address,
        format_indicator,
        sql_query: single_line_query_log,
        start_time,
        row_counter,
        bytes_sent: 0,
        status: 200,
        finished: false,
    };

    let response_body = axum::body::Body::from_stream(tracking_stream);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics);

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

    #[tokio::test]
    async fn test_get_metrics_endpoint() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let connection = Connection::open_in_memory().await.unwrap();
        let connection_pool = ConnectionPool::new(vec![connection]);
        let metrics = Arc::new(ServerMetrics::new());
        let application = create_router(connection_pool, metrics.clone());

        // Perform a query first
        let _ = application
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/?sql=SELECT+1+AS+val")
                    .extension(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 12345))))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let response = application
            .oneshot(
                Request::builder()
                    .uri("/api/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["total_requests"], 1);
        assert_eq!(json["successful_requests"], 1);
        assert_eq!(json["failed_requests"], 0);
    }
}
