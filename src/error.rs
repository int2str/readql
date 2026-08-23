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
//! Error types and HTTP response mappings for client and database errors.
//!

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

/// Application-level errors returned by HTTP request handlers.
#[derive(Debug)]
pub enum AppError {
    /// Invalid client input, missing query parameters, or malformed SQL queries (HTTP 400).
    BadRequest(String),
    /// Database execution or connection failure (HTTP 500).
    Database(tokio_rusqlite::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::BadRequest(message) => (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                message,
            )
                .into_response(),
            AppError::Database(error) => {
                tracing::error!("Database query error: {error}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
                    format!("Database query error: {error}\r\n"),
                )
                    .into_response()
            }
        }
    }
}

impl From<tokio_rusqlite::Error> for AppError {
    fn from(error: tokio_rusqlite::Error) -> Self {
        AppError::Database(error)
    }
}

impl From<&'static str> for AppError {
    fn from(message: &'static str) -> Self {
        AppError::BadRequest(message.to_string())
    }
}
