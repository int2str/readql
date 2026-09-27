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
//! Zero-overhead safe wrapper around raw SQLite prepared statement pointers.
//!

use arrow_schema::DataType;
use tokio_rusqlite::rusqlite::Error as RusqliteError;
use tokio_rusqlite::rusqlite::ffi;

/// Column metadata consisting of column name and optional declared type.
pub type ColumnMetadata = (String, Option<String>);

/// A zero-overhead safe wrapper around a prepared SQLite statement pointer.
pub struct RawStatement {
    statement_pointer: *mut ffi::sqlite3_stmt,
    database_pointer: *mut ffi::sqlite3,
}

impl RawStatement {
    /// Prepares a SQL query directly using the SQLite C API.
    pub fn prepare(
        connection: &tokio_rusqlite::rusqlite::Connection,
        sql_query: &str,
    ) -> Result<Self, RusqliteError> {
        let database_pointer = unsafe { connection.handle() };
        let sql_cstring = std::ffi::CString::new(sql_query)
            .map_err(|error| RusqliteError::ToSqlConversionFailure(Box::new(error)))?;

        let mut statement_pointer: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
        let result_code = unsafe {
            ffi::sqlite3_prepare_v2(
                database_pointer,
                sql_cstring.as_ptr(),
                sql_cstring.as_bytes().len() as std::ffi::c_int,
                &mut statement_pointer,
                std::ptr::null_mut(),
            )
        };

        if result_code != ffi::SQLITE_OK {
            let error_message = unsafe {
                let message_pointer = ffi::sqlite3_errmsg(database_pointer);
                if message_pointer.is_null() {
                    "Failed to prepare SQL statement".to_string()
                } else {
                    std::ffi::CStr::from_ptr(message_pointer)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            return Err(RusqliteError::SqliteFailure(
                ffi::Error::new(result_code),
                Some(error_message),
            ));
        }

        Ok(Self {
            statement_pointer,
            database_pointer,
        })
    }

    /// Steps the statement. Returns `Ok(true)` if a row is available, `Ok(false)` if completed.
    pub fn step(&mut self) -> Result<bool, RusqliteError> {
        let result_code = unsafe { ffi::sqlite3_step(self.statement_pointer) };
        if result_code == ffi::SQLITE_ROW {
            Ok(true)
        } else if result_code == ffi::SQLITE_DONE {
            Ok(false)
        } else {
            let error_message = unsafe {
                let message_pointer = ffi::sqlite3_errmsg(self.database_pointer);
                if message_pointer.is_null() {
                    "Failed to step SQL statement".to_string()
                } else {
                    std::ffi::CStr::from_ptr(message_pointer)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            Err(RusqliteError::SqliteFailure(
                ffi::Error::new(result_code),
                Some(error_message),
            ))
        }
    }

    /// Returns the number of columns in the result set.
    #[inline]
    pub fn column_count(&self) -> usize {
        unsafe { ffi::sqlite3_column_count(self.statement_pointer) as usize }
    }

    /// Extracts column names and declared types for all columns in the result set.
    pub fn extract_column_metadata(&self) -> Vec<ColumnMetadata> {
        let column_count = self.column_count();
        let mut metadata = Vec::with_capacity(column_count);
        for column_index in 0..column_count {
            let column_index_c = column_index as std::ffi::c_int;
            let name = unsafe {
                let name_pointer = ffi::sqlite3_column_name(self.statement_pointer, column_index_c);
                if name_pointer.is_null() {
                    format!("col_{column_index}")
                } else {
                    std::ffi::CStr::from_ptr(name_pointer)
                        .to_string_lossy()
                        .into_owned()
                }
            };
            let decl = unsafe {
                let decl_pointer =
                    ffi::sqlite3_column_decltype(self.statement_pointer, column_index_c);
                if decl_pointer.is_null() {
                    None
                } else {
                    Some(
                        std::ffi::CStr::from_ptr(decl_pointer)
                            .to_string_lossy()
                            .into_owned(),
                    )
                }
            };
            metadata.push((name, decl));
        }
        metadata
    }

    /// Returns `true` if the column value at `column_index` is `NULL`.
    #[inline]
    pub fn is_null(&self, column_index: usize) -> bool {
        unsafe {
            ffi::sqlite3_column_type(self.statement_pointer, column_index as std::ffi::c_int)
                == ffi::SQLITE_NULL
        }
    }

    /// Reads a Float64 value directly from column `column_index`.
    #[inline]
    pub fn column_double(&self, column_index: usize) -> f64 {
        unsafe {
            ffi::sqlite3_column_double(self.statement_pointer, column_index as std::ffi::c_int)
        }
    }

    /// Reads an Int64 value directly from column `column_index`.
    #[inline]
    pub fn column_int64(&self, column_index: usize) -> i64 {
        unsafe {
            ffi::sqlite3_column_int64(self.statement_pointer, column_index as std::ffi::c_int)
        }
    }

    /// Reads a UTF-8 string slice directly from column `column_index`.
    #[inline]
    pub fn column_text(&self, column_index: usize) -> &str {
        unsafe {
            let text_pointer =
                ffi::sqlite3_column_text(self.statement_pointer, column_index as std::ffi::c_int);
            if text_pointer.is_null() {
                ""
            } else {
                let byte_length = ffi::sqlite3_column_bytes(
                    self.statement_pointer,
                    column_index as std::ffi::c_int,
                ) as usize;
                let slice = std::slice::from_raw_parts(text_pointer, byte_length);
                std::str::from_utf8(slice).unwrap_or("")
            }
        }
    }

    /// Reads a binary byte slice directly from column `column_index`.
    #[inline]
    pub fn column_blob(&self, column_index: usize) -> &[u8] {
        unsafe {
            let blob_pointer =
                ffi::sqlite3_column_blob(self.statement_pointer, column_index as std::ffi::c_int);
            if blob_pointer.is_null() {
                &[]
            } else {
                let byte_length = ffi::sqlite3_column_bytes(
                    self.statement_pointer,
                    column_index as std::ffi::c_int,
                ) as usize;
                std::slice::from_raw_parts(blob_pointer as *const u8, byte_length)
            }
        }
    }

    /// Infers an Arrow [`DataType`] for column `column_index` using the current row value.
    pub fn sample_data_type(&self, column_index: usize) -> DataType {
        match unsafe {
            ffi::sqlite3_column_type(self.statement_pointer, column_index as std::ffi::c_int)
        } {
            ffi::SQLITE_INTEGER => DataType::Int64,
            ffi::SQLITE_FLOAT => DataType::Float64,
            ffi::SQLITE_BLOB => DataType::Binary,
            _ => DataType::Utf8,
        }
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        if !self.statement_pointer.is_null() {
            unsafe {
                ffi::sqlite3_finalize(self.statement_pointer);
            }
        }
    }
}
