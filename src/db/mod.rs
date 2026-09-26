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

mod chunk_writer;

pub mod csv;
pub use csv::query_as_csv_stream;

pub mod parquet;
pub use parquet::query_as_parquet_stream;

pub mod pool;
pub use pool::ConnectionPool;

pub const CHUNK_SIZE: usize = 64 * 1_024; // 64 KB per CSV/Parquet chunk
pub const CHANNEL_CAPACITY: usize = 16; // Max 16 chunks buffered (~1 MB max in-memory)

#[cfg(test)]
mod tests {
    use super::*;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::RecordBatchReader;
    use axum::body::Body;
    use tokio_rusqlite::Connection;

    #[tokio::test]
    async fn test_query_as_csv_stream() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE items (id INTEGER, name TEXT, price REAL);
                     INSERT INTO items VALUES (1, 'apple', 1.25);
                     INSERT INTO items VALUES (2, 'banana', 0.75);",
                )
            })
            .await
            .unwrap();

        let stream = query_as_csv_stream(
            &connection,
            "SELECT * FROM items ORDER BY id".to_string(),
            None,
        )
        .await
        .unwrap();
        let body = Body::from_stream(stream);
        let bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        let text = String::from_utf8(bytes.to_vec()).unwrap();
        assert_eq!(text, "id,name,price\r\n1,apple,1.25\r\n2,banana,0.75\r\n");
    }

    #[tokio::test]
    async fn test_query_as_parquet_stream() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch(
                    "CREATE TABLE products (id INTEGER, name TEXT, price REAL, in_stock BOOLEAN);
                     INSERT INTO products VALUES (1, 'widget', 19.99, 1);
                     INSERT INTO products VALUES (2, 'gadget', 49.95, 0);",
                )
            })
            .await
            .unwrap();

        let stream = query_as_parquet_stream(
            &connection,
            "SELECT * FROM products ORDER BY id".to_string(),
            None,
        )
        .await
        .unwrap();
        let body = Body::from_stream(stream);
        let stream_bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        assert_eq!(&stream_bytes[0..4], b"PAR1");

        let stream_reader_builder = ParquetRecordBatchReaderBuilder::try_new(stream_bytes).unwrap();
        let mut stream_reader = stream_reader_builder.build().unwrap();
        let stream_batch = stream_reader.next().unwrap().unwrap();
        assert_eq!(stream_batch.num_rows(), 2);
        assert_eq!(stream_batch.num_columns(), 4);
    }

    #[tokio::test]
    async fn test_query_as_parquet_empty_result() {
        let connection = Connection::open_in_memory().await.unwrap();
        connection
            .call(|raw_connection| {
                raw_connection.execute_batch("CREATE TABLE empty_table (id INTEGER, label TEXT);")
            })
            .await
            .unwrap();

        let stream =
            query_as_parquet_stream(&connection, "SELECT * FROM empty_table".to_string(), None)
                .await
                .unwrap();
        let body = Body::from_stream(stream);
        let stream_bytes = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
        assert_eq!(&stream_bytes[0..4], b"PAR1");

        let stream_reader_builder = ParquetRecordBatchReaderBuilder::try_new(stream_bytes).unwrap();
        assert_eq!(
            stream_reader_builder.metadata().file_metadata().num_rows(),
            0
        );
        let mut stream_reader = stream_reader_builder.build().unwrap();
        assert_eq!(stream_reader.schema().fields().len(), 2);
        if let Some(batch_res) = stream_reader.next() {
            let stream_batch = batch_res.unwrap();
            assert_eq!(stream_batch.num_rows(), 0);
            assert_eq!(stream_batch.num_columns(), 2);
        }
    }

    #[tokio::test]
    async fn test_query_as_csv_stream_invalid_query() {
        let connection = Connection::open_in_memory().await.unwrap();
        let result = query_as_csv_stream(
            &connection,
            "SELECT * FROM nonexistent_table".to_string(),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_query_as_parquet_stream_invalid_query() {
        let connection = Connection::open_in_memory().await.unwrap();
        let result = query_as_parquet_stream(
            &connection,
            "SELECT * FROM nonexistent_table".to_string(),
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connection_pool_round_robin() {
        let first_connection = Connection::open_in_memory().await.unwrap();
        let second_connection = Connection::open_in_memory().await.unwrap();

        let connection_pool = ConnectionPool::new(vec![first_connection, second_connection]);
        assert_eq!(connection_pool.size(), 2);
        assert!(!connection_pool.is_empty());

        let _first_acquired = connection_pool.get_connection();
        let _second_acquired = connection_pool.get_connection();
        let _third_acquired = connection_pool.get_connection();
    }
}
