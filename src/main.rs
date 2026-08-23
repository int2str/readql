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

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

use clap::Parser;
use tokio::net::TcpListener;

use readql::db::open_pool;
use readql::handlers::create_router;

#[derive(Parser, Debug)]
#[command(version, about = "High-throughput read-only SQLite HTTP query server")]
struct Args {
    /// Path to the SQLite database file
    #[arg(value_name = "DATABASE_PATH")]
    database_path: PathBuf,

    /// IP address to listen on
    #[arg(short = 'l', long = "listen", default_value = "0.0.0.0")]
    listen: IpAddr,

    /// Port to listen on
    #[arg(short = 'p', long = "port", default_value_t = 8002)]
    port: u16,

    /// Number of database connections in the connection pool (default: available CPU cores)
    #[arg(short = 'c', long = "connections")]
    connections: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let arguments = Args::parse();
    let pool_size = arguments.connections.unwrap_or(0);

    let connection_pool = open_pool(&arguments.database_path, pool_size).await?;
    tracing::info!(
        "Initialized SQLite connection pool with {} connections",
        connection_pool.size()
    );

    let router = create_router(connection_pool);

    let bind_address = SocketAddr::new(arguments.listen, arguments.port);
    let listener = TcpListener::bind(bind_address).await?;
    tracing::info!("Listening on http://{bind_address}");

    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
