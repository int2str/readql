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
use readql::ui::create_ui_router;

#[derive(Parser, Debug)]
#[command(version, about = "High-throughput read-only SQLite HTTP query server")]
struct Args {
    /// Path to the SQLite database file
    #[arg(value_name = "DATABASE_PATH")]
    database_path: PathBuf,

    /// IP address to listen on
    #[arg(short = 'l', long = "listen", default_value = "0.0.0.0")]
    listen: IpAddr,

    /// Port to listen on for the API endpoint
    #[arg(short = 'p', long = "port", default_value_t = 8002)]
    port: u16,

    /// Port to listen on for the Web UI server
    #[arg(long = "ui-port", default_value_t = 8001)]
    ui_port: u16,

    /// Disable the Web UI server
    #[arg(long = "no-ui", default_value_t = false)]
    no_ui: bool,

    /// Number of database connections in the connection pool (default: available CPU cores)
    #[arg(short = 'c', long = "connections")]
    connections: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .init();

    let arguments = Args::parse();
    let pool_size = arguments.connections.unwrap_or(0);

    let connection_pool = open_pool(&arguments.database_path, pool_size).await?;
    tracing::info!(
        "Initialized SQLite connection pool with {} connections",
        connection_pool.size()
    );

    let api_router = create_router(connection_pool);
    let api_address = SocketAddr::new(arguments.listen, arguments.port);
    let api_listener = TcpListener::bind(api_address).await?;
    tracing::info!("API server listening on http://{api_address}");

    let api_server = axum::serve(
        api_listener,
        api_router.into_make_service_with_connect_info::<SocketAddr>(),
    );

    if !arguments.no_ui {
        let ui_router = create_ui_router(arguments.port);
        let ui_address = SocketAddr::new(arguments.listen, arguments.ui_port);
        let ui_listener = TcpListener::bind(ui_address).await?;
        tracing::info!("Web UI server listening on http://{ui_address}");

        let ui_server = axum::serve(
            ui_listener,
            ui_router.into_make_service_with_connect_info::<SocketAddr>(),
        );

        tokio::try_join!(api_server, ui_server)?;
    } else {
        api_server.await?;
    }

    Ok(())
}
