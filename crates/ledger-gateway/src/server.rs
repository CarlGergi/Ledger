use std::io;
use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::Gateway;

/// Serves the gateway at `/mcp` until `shutdown` is cancelled. An empty
/// `allowed_hosts` keeps the SDK's default of accepting loopback hosts only.
pub async fn serve(
    gateway: Gateway,
    listener: TcpListener,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut config =
        StreamableHttpServerConfig::default().with_cancellation_token(shutdown.child_token());
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts);
    }
    let service = StreamableHttpService::new(
        move || Ok(gateway.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let app = Router::new().nest_service("/mcp", service);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}
