use std::io;
use std::sync::Arc;

use axum::Router;
use rmcp::ServerHandler;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Serves `handler` as an MCP server at `/mcp` until `shutdown` is cancelled.
pub async fn serve_mcp<S>(
    handler: S,
    listener: TcpListener,
    shutdown: CancellationToken,
) -> io::Result<()>
where
    S: ServerHandler + Clone + Send + Sync + 'static,
{
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_cancellation_token(shutdown.child_token()),
    );
    let app = Router::new().nest_service("/mcp", service);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}
