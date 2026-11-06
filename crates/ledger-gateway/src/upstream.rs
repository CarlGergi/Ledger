//! The real MCP tool servers behind the gateway.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rmcp::model::{CallToolRequestParams, CallToolResponse, ClientConfig, Implementation, Tool};
use rmcp::service::{RoleClient, RunningService, ServiceError};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{ErrorData, ServiceExt};
use tokio::sync::Mutex;

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    /// The server answered with a JSON-RPC error.
    #[error("tool server rejected the call: {0}")]
    Rpc(ErrorData),
    /// No answer came back, so the effect may or may not have happened.
    #[error("tool server unreachable: {0}")]
    Transport(String),
}

#[async_trait]
pub trait Upstream: Send + Sync + 'static {
    fn name(&self) -> &str;

    async fn list_tools(&self) -> Result<Vec<Tool>, UpstreamError>;

    /// Sends the call exactly once. Never retries.
    async fn call_tool(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResponse, UpstreamError>;
}

type Connection = RunningService<RoleClient, ClientConfig>;

/// A tool server reached over MCP Streamable HTTP. Connects on first use, and
/// reconnects on the next call after the connection breaks.
pub struct McpUpstream {
    name: String,
    url: String,
    connect_timeout: Duration,
    connection: Mutex<Option<Arc<Connection>>>,
}

impl McpUpstream {
    pub fn new(name: impl Into<String>, url: impl Into<String>, connect_timeout: Duration) -> Self {
        Self {
            name: name.into(),
            url: url.into(),
            connect_timeout,
            connection: Mutex::new(None),
        }
    }

    async fn connection(&self) -> Result<Arc<Connection>, UpstreamError> {
        let mut slot = self.connection.lock().await;
        if let Some(connection) = slot.as_ref()
            && !connection.is_closed()
        {
            return Ok(connection.clone());
        }

        let mut client = ClientConfig::default();
        client.client_info = Implementation::new("ledger", env!("CARGO_PKG_VERSION"));
        let transport = StreamableHttpClientTransport::from_uri(self.url.clone());
        let connection = tokio::time::timeout(self.connect_timeout, client.serve(transport))
            .await
            .map_err(|_| UpstreamError::Transport(format!("connecting to {} timed out", self.url)))?
            .map_err(|e| UpstreamError::Transport(format!("connecting to {}: {e}", self.url)))?;
        let connection = Arc::new(connection);
        *slot = Some(connection.clone());
        Ok(connection)
    }

    async fn handle_error(&self, used: &Arc<Connection>, error: ServiceError) -> UpstreamError {
        match error {
            ServiceError::McpError(error) => UpstreamError::Rpc(error),
            other => {
                let mut slot = self.connection.lock().await;
                if slot.as_ref().is_some_and(|c| Arc::ptr_eq(c, used)) {
                    *slot = None;
                }
                UpstreamError::Transport(other.to_string())
            }
        }
    }
}

#[async_trait]
impl Upstream for McpUpstream {
    fn name(&self) -> &str {
        &self.name
    }

    async fn list_tools(&self) -> Result<Vec<Tool>, UpstreamError> {
        let connection = self.connection().await?;
        match connection.list_all_tools().await {
            Ok(tools) => Ok(tools),
            Err(e) => Err(self.handle_error(&connection, e).await),
        }
    }

    async fn call_tool(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResponse, UpstreamError> {
        let connection = self.connection().await?;
        match connection.call_tool_once(params).await {
            Ok(response) => Ok(response),
            Err(e) => Err(self.handle_error(&connection, e).await),
        }
    }
}
