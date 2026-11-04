use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use ledger_store::{EventStore, NewEvent, Outcome};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, DiscoverRequestMethod, DiscoverResult, ErrorCode,
    Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
    ServerResult,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Value, json};

use crate::session::resolve_session;
use crate::upstream::{Upstream, UpstreamError};

/// Agents see each tool as `<upstream>__<tool>`, which keeps names from different
/// servers apart and makes routing a matter of reading the name.
const TOOL_SEPARATOR: &str = "__";

/// JSON-RPC errors the gateway itself raises, in the server-defined range.
pub mod error_codes {
    use rmcp::model::ErrorCode;

    /// The request could not be recorded, so the call was not forwarded.
    pub const NOT_RECORDED: ErrorCode = ErrorCode(-32050);
    /// The tool may have run, but its outcome could not be recorded.
    pub const OUTCOME_NOT_RECORDED: ErrorCode = ErrorCode(-32051);
    /// The call was forwarded and no answer came back. The tool may have run.
    pub const OUTCOME_UNKNOWN: ErrorCode = ErrorCode(-32052);
}

#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

struct Inner {
    store: Arc<dyn EventStore>,
    upstreams: Vec<Arc<dyn Upstream>>,
    call_timeout: Duration,
}

enum Forwarded {
    Answered(Result<CallToolResponse, UpstreamError>),
    TimedOut,
    NotRouted,
}

impl Gateway {
    pub fn new(
        store: Arc<dyn EventStore>,
        upstreams: Vec<Arc<dyn Upstream>>,
        call_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                upstreams,
                call_timeout,
            }),
        }
    }

    /// The core rule of the log: the request is committed before the tool can run,
    /// and nothing reaches the agent that the log does not also hold. Everything the
    /// agent saw can therefore be replayed from the log.
    async fn record_and_forward(
        &self,
        session: &str,
        rpc_id: Value,
        meta: Value,
        params: CallToolRequestParams,
    ) -> Result<CallToolResponse, ErrorData> {
        let tool = params.name.to_string();
        let store = &self.inner.store;

        let request_payload = json!({ "rpc_id": rpc_id, "meta": meta, "params": params });
        let request = store
            .append(NewEvent::request(session, &tool, request_payload))
            .await
            .map_err(|e| {
                tracing::error!(session, tool, error = %e, "could not record request");
                ErrorData::new(
                    error_codes::NOT_RECORDED,
                    "ledger could not record this call, so it was not forwarded",
                    None,
                )
            })?;

        let forwarded = self.forward(params).await;
        let (outcome, response_payload) = describe(&forwarded);
        store
            .append(NewEvent::response(
                session,
                request.seq,
                &tool,
                outcome,
                response_payload,
            ))
            .await
            .map_err(|e| {
                tracing::error!(
                    session, tool, seq = request.seq, error = %e, "could not record response"
                );
                ErrorData::new(
                    error_codes::OUTCOME_NOT_RECORDED,
                    "the tool may have run, but ledger could not record the outcome",
                    None,
                )
            })?;

        self.agent_view(forwarded, &tool)
    }

    async fn forward(&self, mut params: CallToolRequestParams) -> Forwarded {
        let Some((upstream, tool)) = self.route(&params.name) else {
            return Forwarded::NotRouted;
        };
        params.name = Cow::Owned(tool.to_owned());
        match tokio::time::timeout(self.inner.call_timeout, upstream.call_tool(params)).await {
            Ok(answer) => Forwarded::Answered(answer),
            Err(_) => Forwarded::TimedOut,
        }
    }

    fn route<'a>(&self, name: &'a str) -> Option<(&Arc<dyn Upstream>, &'a str)> {
        let (upstream, tool) = name.split_once(TOOL_SEPARATOR)?;
        let upstream = self.inner.upstreams.iter().find(|u| u.name() == upstream)?;
        Some((upstream, tool))
    }

    fn agent_view(&self, forwarded: Forwarded, tool: &str) -> Result<CallToolResponse, ErrorData> {
        match forwarded {
            Forwarded::Answered(Ok(response)) => Ok(response),
            Forwarded::Answered(Err(UpstreamError::Rpc(error))) => Err(error),
            Forwarded::Answered(Err(UpstreamError::Transport(reason))) => Err(ErrorData::new(
                error_codes::OUTCOME_UNKNOWN,
                format!("the outcome of {tool} is unknown: {reason}"),
                None,
            )),
            Forwarded::TimedOut => Err(ErrorData::new(
                error_codes::OUTCOME_UNKNOWN,
                format!(
                    "the outcome of {tool} is unknown: no answer within {:?}",
                    self.inner.call_timeout
                ),
                None,
            )),
            Forwarded::NotRouted => Err(ErrorData::invalid_params(
                format!(
                    "no tool server for {tool}; tool names look like <server>{TOOL_SEPARATOR}<tool>"
                ),
                None,
            )),
        }
    }
}

fn describe(forwarded: &Forwarded) -> (Outcome, Value) {
    match forwarded {
        Forwarded::Answered(Ok(response)) => {
            let outcome = match response {
                CallToolResponse::Complete(result) if result.is_error == Some(true) => {
                    Outcome::ToolError
                }
                _ => Outcome::Ok,
            };
            let result = ServerResult::from(response.clone());
            (outcome, json!({ "result": result }))
        }
        Forwarded::Answered(Err(UpstreamError::Rpc(error))) => {
            (Outcome::RpcError, json!({ "error": error }))
        }
        Forwarded::Answered(Err(UpstreamError::Transport(reason))) => {
            (Outcome::TransportError, json!({ "reason": reason }))
        }
        Forwarded::TimedOut => (Outcome::Timeout, json!({})),
        Forwarded::NotRouted => (Outcome::NotRouted, json!({})),
    }
}

fn prefixed(upstream: &str, tool: &str) -> String {
    format!("{upstream}{TOOL_SEPARATOR}{tool}")
}

impl ServerHandler for Gateway {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("ledger", env!("CARGO_PKG_VERSION")))
    }

    // The newest protocol revision has no transport sessions, so an agent speaking it
    // would arrive with nothing to log under. Answering its opening `server/discover`
    // the way an older server does sends it back to the `initialize` handshake, which
    // does establish a session. That is what lets an unmodified agent work as it is.
    async fn discover(
        &self,
        _context: RequestContext<RoleServer>,
    ) -> Result<DiscoverResult, ErrorData> {
        Err(ErrorData::method_not_found::<DiscoverRequestMethod>())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let timeout = self.inner.call_timeout;
        let listings = join_all(self.inner.upstreams.iter().map(|upstream| async move {
            let listing = tokio::time::timeout(timeout, upstream.list_tools()).await;
            (upstream.name(), listing)
        }))
        .await;

        let mut tools = Vec::new();
        for (upstream, listing) in listings {
            match listing {
                Ok(Ok(listed)) => tools.extend(listed.into_iter().map(|mut tool| {
                    tool.name = Cow::Owned(prefixed(upstream, &tool.name));
                    tool
                })),
                Ok(Err(e)) => tracing::warn!(upstream, error = %e, "leaving out unavailable tools"),
                Err(_) => tracing::warn!(upstream, "leaving out tools: no answer in time"),
            }
        }
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let parts = context
            .extensions
            .get::<http::request::Parts>()
            .ok_or_else(|| ErrorData::invalid_request("ledger only serves HTTP", None))?;
        let session = resolve_session(parts)?;
        let rpc_id = json!(context.id);
        // The SDK has already moved the agent's `_meta` out of `request`. It gets logged
        // but goes no further, because it describes the agent's exchange with the gateway.
        let meta = json!(context.meta);

        // Detached so that an agent hanging up mid-call cannot stop the tool's outcome
        // from reaching the log.
        let gateway = self.clone();
        tokio::spawn(async move {
            gateway
                .record_and_forward(&session, rpc_id, meta, request)
                .await
        })
        .await
        .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?
    }
}
