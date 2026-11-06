use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::join_all;
use ledger_store::{Event, EventStore, Lease, NewEvent, Outcome, StoreError};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, DiscoverRequestMethod, DiscoverResult, ErrorCode,
    Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig,
    ServerResult, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::policy::{Effect, EffectPolicy};
use crate::session::resolve_session;
use crate::upstream::{Upstream, UpstreamError};

/// Agents see each tool as `<upstream>__<tool>`, which keeps names from different
/// servers apart and makes routing a matter of reading the name.
pub(crate) const TOOL_SEPARATOR: &str = "__";

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
    policy: EffectPolicy,
    /// The latest listing of each tool, by the name agents know it by.
    catalog: Mutex<HashMap<String, Tool>>,
    /// Names this copy of the gateway in the leases it holds.
    owner: String,
    lease_ttl: Duration,
    /// The sessions this copy believes it owns. The store has the final say.
    leases: tokio::sync::Mutex<HashMap<String, Lease>>,
}

/// How long a session stays with a copy that has stopped appending to it.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

enum Forwarded {
    Answered(Result<CallToolResponse, UpstreamError>),
    TimedOut,
    NotRouted,
}

impl Gateway {
    /// A gateway that takes every tool's annotations at their word.
    pub fn new(
        store: Arc<dyn EventStore>,
        upstreams: Vec<Arc<dyn Upstream>>,
        call_timeout: Duration,
    ) -> Self {
        Self::with_policy(store, upstreams, call_timeout, EffectPolicy::default())
    }

    pub fn with_policy(
        store: Arc<dyn EventStore>,
        upstreams: Vec<Arc<dyn Upstream>>,
        call_timeout: Duration,
        policy: EffectPolicy,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                store,
                upstreams,
                call_timeout,
                policy,
                catalog: Mutex::default(),
                owner: Uuid::new_v4().to_string(),
                lease_ttl: DEFAULT_LEASE_TTL,
                leases: tokio::sync::Mutex::default(),
            }),
        }
    }

    /// Sets how long a session stays with this copy after its last append. Another copy
    /// can take the session over only once that has passed. Call it before serving.
    pub fn with_lease_ttl(self, ttl: Duration) -> Self {
        let mut inner = Arc::into_inner(self.inner).expect("configure the gateway before use");
        inner.lease_ttl = ttl;
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Appends under this copy's lease on the session, taking the lease first if it has
    /// none. A copy that has been taken over gets `Fenced`, and does not take the session
    /// back here: whether it may is a question for the session's next call.
    async fn append(&self, event: NewEvent) -> Result<Event, StoreError> {
        let lease = self.lease(&event.session_id).await?;
        let appended = self.inner.store.append(event, &lease).await;
        if matches!(appended, Err(StoreError::Fenced)) {
            let mut leases = self.inner.leases.lock().await;
            if leases.get(&lease.session_id) == Some(&lease) {
                leases.remove(&lease.session_id);
            }
        }
        appended
    }

    async fn lease(&self, session: &str) -> Result<Lease, StoreError> {
        let mut leases = self.inner.leases.lock().await;
        if let Some(lease) = leases.get(session) {
            return Ok(lease.clone());
        }
        let lease = self
            .inner
            .store
            .acquire_lease(session, &self.inner.owner, self.inner.lease_ttl)
            .await?;
        leases.insert(session.to_owned(), lease.clone());
        Ok(lease)
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
        let arguments = json!(params.arguments);

        let effect = self.effect_of(&tool).await;
        let request_payload =
            json!({ "rpc_id": rpc_id, "meta": meta, "effect": effect, "params": params });
        let request = self
            .append(NewEvent::request(
                session,
                &tool,
                &arguments,
                request_payload,
            ))
            .await
            .map_err(|e| {
                tracing::error!(session, tool, error = %e, "could not record request");
                ErrorData::new(
                    error_codes::NOT_RECORDED,
                    format!("ledger could not record this call, so it was not forwarded: {e}"),
                    None,
                )
            })?;

        let forwarded = self.forward(params).await;
        let (outcome, response_payload) = describe(&forwarded);
        self.append(NewEvent::response(
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

    /// What is known about a tool's effects, asking its server if this copy has not seen it.
    async fn effect_of(&self, name: &str) -> Effect {
        let policy = &self.inner.policy;
        if !policy.has_override(name)
            && self.listed(name).is_none()
            && let Some((upstream, _)) = self.route(name)
        {
            self.list_upstream(upstream).await;
        }
        policy.effect_of(name, self.listed(name).as_ref())
    }

    fn listed(&self, name: &str) -> Option<Tool> {
        let catalog = self.inner.catalog.lock().expect("catalog lock poisoned");
        catalog.get(name).cloned()
    }

    /// One server's tools under the names agents know them by, or nothing if it cannot
    /// say. A failed listing leaves what was last learned about its tools in place.
    async fn list_upstream(&self, upstream: &Arc<dyn Upstream>) -> Vec<Tool> {
        let name = upstream.name();
        let listing = tokio::time::timeout(self.inner.call_timeout, upstream.list_tools()).await;
        let listed = match listing {
            Ok(Ok(listed)) => listed,
            Ok(Err(e)) => {
                tracing::warn!(upstream = name, error = %e, "leaving out unavailable tools");
                return Vec::new();
            }
            Err(_) => {
                tracing::warn!(upstream = name, "leaving out tools: no answer in time");
                return Vec::new();
            }
        };
        let tools: Vec<Tool> = listed
            .into_iter()
            .map(|mut tool| {
                tool.name = Cow::Owned(prefixed(name, &tool.name));
                tool
            })
            .collect();

        let own_prefix = prefixed(name, "");
        let mut catalog = self.inner.catalog.lock().expect("catalog lock poisoned");
        catalog.retain(|known, _| !known.starts_with(&own_prefix));
        catalog.extend(tools.iter().map(|t| (t.name.to_string(), t.clone())));
        tools
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
        let listings = join_all(self.inner.upstreams.iter().map(|u| self.list_upstream(u))).await;
        let tools = listings.into_iter().flatten().collect();
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
