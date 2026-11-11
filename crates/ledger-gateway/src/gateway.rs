use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::join_all;
use ledger_store::{Event, EventKind, EventStore, Lease, NewEvent, Outcome, StoreError, args_hash};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, DiscoverRequestMethod, DiscoverResult,
    ErrorCode, Implementation, ListToolsResult, PaginatedRequestParams, ServerCapabilities,
    ServerConfig, ServerResult, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Value, json};
use tokio::sync::watch;
use uuid::Uuid;

use crate::dedup::{self, Decision, Incoming};
use crate::policy::{DEFAULT_KEY_PARAM, Effect, EffectClass, EffectPolicy};
use crate::session::{resolve_session, resolve_step};
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
    /// The tool may have run, and nobody knows whether it did. This includes calls that
    /// ledger has flagged for a person, because running them again is not safe.
    pub const OUTCOME_UNKNOWN: ErrorCode = ErrorCode(-32052);
}

/// How long a session stays with a copy that has stopped appending to it.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(30);

type Answer = Result<CallToolResponse, ErrorData>;
type CallId = (String, i64);

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
    /// One at a time per session: deciding what a call is, and logging that decision.
    session_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Calls this copy is running, so that a retry can wait for the same result.
    in_flight: Mutex<HashMap<CallId, watch::Receiver<Option<Answer>>>>,
}

/// One `tools/call` as it arrived: what the gateway knows about a call besides the call.
pub struct Arrival {
    pub session: String,
    pub rpc_id: Value,
    pub meta: Value,
    /// The MCP transport session (`Mcp-Session-Id`), if there is one.
    pub transport: Option<String>,
    pub step: Option<String>,
    pub params: CallToolRequestParams,
}

/// What to do about a call, decided under its session's lock.
enum Plan {
    Forward {
        key: Option<(String, String)>,
        done: watch::Sender<Option<Answer>>,
    },
    Wait(watch::Receiver<Option<Answer>>),
    Answer(Answer),
}

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
                session_locks: Mutex::default(),
                in_flight: Mutex::default(),
            }),
        }
    }

    /// Names this copy in the leases it holds, instead of a random name. For runs that must
    /// repeat exactly. Call it before serving.
    pub fn with_owner(self, owner: impl Into<String>) -> Self {
        let mut inner = Arc::into_inner(self.inner).expect("configure the gateway before use");
        inner.owner = owner.into();
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Handles a call the way `tools/call` over HTTP does, detached from the caller, so that
    /// the caller going away cannot stop its outcome reaching the log. For driving copies
    /// without HTTP, as the simulator does.
    pub async fn call(&self, arrival: Arrival) -> Answer {
        let gateway = self.clone();
        tokio::spawn(async move { gateway.handle(arrival).await.1 })
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?
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
            self.forget_lease(&lease).await;
        }
        appended
    }

    async fn forget_lease(&self, lease: &Lease) {
        let mut leases = self.inner.leases.lock().await;
        if leases.get(&lease.session_id) == Some(lease) {
            leases.remove(&lease.session_id);
        }
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
        self.flag_open_calls(&lease).await?;
        leases.insert(session.to_owned(), lease.clone());
        Ok(lease)
    }

    /// On taking a session, any call with no final answer is in doubt: whoever forwarded
    /// it may or may not have run the tool. Those not safe to repeat are answered as in
    /// doubt here, for a person to check, so nothing will run them again. Safe and keyed
    /// ones stay open, for a retry to settle.
    async fn flag_open_calls(&self, lease: &Lease) -> Result<(), StoreError> {
        let store = &self.inner.store;
        let events = store.session_events(&lease.session_id).await?;
        let open = events.iter().filter(|e| {
            e.kind == EventKind::CallRequest
                && dedup::final_answer(&events, e.seq).is_none()
                && dedup::logged_class(e) == EffectClass::AtMostOnce
        });
        for request in open {
            let flag = NewEvent::response(
                &lease.session_id,
                request.seq,
                &request.tool,
                Outcome::InDoubt,
                json!({ "reason": "open when this copy took the session over" }),
            );
            match store.append(flag, lease).await {
                Ok(_) => tracing::warn!(
                    session = lease.session_id,
                    seq = request.seq,
                    tool = request.tool,
                    "flagged a call as in doubt"
                ),
                Err(StoreError::Fenced) => return Err(StoreError::Fenced),
                // Most likely answered meanwhile by the copy that forwarded it.
                Err(e) => tracing::warn!(seq = request.seq, error = %e, "could not flag call"),
            }
        }
        Ok(())
    }

    fn session_lock(&self, session: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.inner.session_locks.lock().expect("lock poisoned");
        locks.entry(session.to_owned()).or_default().clone()
    }

    /// The core rule of the log: a call is committed before the tool can run, and nothing
    /// reaches the agent that the log does not also hold. Everything the agent saw can
    /// therefore be replayed from the log. Returns the call the answer belongs to.
    async fn handle(&self, arrival: Arrival) -> (Option<i64>, Answer) {
        let tool = arrival.params.name.to_string();
        let effect = self.effect_of(&tool).await;
        let lock = self.session_lock(&arrival.session);
        let guard = lock.lock().await;
        let planned = self.plan(&arrival, &tool, &effect).await;
        drop(guard);
        let (seq, plan) = match planned {
            Ok(planned) => planned,
            Err(error) => return (None, Err(error)),
        };

        let answer = match plan {
            Plan::Answer(answer) => answer,
            Plan::Wait(mut done) => match done.wait_for(Option::is_some).await {
                Ok(answer) => answer.clone().expect("waited for an answer"),
                Err(_) => Err(ErrorData::internal_error("the call was abandoned", None)),
            },
            Plan::Forward { key, done } => {
                let answer = self.run(&arrival, &tool, seq, key).await;
                let id = (arrival.session.clone(), seq);
                self.inner
                    .in_flight
                    .lock()
                    .expect("lock poisoned")
                    .remove(&id);
                let _ = done.send(Some(answer.clone()));
                answer
            }
        };
        (Some(seq), answer)
    }

    /// Decides what the call is, and logs that decision before anything else happens.
    async fn plan(
        &self,
        arrival: &Arrival,
        tool: &str,
        effect: &Effect,
    ) -> Result<(i64, Plan), ErrorData> {
        let session = arrival.session.as_str();
        let not_recorded = |e: StoreError| {
            tracing::error!(session, tool, error = %e, "could not record call");
            ErrorData::new(
                error_codes::NOT_RECORDED,
                format!("ledger could not record this call, so it was not forwarded: {e}"),
                None,
            )
        };
        self.lease(session).await.map_err(not_recorded)?;
        let events = self
            .inner
            .store
            .session_events(session)
            .await
            .map_err(not_recorded)?;

        let arguments = json!(arrival.params.arguments);
        let hash = args_hash(&arguments);
        let incoming = Incoming {
            tool,
            args_hash: &hash,
            rpc_id: &arrival.rpc_id,
            transport: arrival.transport.as_deref(),
            step: arrival.step.as_deref(),
        };
        let decision = dedup::decide(&events, &incoming);

        let mut payload = json!({
            "rpc_id": arrival.rpc_id,
            "meta": arrival.meta,
            "transport": arrival.transport,
            "step": arrival.step,
        });
        match decision {
            Decision::New | Decision::StepReused(_) => {
                payload["effect"] = json!(effect);
                payload["params"] = json!(arrival.params);
                let request = NewEvent::request(session, tool, &arguments, payload);
                let seq = self.append(request).await.map_err(not_recorded)?.seq;
                if let Decision::StepReused(earlier) = decision {
                    let reason = format!(
                        "step {} already named call #{earlier}, which had different arguments",
                        arrival.step.as_deref().unwrap_or_default()
                    );
                    let refusal = json!({ "reason": reason });
                    let response =
                        NewEvent::response(session, seq, tool, Outcome::Refused, refusal);
                    self.append(response).await.map_err(not_recorded)?;
                    let refused = ErrorData::invalid_params(reason, None);
                    return Ok((seq, Plan::Answer(Err(refused))));
                }
                let key = effect
                    .key_param
                    .clone()
                    .map(|param| (param, key_for(session, seq)));
                Ok((seq, self.start(session, seq, key)))
            }
            Decision::Retry(seq) => {
                let retry = NewEvent::retry(session, seq, tool, payload);
                self.append(retry).await.map_err(not_recorded)?;
                let id = (session.to_owned(), seq);
                let running = self
                    .inner
                    .in_flight
                    .lock()
                    .expect("lock poisoned")
                    .get(&id)
                    .cloned();
                if let Some(done) = running {
                    return Ok((seq, Plan::Wait(done)));
                }
                if let Some(answer) = dedup::final_answer(&events, seq) {
                    return Ok((seq, Plan::Answer(from_log(answer, session))));
                }
                let request = dedup::request(&events, seq).expect("decided from this log");
                let plan = self.settle_in_doubt(request).await.map_err(not_recorded)?;
                Ok((seq, plan))
            }
        }
    }

    /// A retry of a call that has no final answer and is not running here: it may or may
    /// not have run. Safe calls run again, keyed ones with the key they had, and the rest
    /// are answered as in doubt for a person, never run again.
    async fn settle_in_doubt(&self, request: &Event) -> Result<Plan, StoreError> {
        let session = &request.session_id;
        match dedup::logged_class(request) {
            EffectClass::ReadOnly | EffectClass::Idempotent => {
                Ok(self.start(session, request.seq, None))
            }
            EffectClass::Keyed => {
                let param = request.payload["effect"]["key_param"]
                    .as_str()
                    .unwrap_or(DEFAULT_KEY_PARAM)
                    .to_owned();
                let key = Some((param, key_for(session, request.seq)));
                Ok(self.start(session, request.seq, key))
            }
            EffectClass::AtMostOnce => {
                let reason = "retried with no recorded outcome; not safe to run again";
                let flag = NewEvent::response(
                    session,
                    request.seq,
                    &request.tool,
                    Outcome::InDoubt,
                    json!({ "reason": reason }),
                );
                self.append(flag).await?;
                let error = in_doubt_error(&request.tool, session, request.seq);
                Ok(Plan::Answer(Err(error)))
            }
        }
    }

    fn start(&self, session: &str, seq: i64, key: Option<(String, String)>) -> Plan {
        let (done, waiting) = watch::channel(None);
        let id = (session.to_owned(), seq);
        self.inner
            .in_flight
            .lock()
            .expect("lock poisoned")
            .insert(id, waiting);
        Plan::Forward { key, done }
    }

    /// Forwards the call once, keeping the session's lease alive meanwhile, and logs the
    /// answer against call `seq`.
    async fn run(
        &self,
        arrival: &Arrival,
        tool: &str,
        seq: i64,
        key: Option<(String, String)>,
    ) -> Answer {
        let session = arrival.session.as_str();
        let mut params = arrival.params.clone();
        if let Some((param, key)) = &key {
            // Overwrites any key the agent sent: a retry must carry the original's key.
            params
                .arguments
                .get_or_insert_default()
                .insert(param.clone(), Value::String(key.clone()));
        }

        let heartbeat = self.heartbeat(session).await;
        let forwarded = self.forward(params).await;
        drop(heartbeat);

        let (outcome, mut payload) = describe(&forwarded);
        if let Some((_, key)) = key {
            payload["idempotency_key"] = Value::String(key);
        }
        let response = NewEvent::response(session, seq, tool, outcome, payload);
        if let Err(e) = self.append(response).await {
            tracing::error!(session, tool, seq, error = %e, "could not record response");
            return Err(ErrorData::new(
                error_codes::OUTCOME_NOT_RECORDED,
                "the tool may have run, but ledger could not record the outcome",
                None,
            ));
        }
        self.agent_view(forwarded, tool)
    }

    /// Renews the session's lease while a call runs, so that a call longer than the lease
    /// does not let another copy take the session over. Stops when dropped, or once the
    /// session has been taken over anyway.
    async fn heartbeat(&self, session: &str) -> Option<AbortOnDrop> {
        let ttl = self.inner.lease_ttl;
        if ttl.is_zero() {
            return None;
        }
        let lease = self.lease(session).await.ok()?;
        let gateway = self.clone();
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(ttl / 4).await;
                match gateway.inner.store.renew_lease(&lease).await {
                    Ok(()) => {}
                    Err(StoreError::Fenced) => {
                        gateway.forget_lease(&lease).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(session = lease.session_id, error = %e, "could not renew lease");
                    }
                }
            }
        });
        Some(AbortOnDrop(task))
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

    /// A tool as agents should see it. A keyed tool's key input is the gateway's to fill
    /// in, so it is hidden: a model never sees it, and cannot supply one.
    fn agent_listing(&self, mut tool: Tool) -> Tool {
        let effect = self.inner.policy.effect_of(&tool.name, Some(&tool));
        if let Some(param) = effect.key_param {
            let mut schema = (*tool.input_schema).clone();
            if let Some(Value::Object(properties)) = schema.get_mut("properties") {
                properties.remove(&param);
            }
            if let Some(Value::Array(required)) = schema.get_mut("required") {
                required.retain(|r| r.as_str() != Some(param.as_str()));
            }
            tool.input_schema = Arc::new(schema);
        }
        tool
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

    fn agent_view(&self, forwarded: Forwarded, tool: &str) -> Answer {
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
            Forwarded::NotRouted => Err(not_routed(tool)),
        }
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The idempotency key for a call: the same for every attempt at it, and different for
/// every other call.
fn key_for(session: &str, seq: i64) -> String {
    format!("ledger:{session}:{seq}")
}

fn not_routed(tool: &str) -> ErrorData {
    ErrorData::invalid_params(
        format!("no tool server for {tool}; tool names look like <server>{TOOL_SEPARATOR}<tool>"),
        None,
    )
}

fn in_doubt_error(tool: &str, session: &str, seq: i64) -> ErrorData {
    ErrorData::new(
        error_codes::OUTCOME_UNKNOWN,
        format!(
            "{tool} may already have run, and running it again is not safe, so ledger will \
             not; call #{seq} in session {session} is flagged for a person to check \
             (`ledger in-doubt`)"
        ),
        Some(json!({ "in_doubt": true, "session": session, "seq": seq })),
    )
}

/// What the agent was told the first time, read back from the log.
fn from_log(answer: &Event, session: &str) -> Answer {
    let payload = &answer.payload;
    let unreadable = |e: serde_json::Error| {
        ErrorData::internal_error(format!("unreadable logged answer: {e}"), None)
    };
    match answer.outcome {
        Some(Outcome::Ok | Outcome::ToolError) => {
            serde_json::from_value::<CallToolResult>(payload["result"].clone())
                .map(CallToolResponse::from)
                .map_err(unreadable)
        }
        Some(Outcome::RpcError) => {
            Err(serde_json::from_value(payload["error"].clone()).unwrap_or_else(unreadable))
        }
        Some(Outcome::NotRouted) => Err(not_routed(&answer.tool)),
        Some(Outcome::Refused) => Err(ErrorData::invalid_params(
            payload["reason"].as_str().unwrap_or("refused").to_owned(),
            None,
        )),
        _ => Err(in_doubt_error(
            &answer.tool,
            session,
            answer.request_seq.unwrap_or_default(),
        )),
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
        let tools = listings
            .into_iter()
            .flatten()
            .map(|tool| self.agent_listing(tool))
            .collect();
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
        let arrival = Arrival {
            session,
            rpc_id: json!(context.id),
            // The SDK has already moved the agent's `_meta` out of `request`. It gets
            // logged but goes no further: it describes the agent's exchange with ledger.
            meta: json!(context.meta),
            transport: parts
                .headers
                .get("mcp-session-id")
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            step: resolve_step(parts, &json!(context.meta))?,
            params: request,
        };

        self.call(arrival).await
    }
}
