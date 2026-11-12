use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::join_all;
use ledger_store::{
    Event, EventKind, EventStore, Fork, Lease, NewEvent, Outcome, StoreError, args_hash,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, DiscoverRequestMethod,
    DiscoverResult, ErrorCode, Implementation, ListToolsResult, PaginatedRequestParams,
    ServerCapabilities, ServerConfig, ServerResult, Tool,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use serde_json::{Value, json};
use tokio::sync::watch;
use uuid::Uuid;

use crate::dedup::{self, Decision, Incoming};
use crate::fork::{self, Asked, Replay};
use crate::model::{self, ModelUpstream};
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
    /// In a fork, the agent asked for something its source did not ask for before the
    /// fork step, so the run is no longer a fork of the source.
    pub const FORK_DIVERGED: ErrorCode = ErrorCode(-32053);
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
    /// Where model calls go, if this gateway proxies them.
    model: Option<ModelUpstream>,
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
    /// Which sessions are forks. A session that has recorded anything can never become
    /// one, so once it has, the answer is kept either way.
    forks: Mutex<HashMap<String, Option<Fork>>>,
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

/// A model call as it arrived over HTTP.
pub struct ModelCall {
    pub session: String,
    /// The request body, a JSON object.
    pub body: Value,
    /// The agent's credentials for the model, used only if the gateway has none.
    pub credentials: http::HeaderMap,
}

/// A model call's failure, for the agent: the HTTP status and an OpenAI-style error body.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelFailure {
    pub status: http::StatusCode,
    pub body: Value,
}

/// The logged reply on success, which the agent's answer is rebuilt from.
pub type ModelAnswer = Result<Value, ModelFailure>;

/// What a fork does with a model call.
enum ModelPlan {
    /// Answered from the source's log, or turned down; logged either way.
    Answered(ModelAnswer),
    Live,
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
                model: None,
                call_timeout,
                policy,
                catalog: Mutex::default(),
                owner: Uuid::new_v4().to_string(),
                lease_ttl: DEFAULT_LEASE_TTL,
                leases: tokio::sync::Mutex::default(),
                session_locks: Mutex::default(),
                in_flight: Mutex::default(),
                forks: Mutex::default(),
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

    /// Proxies model calls to `model`. Call it before serving.
    pub fn with_model(self, model: ModelUpstream) -> Self {
        let mut inner = Arc::into_inner(self.inner).expect("configure the gateway before use");
        inner.model = Some(model);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Handles a model call, detached from the caller like [`Gateway::call`]: logs the
    /// request, forwards it once, logs the reply, and returns the reply as logged.
    pub async fn model_call(&self, call: ModelCall) -> ModelAnswer {
        let gateway = self.clone();
        tokio::spawn(async move { gateway.handle_model(call).await })
            .await
            .unwrap_or_else(|e| {
                Err(model_failure(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::INTERNAL_ERROR,
                    "internal_error",
                    &e.to_string(),
                ))
            })
    }

    async fn handle_model(&self, call: ModelCall) -> ModelAnswer {
        let Some(upstream) = &self.inner.model else {
            return Err(model_failure(
                http::StatusCode::NOT_FOUND,
                ErrorCode::METHOD_NOT_FOUND,
                "not_configured",
                "this gateway has no [model] configured",
            ));
        };
        let session = call.session.as_str();
        let name = call.body["model"].as_str().unwrap_or_default().to_owned();
        let not_recorded = |e: StoreError| {
            tracing::error!(session, model = name, error = %e, "could not record model call");
            model_failure(
                http::StatusCode::SERVICE_UNAVAILABLE,
                error_codes::NOT_RECORDED,
                "not_recorded",
                &format!("ledger could not record this call, so it was not sent: {e}"),
            )
        };
        let mut payload = json!({ "request": call.body });
        let mut sent = call.body.clone();

        // A fork decides and logs under the session's lock, so that two calls at once
        // cannot both be answered from the same source call.
        let fork = self.fork_of(session).await.map_err(not_recorded)?;
        let lock = self.session_lock(session);
        let guard = match &fork {
            Some(_) => Some(lock.lock().await),
            None => None,
        };
        if let Some(fork) = &fork {
            match self.replay_model(fork, &call.body, &name).await {
                Ok(ModelPlan::Answered(answer)) => return answer,
                Ok(ModelPlan::Live) => {
                    if let Some(model) = &fork.model {
                        sent["model"] = json!(model);
                    }
                    if let Some(prompt) = &fork.system_prompt {
                        set_system_prompt(&mut sent, prompt);
                    }
                    if sent != call.body {
                        payload["sent"] = sent.clone();
                    }
                }
                Err(e) => return Err(not_recorded(e)),
            }
        }

        let request = NewEvent::model_request(session, &name, &call.body, payload);
        let seq = self.append(request).await.map_err(not_recorded)?.seq;
        drop(guard);

        let heartbeat = self.heartbeat(session).await;
        let sent =
            tokio::time::timeout(upstream.timeout(), upstream.send(&sent, &call.credentials)).await;
        drop(heartbeat);

        let (outcome, logged, failure) = match sent {
            Ok(Ok(reply)) => {
                let outcome = if reply.status.is_success() {
                    Outcome::Ok
                } else {
                    Outcome::RpcError
                };
                (outcome, model::logged(&reply), None)
            }
            Ok(Err(e)) => {
                let message = format!("the model could not be reached: {e}");
                let failure = (http::StatusCode::BAD_GATEWAY, "transport_error", message);
                (
                    Outcome::TransportError,
                    json!({ "error": failure.2 }),
                    Some(failure),
                )
            }
            Err(_) => {
                let message = format!("no answer from the model within {:?}", upstream.timeout());
                let failure = (http::StatusCode::GATEWAY_TIMEOUT, "timeout", message);
                (
                    Outcome::Timeout,
                    json!({ "error": failure.2 }),
                    Some(failure),
                )
            }
        };
        let response = NewEvent::model_response(session, seq, &name, outcome, logged.clone());
        if let Err(e) = self.append(response).await {
            tracing::error!(session, model = name, seq, error = %e, "could not record model reply");
            return Err(model_failure(
                http::StatusCode::SERVICE_UNAVAILABLE,
                error_codes::OUTCOME_NOT_RECORDED,
                "outcome_not_recorded",
                "the model answered, but ledger could not record the answer",
            ));
        }
        match failure {
            None => Ok(logged),
            Some((status, kind, message)) => Err(model_failure(
                status,
                error_codes::OUTCOME_UNKNOWN,
                kind,
                &message,
            )),
        }
    }

    /// Answers a model call in a fork from its source's log, or turns it down, logging both
    /// sides. `Live` means it is to be sent, with the fork's model and prompt.
    async fn replay_model(
        &self,
        fork: &Fork,
        body: &Value,
        name: &str,
    ) -> Result<ModelPlan, StoreError> {
        let session = fork.session_id.as_str();
        let own = self.inner.store.session_events(session).await?;
        let source = self
            .inner
            .store
            .session_events(&fork.source_session)
            .await?;
        let hash = args_hash(body);
        let asked = Asked {
            kind: EventKind::ModelRequest,
            name,
            args_hash: &hash,
            read_only: true,
        };
        let mut payload = json!({ "request": body });
        let (outcome, logged, answer) = match fork::decide(fork, &source, &own, &asked) {
            Replay::Live => return Ok(ModelPlan::Live),
            Replay::Source { request, answer } => {
                let from = json!({ "session": fork.source_session, "seq": request.seq });
                payload["replayed_from"] = from.clone();
                let mut logged = answer.payload.clone();
                logged["replayed_from"] = from;
                let outcome = answer.outcome.unwrap_or(Outcome::Ok);
                (outcome, logged, Ok(answer.payload.clone()))
            }
            Replay::Refuse(reason) | Replay::Diverged(reason) => {
                let failure = model_failure(
                    http::StatusCode::CONFLICT,
                    error_codes::FORK_DIVERGED,
                    "fork_diverged",
                    &reason,
                );
                (Outcome::Refused, json!({ "reason": reason }), Err(failure))
            }
        };
        let request = NewEvent::model_request(session, name, body, payload);
        let seq = self.append(request).await?.seq;
        let response = NewEvent::model_response(session, seq, name, outcome, logged);
        self.append(response).await?;
        Ok(ModelPlan::Answered(answer))
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
        let session = event.session_id.clone();
        let appended = self.inner.store.append(event, &lease).await;
        match &appended {
            Err(StoreError::Fenced) => self.forget_lease(&lease).await,
            // It has recorded something now, so if it is not a fork it never will be.
            Ok(_) => {
                let mut forks = self.inner.forks.lock().expect("lock poisoned");
                forks.entry(session).or_insert(None);
            }
            Err(_) => {}
        }
        appended
    }

    /// The fork this session is, if it is one.
    async fn fork_of(&self, session: &str) -> Result<Option<Fork>, StoreError> {
        let known = self
            .inner
            .forks
            .lock()
            .expect("lock poisoned")
            .get(session)
            .cloned();
        if let Some(known) = known {
            return Ok(known);
        }
        let fork = self.inner.store.fork(session).await?;
        if fork.is_some() {
            let mut forks = self.inner.forks.lock().expect("lock poisoned");
            forks.insert(session.to_owned(), fork.clone());
        }
        Ok(fork)
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
                if let Decision::New = decision
                    && let Some(fork) = self.fork_of(session).await.map_err(not_recorded)?
                {
                    let asked = Asked {
                        kind: EventKind::CallRequest,
                        name: tool,
                        args_hash: &hash,
                        read_only: effect.class == EffectClass::ReadOnly,
                    };
                    let answered = self
                        .replay_tool(&fork, &events, &asked, &arguments, payload.clone())
                        .await
                        .map_err(not_recorded)?;
                    if let Some(answered) = answered {
                        return Ok(answered);
                    }
                }
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

    /// Answers a tool call in a fork from its source's log, or turns it down, logging both
    /// sides here. None means the call is to run as it would anywhere else.
    async fn replay_tool(
        &self,
        fork: &Fork,
        own: &[Event],
        asked: &Asked<'_>,
        arguments: &Value,
        mut payload: Value,
    ) -> Result<Option<(i64, Plan)>, StoreError> {
        let session = fork.session_id.as_str();
        let tool = asked.name;
        let source = self
            .inner
            .store
            .session_events(&fork.source_session)
            .await?;
        let (outcome, logged, failure) = match fork::decide(fork, &source, own, asked) {
            Replay::Live => return Ok(None),
            Replay::Source { request, answer } => {
                let from = json!({ "session": fork.source_session, "seq": request.seq });
                payload["replayed_from"] = from.clone();
                let mut logged = answer.payload.clone();
                logged["replayed_from"] = from;
                (answer.outcome.unwrap_or(Outcome::InDoubt), logged, None)
            }
            Replay::Refuse(reason) => {
                let result = CallToolResult::error(vec![ContentBlock::text(reason.clone())]);
                let logged = json!({ "reason": reason, "result": result });
                (Outcome::Refused, logged, None)
            }
            Replay::Diverged(reason) => {
                let error = ErrorData::new(error_codes::FORK_DIVERGED, reason.clone(), None);
                (Outcome::Refused, json!({ "reason": reason }), Some(error))
            }
        };
        let request = NewEvent::request(session, tool, arguments, payload);
        let seq = self.append(request).await?.seq;
        let response = NewEvent::response(session, seq, tool, outcome, logged);
        let answer = self.append(response).await?;
        let answer = match failure {
            Some(error) => Err(error),
            None => from_log(&answer, session),
        };
        Ok(Some((seq, Plan::Answer(answer))))
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
/// Puts `prompt` in place of the conversation's system message, or first if it has none.
fn set_system_prompt(body: &mut Value, prompt: &str) {
    let message = json!({ "role": "system", "content": prompt });
    match body["messages"].as_array_mut() {
        Some(messages) if messages.first().is_some_and(|m| m["role"] == "system") => {
            messages[0] = message;
        }
        Some(messages) => messages.insert(0, message),
        None => body["messages"] = json!([message]),
    }
}

fn model_failure(
    status: http::StatusCode,
    code: ErrorCode,
    kind: &str,
    message: &str,
) -> ModelFailure {
    ModelFailure {
        status,
        body: model::error_body(code.0, kind, message),
    }
}

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
        Some(Outcome::Refused) if !payload["result"].is_null() => {
            serde_json::from_value::<CallToolResult>(payload["result"].clone())
                .map(CallToolResponse::from)
                .map_err(unreadable)
        }
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
