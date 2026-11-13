//! Retries, calls in doubt, idempotency keys, and keeping a session through a long call.

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use common::{
    CALL_TIMEOUT, FaultyStore, Harness, Probe, eventually, spawn_gateway, spawn_tool_server,
    upstream,
};
use ledger_gateway::{Gateway, error_codes};
use ledger_store::{EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::{PATIENCE, TestDb};
use rmcp::ServiceError;
use rmcp::model::{CallToolRequestParams, ErrorCode, ToolAnnotations};
use sample_agent::{call_params, connect};
use serde_json::{Value, json};

fn pg(db: &TestDb) -> Arc<dyn EventStore> {
    Arc::new(PgEventStore::new(db.pool()))
}

/// A call numbered by the agent, as an agent that knows about ledger would send it.
fn step(mut params: CallToolRequestParams, step: &str) -> CallToolRequestParams {
    params.meta = serde_json::from_value(json!({ "ledger/step": step })).unwrap();
    params
}

fn append(content: &str) -> CallToolRequestParams {
    call_params(
        "files__append_file",
        json!({"path": "log.txt", "content": content}),
    )
}

fn send_email() -> CallToolRequestParams {
    call_params(
        "email__send_email",
        json!({"to": "a@example.com", "subject": "s", "body": "b"}),
    )
}

fn error(error: ServiceError) -> rmcp::ErrorData {
    match error {
        ServiceError::McpError(error) => error,
        other => panic!("expected a JSON-RPC error, got {other}"),
    }
}

async fn read(harness: &Harness, path: &str) -> String {
    std::fs::read_to_string(harness.files.path().join(path)).unwrap_or_default()
}

/// The shape of a session's log: kind, the call it links to, and outcome.
async fn shape(store: &Arc<dyn EventStore>) -> Vec<(EventKind, Option<i64>, Option<Outcome>)> {
    let events = store.session_events("s").await.unwrap();
    events
        .iter()
        .map(|e| (e.kind, e.request_seq, e.outcome))
        .collect()
}

/// A harness in front of a store that can be told to lose responses.
async fn faulty_harness(db: &TestDb) -> (Arc<FaultyStore>, Arc<dyn EventStore>, Harness) {
    let real = pg(db);
    let faulty = Arc::new(FaultyStore::new(real.clone()));
    let harness = Harness::start(faulty.clone()).await;
    (faulty, real, harness)
}

/// An MCP client over HTTP that numbers its own requests, so that it can send one again
/// with the same JSON-RPC id over the same connection, as an agent does when an answer is
/// lost. The SDK's client numbers requests itself and never sends one twice.
struct RawAgent {
    http: reqwest::Client,
    url: String,
    session: String,
    transport: Option<String>,
}

impl RawAgent {
    async fn connect(url: &str, session: &str) -> Self {
        let mut agent = Self {
            http: reqwest::Client::new(),
            url: url.to_owned(),
            session: session.to_owned(),
            transport: None,
        };
        let initialize = json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "raw", "version": "1"},
            },
        });
        let response = agent.send(&initialize).await;
        let transport = response.headers()["mcp-session-id"].to_str().unwrap();
        agent.transport = Some(transport.to_owned());
        message(response).await;
        let initialized = json!({"jsonrpc": "2.0", "method": "notifications/initialized"});
        agent.send(&initialized).await;
        agent
    }

    async fn send(&self, body: &Value) -> reqwest::Response {
        let mut request = self
            .http
            .post(&self.url)
            .header("accept", "application/json, text/event-stream")
            .header("x-ledger-session", &self.session)
            .json(body);
        if let Some(transport) = &self.transport {
            request = request
                .header("mcp-session-id", transport)
                .header("mcp-protocol-version", "2025-06-18");
        }
        let response = request.send().await.unwrap();
        assert!(response.status().is_success(), "{}", response.status());
        response
    }

    /// Calls a tool under the given JSON-RPC id, and returns the result.
    async fn call(&self, id: u64, params: &CallToolRequestParams) -> Value {
        let body = json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": params});
        let answer = message(self.send(&body).await).await;
        assert_eq!(answer["id"], id, "{answer}");
        answer["result"].clone()
    }
}

/// The last JSON-RPC message in a response, whether sent as JSON or as an event stream.
async fn message(response: reqwest::Response) -> Value {
    let json = response
        .headers()
        .get("content-type")
        .is_some_and(|t| t.as_bytes().starts_with(b"application/json"));
    let body = response.text().await.unwrap();
    if json {
        return serde_json::from_str(&body).unwrap();
    }
    let data = body
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .rfind(|data| !data.is_empty())
        .unwrap_or_else(|| panic!("no message in {body:?}"));
    serde_json::from_str(data).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_step_is_answered_from_the_log_and_runs_once() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    let first = agent.call_tool(step(append("x"), "1")).await.unwrap();
    let again = agent.call_tool(step(append("x"), "1")).await.unwrap();

    assert_eq!(again, first);
    assert_eq!(read(&harness, "log.txt").await, "x");
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
            (EventKind::CallRetry, Some(1), None),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_step_reused_for_other_arguments_is_refused() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    agent.call_tool(step(append("x"), "1")).await.unwrap();
    let refused = error(agent.call_tool(step(append("y"), "1")).await.unwrap_err());

    assert_eq!(refused.code, ErrorCode::INVALID_PARAMS);
    assert_eq!(read(&harness, "log.txt").await, "x");
    assert_eq!(
        shape(&store).await[2..],
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(3), Some(Outcome::Refused)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_calls_without_a_step_all_run() {
    let db = TestDb::new().await;
    let harness = Harness::start(pg(&db)).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    agent.call_tool(append("x")).await.unwrap();
    agent.call_tool(append("x")).await.unwrap();
    agent.call_tool(send_email()).await.unwrap();
    agent.call_tool(send_email()).await.unwrap();

    // Identical arguments alone never make a retry: the agent may mean both.
    assert_eq!(read(&harness, "log.txt").await, "xx");
    assert_eq!(harness.outbox.sent().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resent_id_is_a_retry_even_after_another_call_on_the_connection() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = RawAgent::connect(&harness.url, "s").await;

    let first = agent.call(5, &append("x")).await;
    agent.call(6, &append("y")).await;
    // The answer to 5 was lost, and the agent sends it again after 6.
    let again = agent.call(5, &append("x")).await;

    assert_eq!(again, first);
    assert_eq!(read(&harness, "log.txt").await, "xy");
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(3), Some(Outcome::Ok)),
            (EventKind::CallRetry, Some(1), None),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn identical_calls_in_different_steps_all_run_and_a_resent_step_does_not() {
    let db = TestDb::new().await;
    let harness = Harness::start(pg(&db)).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    agent.call_tool(step(send_email(), "1")).await.unwrap();
    agent.call_tool(step(send_email(), "2")).await.unwrap();
    agent.call_tool(step(send_email(), "2")).await.unwrap();

    let sent = harness.outbox.sent().await.unwrap();
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0].idempotency_key, sent[1].idempotency_key);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_while_the_call_runs_waits_for_it() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let url = spawn_gateway(gateway).await;
    let agent = Arc::new(connect(&url, Some("s")).await.unwrap());
    let call = || step(call_params("probe__wait", json!({})), "1");

    let first = tokio::spawn({
        let agent = agent.clone();
        async move { agent.call_tool(call()).await }
    });
    probe.wait_until_started().await;
    let second = tokio::spawn({
        let agent = agent.clone();
        async move { agent.call_tool(call()).await }
    });
    eventually(|| async { shape(&store).await.len() == 2 }).await;
    probe.release();

    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first, second);
    assert_eq!(probe.started.load(Ordering::SeqCst), 1);
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallRetry, Some(1), None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_call_whose_outcome_was_lost_runs_again_with_the_same_key() {
    let db = TestDb::new().await;
    let (faulty, store, harness) = faulty_harness(&db).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    faulty.fail_appends_of(EventKind::CallResponse).await;
    let lost = error(agent.call_tool(step(send_email(), "1")).await.unwrap_err());
    assert_eq!(lost.code, error_codes::OUTCOME_NOT_RECORDED);
    faulty.heal().await;
    agent.call_tool(step(send_email(), "1")).await.unwrap();

    // Sent twice with one key, so delivered once.
    let sent = harness.outbox.sent().await.unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].idempotency_key.as_deref(), Some("ledger:s:1"));
    let events = store.session_events("s").await.unwrap();
    assert_eq!(
        events.last().unwrap().payload["idempotency_key"],
        "ledger:s:1"
    );
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallRetry, Some(1), None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agents_own_key_is_replaced() {
    let db = TestDb::new().await;
    let harness = Harness::start(pg(&db)).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();
    let arguments =
        json!({"to": "a@example.com", "subject": "s", "body": "b", "idempotency_key": "mine"});

    agent
        .call_tool(call_params("email__send_email", arguments))
        .await
        .unwrap();

    let sent = harness.outbox.sent().await.unwrap();
    assert_eq!(sent[0].idempotency_key.as_deref(), Some("ledger:s:1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_idempotent_call_in_doubt_runs_again() {
    let db = TestDb::new().await;
    let (faulty, store, harness) = faulty_harness(&db).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();
    let write = || {
        step(
            call_params(
                "files__write_file",
                json!({"path": "f.txt", "content": "x"}),
            ),
            "1",
        )
    };

    faulty.fail_appends_of(EventKind::CallResponse).await;
    agent.call_tool(write()).await.unwrap_err();
    faulty.heal().await;
    agent.call_tool(write()).await.unwrap();

    assert_eq!(read(&harness, "f.txt").await, "x");
    assert_eq!(shape(&store).await.last().unwrap().2, Some(Outcome::Ok));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unsafe_call_in_doubt_is_flagged_and_never_runs_again() {
    let db = TestDb::new().await;
    let (faulty, store, harness) = faulty_harness(&db).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    faulty.fail_appends_of(EventKind::CallResponse).await;
    let lost = error(agent.call_tool(step(append("x"), "1")).await.unwrap_err());
    assert_eq!(lost.code, error_codes::OUTCOME_NOT_RECORDED);
    faulty.heal().await;

    for _ in 0..2 {
        let flagged = error(agent.call_tool(step(append("x"), "1")).await.unwrap_err());
        assert_eq!(flagged.code, error_codes::OUTCOME_UNKNOWN);
        assert_eq!(flagged.data.unwrap()["in_doubt"], true);
    }

    assert_eq!(read(&harness, "log.txt").await, "x");
    let in_doubt = store.in_doubt().await.unwrap();
    assert_eq!(in_doubt.len(), 1);
    assert_eq!(in_doubt[0].request_seq, Some(1));
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallRetry, Some(1), None),
            (EventKind::CallResponse, Some(1), Some(Outcome::InDoubt)),
            (EventKind::CallRetry, Some(1), None),
        ]
    );
}

/// Two gateway copies in front of one probe. The first lets its lease lapse at once, as a
/// paused copy's would.
async fn two_copies(store: &Arc<dyn EventStore>, probe: &Probe) -> (String, String) {
    let probe_url = spawn_tool_server(probe.clone()).await;
    let copy = |ttl| {
        let upstreams = vec![upstream("probe", &probe_url)];
        Gateway::new(store.clone(), upstreams, CALL_TIMEOUT).with_lease_ttl(ttl)
    };
    let old = spawn_gateway(copy(Duration::ZERO)).await;
    let new = spawn_gateway(copy(PATIENCE)).await;
    (old, new)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_flags_an_open_unsafe_call_and_its_late_outcome_is_not_logged() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let (old, new) = two_copies(&store, &probe).await;
    let old = Arc::new(connect(&old, Some("s")).await.unwrap());
    let new = connect(&new, Some("s")).await.unwrap();
    let wait = || step(call_params("probe__wait", json!({})), "1");

    let running = tokio::spawn({
        let old = old.clone();
        async move { old.call_tool(wait()).await }
    });
    probe.wait_until_started().await;
    // Taking the session over finds call 1 open, and it is not safe to repeat.
    new.call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap();
    probe.release();

    // The old copy's tool finished, but it no longer owns the session.
    let late = error(running.await.unwrap().unwrap_err());
    assert_eq!(late.code, error_codes::OUTCOME_NOT_RECORDED);
    // And asking again gets the flag, not a second run.
    let flagged = error(new.call_tool(wait()).await.unwrap_err());
    assert_eq!(flagged.code, error_codes::OUTCOME_UNKNOWN);
    assert_eq!(probe.started.load(Ordering::SeqCst), 2);
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(1), Some(Outcome::InDoubt)),
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(3), Some(Outcome::Ok)),
            (EventKind::CallRetry, Some(1), None),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn after_a_takeover_a_keyed_open_call_runs_again_with_its_key() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let mut probe = Probe::new(store.clone(), "s");
    probe.input_schema =
        json!({"type": "object", "properties": {"idempotency_key": {"type": "string"}}});
    let (old, new) = two_copies(&store, &probe).await;
    let old = Arc::new(connect(&old, Some("s")).await.unwrap());
    let new = connect(&new, Some("s")).await.unwrap();
    let wait = || step(call_params("probe__wait", json!({})), "1");

    let running = tokio::spawn({
        let old = old.clone();
        async move { old.call_tool(wait()).await }
    });
    probe.wait_until_started().await;
    let retried = tokio::spawn(async move { new.call_tool(wait()).await });
    eventually(|| async { probe.started.load(Ordering::SeqCst) == 2 }).await;
    probe.release();
    probe.release();

    retried.await.unwrap().unwrap();
    let late = error(running.await.unwrap().unwrap_err());
    assert_eq!(late.code, error_codes::OUTCOME_NOT_RECORDED);
    let keys: Vec<Value> = probe
        .received
        .lock()
        .unwrap()
        .iter()
        .map(|a| a["idempotency_key"].clone())
        .collect();
    assert_eq!(keys, [json!("ledger:s:1"), json!("ledger:s:1")]);
    assert_eq!(
        shape(&store).await,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallRetry, Some(1), None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn after_a_takeover_an_idempotent_open_call_runs_again() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let mut probe = Probe::new(store.clone(), "s");
    let mut hints = ToolAnnotations::new();
    hints.idempotent_hint = Some(true);
    probe.annotations = Some(hints);
    let (old, new) = two_copies(&store, &probe).await;
    let old = Arc::new(connect(&old, Some("s")).await.unwrap());
    let new = connect(&new, Some("s")).await.unwrap();
    let wait = || step(call_params("probe__wait", json!({})), "1");

    let running = tokio::spawn({
        let old = old.clone();
        async move { old.call_tool(wait()).await }
    });
    probe.wait_until_started().await;
    let retried = tokio::spawn(async move { new.call_tool(wait()).await });
    eventually(|| async { probe.started.load(Ordering::SeqCst) == 2 }).await;
    probe.release();
    probe.release();

    retried.await.unwrap().unwrap();
    running.await.unwrap().unwrap_err();
    assert_eq!(shape(&store).await.last().unwrap().2, Some(Outcome::Ok));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_longer_than_the_lease_keeps_the_session() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let lease = Duration::from_secs(1);
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    )
    .with_lease_ttl(lease);
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    let running =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;
    // Only renewals during the call can push the lease's expiry this far past the start:
    // the call has by then lasted at least twice as long as the lease.
    let started = store.session_events("s").await.unwrap()[0].recorded_at;
    let pool = db.pool();
    eventually(|| async {
        let expires: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT expires_at FROM leases WHERE session_id = 's'")
                .fetch_one(&pool)
                .await
                .unwrap();
        expires > started + lease * 3
    })
    .await;
    probe.release();

    running.await.unwrap().unwrap();
    assert_eq!(shape(&store).await.last().unwrap().2, Some(Outcome::Ok));
}
