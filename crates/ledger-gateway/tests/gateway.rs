mod common;

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{
    CALL_TIMEOUT, FaultyStore, Harness, Probe, eventually, spawn_gateway, spawn_tool_server,
    upstream,
};
use ledger_gateway::{Gateway, McpUpstream, Upstream, error_codes};
use ledger_store::{Event, EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::{PATIENCE, ReservedPort, TestDb};
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::http::serve_mcp;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CancelledNotificationParam,
    ClientConfig, ErrorCode, ProtocolVersion, ServerConfig,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::{ClientLifecycleMode, ClientServiceExt, ServerHandler, ServiceError};
use sample_agent::{call_params, connect, run_script};
use serde_json::{Value, json};
use sqlx::PgPool;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn pg(pool: PgPool) -> Arc<dyn EventStore> {
    Arc::new(PgEventStore::new(pool))
}

/// A gateway with a single `probe` upstream.
async fn probe_gateway(
    store: Arc<dyn EventStore>,
    session: &str,
    call_timeout: Duration,
) -> (String, Probe) {
    let probe = Probe::new(store.clone(), session);
    let probe_url = spawn_tool_server(probe.clone()).await;
    let upstreams = vec![upstream("probe", &probe_url)];
    let url = spawn_gateway(Gateway::new(store, upstreams, call_timeout)).await;
    (url, probe)
}

fn send_email() -> CallToolRequestParams {
    call_params(
        "email__send_email",
        json!({"to": "a@example.com", "subject": "s", "body": "b"}),
    )
}

/// An email server a test can stop and bring back on the same port.
struct RestartableEmailServer {
    port: ReservedPort,
    outbox: Arc<Outbox>,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
    _dir: TempDir,
}

impl RestartableEmailServer {
    async fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let outbox = Arc::new(Outbox::new(dir.path().join("outbox.jsonl")));
        let port = ReservedPort::claim();
        let listener = TcpListener::bind(port.addr()).await.unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(serve_mcp(
            EmailServer::new(outbox.clone()),
            listener,
            shutdown.clone(),
        ));
        Self {
            port,
            outbox,
            shutdown,
            task,
            _dir: dir,
        }
    }

    fn url(&self) -> String {
        format!("http://{}/mcp", self.port.addr())
    }

    async fn stop(&mut self) {
        self.shutdown.cancel();
        (&mut self.task).await.unwrap().unwrap();
    }

    async fn start_again(&mut self) {
        let listener = TcpListener::bind(self.port.addr()).await.unwrap();
        self.shutdown = CancellationToken::new();
        self.task = tokio::spawn(serve_mcp(
            EmailServer::new(self.outbox.clone()),
            listener,
            self.shutdown.clone(),
        ));
    }
}

/// A gateway that shows a test when a cancellation has reached it. The SDK cancels a
/// request before it calls `on_cancelled`.
#[derive(Clone)]
struct CancellationWatcher {
    gateway: Gateway,
    cancellations: Arc<AtomicUsize>,
}

impl ServerHandler for CancellationWatcher {
    fn get_info(&self) -> ServerConfig {
        self.gateway.get_info()
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, rmcp::ErrorData> {
        self.gateway.call_tool(request, context).await
    }

    async fn on_cancelled(
        &self,
        _notification: CancelledNotificationParam,
        _context: NotificationContext<RoleServer>,
    ) {
        self.cancellations.fetch_add(1, Ordering::SeqCst);
    }
}

fn rpc_error(error: ServiceError) -> rmcp::ErrorData {
    match error {
        ServiceError::McpError(error) => error,
        other => panic!("expected a JSON-RPC error, got {other}"),
    }
}

/// Requests and responses must pair up 1:1, each response after its own request.
fn assert_well_formed(events: &[Event]) {
    let seqs: Vec<i64> = events.iter().map(|e| e.seq).collect();
    let expected: Vec<i64> = (1..=events.len() as i64).collect();
    assert_eq!(seqs, expected, "sequence numbers must have no gaps");

    let requests: HashMap<i64, &Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .map(|e| (e.seq, e))
        .collect();
    let mut answered = HashSet::new();
    for response in events.iter().filter(|e| e.kind == EventKind::CallResponse) {
        let request_seq = response.request_seq.expect("responses link to a request");
        let request = requests
            .get(&request_seq)
            .expect("link points at a request");
        assert!(request.seq < response.seq, "response before its request");
        assert_eq!(request.tool, response.tool);
        assert!(
            answered.insert(request_seq),
            "two responses for one request"
        );
    }
    assert_eq!(answered.len(), requests.len(), "a request has no response");
}

fn result_of(event: &Event) -> CallToolResult {
    serde_json::from_value(event.payload["result"].clone()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_call_is_logged_in_order() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("run-1")).await.unwrap();

    let steps = run_script(&agent, "All systems nominal.").await.unwrap();

    let events = store.session_events("run-1").await.unwrap();
    assert_eq!(events.len(), steps.len() * 2);
    assert_well_formed(&events);
    for (step, pair) in steps.iter().zip(events.chunks(2)) {
        let (request, response) = (&pair[0], &pair[1]);
        assert_eq!(request.kind, EventKind::CallRequest);
        assert_eq!(request.tool, step.tool);
        assert_eq!(request.payload["params"]["arguments"], step.arguments);
        assert!(request.payload["rpc_id"].is_number());

        assert_eq!(response.kind, EventKind::CallResponse);
        assert_eq!(response.request_seq, Some(request.seq));
        assert_eq!(response.outcome, Some(Outcome::Ok));
        // The log holds exactly what the agent was told.
        assert_eq!(result_of(response), step.result);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_request_is_committed_before_the_tool_runs() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let (url, probe) = probe_gateway(store.clone(), "s", CALL_TIMEOUT).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    agent
        .call_tool(call_params("probe__observe", json!({"n": 1})))
        .await
        .unwrap();

    // What the log held at the moment the tool was running:
    let observed = probe.observed.lock().await.clone().unwrap();
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].kind, EventKind::CallRequest);
    assert_eq!(observed[0].payload["params"]["arguments"], json!({"n": 1}));

    // And once it had finished:
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].kind, EventKind::CallResponse);
}

#[tokio::test(flavor = "multi_thread")]
async fn tools_pass_through_with_their_annotations_and_schemas() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let harness = Harness::start(pg(pool)).await;
    let through_gateway = connect(&harness.url, Some("s")).await.unwrap();
    let email = connect(&harness.email_url, None).await.unwrap();
    let files = connect(&harness.files_url, None).await.unwrap();

    let mut expected = Vec::new();
    for (prefix, client) in [("email", &email), ("files", &files)] {
        for mut tool in client.list_all_tools().await.unwrap() {
            tool.name = format!("{prefix}__{}", tool.name).into();
            expected.push(tool);
        }
    }
    let actual = through_gateway.list_all_tools().await.unwrap();
    assert_eq!(actual, expected);

    let send = actual
        .iter()
        .find(|t| t.name == "email__send_email")
        .unwrap();
    let hints = send.annotations.as_ref().unwrap();
    assert_eq!(hints.destructive_hint, Some(true));
    assert_eq!(hints.idempotent_hint, Some(false));
}

#[tokio::test(flavor = "multi_thread")]
async fn results_are_the_same_as_calling_the_tool_directly() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let harness = Harness::start(pg(pool)).await;
    let through_gateway = connect(&harness.url, Some("s")).await.unwrap();
    let direct = connect(&harness.files_url, None).await.unwrap();

    let arguments = json!({"path": "same.txt", "content": "same"});
    let via_gateway = through_gateway
        .call_tool(call_params("files__write_file", arguments.clone()))
        .await
        .unwrap();
    let via_direct = direct
        .call_tool(call_params("write_file", arguments))
        .await
        .unwrap();
    assert_eq!(via_gateway, via_direct);
}

#[tokio::test(flavor = "multi_thread")]
async fn tool_failures_are_logged_and_returned() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    let result = agent
        .call_tool(call_params(
            "files__write_file",
            json!({"path": "../escape.txt", "content": "x"}),
        ))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(true));

    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    assert_eq!(events[1].outcome, Some(Outcome::ToolError));
    assert_eq!(result_of(&events[1]), result);
}

#[tokio::test(flavor = "multi_thread")]
async fn rpc_errors_are_logged_and_returned_unchanged() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let (url, _probe) = probe_gateway(store.clone(), "s", CALL_TIMEOUT).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    let error = rpc_error(
        agent
            .call_tool(call_params("probe__reject", json!({})))
            .await
            .unwrap_err(),
    );
    assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
    assert_eq!(error.message, "probe says no");

    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    assert_eq!(events[1].outcome, Some(Outcome::RpcError));
    assert_eq!(events[1].payload["error"]["message"], "probe says no");
}

#[tokio::test(flavor = "multi_thread")]
async fn unroutable_calls_are_logged_but_go_nowhere() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    for name in ["nosuchserver__send_email", "no_separator", "__", "email"] {
        let error = rpc_error(
            agent
                .call_tool(call_params(name, json!({})))
                .await
                .unwrap_err(),
        );
        assert_eq!(error.code, ErrorCode::INVALID_PARAMS, "{name}");
    }

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 8);
    assert_well_formed(&events);
    assert!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::CallResponse)
            .all(|e| e.outcome == Some(Outcome::NotRouted))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn if_the_request_cannot_be_logged_the_tool_never_runs() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let faulty = Arc::new(FaultyStore::new(pg(pool)));
    let harness = Harness::start(faulty.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    faulty.fail_appends_of(EventKind::CallRequest).await;
    let error = rpc_error(agent.call_tool(send_email()).await.unwrap_err());

    assert_eq!(error.code, error_codes::NOT_RECORDED);
    assert!(harness.outbox.sent().await.unwrap().is_empty());
    assert!(faulty.session_events("s").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn if_the_response_cannot_be_logged_the_agent_does_not_get_it() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let faulty = Arc::new(FaultyStore::new(pg(pool)));
    let harness = Harness::start(faulty.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    faulty.fail_appends_of(EventKind::CallResponse).await;
    let error = rpc_error(agent.call_tool(send_email()).await.unwrap_err());

    // The email went out, and the log shows a call whose outcome is in doubt. The agent
    // is told so rather than handed a result the log could not later reproduce.
    assert_eq!(error.code, error_codes::OUTCOME_NOT_RECORDED);
    assert_eq!(harness.outbox.sent().await.unwrap().len(), 1);
    let events = faulty.session_events("s").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::CallRequest);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_tool_server_is_logged_and_recovered_from() {
    let db = TestDb::new().await;
    let store = pg(db.pool());
    let mut email = RestartableEmailServer::start().await;
    let upstreams = vec![upstream("email", &email.url())];
    let url = spawn_gateway(Gateway::new(store.clone(), upstreams, CALL_TIMEOUT)).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    agent.call_tool(send_email()).await.unwrap();

    email.stop().await;
    let error = rpc_error(agent.call_tool(send_email()).await.unwrap_err());
    assert_eq!(error.code, error_codes::OUTCOME_UNKNOWN);

    email.start_again().await;
    agent.call_tool(send_email()).await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    let outcomes: Vec<_> = events.iter().filter_map(|e| e.outcome).collect();
    assert_eq!(
        outcomes,
        [Outcome::Ok, Outcome::TransportError, Outcome::Ok]
    );
    assert_eq!(email.outbox.sent().await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_that_hangs_is_timed_out_and_flagged_as_unknown() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let (url, _probe) = probe_gateway(store.clone(), "s", Duration::from_millis(200)).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    let error = rpc_error(
        agent
            .call_tool(call_params("probe__wait", json!({})))
            .await
            .unwrap_err(),
    );
    assert_eq!(error.code, error_codes::OUTCOME_UNKNOWN);

    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    assert_eq!(events[1].outcome, Some(Outcome::Timeout));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_cancelling_a_call_does_not_lose_the_outcome() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let upstreams = vec![upstream("probe", &probe_url)];
    let watcher = CancellationWatcher {
        gateway: Gateway::new(store.clone(), upstreams, CALL_TIMEOUT),
        cancellations: Arc::default(),
    };
    let url = spawn_tool_server(watcher.clone()).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    // Dropping the pending call sends a cancellation while the connection stays up.
    {
        let call = agent.call_tool_once(call_params("probe__wait", json!({})));
        tokio::pin!(call);
        tokio::select! {
            _ = &mut call => panic!("the tool has not been released yet"),
            _ = probe.wait_until_started() => {}
        }
    }
    // From here on the gateway's request is cancelled, and the tool is still running.
    eventually(|| async { watcher.cancellations.load(Ordering::SeqCst) > 0 }).await;

    probe.release();

    eventually(|| async { store.session_events("s").await.unwrap().len() == 2 }).await;
    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    assert_eq!(events[1].outcome, Some(Outcome::Ok));
    assert_eq!(probe.finished.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_calls_in_one_session_stay_consistent() {
    let db = TestDb::new().await;
    let pool = db.pool();
    const CALLS: usize = 32;
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = Arc::new(connect(&harness.url, Some("s")).await.unwrap());

    let calls: Vec<_> = (0..CALLS)
        .map(|i| {
            let agent = agent.clone();
            tokio::spawn(async move {
                agent
                    .call_tool(call_params(
                        "email__send_email",
                        json!({"to": "a@example.com", "subject": i.to_string(), "body": "b"}),
                    ))
                    .await
                    .unwrap()
            })
        })
        .collect();
    for call in calls {
        call.await.unwrap();
    }

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), CALLS * 2);
    assert_well_formed(&events);

    let mut logged_subjects: Vec<Value> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .map(|e| e.payload["params"]["arguments"]["subject"].clone())
        .collect();
    let mut sent_subjects: Vec<Value> = harness
        .outbox
        .sent()
        .await
        .unwrap()
        .into_iter()
        .map(|email| Value::String(email.subject))
        .collect();
    logged_subjects.sort_by_key(Value::to_string);
    sent_subjects.sort_by_key(Value::to_string);
    assert_eq!(logged_subjects, sent_subjects);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_do_not_mix() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let first = connect(&harness.url, Some("first")).await.unwrap();
    let second = connect(&harness.url, Some("second")).await.unwrap();
    let write =
        |path: &str| call_params("files__write_file", json!({"path": path, "content": "x"}));

    first.call_tool(write("a.txt")).await.unwrap();
    second.call_tool(write("b.txt")).await.unwrap();
    first.call_tool(write("c.txt")).await.unwrap();

    let paths = |events: Vec<Event>| -> Vec<Value> {
        events
            .iter()
            .filter(|e| e.kind == EventKind::CallRequest)
            .map(|e| e.payload["params"]["arguments"]["path"].clone())
            .collect()
    };
    let first_events = store.session_events("first").await.unwrap();
    let second_events = store.session_events("second").await.unwrap();
    assert_well_formed(&first_events);
    assert_well_formed(&second_events);
    assert_eq!(paths(first_events), [json!("a.txt"), json!("c.txt")]);
    assert_eq!(paths(second_events), [json!("b.txt")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agent_that_knows_nothing_about_ledger_gets_one_session_per_connection() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let write = || {
        call_params(
            "files__write_file",
            json!({"path": "f.txt", "content": "x"}),
        )
    };

    let first = connect(&harness.url, None).await.unwrap();
    first.call_tool(write()).await.unwrap();
    first.call_tool(write()).await.unwrap();
    let second = connect(&harness.url, None).await.unwrap();
    second.call_tool(write()).await.unwrap();

    let mut counts: Vec<i64> = store
        .sessions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.event_count)
        .collect();
    counts.sort();
    assert_eq!(counts, [2, 4]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_can_be_named_in_the_url() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let url = format!("{}?session=from-url", harness.url);
    let agent = connect(&url, None).await.unwrap();

    agent
        .call_tool(call_params(
            "files__read_file",
            json!({"path": "missing.txt"}),
        ))
        .await
        .unwrap();

    assert_eq!(store.session_events("from-url").await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_session_id_is_refused_before_anything_happens() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("not a valid id")).await.unwrap();

    let error = rpc_error(agent.call_tool(send_email()).await.unwrap_err());

    assert_eq!(error.code, ErrorCode::INVALID_REQUEST);
    assert!(harness.outbox.sent().await.unwrap().is_empty());
    assert!(store.sessions().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_continues_across_a_gateway_restart() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let write = || {
        call_params(
            "files__write_file",
            json!({"path": "f.txt", "content": "x"}),
        )
    };
    let copy = || {
        let upstreams = vec![upstream("files", &harness.files_url)];
        // Its lease lapses at once, as a crashed copy's eventually does.
        Gateway::new(store.clone(), upstreams, CALL_TIMEOUT).with_lease_ttl(Duration::ZERO)
    };

    let agent = connect(&spawn_gateway(copy()).await, Some("s"))
        .await
        .unwrap();
    agent.call_tool(write()).await.unwrap();
    drop(agent);

    // A second copy with nothing in memory picks the sequence up from the log.
    let url = spawn_gateway(copy()).await;
    let agent = connect(&url, Some("s")).await.unwrap();
    agent.call_tool(write()).await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 4);
    assert_well_formed(&events);
}

#[tokio::test(flavor = "multi_thread")]
async fn awkward_arguments_are_logged_faithfully() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();
    let content = "nul \u{0000} emoji \u{1F980} quote \" backslash \\ newline \n";

    let result = agent
        .call_tool(call_params(
            "files__write_file",
            json!({"path": "awkward.txt", "content": content}),
        ))
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[0].payload["params"]["arguments"]["content"], content);
    assert_eq!(
        std::fs::read_to_string(harness.files.path().join("awkward.txt")).unwrap(),
        content
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_name_the_log_cannot_hold_is_not_forwarded() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    let error = rpc_error(
        agent
            .call_tool(call_params("email__send_email\u{0000}", json!({})))
            .await
            .unwrap_err(),
    );

    assert_eq!(error.code, error_codes::NOT_RECORDED);
    assert!(harness.outbox.sent().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dead_upstream_only_hides_its_own_tools() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let dir = tempfile::tempdir().unwrap();
    let server = ledger_tools::files::FileServer::new(dir.path()).unwrap();
    let files_url = spawn_tool_server(server).await;
    let unused = ReservedPort::claim();
    let upstreams = vec![
        upstream("down", &format!("http://{}/mcp", unused.addr())),
        upstream("files", &files_url),
    ];
    let url = spawn_gateway(Gateway::new(store, upstreams, CALL_TIMEOUT)).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    let tools = agent.list_all_tools().await.unwrap();
    assert!(!tools.is_empty());
    assert!(tools.iter().all(|t| t.name.starts_with("files__")));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_server_that_never_answers_cannot_stall_the_gateway() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    // Connections are accepted by the OS and then ignored.
    let black_hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", black_hole.local_addr().unwrap());
    let upstreams = vec![upstream("hole", &url)];
    let gateway = Gateway::new(store.clone(), upstreams, Duration::from_millis(200));
    let url = spawn_gateway(gateway).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    assert!(agent.list_all_tools().await.unwrap().is_empty());
    let error = rpc_error(
        agent
            .call_tool(call_params("hole__anything", json!({})))
            .await
            .unwrap_err(),
    );
    assert_eq!(error.code, error_codes::OUTCOME_UNKNOWN);
    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    // `timeout`, not `transport_error`: the call's own limit fired, and the connection
    // attempt has a much longer one here.
    assert_eq!(events[1].outcome, Some(Outcome::Timeout));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_attempt_gives_up_on_its_own() {
    let db = TestDb::new().await;
    let store = pg(db.pool());
    let black_hole = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", black_hole.local_addr().unwrap());
    let hole: Arc<dyn Upstream> =
        Arc::new(McpUpstream::new("hole", url, Duration::from_millis(200)));
    let gateway = Gateway::new(store.clone(), vec![hole], CALL_TIMEOUT);
    let url = spawn_gateway(gateway).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    let error = rpc_error(
        agent
            .call_tool(call_params("hole__anything", json!({})))
            .await
            .unwrap_err(),
    );
    assert_eq!(error.code, error_codes::OUTCOME_UNKNOWN);
    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    // `transport_error`, not `timeout`: the connection limit fired, not the call's own.
    assert_eq!(events[1].outcome, Some(Outcome::TransportError));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_server_that_stalls_on_listing_cannot_stall_the_gateway() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let mut probe = Probe::new(store.clone(), "s");
    probe.stall_listing = true;
    let probe_url = spawn_tool_server(probe).await;
    let upstreams = vec![upstream("probe", &probe_url)];
    let gateway = Gateway::new(store, upstreams, Duration::from_millis(200));
    let url = spawn_gateway(gateway).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    let tools = tokio::time::timeout(PATIENCE, agent.list_all_tools())
        .await
        .expect("listing tools must not hang")
        .unwrap();
    assert!(tools.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_agents_meta_is_logged_but_not_forwarded() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let upstreams = vec![upstream("probe", &probe_url)];
    let gateway = Gateway::new(store.clone(), upstreams, CALL_TIMEOUT);
    let url = spawn_gateway(gateway).await;

    let with_meta = |name: &str| -> CallToolRequestParams {
        serde_json::from_value(json!({
            "name": name,
            "arguments": {},
            "_meta": {"example.com/trace": "abc"},
        }))
        .unwrap()
    };
    let probe_saw_meta = || async {
        let received = probe.received_meta.lock().await.clone().unwrap();
        received.0.contains_key("example.com/trace")
    };

    // Sent straight to the probe, it arrives, so the probe is able to see it.
    let direct = connect(&probe_url, None).await.unwrap();
    direct.call_tool(with_meta("observe")).await.unwrap();
    assert!(probe_saw_meta().await);

    let agent = connect(&url, Some("s")).await.unwrap();
    agent.call_tool(with_meta("probe__observe")).await.unwrap();
    assert!(!probe_saw_meta().await);

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[0].payload["meta"]["example.com/trace"], "abc");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_server_restarting_between_calls_causes_no_duplicates() {
    let db = TestDb::new().await;
    let store = pg(db.pool());
    let mut email = RestartableEmailServer::start().await;
    let upstreams = vec![upstream("email", &email.url())];
    let url = spawn_gateway(Gateway::new(store.clone(), upstreams, CALL_TIMEOUT)).await;
    let agent = connect(&url, Some("s")).await.unwrap();

    agent.call_tool(send_email()).await.unwrap();

    // Back up on the same port with no memory of the gateway's connection.
    email.stop().await;
    email.start_again().await;

    let _ = agent.call_tool(send_email()).await;
    agent.call_tool(send_email()).await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_well_formed(&events);
    let delivered = events
        .iter()
        .filter(|e| e.outcome == Some(Outcome::Ok))
        .count();
    assert!(delivered >= 2);
    assert_eq!(email.outbox.sent().await.unwrap().len(), delivered);
}

#[tokio::test(flavor = "multi_thread")]
async fn losing_the_database_stops_calls_from_being_forwarded() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let harness = Harness::start(pg(pool.clone())).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    pool.close().await;

    let error = rpc_error(agent.call_tool(send_email()).await.unwrap_err());
    assert_eq!(error.code, error_codes::NOT_RECORDED);
    assert!(harness.outbox.sent().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_preferring_the_newest_protocol_still_gets_a_session() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = pg(pool);
    let harness = Harness::start(store.clone()).await;
    let transport = StreamableHttpClientTransport::from_uri(harness.url.clone());
    let agent = ClientConfig::default()
        .serve_with_lifecycle(
            transport,
            ClientLifecycleMode::Auto {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                legacy_version: None,
            },
        )
        .await
        .unwrap();

    agent
        .call_tool(call_params(
            "files__write_file",
            json!({"path": "f", "content": "x"}),
        ))
        .await
        .unwrap();
    agent
        .call_tool(call_params(
            "files__write_file",
            json!({"path": "f", "content": "y"}),
        ))
        .await
        .unwrap();

    let sessions = store.sessions().await.unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].event_count, 4);
}
