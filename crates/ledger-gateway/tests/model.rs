//! The model proxy: model calls are logged in the agent's session by the same rule as tool
//! calls, logged before they are sent and answered only from what was logged.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CALL_TIMEOUT, FaultyStore, Harness, spawn_gateway, spawn_tool_server, upstream};
use ledger_gateway::{Gateway, ModelUpstream, error_codes};
use ledger_store::{Event, EventKind, EventStore, NewEvent, Outcome, PgEventStore};
use ledger_testkit::{PATIENCE, ReservedPort, TestDb};
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::files::FileServer;
use ledger_tools::model::{Received, serve_model};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// The scripted model, on an ephemeral port: its base URL and what it received.
async fn spawn_model() -> (String, Received) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let received = Received::default();
    tokio::spawn(serve_model(
        listener,
        received.clone(),
        CancellationToken::new(),
    ));
    (url, received)
}

/// A gateway in front of the scripted model and no tools. Returns its base URL.
async fn model_gateway(store: Arc<dyn EventStore>, model: ModelUpstream) -> String {
    let gateway = Gateway::new(store, Vec::new(), CALL_TIMEOUT).with_model(model);
    base(&spawn_gateway(gateway).await)
}

fn base(mcp_url: &str) -> String {
    mcp_url.trim_end_matches("/mcp").to_owned()
}

fn ask(model: &str) -> Value {
    json!({ "model": model, "messages": [{ "role": "user", "content": "All good." }] })
}

/// The agent's HTTP client. Its limit turns a gateway that never answers into a failed
/// test rather than a hung one.
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(PATIENCE)
        .build()
        .unwrap()
}

async fn post(url: &str, session: Option<&str>, body: &Value) -> reqwest::Response {
    let mut request = client().post(url).json(body);
    if let Some(session) = session {
        request = request.header("x-ledger-session", session);
    }
    request.send().await.unwrap()
}

fn kinds(events: &[Event]) -> Vec<EventKind> {
    events.iter().map(|e| e.kind).collect()
}

async fn pg() -> (TestDb, Arc<dyn EventStore>) {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    (db, store)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_call_is_logged_on_both_sides_and_the_agent_gets_what_was_logged() {
    let (_db, store) = pg().await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let answer: Value = response.json().await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_eq!(
        kinds(&events),
        [EventKind::ModelRequest, EventKind::ModelResponse]
    );
    let (request, reply) = (&events[0], &events[1]);
    assert_eq!(request.tool, "reporter-1");
    assert_eq!(request.payload["request"], ask("reporter-1"));
    assert_eq!(
        request.args_hash,
        Some(ledger_store::args_hash(&ask("reporter-1")))
    );
    assert_eq!(reply.request_seq, Some(request.seq));
    assert_eq!(reply.outcome, Some(Outcome::Ok));
    assert_eq!(reply.payload["body"]["json"], answer);
    assert_eq!(
        answer["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "files__write_file"
    );
    // The model got the request exactly as the agent sent it.
    assert_eq!(received.bodies(), [ask("reporter-1")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_reply_is_logged_event_by_event_and_delivered_whole() {
    let (_db, store) = pg().await;
    let (model_url, _) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;
    let mut body = ask("reporter-1");
    body["stream"] = json!(true);

    let direct = client()
        .post(format!("{model_url}/chat/completions"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let response = post(&format!("{url}/v1/chat/completions"), Some("s"), &body).await;
    assert_eq!(response.headers()["content-type"], "text/event-stream");
    let proxied = response.text().await.unwrap();

    assert_eq!(proxied, direct);
    let reply = &store.session_events("s").await.unwrap()[1];
    let logged = reply.payload["body"]["stream"].as_array().unwrap();
    assert_eq!(logged.last().unwrap(), "[DONE]");
    assert_eq!(logged.len(), proxied.matches("data: ").count());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_session_comes_from_the_header_or_the_path() {
    let (_db, store) = pg().await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;

    let by_header = post(
        &format!("{url}/v1/chat/completions"),
        Some("a"),
        &ask("reporter-1"),
    )
    .await;
    let by_path = post(
        &format!("{url}/s/b/v1/chat/completions"),
        None,
        &ask("reporter-1"),
    )
    .await;
    let neither = post(
        &format!("{url}/v1/chat/completions"),
        None,
        &ask("reporter-1"),
    )
    .await;
    let bad_path = post(
        &format!("{url}/s/a%20b/v1/chat/completions"),
        None,
        &ask("reporter-1"),
    )
    .await;

    assert_eq!(by_header.status(), StatusCode::OK);
    assert_eq!(by_path.status(), StatusCode::OK);
    assert_eq!(neither.status(), StatusCode::BAD_REQUEST);
    assert_eq!(bad_path.status(), StatusCode::BAD_REQUEST);
    assert_eq!(store.session_events("a").await.unwrap().len(), 2);
    assert_eq!(store.session_events("b").await.unwrap().len(), 2);
    assert_eq!(received.count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_reach_the_model_but_never_the_log() {
    let (_db, store) = pg().await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;
    let secret = "sk-agent-secret-7d1f";

    let response = client()
        .post(format!("{url}/v1/chat/completions"))
        .header("x-ledger-session", "s")
        .header("authorization", format!("Bearer {secret}"))
        .header("cookie", "c=1")
        .json(&ask("reporter-1"))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let headers = &received.headers()[0];
    assert_eq!(headers["authorization"], format!("Bearer {secret}"));
    assert!(headers.get("cookie").is_none() && headers.get("x-ledger-session").is_none());
    let logged = serde_json::to_string(&store.session_events("s").await.unwrap()).unwrap();
    assert!(!logged.contains(secret));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_gateways_own_key_replaces_the_agents() {
    let (_db, store) = pg().await;
    let (model_url, received) = spawn_model().await;
    let key = Some("sk-gateway".to_owned());
    let url = model_gateway(store.clone(), ModelUpstream::new(&model_url, key, PATIENCE)).await;

    client()
        .post(format!("{url}/v1/chat/completions"))
        .header("x-ledger-session", "s")
        .header("authorization", "Bearer sk-agent")
        .json(&ask("reporter-1"))
        .send()
        .await
        .unwrap();

    assert_eq!(received.headers()[0]["authorization"], "Bearer sk-gateway");
    let logged = serde_json::to_string(&store.session_events("s").await.unwrap()).unwrap();
    assert!(!logged.contains("sk-gateway") && !logged.contains("sk-agent"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_call_that_cannot_be_logged_is_not_sent() {
    let (_db, pg) = pg().await;
    let faulty = Arc::new(FaultyStore::new(pg.clone()));
    faulty.fail_appends_of(EventKind::ModelRequest).await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(faulty, ModelUpstream::new(&model_url, None, PATIENCE)).await;

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let error: Value = response.json().await.unwrap();
    assert_eq!(error["error"]["code"], error_codes::NOT_RECORDED.0);
    assert_eq!(received.count(), 0);
    assert!(pg.session_events("s").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_that_cannot_be_logged_does_not_reach_the_agent() {
    let (_db, pg) = pg().await;
    let faulty = Arc::new(FaultyStore::new(pg.clone()));
    faulty.fail_appends_of(EventKind::ModelResponse).await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(faulty, ModelUpstream::new(&model_url, None, PATIENCE)).await;

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let text = response.text().await.unwrap();
    assert!(!text.contains("files__write_file"), "{text}");
    let error: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(error["error"]["code"], error_codes::OUTCOME_NOT_RECORDED.0);
    assert_eq!(received.count(), 1);
    assert_eq!(
        kinds(&pg.session_events("s").await.unwrap()),
        [EventKind::ModelRequest]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_error_is_logged_and_passed_on() {
    let (_db, store) = pg().await;
    let (model_url, _) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;

    let failed = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("fail"),
    )
    .await;
    let unknown = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("gpt-none"),
    )
    .await;

    assert_eq!(failed.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        failed.text().await.unwrap(),
        "<h1>Internal Server Error</h1>"
    );
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND);
    let unknown: Value = unknown.json().await.unwrap();
    assert_eq!(unknown["error"]["code"], "model_not_found");
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::RpcError));
    assert_eq!(events[1].payload["status"], 500);
    assert_eq!(events[3].outcome, Some(Outcome::RpcError));
    assert_eq!(events[3].payload["body"]["json"], unknown);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_model_is_logged_as_a_transport_error() {
    let (_db, store) = pg().await;
    // A reserved port that nothing listens on.
    let port = ReservedPort::claim();
    let model = ModelUpstream::new(&format!("http://{}/v1", port.addr()), None, PATIENCE);
    let url = model_gateway(store.clone(), model).await;

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::TransportError));
    // Not a final answer: the agent may ask again, which is a new model call.
    post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;
    assert_eq!(store.session_events("s").await.unwrap().len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_that_never_answers_is_logged_as_timed_out() {
    let (_db, store) = pg().await;
    let (model_url, _) = spawn_model().await;
    // `hang` never answers, so this limit is reached however loaded the machine is.
    let model = ModelUpstream::new(&model_url, None, Duration::from_millis(200));
    let url = model_gateway(store.clone(), model).await;

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("hang"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let error: Value = response.json().await.unwrap();
    assert_eq!(error["error"]["code"], error_codes::OUTCOME_UNKNOWN.0);
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::Timeout));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_disallowed_host_or_origin_is_refused_before_anything_is_logged() {
    let (_db, store) = pg().await;
    let (model_url, received) = spawn_model().await;
    let url = model_gateway(
        store.clone(),
        ModelUpstream::new(&model_url, None, PATIENCE),
    )
    .await;
    let send = |name: &'static str, value: &'static str| {
        client()
            .post(format!("{url}/v1/chat/completions"))
            .header("x-ledger-session", "s")
            .header(name, value)
            .json(&ask("reporter-1"))
            .send()
    };

    assert_eq!(
        send("host", "evil.example").await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    let origin = send("origin", "http://evil.example").await.unwrap();
    assert_eq!(origin.status(), StatusCode::FORBIDDEN);
    assert_eq!(received.count(), 0);
    assert!(store.session_events("s").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn without_a_model_the_route_says_so() {
    let (_db, store) = pg().await;
    let gateway = Gateway::new(store.clone(), Vec::new(), CALL_TIMEOUT);
    let url = base(&spawn_gateway(gateway).await);

    let response = post(
        &format!("{url}/v1/chat/completions"),
        Some("s"),
        &ask("reporter-1"),
    )
    .await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(store.session_events("s").await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_driven_agent_leaves_one_log_of_both_kinds_of_call() {
    let (_db, store) = pg().await;
    let (model_url, _) = spawn_model().await;
    let files = tempfile::tempdir().unwrap();
    let outbox = Arc::new(Outbox::new(files.path().join(".outbox.jsonl")));
    let email = spawn_tool_server(EmailServer::new(outbox.clone())).await;
    let file_server = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("email", &email), upstream("files", &file_server)],
        CALL_TIMEOUT,
    )
    .with_model(ModelUpstream::new(&model_url, None, PATIENCE));
    let mcp = spawn_gateway(gateway).await;

    let client = sample_agent::connect(&mcp, Some("s")).await.unwrap();
    let conversation = sample_agent::run_model_agent(
        &client,
        &format!("{}/v1", base(&mcp)),
        Some("s"),
        "reporter-1",
        "All good.",
    )
    .await
    .unwrap();

    assert_eq!(conversation.answer, "Sent the daily report.");
    use EventKind::*;
    let events = store.session_events("s").await.unwrap();
    assert_eq!(
        kinds(&events),
        [
            ModelRequest,
            ModelResponse,
            CallRequest,
            CallResponse,
            ModelRequest,
            ModelResponse,
            CallRequest,
            CallResponse,
            ModelRequest,
            ModelResponse,
        ]
    );
    let sent = outbox.sent().await.unwrap();
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].subject, "Daily report");
    assert_eq!(
        std::fs::read_to_string(files.path().join("reports/daily.md")).unwrap(),
        "All good."
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_leaves_an_open_model_call_alone() {
    let (_db, store) = pg().await;
    let harness = Harness::start(store.clone()).await;
    // A copy that logged a model call and died before the reply.
    let dead = store
        .acquire_lease("s", "dead", Duration::ZERO)
        .await
        .unwrap();
    let body = ask("reporter-1");
    store
        .append(
            NewEvent::model_request("s", "reporter-1", &body, json!({ "request": body })),
            &dead,
        )
        .await
        .unwrap();

    let client = sample_agent::connect(&harness.url, Some("s"))
        .await
        .unwrap();
    client
        .call_tool(sample_agent::call_params(
            "files__read_file",
            json!({ "path": "x" }),
        ))
        .await
        .unwrap();

    let events = store.session_events("s").await.unwrap();
    assert!(
        events.iter().all(|e| e.outcome != Some(Outcome::InDoubt)),
        "{events:#?}"
    );
    assert_eq!(
        kinds(&events),
        [
            EventKind::ModelRequest,
            EventKind::CallRequest,
            EventKind::CallResponse
        ]
    );
}
