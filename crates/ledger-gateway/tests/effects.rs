//! How each tool's effect class is worked out, and what the log records about it.

mod common;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::{CALL_TIMEOUT, Harness, Probe, spawn_gateway, spawn_tool_server, upstream};
use ledger_gateway::{EffectPolicy, Gateway, Upstream, UpstreamError};
use ledger_store::{Event, EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::{ReservedPort, TestDb};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Tool, ToolAnnotations,
};
use sample_agent::{call_params, connect};
use serde_json::{Value, json};

fn pg(db: &TestDb) -> Arc<dyn EventStore> {
    Arc::new(PgEventStore::new(db.pool()))
}

fn read_only_hint() -> ToolAnnotations {
    let mut annotations = ToolAnnotations::new();
    annotations.read_only_hint = Some(true);
    annotations
}

fn listed_tool(name: &'static str, schema: Value) -> Tool {
    Tool::new(
        name,
        "a tool",
        Arc::new(schema.as_object().unwrap().clone()),
    )
}

fn listed_as_read_only(name: &'static str) -> Tool {
    listed_tool(name, json!({"type": "object"})).with_annotations(read_only_hint())
}

#[derive(Clone)]
enum Listing {
    Tools(Vec<Tool>),
    Fails,
    Stalls,
}

/// A tool server inside the test process, whose listing a test can change at any point.
/// Its calls finish the moment they are polled, so no limit on a call can ever fire.
struct ScriptedUpstream {
    listing: Mutex<Listing>,
}

impl ScriptedUpstream {
    fn new(tools: Vec<Tool>) -> Arc<Self> {
        Arc::new(Self {
            listing: Mutex::new(Listing::Tools(tools)),
        })
    }

    fn set_listing(&self, listing: Listing) {
        *self.listing.lock().unwrap() = listing;
    }
}

#[async_trait]
impl Upstream for ScriptedUpstream {
    fn name(&self) -> &str {
        "s"
    }

    async fn list_tools(&self) -> Result<Vec<Tool>, UpstreamError> {
        let listing = self.listing.lock().unwrap().clone();
        match listing {
            Listing::Tools(tools) => Ok(tools),
            Listing::Fails => Err(UpstreamError::Transport("scripted failure".to_owned())),
            Listing::Stalls => std::future::pending().await,
        }
    }

    async fn call_tool(
        &self,
        _params: CallToolRequestParams,
    ) -> Result<CallToolResponse, UpstreamError> {
        Ok(CallToolResult::success(vec![ContentBlock::text("done")]).into())
    }
}

async fn requests(store: &Arc<dyn EventStore>, session: &str) -> Vec<Event> {
    let mut events = store.session_events(session).await.unwrap();
    events.retain(|e| e.kind == EventKind::CallRequest);
    events
}

/// The effect recorded for each request of a session, in order.
async fn effects(store: &Arc<dyn EventStore>, session: &str) -> Vec<Value> {
    let requests = requests(store, session).await;
    requests
        .iter()
        .map(|e| e.payload["effect"].clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_real_tools_are_logged_with_the_class_their_annotations_give() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    for (tool, arguments) in [
        (
            "files__write_file",
            json!({"path": "a.txt", "content": "x"}),
        ),
        ("files__read_file", json!({"path": "a.txt"})),
        (
            "email__send_email",
            json!({"to": "a@example.com", "subject": "s", "body": "b"}),
        ),
    ] {
        agent.call_tool(call_params(tool, arguments)).await.unwrap();
    }

    assert_eq!(
        effects(&store, "s").await,
        [
            json!({"class": "idempotent", "source": "listing"}),
            json!({"class": "read_only", "source": "listing"}),
            json!({"class": "at_most_once", "source": "listing"}),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_annotation_is_believed_until_the_override_file_corrects_it() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let mut probe = Probe::new(store.clone(), "unused");
    probe.annotations = Some(read_only_hint());
    let probe_url = spawn_tool_server(probe).await;
    let params = || call_params("probe__observe", json!({}));

    let believing = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let url = spawn_gateway(believing).await;
    let agent = connect(&url, Some("believing")).await.unwrap();
    agent.call_tool(params()).await.unwrap();

    let policy = EffectPolicy::parse("[tool.probe__observe]\nclass = \"at_most_once\"").unwrap();
    let corrected = Gateway::with_policy(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
        policy,
    );
    let url = spawn_gateway(corrected).await;
    let agent = connect(&url, Some("corrected")).await.unwrap();
    agent.call_tool(params()).await.unwrap();

    assert_eq!(
        effects(&store, "believing").await,
        [json!({"class": "read_only", "source": "listing"})]
    );
    assert_eq!(
        effects(&store, "corrected").await,
        [json!({"class": "at_most_once", "source": "override"})]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_keyed_tool_is_logged_with_the_input_that_carries_its_key() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let schema = json!({"type": "object", "properties": {"idempotency_key": {"type": "string"}}});
    let scripted = ScriptedUpstream::new(vec![listed_tool("keyed", schema)]);
    let gateway = Gateway::new(store.clone(), vec![scripted], CALL_TIMEOUT);
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    agent
        .call_tool(call_params("s__keyed", json!({})))
        .await
        .unwrap();

    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "keyed", "source": "listing", "key_param": "idempotency_key"})]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_server_is_asked_once_rather_than_on_every_call() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    // The agent never lists, so the gateway has to ask for itself.
    assert_eq!(probe.listings.load(Ordering::SeqCst), 0);
    for _ in 0..3 {
        agent
            .call_tool(call_params("probe__observe", json!({})))
            .await
            .unwrap();
    }
    assert_eq!(probe.listings.load(Ordering::SeqCst), 1);
    assert_eq!(
        effects(&store, "s").await,
        vec![json!({"class": "at_most_once", "source": "listing"}); 3]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_agents_own_listing_teaches_the_gateway_too() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    agent.list_all_tools().await.unwrap();
    agent
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap();

    assert_eq!(probe.listings.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_latest_listing_decides_and_a_failed_one_changes_nothing() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let scripted = ScriptedUpstream::new(vec![listed_as_read_only("t")]);
    let gateway = Gateway::new(store.clone(), vec![scripted.clone()], CALL_TIMEOUT);
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();
    let call = || agent.call_tool(call_params("s__t", json!({})));

    agent.list_all_tools().await.unwrap();
    call().await.unwrap();

    // A failed listing is no news about a tool's effects.
    scripted.set_listing(Listing::Fails);
    assert!(agent.list_all_tools().await.unwrap().is_empty());
    call().await.unwrap();

    // The server now says something different.
    scripted.set_listing(Listing::Tools(vec![listed_tool(
        "t",
        json!({"type": "object"}),
    )]));
    agent.list_all_tools().await.unwrap();
    call().await.unwrap();

    // The server no longer lists the tool at all.
    scripted.set_listing(Listing::Tools(Vec::new()));
    agent.list_all_tools().await.unwrap();
    call().await.unwrap();

    assert_eq!(
        effects(&store, "s").await,
        [
            json!({"class": "read_only", "source": "listing"}),
            json!({"class": "read_only", "source": "listing"}),
            json!({"class": "at_most_once", "source": "listing"}),
            json!({"class": "at_most_once", "source": "unlisted"}),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_its_server_does_not_list_gets_the_safest_class() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    agent
        .call_tool(call_params("files__no_such_tool", json!({})))
        .await
        .unwrap_err();

    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "at_most_once", "source": "unlisted"})]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_that_cannot_be_reached_gets_the_safest_class() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let nothing_there = ReservedPort::claim();
    let url = format!("http://{}/mcp", nothing_there.addr());
    let gateway = Gateway::new(store.clone(), vec![upstream("down", &url)], CALL_TIMEOUT);
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    agent
        .call_tool(call_params("down__anything", json!({})))
        .await
        .unwrap_err();

    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "at_most_once", "source": "unlisted"})]
    );
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::TransportError));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_that_goes_nowhere_gets_the_safest_class() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();

    agent
        .call_tool(call_params("nowhere__anything", json!({})))
        .await
        .unwrap_err();

    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "at_most_once", "source": "unlisted"})]
    );
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::NotRouted));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listing_that_stalls_does_not_stop_the_call() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let scripted = ScriptedUpstream::new(Vec::new());
    scripted.set_listing(Listing::Stalls);
    let gateway = Gateway::new(store.clone(), vec![scripted], Duration::from_millis(200));
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    agent
        .call_tool(call_params("s__t", json!({})))
        .await
        .unwrap();

    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "at_most_once", "source": "unlisted"})]
    );
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[1].outcome, Some(Outcome::Ok));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_overridden_tool_needs_no_listing() {
    let db = TestDb::new().await;
    let store = pg(&db);
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let policy = EffectPolicy::parse("[tool.probe__observe]\nclass = \"idempotent\"").unwrap();
    let gateway = Gateway::with_policy(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
        policy,
    );
    let agent = connect(&spawn_gateway(gateway).await, Some("s"))
        .await
        .unwrap();

    agent
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap();

    assert_eq!(probe.listings.load(Ordering::SeqCst), 0);
    assert_eq!(
        effects(&store, "s").await,
        [json!({"class": "idempotent", "source": "override"})]
    );
}
