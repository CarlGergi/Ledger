//! The email demo, against a stand-in for AgentMail's API: an answer lost after the email
//! went out, and an agent that retries. Connected straight to the tool, the retry sends the
//! email again. Through Ledger, it does not, wherever the answer was lost.

mod common;

use std::sync::Arc;

use common::{CALL_TIMEOUT, spawn_gateway, spawn_tool_server, upstream};
use ledger_gateway::Gateway;
use ledger_store::{EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::TestDb;
use ledger_tools::agentmail::{AgentMail, AgentMailServer, FakeAgentMail};
use ledger_tools::lossy::LossyProxy;
use sample_agent::{Delivery, send_until_answered};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const API_KEY: &str = "am-test-key";
const INBOX: &str = "ledger-demo@agentmail.to";

/// The stand-in API and an AgentMail tool server in front of it: the server's MCP URL.
async fn agentmail() -> (FakeAgentMail, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api = format!("http://{}/v0", listener.local_addr().unwrap());
    let fake = FakeAgentMail::new(API_KEY);
    tokio::spawn(fake.clone().serve(listener, CancellationToken::new()));
    let mail = AgentMail::new(&api, API_KEY.to_owned(), INBOX);
    (fake, spawn_tool_server(AgentMailServer::new(mail)).await)
}

/// A proxy in front of `mcp_url` that loses the first tool call's answer: the proxy's URL.
async fn losing_one_answer(mcp_url: &str) -> (LossyProxy, String) {
    let target = mcp_url.trim_end_matches("/mcp");
    let proxy = LossyProxy::new(target, 1);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(proxy.clone().serve(listener, CancellationToken::new()));
    (proxy, url)
}

fn email() -> Value {
    json!({ "to": "carl@example.com", "subject": "Ledger demo", "body": "Sent once." })
}

async fn agent(url: &str, session: Option<&str>, tool: &str) -> Delivery {
    send_until_answered(url, session, "1", tool, email(), 3).await
}

#[tokio::test(flavor = "multi_thread")]
async fn connected_straight_to_the_tool_a_lost_answer_sends_the_email_twice() {
    let (api, tool) = agentmail().await;
    let (proxy, url) = losing_one_answer(&tool).await;

    let delivery = agent(&url, None, "send_email").await;

    assert_eq!(delivery.attempts, 2);
    assert!(delivery.result.is_ok(), "{:?}", delivery.result);
    assert_eq!(proxy.lost(), 1);
    assert_eq!(api.sent().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn through_ledger_an_answer_lost_on_the_way_to_the_agent_sends_once() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let (api, tool) = agentmail().await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("agentmail", &tool)],
        CALL_TIMEOUT,
    );
    let (proxy, url) = losing_one_answer(&spawn_gateway(gateway).await).await;

    let delivery = agent(&url, Some("demo"), "agentmail__send_email").await;

    assert_eq!(delivery.attempts, 2);
    assert!(delivery.result.is_ok(), "{:?}", delivery.result);
    assert_eq!(proxy.lost(), 1);
    assert_eq!(api.sent().len(), 1);
    // The retry was answered from the log.
    let kinds: Vec<EventKind> = store
        .session_events("demo")
        .await
        .unwrap()
        .iter()
        .map(|e| e.kind)
        .collect();
    use EventKind::*;
    assert_eq!(kinds, [CallRequest, CallResponse, CallRetry]);
}

#[tokio::test(flavor = "multi_thread")]
async fn through_ledger_an_answer_lost_on_the_way_from_the_tool_sends_once() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let (api, tool) = agentmail().await;
    let (proxy, lossy_tool) = losing_one_answer(&tool).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("agentmail", &lossy_tool)],
        CALL_TIMEOUT,
    );
    let url = spawn_gateway(gateway).await;

    let delivery = agent(&url, Some("demo"), "agentmail__send_email").await;

    assert_eq!(delivery.attempts, 2);
    assert!(delivery.result.is_ok(), "{:?}", delivery.result);
    assert_eq!(proxy.lost(), 1);
    // AgentMail got the email twice, with the same key, and sent it once.
    assert_eq!(api.sent().len(), 1);
    let events = store.session_events("demo").await.unwrap();
    let outcomes: Vec<Option<Outcome>> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallResponse)
        .map(|e| e.outcome)
        .collect();
    assert_eq!(outcomes, [Some(Outcome::TransportError), Some(Outcome::Ok)]);
    let keys: Vec<&Value> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallResponse)
        .map(|e| &e.payload["idempotency_key"])
        .collect();
    assert_eq!(keys[0], keys[1]);
    assert_eq!(keys[0], "ledger:demo:1");
}
