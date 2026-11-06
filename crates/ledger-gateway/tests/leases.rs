//! Several gateway copies sharing one log: each session has one owner at a time.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CALL_TIMEOUT, Harness, spawn_gateway, upstream};
use ledger_gateway::{Gateway, error_codes};
use ledger_store::{EventKind, EventStore, PgEventStore};
use ledger_testkit::{PATIENCE, TestDb};
use rmcp::ServiceError;
use sample_agent::{call_params, connect};
use serde_json::json;

fn send_email() -> rmcp::model::CallToolRequestParams {
    call_params(
        "email__send_email",
        json!({"to": "a@example.com", "subject": "s", "body": "b"}),
    )
}

/// A gateway copy in front of the harness's email server.
fn copy(store: &Arc<dyn EventStore>, harness: &Harness, lease_ttl: Duration) -> Gateway {
    let upstreams = vec![upstream("email", &harness.email_url)];
    Gateway::new(store.clone(), upstreams, CALL_TIMEOUT).with_lease_ttl(lease_ttl)
}

fn error_code(error: ServiceError) -> rmcp::model::ErrorCode {
    match error {
        ServiceError::McpError(error) => error.code,
        other => panic!("expected a JSON-RPC error, got {other}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_cannot_record_in_a_session_another_copy_holds() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let harness = Harness::start(store.clone()).await;
    let first = connect(
        &spawn_gateway(copy(&store, &harness, PATIENCE)).await,
        Some("s"),
    )
    .await
    .unwrap();
    let second = connect(
        &spawn_gateway(copy(&store, &harness, PATIENCE)).await,
        Some("s"),
    )
    .await
    .unwrap();

    first.call_tool(send_email()).await.unwrap();
    let refused = second.call_tool(send_email()).await.unwrap_err();

    assert_eq!(error_code(refused), error_codes::NOT_RECORDED);
    assert_eq!(harness.outbox.sent().await.unwrap().len(), 1);
    assert_eq!(store.session_events("s").await.unwrap().len(), 2);
    // Other sessions are free for the second copy.
    let other = connect(
        &spawn_gateway(copy(&store, &harness, PATIENCE)).await,
        Some("t"),
    )
    .await
    .unwrap();
    other.call_tool(send_email()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_that_has_been_taken_over_is_fenced_out() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let harness = Harness::start(store.clone()).await;
    // The old copy's lease lapses at once, as a paused copy's would.
    let old = connect(
        &spawn_gateway(copy(&store, &harness, Duration::ZERO)).await,
        Some("s"),
    )
    .await
    .unwrap();
    let new = connect(
        &spawn_gateway(copy(&store, &harness, PATIENCE)).await,
        Some("s"),
    )
    .await
    .unwrap();

    old.call_tool(send_email()).await.unwrap();
    new.call_tool(send_email()).await.unwrap();
    let fenced = old.call_tool(send_email()).await.unwrap_err();

    // Refused before forwarding: the tool did not run for the old copy.
    assert_eq!(error_code(fenced), error_codes::NOT_RECORDED);
    assert_eq!(harness.outbox.sent().await.unwrap().len(), 2);
    let events = store.session_events("s").await.unwrap();
    let tokens: Vec<_> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .map(|e| e.fence_token.unwrap())
        .collect();
    assert_eq!(tokens.len(), 2);
    assert!(tokens[1] > tokens[0], "{tokens:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn requests_are_logged_with_their_arguments_hash() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let harness = Harness::start(store.clone()).await;
    let agent = connect(&harness.url, Some("s")).await.unwrap();
    let arguments = json!({"to": "a@example.com", "subject": "s", "body": "b"});

    agent.call_tool(send_email()).await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_eq!(
        events[0].args_hash,
        Some(ledger_store::args_hash(&arguments))
    );
    assert_eq!(events[1].args_hash, None);
}
