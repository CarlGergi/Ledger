//! Health checks for load balancers, and waiting for running calls before stopping.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{CALL_TIMEOUT, Probe, spawn_gateway, spawn_tool_server, upstream};
use ledger_gateway::Gateway;
use ledger_store::{EventStore, PgEventStore};
use ledger_testkit::{PATIENCE, ReservedPort, TestDb};
use sample_agent::{call_params, connect};
use serde_json::json;

fn base(mcp: &str) -> String {
    mcp.trim_end_matches("/mcp").to_owned()
}

async fn status(url: &str) -> u16 {
    reqwest::get(url).await.unwrap().status().as_u16()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_is_ready_until_it_starts_shutting_down() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let url = base(&spawn_gateway(gateway.clone()).await);

    assert_eq!(status(&format!("{url}/healthz")).await, 200);
    assert_eq!(status(&format!("{url}/readyz")).await, 200);
    // A call first, so this copy holds the session's lease when it starts shutting down.
    let agent = connect(&format!("{url}/mcp"), Some("s")).await.unwrap();
    agent
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap();
    let before = store.session_events("s").await.unwrap().len();
    gateway.begin_shutdown();
    assert_eq!(status(&format!("{url}/readyz")).await, 503);
    // And new calls are turned away before anything is recorded, even in a session it
    // holds. A tool that answers at once, so that a call taken fails the test, not hangs it.
    let refused = agent
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap_err();
    match refused {
        rmcp::ServiceError::McpError(e) => {
            assert_eq!(e.code, ledger_gateway::error_codes::NOT_RECORDED)
        }
        other => panic!("{other}"),
    }
    let model = reqwest::Client::new()
        .post(format!("{url}/v1/chat/completions"))
        .header("x-ledger-session", "s")
        .json(&json!({ "model": "m" }))
        .send()
        .await
        .unwrap();
    assert_eq!(model.status(), 503);
    assert_eq!(store.session_events("s").await.unwrap().len(), before);
    // Still alive, so not to be restarted.
    assert_eq!(status(&format!("{url}/healthz")).await, 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_gateway_that_cannot_reach_its_log_is_not_ready() {
    // Nothing listens on a reserved port.
    let port = ReservedPort::claim();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(1))
        .connect_lazy(&format!("postgres://ledger:ledger@{}/ledger", port.addr()))
        .unwrap();
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(pool));
    let gateway = Gateway::new(store, Vec::new(), CALL_TIMEOUT);
    let url = base(&spawn_gateway(gateway).await);

    assert_eq!(status(&format!("{url}/healthz")).await, 200);
    assert_eq!(status(&format!("{url}/readyz")).await, 503);
}

#[tokio::test(flavor = "multi_thread")]
async fn draining_waits_for_running_calls_and_no_longer() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let gateway = Gateway::new(store, vec![upstream("probe", &probe_url)], CALL_TIMEOUT);
    let url = spawn_gateway(gateway.clone()).await;
    assert!(gateway.drain(Duration::ZERO).await, "nothing was running");

    let agent = connect(&url, Some("s")).await.unwrap();
    let call =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;
    assert_eq!(gateway.active_calls(), 1);
    assert!(!gateway.drain(Duration::ZERO).await, "a call is running");

    probe.release();
    assert!(gateway.drain(PATIENCE).await);
    assert_eq!(gateway.active_calls(), 0);
    call.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_running_when_leases_are_released_takes_nothing_back() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let gateway = Gateway::new(
        store.clone(),
        vec![upstream("probe", &probe_url)],
        CALL_TIMEOUT,
    );
    let url = spawn_gateway(gateway.clone()).await;
    let agent = connect(&url, Some("s")).await.unwrap();
    let call =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;

    // Shutting down, past its grace: it lets go of the session with the call still running.
    gateway.begin_shutdown();
    gateway.release_leases().await;
    probe.release();
    let _ = call.await.unwrap();

    // The call's answer was not logged under a lease taken back, so the call is open, for
    // the copy that takes the session to settle, and that copy can take it at once.
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 1, "{events:?}");
    store
        .acquire_lease("s", "next copy", PATIENCE)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_released_lease_is_not_renewed_by_a_call_still_running() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(pool.clone()));
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    // A short lease, so the running call's heartbeat renews it every 50 ms.
    let ttl = Duration::from_millis(200);
    let gateway =
        Gateway::new(store, vec![upstream("probe", &probe_url)], CALL_TIMEOUT).with_lease_ttl(ttl);
    let url = spawn_gateway(gateway.clone()).await;
    let agent = connect(&url, Some("s")).await.unwrap();
    let call =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;

    gateway.begin_shutdown();
    gateway.release_leases().await;
    // Longer than the lease, with the call still running: had the heartbeat renewed it,
    // it would not have expired.
    tokio::time::sleep(2 * ttl).await;
    let expired: bool =
        sqlx::query_scalar("SELECT expires_at <= now() FROM leases WHERE session_id = 's'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(expired, "the released lease was renewed");
    probe.release();
    let _ = call.await.unwrap();
}
