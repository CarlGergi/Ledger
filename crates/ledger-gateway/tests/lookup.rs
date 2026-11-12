//! A call costs the same however long its session has run: recognising a retry reads only
//! the events that decide it, never the whole log.
//!
//! Counted, not timed: how much a call reads does not depend on how busy the machine is.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::{CALL_TIMEOUT, spawn_gateway, spawn_tool_server, upstream};
use ledger_gateway::Gateway;
use ledger_store::{
    CallKey, Event, EventStore, Fork, Lease, NewEvent, PgEventStore, SessionSummary, StoreError,
};
use ledger_testkit::TestDb;
use ledger_tools::files::FileServer;
use sample_agent::RetryingAgent;
use serde_json::json;

/// How many events the gateway has read, by method.
#[derive(Default, Debug, Clone, Copy, PartialEq)]
struct Reads {
    whole_sessions: usize,
    events_from_whole_sessions: usize,
    lookups: usize,
    events_from_lookups: usize,
}

struct Counting {
    inner: Arc<dyn EventStore>,
    reads: Mutex<Reads>,
}

impl Counting {
    fn reads(&self) -> Reads {
        *self.reads.lock().unwrap()
    }
}

#[async_trait]
impl EventStore for Counting {
    async fn acquire_lease(&self, s: &str, o: &str, ttl: Duration) -> Result<Lease, StoreError> {
        self.inner.acquire_lease(s, o, ttl).await
    }
    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError> {
        self.inner.append(event, lease).await
    }
    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.inner.renew_lease(lease).await
    }
    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.inner.release_lease(lease).await
    }
    async fn session_events(&self, session: &str) -> Result<Vec<Event>, StoreError> {
        let events = self.inner.session_events(session).await?;
        let mut reads = self.reads.lock().unwrap();
        reads.whole_sessions += 1;
        reads.events_from_whole_sessions += events.len();
        Ok(events)
    }
    async fn call_lookup(&self, session: &str, key: &CallKey) -> Result<Vec<Event>, StoreError> {
        let events = self.inner.call_lookup(session, key).await?;
        let mut reads = self.reads.lock().unwrap();
        reads.lookups += 1;
        reads.events_from_lookups += events.len();
        Ok(events)
    }
    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError> {
        self.inner.sessions().await
    }
    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError> {
        self.inner.in_doubt().await
    }
    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError> {
        self.inner.create_fork(fork).await
    }
    async fn fork(&self, session: &str) -> Result<Option<Fork>, StoreError> {
        self.inner.fork(session).await
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_thousandth_call_reads_no_more_than_the_hundredth() {
    let db = TestDb::new().await;
    let store = Arc::new(Counting {
        inner: Arc::new(PgEventStore::new(db.pool())),
        reads: Mutex::default(),
    });
    let files = tempfile::tempdir().unwrap();
    std::fs::write(files.path().join("f.txt"), "x").unwrap();
    let tool = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
    let gateway = Gateway::new(store.clone(), vec![upstream("files", &tool)], CALL_TIMEOUT);
    let url = spawn_gateway(gateway).await;
    let mut agent = RetryingAgent::new(&url, Some("long"));
    let read = || json!({ "path": "f.txt" });

    // A retry of call `step`, and what it read.
    let retry = async |agent: &mut RetryingAgent, step: usize| {
        let before = store.reads();
        let delivery = agent
            .call(&step.to_string(), "files__read_file", read(), 1)
            .await;
        assert!(delivery.result.is_ok(), "{:?}", delivery.result);
        let after = store.reads();
        (
            after.events_from_lookups - before.events_from_lookups,
            after.whole_sessions - before.whole_sessions,
        )
    };

    let mut at_100 = None;
    for step in 1..=1000 {
        let before = store.reads();
        let delivery = agent
            .call(&step.to_string(), "files__read_file", read(), 1)
            .await;
        assert!(delivery.result.is_ok(), "{:?}", delivery.result);
        // A new call finds nothing to repeat, and reads nothing else.
        let after = store.reads();
        assert_eq!(
            after.events_from_lookups, before.events_from_lookups,
            "call {step}"
        );
        if step == 100 {
            at_100 = Some(retry(&mut agent, 100).await);
        }
    }
    let at_1000 = retry(&mut agent, 1000).await;

    // Each retry reads its call and its answer, the same at call 1,000 as at call 100.
    assert_eq!(at_100, Some((2, 0)));
    assert_eq!(at_1000, (2, 0));
    // The whole session was read once, when this copy took it, while it was empty.
    let reads = store.reads();
    assert_eq!(reads.whole_sessions, 1, "{reads:?}");
    assert_eq!(reads.events_from_whole_sessions, 0, "{reads:?}");
    assert_eq!(reads.lookups, 1002);
    agent.close().await;
    // A request and an answer per call, and a retry event for each of the two retries.
    assert_eq!(
        store.session_events("long").await.unwrap().len(),
        2 * 1000 + 2
    );
}
