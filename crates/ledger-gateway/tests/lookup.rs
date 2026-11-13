//! A call costs the same however long its session has run: recognising a retry reads only
//! the events that decide it, never the whole log.
//!
//! Counted, not timed: how much a call reads does not depend on how busy the machine is.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::CALL_TIMEOUT;
use ledger_gateway::{Arrival, Gateway, Upstream, UpstreamError};
use ledger_store::{
    CallKey, Event, EventStore, Fork, Lease, NewEvent, PgEventStore, SessionSummary, StoreError,
};
use ledger_testkit::TestDb;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Tool, ToolAnnotations,
};
use sample_agent::call_params;
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

/// A read-only tool in this process. Over HTTP, a thousand calls open two thousand TCP
/// connections, because the MCP client opens one per request, and each holds a local port
/// for 30 s after it closes: with a few suites running at once, enough to run the machine
/// out of ports. What this file counts is reads from the log, which HTTP does not change.
struct ReadFile;

#[async_trait]
impl Upstream for ReadFile {
    fn name(&self) -> &str {
        "files"
    }

    async fn list_tools(&self) -> Result<Vec<Tool>, UpstreamError> {
        let schema = json!({ "type": "object", "properties": { "path": { "type": "string" } } });
        let mut hints = ToolAnnotations::new();
        hints.read_only_hint = Some(true);
        let schema = Arc::new(schema.as_object().unwrap().clone());
        Ok(vec![
            Tool::new("read_file", "Reads a file", schema).with_annotations(hints),
        ])
    }

    async fn call_tool(
        &self,
        _params: CallToolRequestParams,
    ) -> Result<CallToolResponse, UpstreamError> {
        Ok(CallToolResult::success(vec![ContentBlock::text("x")]).into())
    }
}

/// A call to read the file under `step`, arriving the way `tools/call` does. Every request
/// has an id of its own, so only the step says which call a retry repeats.
fn read(step: usize, id: usize) -> Arrival {
    Arrival {
        session: "long".to_owned(),
        rpc_id: json!(id),
        meta: json!({}),
        transport: Some("t".to_owned()),
        step: Some(step.to_string()),
        params: call_params("files__read_file", json!({ "path": "f.txt" })),
        traceparent: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_thousandth_call_reads_no_more_than_the_hundredth() {
    let db = TestDb::new().await;
    let store = Arc::new(Counting {
        inner: Arc::new(PgEventStore::new(db.pool())),
        reads: Mutex::default(),
    });
    let gateway = Gateway::new(store.clone(), vec![Arc::new(ReadFile)], CALL_TIMEOUT);
    let mut ids = 0..;

    // A retry of call `step`, and what it read.
    let retry = async |step: usize, id: usize| {
        let before = store.reads();
        let answer = gateway.call(read(step, id)).await;
        assert!(answer.is_ok(), "{answer:?}");
        let after = store.reads();
        (
            after.events_from_lookups - before.events_from_lookups,
            after.whole_sessions - before.whole_sessions,
        )
    };

    let mut at_100 = None;
    for step in 1..=1000 {
        let before = store.reads();
        let answer = gateway.call(read(step, ids.next().unwrap())).await;
        assert!(answer.is_ok(), "{answer:?}");
        // A new call finds nothing to repeat, and reads nothing else.
        let after = store.reads();
        assert_eq!(
            after.events_from_lookups, before.events_from_lookups,
            "call {step}"
        );
        if step == 100 {
            at_100 = Some(retry(100, ids.next().unwrap()).await);
        }
    }
    let at_1000 = retry(1000, ids.next().unwrap()).await;

    // Each retry reads its call and its answer, the same at call 1,000 as at call 100.
    assert_eq!(at_100, Some((2, 0)));
    assert_eq!(at_1000, (2, 0));
    // The whole session was read once, when this copy took it, while it was empty.
    let reads = store.reads();
    assert_eq!(reads.whole_sessions, 1, "{reads:?}");
    assert_eq!(reads.events_from_whole_sessions, 0, "{reads:?}");
    assert_eq!(reads.lookups, 1002);
    // A request and an answer per call, and a retry event for each of the two retries.
    assert_eq!(
        store.session_events("long").await.unwrap().len(),
        2 * 1000 + 2
    );
}
