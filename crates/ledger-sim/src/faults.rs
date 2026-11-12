//! What one gateway copy sees of the store and of the tools, with faults injected. A copy
//! can be killed, which stops everything it was doing at whatever point it had reached, or
//! paused, which freezes it until it wakes up still believing what it believed.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use ledger_gateway::{Upstream, UpstreamError};
use ledger_store::{
    CallKey, Event, EventStore, Fork, Lease, MemoryStore, NewEvent, SessionSummary, StoreError,
};
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Tool, ToolAnnotations,
};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::world::{Appended, Effect, World};

/// One incarnation of a gateway copy: dead for good once killed.
#[derive(Debug)]
pub struct Life {
    pub name: String,
    dead: AtomicBool,
    paused_until: Mutex<Option<Instant>>,
}

impl Life {
    pub fn new(name: String) -> Arc<Self> {
        Arc::new(Self {
            name,
            dead: AtomicBool::new(false),
            paused_until: Mutex::new(None),
        })
    }

    pub fn kill(&self) {
        self.dead.store(true, Ordering::SeqCst);
    }

    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    pub fn pause_for(&self, duration: Duration) {
        *self.paused_until.lock().expect("lock poisoned") = Some(Instant::now() + duration);
    }

    pub fn wake(&self) {
        *self.paused_until.lock().expect("lock poisoned") = None;
    }

    /// Where every step the copy takes outside itself begins. A dead copy never gets past
    /// it, and a paused one only once it wakes.
    async fn step(&self) {
        loop {
            if self.is_dead() {
                std::future::pending::<()>().await;
            }
            let until = *self.paused_until.lock().expect("lock poisoned");
            match until {
                Some(until) if until > Instant::now() => tokio::time::sleep_until(until).await,
                _ => return,
            }
        }
    }
}

/// How often each fault happens, per opportunity.
pub const FAIL_BEFORE_WRITE: f64 = 0.01;
pub const DIE_AFTER_WRITE: f64 = 0.005;
pub const LOSE_WRITE_ACK: f64 = 0.01;
pub const TOOL_REFUSES: f64 = 0.02;
pub const TOOL_HANGS: f64 = 0.01;
pub const TOOL_LOSES_ANSWER: f64 = 0.03;

/// The shared store, as one copy reaches it.
pub struct CopyStore {
    pub inner: Arc<MemoryStore>,
    pub life: Arc<Life>,
    pub world: World,
    /// False for the sweeper at the end, which has to work.
    pub faulty: bool,
}

impl CopyStore {
    fn note(&self, what: &str) {
        self.world.note(format!("{} store: {what}", self.life.name));
    }
}

#[async_trait]
impl EventStore for CopyStore {
    async fn acquire_lease(
        &self,
        session_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.life.step().await;
        let lease = self.inner.acquire_lease(session_id, owner, ttl).await;
        match &lease {
            Ok(lease) => self.note(&format!("took {session_id}, token {}", lease.token)),
            Err(e) => self.note(&format!("could not take {session_id}: {e}")),
        }
        lease
    }

    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError> {
        self.life.step().await;
        if self.faulty && self.world.chance(FAIL_BEFORE_WRITE) {
            self.note("write failed");
            return Err(StoreError::Rejected("injected failure".to_owned()));
        }
        let what = format!("{:?} in {}", event.kind, event.session_id);
        let appended = self.inner.append(event, lease).await;
        let Ok(event) = appended else {
            self.note(&format!(
                "{what} refused: {}",
                appended.as_ref().unwrap_err()
            ));
            return appended;
        };
        let tick = self.world.tick();
        self.world.state().appended.push(Appended {
            session: event.session_id.clone(),
            seq: event.seq,
            tick,
        });
        self.note(&format!("{what} #{}", event.seq));
        if self.faulty && self.world.chance(DIE_AFTER_WRITE) {
            self.note("killed just after a write");
            self.life.kill();
            self.life.step().await;
        }
        if self.faulty && self.world.chance(LOSE_WRITE_ACK) {
            // Committed, but the copy never hears so: what a dropped connection looks like.
            self.note("lost the acknowledgement of a write");
            return Err(StoreError::Rejected(
                "injected lost acknowledgement".to_owned(),
            ));
        }
        Ok(event)
    }

    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.life.step().await;
        self.inner.renew_lease(lease).await
    }

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.life.step().await;
        self.inner.release_lease(lease).await
    }

    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError> {
        self.life.step().await;
        self.inner.session_events(session_id).await
    }

    async fn call_lookup(&self, session_id: &str, key: &CallKey) -> Result<Vec<Event>, StoreError> {
        self.life.step().await;
        self.inner.call_lookup(session_id, key).await
    }

    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError> {
        self.inner.sessions().await
    }

    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError> {
        self.inner.create_fork(fork).await
    }

    async fn fork(&self, session_id: &str) -> Result<Option<Fork>, StoreError> {
        self.life.step().await;
        self.inner.fork(session_id).await
    }

    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError> {
        self.inner.in_doubt().await
    }
}

/// How a tool really behaves, whatever its annotations claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Truth {
    ReadOnly,
    Idempotent,
    /// Repeats with the same key take effect once.
    Keyed,
    AtMostOnce,
}

/// The tools of the simulated outside world, and what each really is.
pub const TOOLS: [(&str, Truth); 6] = [
    ("read", Truth::ReadOnly),
    ("put", Truth::Idempotent),
    ("send", Truth::Keyed),
    ("charge", Truth::AtMostOnce),
    // Claims to be read-only. The override file says what it really is.
    ("lie", Truth::AtMostOnce),
    // Claims to be read-only, and nothing corrects it: what an override file is for.
    ("lie_uncorrected", Truth::AtMostOnce),
];

pub const OVERRIDES: &str = "[tool.svc__lie]\nclass = \"at_most_once\"\n";

pub fn truth(tool: &str) -> Truth {
    TOOLS
        .iter()
        .find(|(name, _)| *name == tool)
        .map_or(Truth::AtMostOnce, |(_, truth)| *truth)
}

fn listing() -> Vec<Tool> {
    TOOLS
        .iter()
        .map(|(name, truth)| {
            let mut properties = json!({ "intent": { "type": "string" } });
            if *truth == Truth::Keyed {
                properties["idempotency_key"] = json!({ "type": "string" });
            }
            let schema = json!({ "type": "object", "properties": properties });
            let tool = Tool::new(
                *name,
                "simulated",
                Arc::new(schema.as_object().unwrap().clone()),
            );
            let mut hints = ToolAnnotations::new();
            match (*name, truth) {
                (_, Truth::ReadOnly) => hints.read_only_hint = Some(true),
                (_, Truth::Idempotent) => hints.idempotent_hint = Some(true),
                ("lie" | "lie_uncorrected", _) => hints.read_only_hint = Some(true),
                _ => {}
            }
            tool.with_annotations(hints)
        })
        .collect()
}

/// The tool server, as one copy reaches it. Its effects land in the world whoever calls.
pub struct CopyUpstream {
    pub life: Arc<Life>,
    pub world: World,
    pub faulty: bool,
}

#[async_trait]
impl Upstream for CopyUpstream {
    fn name(&self) -> &str {
        "svc"
    }

    async fn list_tools(&self) -> Result<Vec<Tool>, UpstreamError> {
        self.life.step().await;
        Ok(listing())
    }

    async fn call_tool(
        &self,
        params: CallToolRequestParams,
    ) -> Result<CallToolResponse, UpstreamError> {
        self.life.step().await;
        let tool = params.name.to_string();
        let arguments = Value::Object(params.arguments.unwrap_or_default());
        let intent = arguments["intent"].as_str().unwrap_or_default().to_owned();
        let key = arguments["idempotency_key"].as_str().map(str::to_owned);
        let note = |what: &str| {
            self.world.note(format!(
                "{} tool {tool} for {intent}: {what}",
                self.life.name
            ))
        };

        if self.faulty && self.world.chance(TOOL_REFUSES) {
            note("connection refused");
            return Err(UpstreamError::Transport(
                "injected: connection refused".to_owned(),
            ));
        }
        if self.faulty && self.world.chance(TOOL_HANGS) {
            note("hangs");
            std::future::pending::<()>().await;
        }

        // A keyed tool sends nothing for a key it has seen.
        let repeat = truth(&tool) == Truth::Keyed
            && key.is_some()
            && self
                .world
                .state()
                .effects
                .iter()
                .any(|e| e.tool == tool && e.key == key);
        if repeat {
            note("repeated key, no effect");
        } else {
            let tick = self.world.tick();
            self.world.state().effects.push(Effect {
                tool: tool.clone(),
                intent: intent.clone(),
                key,
                tick,
            });
            note("ran");
        }

        if self.faulty && self.world.chance(TOOL_LOSES_ANSWER) {
            note("answer lost");
            return Err(UpstreamError::Transport("injected: answer lost".to_owned()));
        }
        let result = CallToolResult::success(vec![ContentBlock::text(format!("{tool} {intent}"))]);
        Ok(result.into())
    }
}
