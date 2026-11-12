//! An event store held in memory, for the simulator.
//!
//! It must behave exactly like the PostgreSQL store, or the simulator proves things about
//! a store that does not exist. `tests/conformance.rs` runs one suite against both, so a
//! difference fails a test. Each rule below names the SQL it mirrors.
//!
//! Its clock is tokio's, so under a paused runtime leases expire in simulated time.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::time::Instant;

use crate::{
    CallKey, Event, EventKind, EventStore, Fork, Lease, NewEvent, Outcome, SessionSummary,
    StoreError, lookup_in,
};

#[derive(Debug)]
pub struct MemoryStore {
    state: Mutex<State>,
    /// `recorded_at` is this wall-clock time plus however long tokio's clock has run since
    /// `started`, so that a simulated run records the same times every time.
    epoch: DateTime<Utc>,
    started: Instant,
}

#[derive(Debug, Default)]
struct State {
    sessions: BTreeMap<String, Session>,
    leases: BTreeMap<String, LeaseRow>,
    forks: BTreeMap<String, Fork>,
}

#[derive(Debug)]
struct Session {
    events: Vec<Event>,
    created_at: DateTime<Utc>,
}

#[derive(Debug)]
struct LeaseRow {
    owner: String,
    token: i64,
    expires_at: Instant,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::with_epoch(Utc::now())
    }

    /// A store whose clock starts at `epoch`, for runs that must repeat exactly.
    pub fn with_epoch(epoch: DateTime<Utc>) -> Self {
        Self {
            state: Mutex::default(),
            epoch,
            started: Instant::now(),
        }
    }

    fn now(&self) -> DateTime<Utc> {
        let elapsed = Instant::now() - self.started;
        self.epoch + chrono::Duration::from_std(elapsed).expect("elapsed time fits")
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("memory store lock poisoned")
    }
}

fn rejected(why: &str) -> StoreError {
    StoreError::Rejected(why.to_owned())
}

/// The checks the schema makes on a row, from the migrations.
fn check(event: &NewEvent, session: Option<&Session>, seq: i64) -> Result<(), StoreError> {
    if event.session_id.is_empty() {
        return Err(rejected("sessions.session_id <> ''"));
    }
    // events_shape_check
    let shaped = match event.kind {
        EventKind::CallRequest | EventKind::ModelRequest => {
            event.request_seq.is_none() && event.outcome.is_none()
        }
        EventKind::CallRetry => {
            event.request_seq.is_some_and(|r| r < seq) && event.outcome.is_none()
        }
        EventKind::CallResponse | EventKind::ModelResponse => {
            event.request_seq.is_some_and(|r| r < seq) && event.outcome.is_some()
        }
    };
    if !shaped {
        return Err(rejected("events_shape_check"));
    }
    // events_args_hash_check and events_requests_carry_an_args_hash
    let hash_ok = event.args_hash.as_deref().is_none_or(|h| {
        h.len() == 64
            && h.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    });
    if !hash_ok || event.kind.is_request() != event.args_hash.is_some() {
        return Err(rejected("events_requests_carry_an_args_hash"));
    }
    let events = session.map(|s| s.events.as_slice()).unwrap_or_default();
    // The foreign key from (session_id, request_seq) to events (session_id, seq).
    if let Some(linked) = event.request_seq
        && !events.iter().any(|e| e.seq == linked)
    {
        return Err(rejected("events_session_id_request_seq_fkey"));
    }
    // events_one_final_response_per_request and events_one_final_model_response_per_request
    let is_final = |outcome: Option<Outcome>| outcome.is_some_and(Outcome::is_final);
    if event.kind.is_response() && is_final(event.outcome) {
        let answered = events.iter().any(|e| {
            e.kind == event.kind && e.request_seq == event.request_seq && is_final(e.outcome)
        });
        if answered {
            return Err(rejected("events_one_final_response_per_request"));
        }
    }
    Ok(())
}

fn expiry(now: Instant, ttl: Duration) -> Instant {
    now + ttl
}

#[async_trait]
impl EventStore for MemoryStore {
    async fn acquire_lease(
        &self,
        session_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        if session_id.is_empty() || owner.is_empty() {
            return Err(rejected("leases_check"));
        }
        let now = Instant::now();
        let mut state = self.state();
        // ACQUIRE: free, expired, or already this owner's; the token goes up either way.
        let token = match state.leases.get_mut(session_id) {
            None => {
                state.leases.insert(
                    session_id.to_owned(),
                    LeaseRow {
                        owner: owner.to_owned(),
                        token: 1,
                        expires_at: expiry(now, ttl),
                    },
                );
                1
            }
            Some(row) if row.expires_at <= now || row.owner == owner => {
                row.owner = owner.to_owned();
                row.token += 1;
                row.expires_at = expiry(now, ttl);
                row.token
            }
            Some(row) => {
                return Err(StoreError::LeaseHeld {
                    owner: row.owner.clone(),
                });
            }
        };
        Ok(Lease {
            session_id: session_id.to_owned(),
            owner: owner.to_owned(),
            token,
            ttl,
        })
    }

    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError> {
        assert_eq!(
            event.session_id, lease.session_id,
            "lease is for another session"
        );
        let now = Instant::now();
        let recorded_at = self.now();
        let mut state = self.state();
        // APPEND's fence: the token must still be current. Expired or not does not matter.
        match state.leases.get(&event.session_id) {
            Some(row) if row.token == lease.token => {}
            _ => return Err(StoreError::Fenced),
        }
        let session = state.sessions.get(&event.session_id);
        let seq = session.map_or(0, |s| s.events.len() as i64) + 1;
        // The whole statement fails together: nothing is bumped or renewed on a rejection.
        check(&event, session, seq)?;

        let row = state
            .leases
            .get_mut(&event.session_id)
            .expect("checked above");
        row.expires_at = expiry(now, lease.ttl);
        let appended = Event {
            session_id: event.session_id,
            seq,
            kind: event.kind,
            request_seq: event.request_seq,
            tool: event.tool,
            outcome: event.outcome,
            args_hash: event.args_hash,
            fence_token: Some(lease.token),
            payload: event.payload,
            recorded_at,
        };
        let session = state
            .sessions
            .entry(appended.session_id.clone())
            .or_insert_with(|| Session {
                events: Vec::new(),
                created_at: recorded_at,
            });
        session.events.push(appended.clone());
        Ok(appended)
    }

    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        let now = Instant::now();
        let mut state = self.state();
        match state.leases.get_mut(&lease.session_id) {
            Some(row) if row.token == lease.token => {
                row.expires_at = expiry(now, lease.ttl);
                Ok(())
            }
            _ => Err(StoreError::Fenced),
        }
    }

    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError> {
        let state = self.state();
        Ok(state
            .sessions
            .get(session_id)
            .map(|s| s.events.clone())
            .unwrap_or_default())
    }

    async fn call_lookup(&self, session_id: &str, key: &CallKey) -> Result<Vec<Event>, StoreError> {
        let state = self.state();
        let events = state.sessions.get(session_id).map(|s| s.events.as_slice());
        Ok(lookup_in(events.unwrap_or_default(), key))
    }

    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError> {
        let state = self.state();
        let mut sessions: Vec<SessionSummary> = state
            .sessions
            .iter()
            .map(|(id, s)| SessionSummary {
                session_id: id.clone(),
                event_count: s.events.len() as i64,
                created_at: s.created_at,
            })
            .collect();
        // ORDER BY created_at DESC, session_id
        sessions.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| a.session_id.cmp(&b.session_id))
        });
        Ok(sessions)
    }

    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError> {
        let now = self.now();
        let mut state = self.state();
        // CREATE_FORK: a session that already has a row inserts nothing, so this comes
        // before any of the forks table's checks.
        if state.sessions.contains_key(&fork.session_id) {
            return Err(StoreError::SessionExists(fork.session_id.clone()));
        }
        // The checks and foreign keys of the sessions and forks tables.
        if fork.session_id.is_empty() || fork.session_id == fork.source_session {
            return Err(rejected("forks_check"));
        }
        if fork.at_step <= 0 {
            return Err(rejected("forks_at_step_check"));
        }
        if fork.model.as_deref() == Some("") {
            return Err(rejected("forks_model_check"));
        }
        if !state.sessions.contains_key(&fork.source_session) {
            return Err(rejected("forks_source_session_fkey"));
        }
        let session = Session {
            events: Vec::new(),
            created_at: now,
        };
        state.sessions.insert(fork.session_id.clone(), session);
        state.forks.insert(fork.session_id.clone(), fork.clone());
        Ok(())
    }

    async fn fork(&self, session_id: &str) -> Result<Option<Fork>, StoreError> {
        Ok(self.state().forks.get(session_id).cloned())
    }

    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError> {
        let state = self.state();
        let mut flagged: Vec<Event> = state
            .sessions
            .values()
            .flat_map(|s| s.events.iter())
            .filter(|e| e.outcome == Some(Outcome::InDoubt))
            .cloned()
            .collect();
        // ORDER BY recorded_at, session_id, seq
        flagged.sort_by(|a, b| {
            (a.recorded_at, &a.session_id, a.seq).cmp(&(b.recorded_at, &b.session_id, b.seq))
        });
        Ok(flagged)
    }
}
