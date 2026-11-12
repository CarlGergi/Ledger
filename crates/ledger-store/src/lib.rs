//! The append-only event log: the record of every call that passes through Ledger.

mod event;
mod fork;
mod lookup;
mod memory;
mod postgres;

use std::time::Duration;

use async_trait::async_trait;

pub use event::{Event, EventKind, NewEvent, Outcome, SessionSummary, args_hash};
pub use fork::{Fork, ToolMode};
pub use lookup::{CallKey, lookup_in};
pub use memory::MemoryStore;
pub use postgres::{MIGRATOR, PgEventStore};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("event store database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("event store holds an unreadable row: {0}")]
    Corrupt(String),
    /// The in-memory store refusing what the database's constraints would refuse.
    #[error("event store rejected the event: {0}")]
    Rejected(String),
    /// Another owner has taken the session over since this lease was granted.
    #[error("the session has been taken over by another owner")]
    Fenced,
    #[error("the session is owned by {owner} until its lease expires")]
    LeaseHeld { owner: String },
    /// A fork was to start a session that already exists.
    #[error("session {0:?} already exists, so it cannot become a fork")]
    SessionExists(String),
}

/// The right to append to one session, until another owner takes it over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub session_id: String,
    pub owner: String,
    /// Higher than any token granted for this session before.
    pub token: i64,
    /// How long each append extends the lease by.
    pub ttl: Duration,
}

/// Durable, ordered storage for session events.
///
/// `append` returning `Ok` means the event is committed, and each session's
/// sequence numbers run 1, 2, 3, ... with no gaps.
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    /// Grants the session to `owner`, unless another owner holds an unexpired lease on
    /// it. Granting it again to the same owner also works, with a new token.
    async fn acquire_lease(
        &self,
        session_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Lease, StoreError>;

    /// Appends under `lease`, and extends it. Fails with `Fenced`, appending nothing, if
    /// the session has been taken over since.
    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError>;

    /// Extends `lease` without appending, for a call still running. `Fenced` if the
    /// session has been taken over.
    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError>;

    /// All events of a session, ordered by sequence.
    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError>;

    /// The few events that decide whether a call repeats an earlier one, exactly as
    /// [`lookup_in`] finds them in the whole log, but without reading it, so a call costs
    /// the same however long its session has run.
    async fn call_lookup(&self, session_id: &str, key: &CallKey) -> Result<Vec<Event>, StoreError>;

    /// All sessions, most recently created first.
    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError>;

    /// Every call answered as in doubt, for a person to check, oldest first.
    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError>;

    /// Starts `fork.session_id` as a fork of `fork.source_session`, which must exist.
    /// Fails with `SessionExists` if the session has already been started, as a fork or
    /// by recording anything, so a session is a fork from its first event or never.
    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError>;

    /// The fork a session is, if it is one.
    async fn fork(&self, session_id: &str) -> Result<Option<Fork>, StoreError>;

    /// Whether the store answers at all, for readiness checks.
    async fn healthy(&self) -> Result<(), StoreError> {
        Ok(())
    }
}
