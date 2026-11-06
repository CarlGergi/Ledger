//! The append-only event log: the record of every call that passes through Ledger.

mod event;
mod postgres;

use std::time::Duration;

use async_trait::async_trait;

pub use event::{Event, EventKind, NewEvent, Outcome, SessionSummary, args_hash};
pub use postgres::{MIGRATOR, PgEventStore};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("event store database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("event store holds an unreadable row: {0}")]
    Corrupt(String),
    /// Another owner has taken the session over since this lease was granted.
    #[error("the session has been taken over by another owner")]
    Fenced,
    #[error("the session is owned by {owner} until its lease expires")]
    LeaseHeld { owner: String },
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

    /// All events of a session, ordered by sequence.
    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError>;

    /// All sessions, most recently created first.
    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError>;
}
