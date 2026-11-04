//! The append-only event log: the record of every call that passes through Ledger.

mod event;
mod postgres;

use async_trait::async_trait;

pub use event::{Event, EventKind, NewEvent, Outcome, SessionSummary};
pub use postgres::{MIGRATOR, PgEventStore};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("event store database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("event store holds an unreadable row: {0}")]
    Corrupt(String),
}

/// Durable, ordered storage for session events.
///
/// `append` returning `Ok` means the event is committed, and each session's
/// sequence numbers run 1, 2, 3, ... with no gaps.
#[async_trait]
pub trait EventStore: Send + Sync + 'static {
    async fn append(&self, event: NewEvent) -> Result<Event, StoreError>;

    /// All events of a session, ordered by sequence.
    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError>;

    /// All sessions, most recently created first.
    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError>;
}
