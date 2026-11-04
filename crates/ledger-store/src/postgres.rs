use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{Event, EventKind, EventStore, NewEvent, Outcome, SessionSummary, StoreError};

pub static MIGRATOR: Migrator = sqlx::migrate!();

// A single statement, so one round trip and one transaction: the sequence bump and
// the event insert land together or not at all. The payload travels as text and is
// cast straight to JSON so that it never passes through JSONB (see the migration).
const APPEND: &str = "
    WITH bumped AS (
        INSERT INTO sessions (session_id, last_seq) VALUES ($1, 1)
        ON CONFLICT (session_id) DO UPDATE SET last_seq = sessions.last_seq + 1
        RETURNING last_seq
    )
    INSERT INTO events (session_id, seq, kind, request_seq, tool, outcome, payload)
    SELECT $1, last_seq, $2, $3, $4, $5, $6::text::json FROM bumped
    RETURNING seq, recorded_at";

#[derive(Debug, Clone)]
pub struct PgEventStore {
    pool: PgPool,
}

impl PgEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await?;
        Ok(Self::new(pool))
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR.run(&self.pool).await.map_err(sqlx::Error::from)?;
        Ok(())
    }
}

#[derive(sqlx::FromRow)]
struct EventRow {
    session_id: String,
    seq: i64,
    kind: String,
    request_seq: Option<i64>,
    tool: String,
    outcome: Option<String>,
    payload: String,
    recorded_at: DateTime<Utc>,
}

impl TryFrom<EventRow> for Event {
    type Error = StoreError;

    fn try_from(row: EventRow) -> Result<Self, StoreError> {
        let payload: Value = serde_json::from_str(&row.payload)
            .map_err(|e| StoreError::Corrupt(format!("payload is not JSON: {e}")))?;
        Ok(Event {
            session_id: row.session_id,
            seq: row.seq,
            kind: EventKind::parse(&row.kind)?,
            request_seq: row.request_seq,
            tool: row.tool,
            outcome: row.outcome.as_deref().map(Outcome::parse).transpose()?,
            payload,
            recorded_at: row.recorded_at,
        })
    }
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn append(&self, event: NewEvent) -> Result<Event, StoreError> {
        let (seq, recorded_at): (i64, DateTime<Utc>) = sqlx::query_as(APPEND)
            .bind(&event.session_id)
            .bind(event.kind.as_str())
            .bind(event.request_seq)
            .bind(&event.tool)
            .bind(event.outcome.map(Outcome::as_str))
            .bind(event.payload.to_string())
            .fetch_one(&self.pool)
            .await?;
        Ok(Event {
            session_id: event.session_id,
            seq,
            kind: event.kind,
            request_seq: event.request_seq,
            tool: event.tool,
            outcome: event.outcome,
            payload: event.payload,
            recorded_at,
        })
    }

    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError> {
        let rows: Vec<EventRow> = sqlx::query_as(
            "SELECT session_id, seq, kind, request_seq, tool, outcome,
                    payload::text AS payload, recorded_at
             FROM events WHERE session_id = $1 ORDER BY seq",
        )
        .bind(session_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Event::try_from).collect()
    }

    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError> {
        let rows: Vec<(String, i64, DateTime<Utc>)> = sqlx::query_as(
            "SELECT session_id, last_seq, created_at FROM sessions
             ORDER BY created_at DESC, session_id",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|(session_id, event_count, created_at)| SessionSummary {
                session_id,
                event_count,
                created_at,
            })
            .collect())
    }
}
