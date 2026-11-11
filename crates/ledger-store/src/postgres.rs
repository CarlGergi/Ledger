use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{Event, EventKind, EventStore, Lease, NewEvent, Outcome, SessionSummary, StoreError};

pub static MIGRATOR: Migrator = sqlx::migrate!();

// A single statement, so one round trip and one transaction: the fencing check, the lease
// renewal, the sequence bump and the event insert land together or not at all. Nothing
// is bumped unless the token is still current, so a fenced append consumes no sequence
// number. The row lock taken by renewing the lease also makes a takeover wait for appends
// already under way. The payload travels as text and is cast straight to JSON so that it
// never passes through JSONB (see the first migration).
const APPEND: &str = "
    WITH fence AS (
        UPDATE leases SET expires_at = now() + make_interval(secs => $9)
        WHERE session_id = $1 AND token = $8
        RETURNING token
    ), bumped AS (
        INSERT INTO sessions (session_id, last_seq) SELECT $1, 1 FROM fence
        ON CONFLICT (session_id) DO UPDATE SET last_seq = sessions.last_seq + 1
        RETURNING last_seq
    )
    INSERT INTO events
        (session_id, seq, kind, request_seq, tool, outcome, payload, args_hash, fence_token)
    SELECT $1, last_seq, $2, $3, $4, $5, $6::text::json, $7, $8 FROM bumped
    RETURNING seq, recorded_at";

// Takes the session if it is free, expired, or already this owner's, and bumps the token.
const ACQUIRE: &str = "
    INSERT INTO leases (session_id, owner, token, expires_at)
    VALUES ($1, $2, 1, now() + make_interval(secs => $3))
    ON CONFLICT (session_id) DO UPDATE
        SET owner = excluded.owner, token = leases.token + 1, expires_at = excluded.expires_at
        WHERE leases.expires_at <= now() OR leases.owner = excluded.owner
    RETURNING token";

#[derive(Debug, Clone)]
pub struct PgEventStore {
    pool: PgPool,
}

impl PgEventStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// `acquire_timeout` is how long an append may wait for a connection before failing.
    pub async fn connect(
        database_url: &str,
        acquire_timeout: Duration,
    ) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .acquire_timeout(acquire_timeout)
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
    args_hash: Option<String>,
    fence_token: Option<i64>,
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
            args_hash: row.args_hash,
            fence_token: row.fence_token,
            payload,
            recorded_at: row.recorded_at,
        })
    }
}

#[async_trait]
impl EventStore for PgEventStore {
    async fn acquire_lease(
        &self,
        session_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        let token: Option<i64> = sqlx::query_scalar(ACQUIRE)
            .bind(session_id)
            .bind(owner)
            .bind(ttl.as_secs_f64())
            .fetch_optional(&self.pool)
            .await?;
        match token {
            Some(token) => Ok(Lease {
                session_id: session_id.to_owned(),
                owner: owner.to_owned(),
                token,
                ttl,
            }),
            None => {
                let holder: Option<String> =
                    sqlx::query_scalar("SELECT owner FROM leases WHERE session_id = $1")
                        .bind(session_id)
                        .fetch_optional(&self.pool)
                        .await?;
                Err(StoreError::LeaseHeld {
                    owner: holder.unwrap_or_else(|| "another owner".to_owned()),
                })
            }
        }
    }

    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError> {
        assert_eq!(
            event.session_id, lease.session_id,
            "lease is for another session"
        );
        let appended: Option<(i64, DateTime<Utc>)> = sqlx::query_as(APPEND)
            .bind(&event.session_id)
            .bind(event.kind.as_str())
            .bind(event.request_seq)
            .bind(&event.tool)
            .bind(event.outcome.map(Outcome::as_str))
            .bind(event.payload.to_string())
            .bind(&event.args_hash)
            .bind(lease.token)
            .bind(lease.ttl.as_secs_f64())
            .fetch_optional(&self.pool)
            .await?;
        let (seq, recorded_at) = appended.ok_or(StoreError::Fenced)?;
        Ok(Event {
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
        })
    }

    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        let renewed = sqlx::query(
            "UPDATE leases SET expires_at = now() + make_interval(secs => $3)
             WHERE session_id = $1 AND token = $2",
        )
        .bind(&lease.session_id)
        .bind(lease.token)
        .bind(lease.ttl.as_secs_f64())
        .execute(&self.pool)
        .await?;
        if renewed.rows_affected() == 0 {
            return Err(StoreError::Fenced);
        }
        Ok(())
    }

    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError> {
        let rows: Vec<EventRow> = sqlx::query_as(
            "SELECT session_id, seq, kind, request_seq, tool, outcome, args_hash, fence_token,
                    payload::text AS payload, recorded_at
             FROM events WHERE outcome = 'in_doubt' ORDER BY recorded_at, session_id, seq",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(Event::try_from).collect()
    }

    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError> {
        let rows: Vec<EventRow> = sqlx::query_as(
            "SELECT session_id, seq, kind, request_seq, tool, outcome, args_hash, fence_token,
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
