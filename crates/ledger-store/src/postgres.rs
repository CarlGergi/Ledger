use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::{
    CallKey, Event, EventKind, EventStore, Fork, Lease, NewEvent, Outcome, SessionSummary,
    StoreError, ToolMode,
};

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
        (session_id, seq, kind, request_seq, tool, outcome, payload, args_hash, fence_token,
         ask_step, ask_transport, ask_rpc_id)
    SELECT $1, last_seq, $2, $3, $4, $5, $6::text::json, $7, $8, $10, $11, $12 FROM bumped
    RETURNING seq, recorded_at";

// Takes the session if it is free, expired, or already this owner's, and bumps the token.
const ACQUIRE: &str = "
    INSERT INTO leases (session_id, owner, token, expires_at)
    VALUES ($1, $2, 1, now() + make_interval(secs => $3))
    ON CONFLICT (session_id) DO UPDATE
        SET owner = excluded.owner, token = leases.token + 1, expires_at = excluded.expires_at
        WHERE leases.expires_at <= now() OR leases.owner = excluded.owner
    RETURNING token";

// Claims the session's row and records the fork in one statement, so that the fork exists
// before anything is recorded in its session, or not at all. A session that already has
// a row inserts nothing, and the statement returns no row.
const CREATE_FORK: &str = "
    WITH claimed AS (
        INSERT INTO sessions (session_id, last_seq) VALUES ($1, 0)
        ON CONFLICT (session_id) DO NOTHING
        RETURNING session_id
    )
    INSERT INTO forks (session_id, source_session, at_step, model, system_prompt, tools)
    SELECT session_id, $2, $3, $4, $5, $6 FROM claimed
    RETURNING session_id";

// The request or retry that identifies a call, by step (the oldest) or by JSON-RPC id in a
// transport session (the newest). Each matches an index in migration 0006 exactly.
pub(crate) const ASK_BY_STEP: &str = "
    SELECT seq, kind, request_seq FROM events
    WHERE session_id = $1 AND kind IN ('call_request', 'call_retry') AND ask_step = $2
    ORDER BY seq LIMIT 1";

pub(crate) const ASK_BY_RPC_ID: &str = "
    SELECT seq, kind, request_seq FROM events
    WHERE session_id = $1 AND kind IN ('call_request', 'call_retry')
      AND ask_transport = $2 AND ask_rpc_id = $3
    ORDER BY seq DESC LIMIT 1";

/// The keys a request or retry is found by, as written to their own columns: the step, the
/// transport session, and the JSON-RPC id as compact JSON. None for other events.
struct AskKeys {
    step: Option<String>,
    transport: Option<String>,
    rpc_id: Option<String>,
}

impl AskKeys {
    fn of(event: &NewEvent) -> Self {
        if !matches!(event.kind, EventKind::CallRequest | EventKind::CallRetry) {
            return Self {
                step: None,
                transport: None,
                rpc_id: None,
            };
        }
        let text = |key: &str| event.payload[key].as_str().map(str::to_owned);
        Self {
            step: text("step"),
            transport: text("transport"),
            rpc_id: event.payload.get("rpc_id").map(Value::to_string),
        }
    }
}

// The ask itself, its call, and the call's answers. Two parts rather than one condition
// with OR, which would make PostgreSQL read the whole session: the first finds events by
// their own number, the second answers by the call they answer.
pub(crate) const CALL_AND_ANSWERS: &str = "
    SELECT session_id, seq, kind, request_seq, tool, outcome, args_hash, fence_token,
           payload::text AS payload, recorded_at
    FROM events
    WHERE session_id = $1 AND seq IN ($2, $3)
    UNION
    SELECT session_id, seq, kind, request_seq, tool, outcome, args_hash, fence_token,
           payload::text AS payload, recorded_at
    FROM events
    WHERE session_id = $1 AND request_seq = $3 AND kind = 'call_response'
    ORDER BY seq";

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
struct ForkRow {
    session_id: String,
    source_session: String,
    at_step: i64,
    model: Option<String>,
    system_prompt: Option<String>,
    tools: String,
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
        let keys = AskKeys::of(&event);
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
            .bind(keys.step)
            .bind(keys.transport)
            .bind(keys.rpc_id)
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

    async fn release_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE leases SET expires_at = now()
             WHERE session_id = $1 AND token = $2 AND expires_at > now()",
        )
        .bind(&lease.session_id)
        .bind(lease.token)
        .execute(&self.pool)
        .await?;
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

    async fn call_lookup(&self, session_id: &str, key: &CallKey) -> Result<Vec<Event>, StoreError> {
        let ask: Option<(i64, String, Option<i64>)> = match (&key.step, &key.transport) {
            (Some(step), _) => {
                sqlx::query_as(ASK_BY_STEP)
                    .bind(session_id)
                    .bind(step)
                    .fetch_optional(&self.pool)
                    .await?
            }
            (None, Some(transport)) => {
                // Compact JSON, as the payload was written, so the texts compare equal.
                let id = serde_json::to_string(&key.rpc_id)
                    .map_err(|e| StoreError::Corrupt(e.to_string()))?;
                sqlx::query_as(ASK_BY_RPC_ID)
                    .bind(session_id)
                    .bind(transport)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await?
            }
            (None, None) => None,
        };
        let Some((seq, kind, request_seq)) = ask else {
            return Ok(Vec::new());
        };
        let call = match EventKind::parse(&kind)? {
            EventKind::CallRequest => seq,
            _ => request_seq
                .ok_or_else(|| StoreError::Corrupt(format!("retry #{seq} names no call")))?,
        };
        let rows: Vec<EventRow> = sqlx::query_as(CALL_AND_ANSWERS)
            .bind(session_id)
            .bind(seq)
            .bind(call)
            .fetch_all(&self.pool)
            .await?;
        rows.into_iter().map(Event::try_from).collect()
    }

    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError> {
        let created: Option<(String,)> = sqlx::query_as(CREATE_FORK)
            .bind(&fork.session_id)
            .bind(&fork.source_session)
            .bind(fork.at_step)
            .bind(&fork.model)
            .bind(&fork.system_prompt)
            .bind(fork.tools.as_str())
            .fetch_optional(&self.pool)
            .await?;
        match created {
            Some(_) => Ok(()),
            None => Err(StoreError::SessionExists(fork.session_id.clone())),
        }
    }

    async fn healthy(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    async fn fork(&self, session_id: &str) -> Result<Option<Fork>, StoreError> {
        let row: Option<ForkRow> = sqlx::query_as(
            "SELECT session_id, source_session, at_step, model, system_prompt, tools
             FROM forks WHERE session_id = $1",
        )
        .bind(session_id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| {
            Ok(Fork {
                session_id: r.session_id,
                source_session: r.source_session,
                at_step: r.at_step,
                model: r.model,
                system_prompt: r.system_prompt,
                tools: ToolMode::parse(&r.tools)?,
            })
        })
        .transpose()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The plan PostgreSQL would use for `query`, with sequential scans ruled out, so that
    /// the answer shows whether an index matches the query at all.
    async fn plan(pool: &PgPool, query: &str, binds: &[&str]) -> String {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("SET enable_seqscan = off")
            .execute(&mut *conn)
            .await
            .unwrap();
        let explain = format!("EXPLAIN {query}");
        let mut q = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(explain));
        for bind in binds {
            q = q.bind(*bind);
        }
        q.fetch_all(&mut *conn).await.unwrap().join("\n")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn each_lookup_query_has_an_index_that_matches_it() {
        let db = ledger_testkit::TestDb::new().await;
        let pool = db.pool();
        // The key has to be in the index condition: an index on the session alone, with the
        // key checked afterwards as a filter, would still read the whole session.
        let in_index = |plan: &str, column: &str| {
            plan.lines()
                .any(|line| line.contains("Index Cond") && line.contains(column))
        };
        let by_step = plan(&pool, ASK_BY_STEP, &["s", "1"]).await;
        assert!(by_step.contains("events_asks_by_step"), "{by_step}");
        assert!(in_index(&by_step, "ask_step"), "{by_step}");
        let by_id = plan(&pool, ASK_BY_RPC_ID, &["s", "t1", "5"]).await;
        assert!(by_id.contains("events_asks_by_rpc_id"), "{by_id}");
        assert!(in_index(&by_id, "ask_rpc_id"), "{by_id}");
        // seq and request_seq are numbers: bind them through a cast.
        let answers_query = CALL_AND_ANSWERS
            .replace("$2", "$2::bigint")
            .replace("$3", "$3::bigint");
        let answers = plan(&pool, &answers_query, &["s", "1", "1"]).await;
        assert!(answers.contains("events_answers"), "{answers}");
        assert!(in_index(&answers, "request_seq"), "{answers}");
        for plan in [by_step, by_id, answers] {
            assert!(!plan.contains("Seq Scan"), "{plan}");
        }
    }
}
