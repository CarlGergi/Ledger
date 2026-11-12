//! What only the PostgreSQL store can show: the schema refusing bad rows written by
//! anything, and the log refusing to be rewritten. The interface itself is checked
//! against both stores in `conformance.rs`.

use std::time::Duration;

use ledger_store::{Event, EventStore, NewEvent, PgEventStore, StoreError, args_hash};
use ledger_testkit::TestDb;
use serde_json::{Value, json};

const HOUR: Duration = Duration::from_secs(3600);

fn request(session: &str, tool: &str) -> NewEvent {
    NewEvent::request(session, tool, &json!({}), json!({"arguments": {}}))
}

/// Appends as the only owner a test has, taking the session's lease for it.
async fn append(store: &PgEventStore, event: NewEvent) -> Result<Event, StoreError> {
    let lease = store.acquire_lease(&event.session_id, "test", HOUR).await?;
    store.append(event, &lease).await
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_session_and_sequence_is_rejected() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    append(&store, request("s", "t")).await.unwrap();

    // Valid in every other way, so only the duplicate can be what is refused.
    let statement = format!(
        "INSERT INTO events (session_id, seq, kind, tool, payload, fence_token, args_hash)
         VALUES ('s', 1, 'call_request', 't', '{{}}', 1, '{}')",
        args_hash(&json!({}))
    );
    let err = sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(&pool)
        .await
        .unwrap_err();
    assert!(err.as_database_error().unwrap().is_unique_violation());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_refuses_malformed_events() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    append(&store, request("s", "t")).await.unwrap();
    let hash = args_hash(&json!({}));

    for (what, values) in [
        (
            "a response linked to nothing",
            "(2, 'call_response', NULL, 'ok')",
        ),
        (
            "a response with no outcome",
            "(2, 'call_response', 1, NULL)",
        ),
        (
            "a response to a later event",
            "(2, 'call_response', 3, 'ok')",
        ),
        (
            "a request with an outcome",
            "(2, 'call_request', NULL, 'ok')",
        ),
        (
            "a request linked to another",
            "(2, 'call_request', 1, NULL)",
        ),
        ("an unknown kind", "(2, 'other', NULL, NULL)"),
        ("an unknown outcome", "(2, 'call_response', 1, 'other')"),
        ("a sequence below one", "(0, 'call_request', NULL, NULL)"),
    ] {
        // Each row is valid apart from the one thing being tested.
        let statement = format!(
            "INSERT INTO events
                (session_id, seq, kind, request_seq, outcome, tool, payload, fence_token, args_hash)
             SELECT 's', v.*, 't', '{{}}'::json, 1,
                    CASE WHEN v.kind = 'call_request' THEN '{hash}' END
             FROM (VALUES {values}) AS v(seq, kind, request_seq, outcome)"
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&pool)
            .await;
        assert!(result.is_err(), "the schema accepted {what}");
    }

    for (what, fence_token, args_hash) in [
        ("an event with no fence token", "NULL", format!("'{hash}'")),
        ("a request with no arguments hash", "1", "NULL".to_owned()),
        ("an arguments hash that is not one", "1", "'abc'".to_owned()),
    ] {
        let statement = format!(
            "INSERT INTO events
                (session_id, seq, kind, tool, payload, fence_token, args_hash)
             VALUES ('s', 2, 'call_request', 't', '{{}}'::json, {fence_token}, {args_hash})"
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&pool)
            .await;
        assert!(result.is_err(), "the schema accepted {what}");
    }

    // The control: the same row with everything present is accepted.
    let statement = format!(
        "INSERT INTO events (session_id, seq, kind, tool, payload, fence_token, args_hash)
         VALUES ('s', 2, 'call_request', 't', '{{}}'::json, 1, '{hash}')"
    );
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(&pool)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_log_cannot_be_rewritten() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    append(&store, request("s", "t")).await.unwrap();

    for statement in [
        "UPDATE events SET tool = 'other'",
        "DELETE FROM events",
        "TRUNCATE events CASCADE",
    ] {
        let err = sqlx::query(statement).execute(&pool).await.unwrap_err();
        assert!(
            err.to_string().contains("append-only"),
            "{statement} should be refused, got: {err}"
        );
    }
    assert_eq!(store.session_events("s").await.unwrap().len(), 1);
}

#[test]
fn the_arguments_hash_ignores_key_order_and_nothing_else() {
    let hash = |v: Value| args_hash(&v);
    assert_eq!(
        hash(json!({"a": 1, "b": [1, 2]})),
        hash(json!({"b": [1, 2], "a": 1}))
    );
    assert_eq!(
        hash(json!({"x": {"q": 1, "p": 2}})),
        hash(json!({"x": {"p": 2, "q": 1}}))
    );
    assert_ne!(hash(json!({"a": 1})), hash(json!({"a": 2})));
    assert_ne!(hash(json!({"a": [1, 2]})), hash(json!({"a": [2, 1]})));
    assert_ne!(hash(json!({"a": 1})), hash(json!({"a": "1"})));
    assert_eq!(hash(json!({})).len(), 64);
}

/// Requests logged before migration 0006 had no key columns: its backfill fills them, so
/// a retry of an old call is still recognised, except where the payload holds \u0000,
/// which PostgreSQL cannot read and the backfill leaves alone.
#[tokio::test(flavor = "multi_thread")]
async fn the_lookup_migration_fills_in_requests_logged_before_it() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let hash = args_hash(&json!({}));
    sqlx::query("INSERT INTO sessions (session_id, last_seq) VALUES ('old', 3)")
        .execute(&pool)
        .await
        .unwrap();
    // As written before 0006: no key columns.
    for (seq, payload) in [
        (1, r#"{"step":"1","rpc_id":5,"transport":"t1"}"#),
        (
            2,
            r#"{"step":"2","rpc_id":"5","transport":"t1","note":"a\u0000b"}"#,
        ),
    ] {
        let statement = format!(
            "INSERT INTO events (session_id, seq, kind, tool, payload, fence_token, args_hash)
             VALUES ('old', {seq}, 'call_request', 't', '{payload}', 1, '{hash}')"
        );
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&pool)
            .await
            .unwrap();
    }
    sqlx::query(
        "INSERT INTO events (session_id, seq, kind, request_seq, tool, outcome, payload, fence_token)
         VALUES ('old', 3, 'call_response', 1, 't', 'ok', '{}', 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let store = PgEventStore::new(pool.clone());
    let key = |step: &str| ledger_store::CallKey {
        step: Some(step.to_owned()),
        rpc_id: json!(5),
        transport: Some("t1".to_owned()),
    };
    assert!(
        store
            .call_lookup("old", &key("1"))
            .await
            .unwrap()
            .is_empty()
    );

    // The migration's backfill, exactly as written in the migration.
    let migration = include_str!("../migrations/0006_call_lookup.sql");
    let start = migration
        .find("ALTER TABLE events DISABLE TRIGGER")
        .unwrap();
    let end = migration.find("CREATE INDEX").unwrap();
    for statement in migration[start..end]
        .split(';')
        .filter(|s| !s.trim().is_empty())
    {
        sqlx::query(sqlx::AssertSqlSafe(statement.to_owned()))
            .execute(&pool)
            .await
            .unwrap();
    }

    let found: Vec<i64> = store
        .call_lookup("old", &key("1"))
        .await
        .unwrap()
        .iter()
        .map(|e| e.seq)
        .collect();
    assert_eq!(found, [1, 3]);
    let by_id = ledger_store::CallKey {
        step: None,
        ..key("1")
    };
    assert_eq!(store.call_lookup("old", &by_id).await.unwrap()[0].seq, 1);
    // The row holding \u0000 keeps no keys, and is still readable in full.
    assert!(
        store
            .call_lookup("old", &key("2"))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(store.session_events("old").await.unwrap().len(), 3);
    // And the log refuses updates again.
    let update = sqlx::query("UPDATE events SET tool = 'x' WHERE session_id = 'old'")
        .execute(&pool)
        .await;
    assert!(update.is_err());
}
