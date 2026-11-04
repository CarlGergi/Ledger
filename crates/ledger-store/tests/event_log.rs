use std::collections::HashSet;

use ledger_store::{EventKind, EventStore, NewEvent, Outcome, PgEventStore};
use ledger_testkit::TestDb;
use serde_json::json;

fn request(session: &str, tool: &str) -> NewEvent {
    NewEvent::request(session, tool, json!({"arguments": {}}))
}

#[tokio::test(flavor = "multi_thread")]
async fn sequences_start_at_one_and_have_no_gaps() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    for expected in 1..=5 {
        let event = store.append(request("s", "t")).await.unwrap();
        assert_eq!(event.seq, expected);
    }
    let seqs: Vec<i64> = store
        .session_events("s")
        .await
        .unwrap()
        .iter()
        .map(|e| e.seq)
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_have_independent_sequences() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    assert_eq!(store.append(request("a", "t")).await.unwrap().seq, 1);
    assert_eq!(store.append(request("a", "t")).await.unwrap().seq, 2);
    assert_eq!(store.append(request("b", "t")).await.unwrap().seq, 1);
    assert_eq!(store.append(request("a", "t")).await.unwrap().seq, 3);
    assert_eq!(store.session_events("b").await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_appends_stay_gapless() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    let tasks: Vec<_> = (0..64)
        .map(|_| {
            let store = store.clone();
            tokio::spawn(async move { store.append(request("s", "t")).await.unwrap().seq })
        })
        .collect();
    let mut seen = HashSet::new();
    for task in tasks {
        assert!(
            seen.insert(task.await.unwrap()),
            "sequence handed out twice"
        );
    }
    assert_eq!(seen, (1..=64).collect::<HashSet<i64>>());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_append_consumes_no_sequence_number() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    store.append(request("s", "t")).await.unwrap();

    // Points at a request that does not exist, so the foreign key rejects it.
    let orphan = NewEvent::response("s", 99, "t", Outcome::Ok, json!({}));
    assert!(store.append(orphan).await.is_err());

    assert_eq!(store.append(request("s", "t")).await.unwrap().seq, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_session_and_sequence_is_rejected() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    store.append(request("s", "t")).await.unwrap();

    let err = sqlx::query(
        "INSERT INTO events (session_id, seq, kind, tool, payload)
         VALUES ('s', 1, 'call_request', 't', '{}')",
    )
    .execute(&pool)
    .await
    .unwrap_err();
    assert!(err.as_database_error().unwrap().is_unique_violation());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_cannot_have_two_responses() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    let req = store.append(request("s", "t")).await.unwrap();
    let response = NewEvent::response("s", req.seq, "t", Outcome::Ok, json!({}));
    store.append(response.clone()).await.unwrap();
    assert!(store.append(response).await.is_err());
    assert_eq!(store.session_events("s").await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_refuses_malformed_events() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    store.append(request("s", "t")).await.unwrap();

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
        let statement = format!(
            "INSERT INTO events (session_id, seq, kind, request_seq, outcome, tool, payload)
             SELECT 's', v.*, 't', '{{}}'::json
             FROM (VALUES {values}) AS v(seq, kind, request_seq, outcome)"
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&pool)
            .await;
        assert!(result.is_err(), "the schema accepted {what}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_log_cannot_be_rewritten() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool.clone());
    store.append(request("s", "t")).await.unwrap();

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

#[tokio::test(flavor = "multi_thread")]
async fn payloads_round_trip_exactly() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    // NUL is legal JSON that JSONB would refuse.
    let payload = json!({
        "zebra": "a\u{0000}b",
        "apple": "\u{1F980} \"quoted\" \\ newline\n",
        "big": 12345678901234567_i64,
        "nested": {"y": [1, 2.5, null, true], "x": {}},
    });
    let stored = store
        .append(NewEvent::request("s", "t", payload.clone()))
        .await
        .unwrap();
    let read = store.session_events("s").await.unwrap().remove(0);
    assert_eq!(read, stored);
    assert_eq!(read.payload, payload);
}

#[tokio::test(flavor = "multi_thread")]
async fn responses_link_back_to_their_request() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    let req = store.append(request("s", "t")).await.unwrap();
    let res = store
        .append(NewEvent::response(
            "s",
            req.seq,
            "t",
            Outcome::ToolError,
            json!({"result": {"isError": true}}),
        ))
        .await
        .unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[0].kind, EventKind::CallRequest);
    assert_eq!(events[1].kind, EventKind::CallResponse);
    assert_eq!(events[1].request_seq, Some(req.seq));
    assert_eq!(events[1].outcome, Some(Outcome::ToolError));
    assert_eq!(events[1], res);
}

#[tokio::test(flavor = "multi_thread")]
async fn sessions_are_listed_with_their_event_counts() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    store.append(request("a", "t")).await.unwrap();
    store.append(request("a", "t")).await.unwrap();
    store.append(request("b", "t")).await.unwrap();

    let mut counts: Vec<(String, i64)> = store
        .sessions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| (s.session_id, s.event_count))
        .collect();
    counts.sort();
    assert_eq!(counts, vec![("a".to_owned(), 2), ("b".to_owned(), 1)]);
}
