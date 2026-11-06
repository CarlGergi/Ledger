use std::collections::HashSet;

use std::time::Duration;

use ledger_store::{
    Event, EventKind, EventStore, Lease, NewEvent, Outcome, PgEventStore, StoreError, args_hash,
};
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
async fn sequences_start_at_one_and_have_no_gaps() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    for expected in 1..=5 {
        let event = append(&store, request("s", "t")).await.unwrap();
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
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 1);
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 2);
    assert_eq!(append(&store, request("b", "t")).await.unwrap().seq, 1);
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 3);
    assert_eq!(store.session_events("b").await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_appends_stay_gapless() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    let lease = store.acquire_lease("s", "test", HOUR).await.unwrap();
    let tasks: Vec<_> = (0..64)
        .map(|_| {
            let store = store.clone();
            let lease = lease.clone();
            tokio::spawn(async move { store.append(request("s", "t"), &lease).await.unwrap().seq })
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
    append(&store, request("s", "t")).await.unwrap();

    // Points at a request that does not exist, so the foreign key rejects it.
    let orphan = NewEvent::response("s", 99, "t", Outcome::Ok, json!({}));
    assert!(append(&store, orphan).await.is_err());

    assert_eq!(append(&store, request("s", "t")).await.unwrap().seq, 2);
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
async fn a_call_cannot_have_two_responses() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store = PgEventStore::new(pool);
    let req = append(&store, request("s", "t")).await.unwrap();
    let response = NewEvent::response("s", req.seq, "t", Outcome::Ok, json!({}));
    append(&store, response.clone()).await.unwrap();
    assert!(append(&store, response).await.is_err());
    assert_eq!(store.session_events("s").await.unwrap().len(), 2);
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
    let stored = append(
        &store,
        NewEvent::request("s", "t", &json!({}), payload.clone()),
    )
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
    let req = append(&store, request("s", "t")).await.unwrap();
    let response = NewEvent::response(
        "s",
        req.seq,
        "t",
        Outcome::ToolError,
        json!({"result": {"isError": true}}),
    );
    let res = append(&store, response).await.unwrap();

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
    append(&store, request("a", "t")).await.unwrap();
    append(&store, request("a", "t")).await.unwrap();
    append(&store, request("b", "t")).await.unwrap();

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

#[tokio::test(flavor = "multi_thread")]
async fn requests_record_their_arguments_hash_and_fence_token() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let arguments = json!({"to": "x@example.com"});
    let request = NewEvent::request("s", "t", &arguments, json!({}));
    let req = store.append(request, &lease).await.unwrap();
    let response = NewEvent::response("s", req.seq, "t", Outcome::Ok, json!({}));
    store.append(response, &lease).await.unwrap();

    let events = store.session_events("s").await.unwrap();
    assert_eq!(events[0].args_hash, Some(args_hash(&arguments)));
    assert_eq!(events[1].args_hash, None);
    assert!(events.iter().all(|e| e.fence_token == Some(lease.token)));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_append_with_a_stale_token_is_rejected() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    // A lease that has already expired, so that another owner may take it at once.
    let old = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();
    store.append(request("s", "t"), &old).await.unwrap();

    let new = store.acquire_lease("s", "b", HOUR).await.unwrap();
    assert!(new.token > old.token);

    let stale = store.append(request("s", "t"), &old).await;
    assert!(matches!(stale, Err(StoreError::Fenced)), "{stale:?}");
    // Nothing was appended, and no sequence number was used up.
    let next = store.append(request("s", "t"), &new).await.unwrap();
    assert_eq!(next.seq, 2);
    let tokens: Vec<_> = store
        .session_events("s")
        .await
        .unwrap()
        .iter()
        .map(|e| e.fence_token)
        .collect();
    assert_eq!(tokens, [Some(old.token), Some(new.token)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_lease_that_has_not_expired_cannot_be_taken() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    store.acquire_lease("s", "a", HOUR).await.unwrap();

    let taken = store.acquire_lease("s", "b", HOUR).await;
    match taken {
        Err(StoreError::LeaseHeld { owner }) => assert_eq!(owner, "a"),
        other => panic!("expected the lease to be held, got {other:?}"),
    }
    // Other sessions are unaffected.
    store.acquire_lease("t", "b", HOUR).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_owner_can_take_its_own_session_again_with_a_new_token() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    let first = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let second = store.acquire_lease("s", "a", HOUR).await.unwrap();
    assert!(second.token > first.token);
    let stale = store.append(request("s", "t"), &first).await;
    assert!(matches!(stale, Err(StoreError::Fenced)), "{stale:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn appending_extends_the_lease() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    let expired = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();
    let renewing = Lease {
        ttl: HOUR,
        ..expired
    };
    store.append(request("s", "t"), &renewing).await.unwrap();

    // Expired before the append, so only the append can have kept it.
    let taken = store.acquire_lease("s", "b", HOUR).await;
    assert!(
        matches!(taken, Err(StoreError::LeaseHeld { .. })),
        "{taken:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_append_with_the_old_token_lands_after_a_takeover() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    let old = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();

    let appends: Vec<_> = (0..64)
        .map(|_| {
            let (store, old) = (store.clone(), old.clone());
            tokio::spawn(async move { store.append(request("s", "t"), &old).await })
        })
        .collect();
    let new = store.acquire_lease("s", "b", HOUR).await.unwrap();
    store.append(request("s", "t"), &new).await.unwrap();

    for append in appends {
        match append.await.unwrap() {
            Ok(_) | Err(StoreError::Fenced) => {}
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    let events = store.session_events("s").await.unwrap();
    let tokens: Vec<i64> = events.iter().map(|e| e.fence_token.unwrap()).collect();
    let first_new = tokens.iter().position(|&t| t == new.token).unwrap();
    assert!(
        tokens[first_new..].iter().all(|&t| t == new.token),
        "an old-token append landed after the takeover: {tokens:?}"
    );
    let seqs: Vec<i64> = events.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=events.len() as i64).collect::<Vec<_>>());
}
