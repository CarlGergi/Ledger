//! One suite, run against both stores. The simulator uses the in-memory store, so the
//! suite is what makes its results hold for the PostgreSQL store the gateway really uses.
//! Anything the two do differently fails here.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ledger_store::{
    Event, EventKind, EventStore, Fork, Lease, MemoryStore, NewEvent, Outcome, PgEventStore,
    StoreError, ToolMode, args_hash,
};
use serde_json::json;

const HOUR: Duration = Duration::from_secs(3600);

fn request(session: &str, tool: &str) -> NewEvent {
    NewEvent::request(session, tool, &json!({}), json!({"arguments": {}}))
}

/// Appends as the only owner a test has, taking the session's lease for it.
async fn append(store: &Arc<dyn EventStore>, event: NewEvent) -> Result<Event, StoreError> {
    let lease = store.acquire_lease(&event.session_id, "test", HOUR).await?;
    store.append(event, &lease).await
}

macro_rules! conformance {
    ($($name:ident),* $(,)?) => {
        mod postgres {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    let db = ledger_testkit::TestDb::new().await;
                    super::$name(std::sync::Arc::new(super::PgEventStore::new(db.pool()))).await;
                }
            )*
        }

        mod memory {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $name() {
                    super::$name(std::sync::Arc::new(super::MemoryStore::new())).await;
                }
            )*
        }
    };
}

conformance!(
    sequences_start_at_one_and_have_no_gaps,
    sessions_have_independent_sequences,
    concurrent_appends_stay_gapless,
    a_rejected_append_consumes_no_sequence_number,
    a_call_cannot_have_two_responses,
    payloads_round_trip_exactly,
    responses_link_back_to_their_request,
    sessions_are_listed_with_their_event_counts,
    requests_record_their_arguments_hash_and_fence_token,
    an_append_with_a_stale_token_is_rejected,
    a_lease_that_has_not_expired_cannot_be_taken,
    an_owner_can_take_its_own_session_again_with_a_new_token,
    appending_extends_the_lease,
    no_append_with_the_old_token_lands_after_a_takeover,
    a_retry_links_to_its_call_and_a_call_gets_one_final_answer,
    the_schema_refuses_malformed_retries,
    renewing_keeps_a_lease_from_expiring_until_it_is_taken_over,
    calls_answered_as_in_doubt_are_listed_for_a_person,
    malformed_events_are_refused_and_change_nothing,
    a_refused_append_does_not_extend_the_lease,
    model_calls_share_the_session_sequence_and_get_one_final_answer,
    the_schema_refuses_malformed_model_calls,
    a_fork_claims_a_new_session_and_names_its_source,
    a_fork_cannot_take_a_started_session_or_name_a_missing_source,
);

async fn sequences_start_at_one_and_have_no_gaps(store: Arc<dyn EventStore>) {
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

async fn sessions_have_independent_sequences(store: Arc<dyn EventStore>) {
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 1);
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 2);
    assert_eq!(append(&store, request("b", "t")).await.unwrap().seq, 1);
    assert_eq!(append(&store, request("a", "t")).await.unwrap().seq, 3);
    assert_eq!(store.session_events("b").await.unwrap().len(), 1);
}

async fn concurrent_appends_stay_gapless(store: Arc<dyn EventStore>) {
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

async fn a_rejected_append_consumes_no_sequence_number(store: Arc<dyn EventStore>) {
    append(&store, request("s", "t")).await.unwrap();

    // Points at a request that does not exist, so the foreign key rejects it.
    let orphan = NewEvent::response("s", 99, "t", Outcome::Ok, json!({}));
    assert!(append(&store, orphan).await.is_err());

    assert_eq!(append(&store, request("s", "t")).await.unwrap().seq, 2);
}

async fn a_call_cannot_have_two_responses(store: Arc<dyn EventStore>) {
    let req = append(&store, request("s", "t")).await.unwrap();
    let response = NewEvent::response("s", req.seq, "t", Outcome::Ok, json!({}));
    append(&store, response.clone()).await.unwrap();
    assert!(append(&store, response).await.is_err());
    assert_eq!(store.session_events("s").await.unwrap().len(), 2);
}

async fn payloads_round_trip_exactly(store: Arc<dyn EventStore>) {
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

async fn responses_link_back_to_their_request(store: Arc<dyn EventStore>) {
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

async fn sessions_are_listed_with_their_event_counts(store: Arc<dyn EventStore>) {
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

async fn requests_record_their_arguments_hash_and_fence_token(store: Arc<dyn EventStore>) {
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

async fn an_append_with_a_stale_token_is_rejected(store: Arc<dyn EventStore>) {
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

async fn a_lease_that_has_not_expired_cannot_be_taken(store: Arc<dyn EventStore>) {
    store.acquire_lease("s", "a", HOUR).await.unwrap();

    let taken = store.acquire_lease("s", "b", HOUR).await;
    match taken {
        Err(StoreError::LeaseHeld { owner }) => assert_eq!(owner, "a"),
        other => panic!("expected the lease to be held, got {other:?}"),
    }
    // Other sessions are unaffected.
    store.acquire_lease("t", "b", HOUR).await.unwrap();
}

async fn an_owner_can_take_its_own_session_again_with_a_new_token(store: Arc<dyn EventStore>) {
    let first = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let second = store.acquire_lease("s", "a", HOUR).await.unwrap();
    assert!(second.token > first.token);
    let stale = store.append(request("s", "t"), &first).await;
    assert!(matches!(stale, Err(StoreError::Fenced)), "{stale:?}");
}

async fn appending_extends_the_lease(store: Arc<dyn EventStore>) {
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

async fn no_append_with_the_old_token_lands_after_a_takeover(store: Arc<dyn EventStore>) {
    let old = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();

    let appends: Vec<_> = (0..64)
        .map(|_| {
            let (store, old) = (store.clone(), old.clone());
            tokio::spawn(async move { store.append(request("s", "t"), &old).await })
        })
        .collect();
    // Each old append moves expiry to its own transaction's "now", which can be later
    // than the takeover's, so the takeover may have to wait for the old appends to stop.
    // Being refused until then is correct; the test is about what lands afterwards.
    let deadline = tokio::time::Instant::now() + ledger_testkit::PATIENCE;
    let new = loop {
        match store.acquire_lease("s", "b", HOUR).await {
            Ok(new) => break new,
            Err(StoreError::LeaseHeld { .. }) if tokio::time::Instant::now() < deadline => {
                tokio::task::yield_now().await;
            }
            Err(other) => panic!("could not take the session over: {other}"),
        }
    };
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

async fn a_retry_links_to_its_call_and_a_call_gets_one_final_answer(store: Arc<dyn EventStore>) {
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let req = store.append(request("s", "t"), &lease).await.unwrap();
    let respond = |outcome| NewEvent::response("s", req.seq, "t", outcome, json!({}));

    // Answers that say nothing came back can be followed by a real one.
    store
        .append(respond(Outcome::Timeout), &lease)
        .await
        .unwrap();
    store
        .append(NewEvent::retry("s", req.seq, "t", json!({})), &lease)
        .await
        .unwrap();
    store
        .append(respond(Outcome::TransportError), &lease)
        .await
        .unwrap();
    store.append(respond(Outcome::Ok), &lease).await.unwrap();
    // But a final answer is final, including a flag for a person.
    assert!(
        store
            .append(respond(Outcome::InDoubt), &lease)
            .await
            .is_err()
    );
    assert!(store.append(respond(Outcome::Ok), &lease).await.is_err());

    let kinds: Vec<_> = store
        .session_events("s")
        .await
        .unwrap()
        .iter()
        .map(|e| (e.kind, e.request_seq, e.outcome))
        .collect();
    assert_eq!(
        kinds,
        [
            (EventKind::CallRequest, None, None),
            (EventKind::CallResponse, Some(1), Some(Outcome::Timeout)),
            (EventKind::CallRetry, Some(1), None),
            (
                EventKind::CallResponse,
                Some(1),
                Some(Outcome::TransportError)
            ),
            (EventKind::CallResponse, Some(1), Some(Outcome::Ok)),
        ]
    );
}

async fn the_schema_refuses_malformed_retries(store: Arc<dyn EventStore>) {
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    store.append(request("s", "t"), &lease).await.unwrap();

    let linked_to_nothing = NewEvent::retry("s", 99, "t", json!({}));
    assert!(store.append(linked_to_nothing, &lease).await.is_err());
    let mut with_outcome = NewEvent::retry("s", 1, "t", json!({}));
    with_outcome.outcome = Some(Outcome::Ok);
    assert!(store.append(with_outcome, &lease).await.is_err());
    store
        .append(NewEvent::retry("s", 1, "t", json!({})), &lease)
        .await
        .unwrap();
}

async fn renewing_keeps_a_lease_from_expiring_until_it_is_taken_over(store: Arc<dyn EventStore>) {
    let expired = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();
    let renewing = Lease {
        ttl: HOUR,
        ..expired.clone()
    };
    store.renew_lease(&renewing).await.unwrap();
    let taken = store.acquire_lease("s", "b", HOUR).await;
    assert!(
        matches!(taken, Err(StoreError::LeaseHeld { .. })),
        "{taken:?}"
    );

    // Once taken over, renewing is fenced like appending.
    store.renew_lease(&expired).await.unwrap();
    let new = store.acquire_lease("s", "b", HOUR).await.unwrap();
    assert!(matches!(
        store.renew_lease(&expired).await,
        Err(StoreError::Fenced)
    ));
    store.renew_lease(&new).await.unwrap();
}

async fn calls_answered_as_in_doubt_are_listed_for_a_person(store: Arc<dyn EventStore>) {
    for session in ["a", "b"] {
        let lease = store.acquire_lease(session, "o", HOUR).await.unwrap();
        for _ in 0..2 {
            store.append(request(session, "t"), &lease).await.unwrap();
        }
        let flagged = NewEvent::response(session, 2, "t", Outcome::InDoubt, json!({}));
        store.append(flagged, &lease).await.unwrap();
        let ok = NewEvent::response(session, 1, "t", Outcome::Ok, json!({}));
        store.append(ok, &lease).await.unwrap();
    }
    let listed: Vec<_> = store
        .in_doubt()
        .await
        .unwrap()
        .iter()
        .map(|e| (e.session_id.clone(), e.request_seq))
        .collect();
    assert_eq!(
        listed,
        [("a".to_owned(), Some(2)), ("b".to_owned(), Some(2))]
    );
}

async fn malformed_events_are_refused_and_change_nothing(store: Arc<dyn EventStore>) {
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    store.append(request("s", "t"), &lease).await.unwrap();

    let response = |seq, outcome| NewEvent::response("s", seq, "t", outcome, json!({}));
    let mut malformed = vec![
        ("a response to nothing", response(99, Outcome::Ok)),
        ("a response to itself or later", response(2, Outcome::Ok)),
        (
            "a retry of nothing",
            NewEvent::retry("s", 99, "t", json!({})),
        ),
    ];
    let mut no_outcome = response(1, Outcome::Ok);
    no_outcome.outcome = None;
    malformed.push(("a response with no outcome", no_outcome));
    let mut request_with_outcome = request("s", "t");
    request_with_outcome.outcome = Some(Outcome::Ok);
    malformed.push(("a request with an outcome", request_with_outcome));
    let mut linked_request = request("s", "t");
    linked_request.request_seq = Some(1);
    malformed.push(("a request linked to another", linked_request));
    let mut no_hash = request("s", "t");
    no_hash.args_hash = None;
    malformed.push(("a request with no arguments hash", no_hash));
    let mut bad_hash = request("s", "t");
    bad_hash.args_hash = Some("abc".to_owned());
    malformed.push(("an arguments hash that is not one", bad_hash));
    let mut hashed_response = response(1, Outcome::Ok);
    hashed_response.args_hash = Some(args_hash(&json!({})));
    malformed.push(("a response with an arguments hash", hashed_response));

    for (what, event) in malformed {
        assert!(
            store.append(event, &lease).await.is_err(),
            "accepted {what}"
        );
    }
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        store.append(request("s", "t"), &lease).await.unwrap().seq,
        2
    );
}

async fn a_refused_append_does_not_extend_the_lease(store: Arc<dyn EventStore>) {
    let expired = store.acquire_lease("s", "a", Duration::ZERO).await.unwrap();
    let renewing = Lease {
        ttl: HOUR,
        ..expired
    };
    let orphan = NewEvent::response("s", 99, "t", Outcome::Ok, json!({}));
    assert!(store.append(orphan, &renewing).await.is_err());
    // Had the refused append extended the lease, this takeover would fail.
    store.acquire_lease("s", "b", HOUR).await.unwrap();
}

async fn model_calls_share_the_session_sequence_and_get_one_final_answer(
    store: Arc<dyn EventStore>,
) {
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let body = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]});
    let tool = store.append(request("s", "t"), &lease).await.unwrap();
    let asked = store
        .append(
            NewEvent::model_request("s", "m", &body, json!({"request": body})),
            &lease,
        )
        .await
        .unwrap();
    assert_eq!((tool.seq, asked.seq), (1, 2));
    assert_eq!(asked.kind, EventKind::ModelRequest);
    assert_eq!(asked.args_hash.as_deref(), Some(args_hash(&body).as_str()));

    let answer = |outcome| NewEvent::model_response("s", asked.seq, "m", outcome, json!({}));
    store
        .append(answer(Outcome::Timeout), &lease)
        .await
        .unwrap();
    store.append(answer(Outcome::Ok), &lease).await.unwrap();
    assert!(store.append(answer(Outcome::Ok), &lease).await.is_err());
    // The tool call and the model call are answered separately.
    store
        .append(
            NewEvent::response("s", tool.seq, "t", Outcome::Ok, json!({})),
            &lease,
        )
        .await
        .unwrap();

    let kinds: Vec<EventKind> = store
        .session_events("s")
        .await
        .unwrap()
        .iter()
        .map(|e| e.kind)
        .collect();
    use EventKind::*;
    assert_eq!(
        kinds,
        [
            CallRequest,
            ModelRequest,
            ModelResponse,
            ModelResponse,
            CallResponse
        ]
    );
}

async fn the_schema_refuses_malformed_model_calls(store: Arc<dyn EventStore>) {
    let lease = store.acquire_lease("s", "a", HOUR).await.unwrap();
    let body = json!({"model": "m"});
    let asked = store
        .append(NewEvent::model_request("s", "m", &body, json!({})), &lease)
        .await
        .unwrap();

    let mut unhashed = NewEvent::model_request("s", "m", &body, json!({}));
    unhashed.args_hash = None;
    let mut unanswered = NewEvent::model_response("s", asked.seq, "m", Outcome::Ok, json!({}));
    unanswered.outcome = None;
    let mut linked = NewEvent::model_request("s", "m", &body, json!({}));
    linked.request_seq = Some(asked.seq);
    let mut hashed = NewEvent::model_response("s", asked.seq, "m", Outcome::Ok, json!({}));
    hashed.args_hash = Some(args_hash(&body));
    let dangling = NewEvent::model_response("s", 99, "m", Outcome::Ok, json!({}));
    for bad in [unhashed, unanswered, linked, hashed, dangling] {
        assert!(store.append(bad.clone(), &lease).await.is_err(), "{bad:?}");
    }
    assert_eq!(store.session_events("s").await.unwrap().len(), 1);
}

fn fork_of(source: &str, session: &str, at_step: i64) -> Fork {
    Fork {
        session_id: session.to_owned(),
        source_session: source.to_owned(),
        at_step,
        model: Some("m2".to_owned()),
        system_prompt: Some("Be brief.".to_owned()),
        tools: ToolMode::Safe,
    }
}

async fn a_fork_claims_a_new_session_and_names_its_source(store: Arc<dyn EventStore>) {
    append(&store, request("src", "t")).await.unwrap();
    assert_eq!(store.fork("src").await.unwrap(), None);

    let fork = fork_of("src", "f", 2);
    store.create_fork(&fork).await.unwrap();

    assert_eq!(store.fork("f").await.unwrap(), Some(fork));
    // The session exists, with nothing recorded yet, and records from 1.
    let listed = store.sessions().await.unwrap();
    let f = listed.iter().find(|s| s.session_id == "f").unwrap();
    assert_eq!(f.event_count, 0);
    assert_eq!(append(&store, request("f", "t")).await.unwrap().seq, 1);
    // Other modes and no overrides round-trip too.
    let plain = Fork {
        model: None,
        system_prompt: None,
        tools: ToolMode::Replay,
        ..fork_of("src", "g", 1)
    };
    store.create_fork(&plain).await.unwrap();
    assert_eq!(store.fork("g").await.unwrap(), Some(plain));
}

async fn a_fork_cannot_take_a_started_session_or_name_a_missing_source(store: Arc<dyn EventStore>) {
    append(&store, request("src", "t")).await.unwrap();
    append(&store, request("busy", "t")).await.unwrap();

    let started = store.create_fork(&fork_of("src", "busy", 1)).await;
    assert!(
        matches!(started, Err(StoreError::SessionExists(_))),
        "{started:?}"
    );
    store.create_fork(&fork_of("src", "f", 1)).await.unwrap();
    let twice = store.create_fork(&fork_of("src", "f", 1)).await;
    assert!(
        matches!(twice, Err(StoreError::SessionExists(_))),
        "{twice:?}"
    );

    let empty_model = Fork {
        model: Some(String::new()),
        ..fork_of("src", "x3", 1)
    };
    for bad in [
        fork_of("missing", "x1", 1),
        fork_of("src", "x2", 0),
        empty_model,
        fork_of("src", "src", 1),
    ] {
        assert!(store.create_fork(&bad).await.is_err(), "{bad:?}");
    }
    // Nothing refused claimed a session.
    let mut names: Vec<String> = store
        .sessions()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.session_id)
        .collect();
    names.sort();
    assert_eq!(names, ["busy", "f", "src"]);
    assert_eq!(store.fork("busy").await.unwrap(), None);
    assert_eq!(store.session_events("busy").await.unwrap().len(), 1);
}
