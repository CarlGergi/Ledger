//! Forks: the agent runs again in a new session that answers its calls from the source's
//! log up to the fork step, then goes live, on another model or prompt if asked.

mod common;

use common::{REPORT, Stack};
use ledger_gateway::error_codes;
use ledger_store::{Event, EventKind, Outcome, ToolMode};
use rmcp::ServiceError;
use sample_agent::{call_params, connect};
use serde_json::json;

fn requests(events: &[Event]) -> Vec<&Event> {
    events.iter().filter(|e| e.kind.is_request()).collect()
}

fn replayed_from(request: &Event) -> Option<i64> {
    request.payload["replayed_from"]["seq"].as_i64()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fork_past_the_end_replays_the_whole_run_and_runs_nothing() {
    let stack = Stack::start().await;
    let fork = stack.fork(11, None, ToolMode::Safe).await;

    let replayed = stack.run(&fork, REPORT).await.unwrap();

    assert_eq!(replayed.answer, "Sent the daily report.");
    assert_eq!(stack.model.count(), 3, "the model was asked again");
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
    let (source, own) = (stack.events("src").await, stack.events(&fork).await);
    let kinds = |events: &[Event]| events.iter().map(|e| e.kind).collect::<Vec<_>>();
    assert_eq!(kinds(&own), kinds(&source));
    let from: Vec<Option<i64>> = requests(&own).into_iter().map(replayed_from).collect();
    assert_eq!(from, [Some(1), Some(3), Some(5), Some(7), Some(9)]);
    // Each answer is the source's, outcome and all.
    for (mine, theirs) in own
        .iter()
        .zip(&source)
        .filter(|(e, _)| e.kind.is_response())
    {
        assert_eq!(mine.outcome, theirs.outcome);
        assert_eq!(mine.payload["result"], theirs.payload["result"]);
        assert_eq!(mine.payload["body"], theirs.payload["body"]);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fork_goes_live_at_its_step_on_the_new_model_and_runs_no_new_effects() {
    let stack = Stack::start().await;
    // Steps 1 to 4 are the first model call and the write; step 5, the second model call,
    // goes to reporter-2, which emails with another subject.
    let fork = stack.fork(5, Some("reporter-2"), ToolMode::Safe).await;

    let conversation = stack.run(&fork, REPORT).await.unwrap();

    let bodies = stack.later_model_bodies();
    let asked: Vec<&str> = bodies
        .iter()
        .map(|b| b["model"].as_str().unwrap())
        .collect();
    assert_eq!(asked, ["reporter-2", "reporter-2"]);
    // The new email is not in the source, and sending is not read-only: not sent.
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
    let refusal = conversation
        .messages
        .iter()
        .find(|m| m["role"] == "tool" && m["content"].as_str().unwrap().contains("not run"));
    assert!(refusal.is_some(), "{:#?}", conversation.messages);
    let own = stack.events(&fork).await;
    let from: Vec<Option<i64>> = requests(&own).into_iter().map(replayed_from).collect();
    assert_eq!(from, [Some(1), Some(3), None, None, None]);
    let send = own
        .iter()
        .find(|e| e.tool == "email__send_email" && e.kind.is_response());
    assert_eq!(send.unwrap().outcome, Some(Outcome::Refused));
    // Live model calls are logged under the name the agent used, with what was sent.
    let live = requests(&own)[2];
    assert_eq!(live.tool, "reporter-1");
    assert_eq!(live.payload["sent"]["model"], "reporter-2");
    assert_eq!(live.payload["request"]["model"], "reporter-1");
}

#[tokio::test(flavor = "multi_thread")]
async fn after_the_fork_step_a_call_the_source_made_is_answered_from_its_log() {
    let stack = Stack::start().await;
    let fork = stack.fork(5, None, ToolMode::Safe).await;

    stack.run(&fork, REPORT).await.unwrap();

    // The same model asks for the same email, which the source already sent.
    assert_eq!(stack.later_model_bodies().len(), 2);
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
    let own = stack.events(&fork).await;
    let from: Vec<Option<i64>> = requests(&own).into_iter().map(replayed_from).collect();
    assert_eq!(from, [Some(1), Some(3), None, Some(7), None]);
}

#[tokio::test(flavor = "multi_thread")]
async fn in_live_mode_a_fork_runs_its_tool_calls_for_real() {
    let stack = Stack::start().await;
    let fork = stack.fork(5, Some("reporter-2"), ToolMode::Live).await;

    stack.run(&fork, REPORT).await.unwrap();

    let sent = stack.outbox.sent().await.unwrap();
    let subjects: Vec<&str> = sent.iter().map(|e| e.subject.as_str()).collect();
    assert_eq!(subjects, ["Daily report", "Daily report (v2)"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn in_replay_mode_nothing_new_runs_not_even_a_read() {
    let stack = Stack::start().await;
    // reporter-3 reads the report back before emailing: a call the source never made.
    let fork = stack.fork(1, Some("reporter-3"), ToolMode::Replay).await;

    let conversation = stack.run(&fork, REPORT).await.unwrap();

    assert_eq!(conversation.answer, "Sent the daily report.");
    let own = stack.events(&fork).await;
    let outcome_of = |tool: &str| {
        own.iter()
            .find(|e| e.tool == tool && e.kind.is_response())
            .and_then(|e| e.outcome)
    };
    assert_eq!(outcome_of("files__read_file"), Some(Outcome::Refused));
    // The write and the send match the source's and are answered from its log.
    let from: Vec<(String, Option<i64>)> = requests(&own)
        .into_iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .map(|e| (e.tool.clone(), replayed_from(e)))
        .collect();
    assert_eq!(
        from,
        [
            ("files__write_file".to_owned(), Some(3)),
            ("files__read_file".to_owned(), None),
            ("email__send_email".to_owned(), Some(7)),
        ]
    );
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_system_prompt_is_sent_and_logged_beside_the_agents_request() {
    let stack = Stack::start().await;
    let fork = stack
        .fork_with(1, None, Some("Be brief."), ToolMode::Replay)
        .await;

    stack.run(&fork, REPORT).await.unwrap();

    for body in stack.later_model_bodies() {
        assert_eq!(
            body["messages"][0],
            json!({ "role": "system", "content": "Be brief." })
        );
    }
    let own = stack.events(&fork).await;
    let first = requests(&own)[0];
    let agents = &first.payload["request"];
    assert_eq!(
        agents["messages"][0]["content"],
        "You write and send the daily report."
    );
    assert_eq!(first.payload["sent"]["messages"][0]["content"], "Be brief.");
    // The hash is of what the agent sent, so a fork of this fork still matches.
    assert_eq!(first.args_hash, Some(ledger_store::args_hash(agents)));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fork_whose_agent_goes_another_way_before_the_step_is_refused() {
    let stack = Stack::start().await;
    let fork = stack.fork(9, None, ToolMode::Safe).await;

    // Another report makes the very first model call different.
    let error = stack.run(&fork, "Something else.").await.unwrap_err();

    assert!(format!("{error:#}").contains("409"), "{error:#}");
    assert_eq!(stack.model.count(), 3);
    let own = stack.events(&fork).await;
    assert_eq!(own.len(), 2);
    assert_eq!(own[1].outcome, Some(Outcome::Refused));
    assert!(
        own[1].payload["reason"]
            .as_str()
            .unwrap()
            .contains("before step 9")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tool_call_that_goes_another_way_before_the_step_gets_its_own_error() {
    let stack = Stack::start().await;
    let fork = stack.fork(5, None, ToolMode::Safe).await;
    let client = connect(&stack.mcp, Some(&fork)).await.unwrap();

    let other = json!({ "path": "reports/daily.md", "content": "Not the report." });
    let error = client
        .call_tool(call_params("files__write_file", other))
        .await
        .unwrap_err();

    match error {
        ServiceError::McpError(error) => assert_eq!(error.code, error_codes::FORK_DIVERGED),
        other => panic!("expected a JSON-RPC error, got {other}"),
    }
    // The same write as the source's is answered from its log, without writing.
    let same = json!({ "path": "reports/daily.md", "content": REPORT });
    let result = client
        .call_tool(call_params("files__write_file", same))
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
    let own = stack.events(&fork).await;
    assert_eq!(replayed_from(requests(&own)[1]), Some(3));
}
