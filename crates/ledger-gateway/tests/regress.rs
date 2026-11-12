//! Regression replay: a recorded run, replayed on another model with every tool call
//! answered from its log, and compared with the original.

mod common;

use common::{REPORT, Stack};
use ledger_gateway::compare::{Action, Report};
use ledger_gateway::regress::{report, start_replay};
use ledger_store::ToolMode;

/// Replays the stack's source run on `model`, and compares.
async fn replay_on(stack: &Stack, model: &str) -> Report {
    let replay = start_replay(stack.store.as_ref(), "src", model)
        .await
        .unwrap();
    let fork = stack.store.fork(&replay).await.unwrap().unwrap();
    assert_eq!((fork.at_step, fork.tools), (1, ToolMode::Replay));
    stack.run(&replay, REPORT).await.unwrap();
    report(stack.store.as_ref(), "src", &replay).await.unwrap()
}

fn tool(action: &Option<Action>) -> (&str, &serde_json::Value) {
    match action {
        Some(Action::Tool {
            name, arguments, ..
        }) => (name, arguments),
        other => panic!("expected a tool call, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_on_the_same_model_acts_the_same_and_runs_no_tool() {
    let stack = Stack::start().await;

    let report = replay_on(&stack, "reporter-1").await;

    assert!(report.same(), "{}", report.summary());
    assert_eq!(report.same_steps, 3);
    assert_eq!(report.source_cost.tools_run, 2);
    let cost = &report.replay_cost;
    assert_eq!(
        (cost.model_calls, cost.tools_run, cost.tools_from_log),
        (3, 0, 2)
    );
    assert_eq!(cost.tokens, report.source_cost.tokens);
    assert_eq!(stack.later_model_bodies().len(), 3);
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_that_emails_differently_diverges_at_the_email() {
    let stack = Stack::start().await;

    let report = replay_on(&stack, "reporter-2").await;

    let divergence = report.divergence.clone().unwrap();
    assert_eq!((report.same_steps, divergence.step), (1, 2));
    let (before, after) = (tool(&divergence.source), tool(&divergence.replay));
    assert_eq!(
        (before.0, after.0),
        ("email__send_email", "email__send_email")
    );
    assert_eq!(before.1["subject"], "Daily report");
    assert_eq!(after.1["subject"], "Daily report (v2)");
    assert!(
        report
            .summary()
            .starts_with("diverges at step 2: email__send_email")
    );
    // The new email was turned down, not sent.
    assert_eq!(report.replay_cost.tools_refused, 1);
    assert_eq!(report.replay_cost.tools_run, 0);
    assert_eq!(stack.outbox.sent().await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_model_that_reads_first_diverges_at_the_read() {
    let stack = Stack::start().await;

    let report = replay_on(&stack, "reporter-3").await;

    let divergence = report.divergence.clone().unwrap();
    assert_eq!(divergence.step, 2);
    assert_eq!(tool(&divergence.source).0, "email__send_email");
    assert_eq!(tool(&divergence.replay).0, "files__read_file");
    let cost = &report.replay_cost;
    assert_eq!(
        (cost.tools_from_log, cost.tools_refused, cost.tools_run),
        (2, 1, 0)
    );
    assert_eq!(cost.model_calls, 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replay_of_a_session_that_does_not_exist_is_refused() {
    let stack = Stack::start().await;
    let error = start_replay(stack.store.as_ref(), "missing", "reporter-1")
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("no events"), "{error:#}");
}
