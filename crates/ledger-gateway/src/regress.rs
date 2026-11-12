//! Regression replay: run recorded sessions again on a new model, with every tool call
//! answered from the recorded log, and report where each run's behaviour changed.
//!
//! Ledger cannot run an agent by itself, so a suite names the command that does, and the
//! gateway it talks to must be running. Each run gets a session of its own, which the
//! command is given in place of `{session}`.

use std::path::Path;

use anyhow::Context;
use ledger_store::{EventStore, Fork, ToolMode};
use serde::Deserialize;

use crate::compare::{self, Report};

/// A suite file.
///
/// ```toml
/// model = "gpt-5-mini"
/// agent = ["my-agent", "--session", "{session}"]
///
/// [[run]]
/// session = "run-1"
/// args = ["--task", "Write the daily report."]
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Suite {
    /// The model to replay on.
    pub model: String,
    /// The agent's command, with `{session}` where the session id goes.
    pub agent: Vec<String>,
    #[serde(rename = "run")]
    pub runs: Vec<Run>,
}

/// One recorded session to replay, and what to add to the agent's command for it: whatever
/// gave the agent the same task as when it was recorded.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Run {
    pub session: String,
    #[serde(default)]
    pub args: Vec<String>,
}

impl Suite {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in {}", path.display()))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let suite: Self = toml::from_str(text)?;
        anyhow::ensure!(!suite.model.is_empty(), "model must not be empty");
        anyhow::ensure!(!suite.agent.is_empty(), "agent must name a command");
        anyhow::ensure!(
            suite.agent.iter().any(|a| a.contains("{session}")),
            "agent must pass {{session}} to the agent, or the replay would not be recorded"
        );
        anyhow::ensure!(!suite.runs.is_empty(), "at least one [[run]] is required");
        Ok(suite)
    }

    /// The command that replays `run` under `session`.
    pub fn command(&self, run: &Run, session: &str) -> Vec<String> {
        self.agent
            .iter()
            .map(|a| a.replace("{session}", session))
            .chain(run.args.iter().cloned())
            .collect()
    }
}

/// Starts a replay of `source` on `model`: a fork at its first step that runs no tool, so
/// every model call goes to the new model and every tool call is answered from the log.
pub async fn start_replay(
    store: &dyn EventStore,
    source: &str,
    model: &str,
) -> anyhow::Result<String> {
    anyhow::ensure!(
        !store.session_events(source).await?.is_empty(),
        "no events for session {source:?}"
    );
    let id = uuid::Uuid::new_v4().simple().to_string();
    let session = format!("{source}.replay-{}", &id[..8]);
    let fork = Fork {
        session_id: session.clone(),
        source_session: source.to_owned(),
        at_step: 1,
        model: Some(model.to_owned()),
        system_prompt: None,
        tools: ToolMode::Replay,
    };
    store.create_fork(&fork).await?;
    Ok(session)
}

/// Compares a finished replay with its source.
pub async fn report(store: &dyn EventStore, source: &str, replay: &str) -> anyhow::Result<Report> {
    let source_events = store.session_events(source).await?;
    let replay_events = store.session_events(replay).await?;
    anyhow::ensure!(
        !source_events.is_empty(),
        "no events for session {source:?}"
    );
    let mut report = compare::compare(&source_events, &replay_events);
    // A replay that recorded nothing still has a name.
    report.replay = replay.to_owned();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUITE: &str = r#"
        model = "reporter-2"
        agent = ["sample-agent", "--session", "{session}", "--model", "reporter-1"]

        [[run]]
        session = "a"
        args = ["--report", "All good."]

        [[run]]
        session = "b"
    "#;

    #[test]
    fn a_suite_names_the_model_the_agent_and_its_runs() {
        let suite = Suite::parse(SUITE).unwrap();
        assert_eq!(suite.model, "reporter-2");
        assert_eq!(suite.runs.len(), 2);
        assert_eq!(
            suite.command(&suite.runs[0], "a.replay-1"),
            [
                "sample-agent",
                "--session",
                "a.replay-1",
                "--model",
                "reporter-1",
                "--report",
                "All good."
            ]
        );
        assert_eq!(suite.command(&suite.runs[1], "b.r").len(), 5);
    }

    #[test]
    fn unusable_suites_are_refused() {
        for text in [
            SUITE.replace(r#"model = "reporter-2""#, r#"model = """#),
            SUITE.replace(r#""--session", "{session}", "#, ""),
            r#"model = "m"
               agent = ["a", "{session}"]"#
                .to_owned(),
            SUITE.replace("args =", "argz ="),
        ] {
            assert!(Suite::parse(&text).is_err(), "{text}");
        }
    }
}
