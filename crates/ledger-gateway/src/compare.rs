//! Compares a replayed run with the run it replays: the first step where they act
//! differently, and what each cost.
//!
//! What a run did is the sequence of tool calls it asked for, each with its arguments, and
//! then the model's final answer. Model calls themselves are not compared one by one: a new
//! model may word its requests differently and still do the same thing, which is what
//! matters to whoever runs the agent.

use std::collections::BTreeMap;

use ledger_store::{Event, EventKind, Outcome};
use serde::Serialize;
use serde_json::Value;

/// One thing a run did.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Action {
    Tool {
        name: String,
        arguments: Value,
        seq: i64,
    },
    /// The model's last words, once it stopped calling tools.
    Answer { text: String, seq: i64 },
}

impl Action {
    fn same_as(&self, other: &Action) -> bool {
        match (self, other) {
            (
                Action::Tool {
                    name: a,
                    arguments: x,
                    ..
                },
                Action::Tool {
                    name: b,
                    arguments: y,
                    ..
                },
            ) => a == b && x == y,
            (Action::Answer { text: a, .. }, Action::Answer { text: b, .. }) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Divergence {
    /// Which action, counting from 1.
    pub step: usize,
    /// What each run did there; None if it had stopped.
    pub source: Option<Action>,
    pub replay: Option<Action>,
}

/// What a run cost.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct Cost {
    pub model_calls: usize,
    /// Total tokens the model reported, where it reported them.
    pub tokens: u64,
    /// Tool calls that ran.
    pub tools_run: usize,
    /// Tool calls answered from another session's log instead of running.
    pub tools_from_log: usize,
    /// Tool calls turned down.
    pub tools_refused: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Report {
    pub source: String,
    pub replay: String,
    /// Actions the two runs had in common before the first divergence.
    pub same_steps: usize,
    pub divergence: Option<Divergence>,
    pub source_cost: Cost,
    pub replay_cost: Cost,
}

impl Report {
    pub fn same(&self) -> bool {
        self.divergence.is_none()
    }

    /// One line for a person.
    pub fn summary(&self) -> String {
        match &self.divergence {
            None => format!("same: all {} steps match", self.same_steps),
            Some(d) => format!(
                "diverges at step {}: {} became {}",
                d.step,
                describe(d.source.as_ref()),
                describe(d.replay.as_ref())
            ),
        }
    }
}

fn describe(action: Option<&Action>) -> String {
    match action {
        None => "nothing (the run had stopped)".to_owned(),
        Some(Action::Tool {
            name, arguments, ..
        }) => format!("{name} {arguments}"),
        Some(Action::Answer { text, .. }) => format!("the answer {text:?}"),
    }
}

pub fn compare(source: &[Event], replay: &[Event]) -> Report {
    let (a, b) = (actions(source), actions(replay));
    let same_steps = a.iter().zip(&b).take_while(|(x, y)| x.same_as(y)).count();
    let divergence = (same_steps < a.len().max(b.len())).then(|| Divergence {
        step: same_steps + 1,
        source: a.get(same_steps).cloned(),
        replay: b.get(same_steps).cloned(),
    });
    let session = |events: &[Event]| {
        events
            .first()
            .map(|e| e.session_id.clone())
            .unwrap_or_default()
    };
    Report {
        source: session(source),
        replay: session(replay),
        same_steps,
        divergence,
        source_cost: cost(source),
        replay_cost: cost(replay),
    }
}

/// The tool calls a run asked for, in order, then its final answer if it gave one.
pub fn actions(events: &[Event]) -> Vec<Action> {
    let mut actions: Vec<Action> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .map(|e| Action::Tool {
            name: e.tool.clone(),
            arguments: e.payload["params"]["arguments"].clone(),
            seq: e.seq,
        })
        .collect();
    let last_reply = events
        .iter()
        .rev()
        .find(|e| e.kind == EventKind::ModelResponse && e.outcome == Some(Outcome::Ok));
    if let Some(reply) = last_reply
        && let Some(message) = message(&reply.payload)
        && message["tool_calls"].as_array().is_none_or(Vec::is_empty)
    {
        let text = message["content"].as_str().unwrap_or_default().to_owned();
        actions.push(Action::Answer {
            text,
            seq: reply.seq,
        });
    }
    actions
}

fn cost(events: &[Event]) -> Cost {
    let mut cost = Cost::default();
    let answers: BTreeMap<i64, &Event> = events
        .iter()
        .filter(|e| e.kind == EventKind::CallResponse && e.outcome.is_some_and(Outcome::is_final))
        .filter_map(|e| Some((e.request_seq?, e)))
        .collect();
    for event in events {
        match event.kind {
            EventKind::ModelRequest if event.payload["replayed_from"].is_null() => {
                cost.model_calls += 1;
            }
            EventKind::ModelResponse if event.payload["replayed_from"].is_null() => {
                cost.tokens += tokens(&event.payload);
            }
            EventKind::CallRequest => match answers.get(&event.seq) {
                _ if !event.payload["replayed_from"].is_null() => cost.tools_from_log += 1,
                Some(answer) if answer.outcome == Some(Outcome::Refused) => {
                    cost.tools_refused += 1;
                }
                _ => cost.tools_run += 1,
            },
            _ => {}
        }
    }
    cost
}

/// The assistant message of a logged model reply, put back together if it was streamed.
pub fn message(logged: &Value) -> Option<Value> {
    let body = &logged["body"];
    if let Some(message) = body["json"]["choices"][0].get("message") {
        return Some(message.clone());
    }
    let chunks = body["stream"].as_array()?;
    let mut content = String::new();
    let mut calls: Vec<Value> = Vec::new();
    for chunk in chunks
        .iter()
        .filter_map(|c| serde_json::from_str::<Value>(c.as_str()?).ok())
    {
        let delta = &chunk["choices"][0]["delta"];
        content.push_str(delta["content"].as_str().unwrap_or_default());
        for call in delta["tool_calls"].as_array().into_iter().flatten() {
            let index = call["index"].as_u64().unwrap_or(0) as usize;
            if calls.len() <= index {
                calls.resize(
                    index + 1,
                    serde_json::json!({ "function": { "name": "", "arguments": "" } }),
                );
            }
            let function = &mut calls[index]["function"];
            for part in ["name", "arguments"] {
                let more = call["function"][part].as_str().unwrap_or_default();
                let so_far = function[part].as_str().unwrap_or_default().to_owned();
                function[part] = Value::String(so_far + more);
            }
        }
    }
    Some(serde_json::json!({ "role": "assistant", "content": content, "tool_calls": calls }))
}

fn tokens(logged: &Value) -> u64 {
    let body = &logged["body"];
    if let Some(total) = body["json"]["usage"]["total_tokens"].as_u64() {
        return total;
    }
    // A stream reports usage in its last chunk, if asked to.
    body["stream"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| serde_json::from_str::<Value>(c.as_str()?).ok())
        .filter_map(|c| c["usage"]["total_tokens"].as_u64())
        .next_back()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use super::*;

    #[derive(Default)]
    struct Log(Vec<Event>);

    impl Log {
        fn push(
            &mut self,
            kind: EventKind,
            tool: &str,
            link: Option<i64>,
            outcome: Option<Outcome>,
            payload: Value,
        ) -> i64 {
            let seq = self.0.len() as i64 + 1;
            self.0.push(Event {
                session_id: "s".into(),
                seq,
                kind,
                request_seq: link,
                tool: tool.into(),
                outcome,
                args_hash: None,
                fence_token: Some(1),
                payload,
                recorded_at: Utc::now(),
            });
            seq
        }

        fn model(&mut self, message: Value, tokens: u64) {
            let seq = self.push(EventKind::ModelRequest, "m", None, None, json!({}));
            let body = json!({ "json": { "choices": [{ "message": message }], "usage": { "total_tokens": tokens } } });
            self.push(
                EventKind::ModelResponse,
                "m",
                Some(seq),
                Some(Outcome::Ok),
                json!({ "status": 200, "body": body }),
            );
        }

        fn tool(&mut self, name: &str, arguments: Value, outcome: Outcome, replayed: bool) {
            let mut payload = json!({ "params": { "name": name, "arguments": arguments } });
            if replayed {
                payload["replayed_from"] = json!({ "session": "src", "seq": 1 });
            }
            let seq = self.push(EventKind::CallRequest, name, None, None, payload);
            self.push(
                EventKind::CallResponse,
                name,
                Some(seq),
                Some(outcome),
                json!({}),
            );
        }
    }

    fn calls(name: &str) -> Value {
        json!({ "role": "assistant", "tool_calls": [{ "function": { "name": name } }] })
    }

    fn says(text: &str) -> Value {
        json!({ "role": "assistant", "content": text })
    }

    /// write, send, answer: what reporter-1 does.
    fn run(subject: &str, answer: &str) -> Log {
        let mut log = Log::default();
        log.model(calls("write"), 10);
        log.tool("write", json!({ "path": "r" }), Outcome::Ok, false);
        log.model(calls("send"), 20);
        log.tool("send", json!({ "subject": subject }), Outcome::Ok, false);
        log.model(says(answer), 30);
        log
    }

    #[test]
    fn the_same_actions_make_the_same_run() {
        let report = compare(&run("a", "done").0, &run("a", "done").0);
        assert!(report.same());
        assert_eq!(report.same_steps, 3);
        assert_eq!(report.summary(), "same: all 3 steps match");
        assert_eq!(report.source_cost.model_calls, 3);
        assert_eq!(report.source_cost.tokens, 60);
        assert_eq!(report.source_cost.tools_run, 2);
    }

    #[test]
    fn other_arguments_diverge_at_their_step() {
        let report = compare(&run("a", "done").0, &run("b", "done").0);
        let d = report.divergence.clone().unwrap();
        assert_eq!((report.same_steps, d.step), (1, 2));
        assert!(
            matches!(d.replay, Some(Action::Tool { ref arguments, .. }) if arguments["subject"] == "b")
        );
        assert_eq!(
            report.summary(),
            r#"diverges at step 2: send {"subject":"a"} became send {"subject":"b"}"#
        );
    }

    #[test]
    fn another_answer_diverges_at_the_end() {
        let report = compare(&run("a", "done").0, &run("a", "sent").0);
        assert_eq!(report.divergence.unwrap().step, 3);
    }

    #[test]
    fn a_run_that_stops_early_or_does_more_diverges_where_it_differs() {
        let full = run("a", "done");
        let mut short = Log::default();
        short.model(calls("write"), 10);
        short.tool("write", json!({ "path": "r" }), Outcome::Ok, false);
        let report = compare(&full.0, &short.0);
        let d = report.divergence.unwrap();
        assert_eq!(d.step, 2);
        assert!(d.replay.is_none());
        assert!(
            compare(&short.0, &full.0)
                .divergence
                .unwrap()
                .source
                .is_none()
        );
    }

    #[test]
    fn a_replays_cost_counts_what_it_did_not_run() {
        let mut replay = Log::default();
        replay.model(calls("write"), 7);
        replay.tool("write", json!({}), Outcome::Ok, true);
        replay.model(calls("read"), 8);
        replay.tool("read", json!({}), Outcome::Refused, false);
        let cost = compare(&run("a", "x").0, &replay.0).replay_cost;
        assert_eq!(
            cost,
            Cost {
                model_calls: 2,
                tokens: 15,
                tools_run: 0,
                tools_from_log: 1,
                tools_refused: 1,
            }
        );
    }

    #[test]
    fn a_streamed_reply_is_put_back_together() {
        let chunks = [
            json!({ "choices": [{ "delta": { "role": "assistant" } }] }),
            json!({ "choices": [{ "delta": { "content": "Sent the " } }] }),
            json!({ "choices": [{ "delta": { "content": "report." } }] }),
            json!({ "choices": [{ "delta": {} , "finish_reason": "stop" }], "usage": { "total_tokens": 42 } }),
        ];
        let mut stream: Vec<Value> = chunks.iter().map(|c| json!(c.to_string())).collect();
        stream.push(json!("[DONE]"));
        let logged = json!({ "status": 200, "body": { "stream": stream } });
        assert_eq!(message(&logged).unwrap()["content"], "Sent the report.");
        assert_eq!(tokens(&logged), 42);

        let call = json!({ "choices": [{ "delta": { "tool_calls": [
            { "index": 0, "function": { "name": "send", "arguments": "{\"a\":" } }
        ] } }] });
        let more = json!({ "choices": [{ "delta": { "tool_calls": [
            { "index": 0, "function": { "arguments": "1}" } }
        ] } }] });
        let logged =
            json!({ "body": { "stream": [call.to_string(), more.to_string(), "[DONE]"] } });
        let function = &message(&logged).unwrap()["tool_calls"][0]["function"];
        assert_eq!(
            (function["name"].as_str(), function["arguments"].as_str()),
            (Some("send"), Some("{\"a\":1}"))
        );
    }
}
