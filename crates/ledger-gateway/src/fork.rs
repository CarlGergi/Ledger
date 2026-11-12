//! What a fork does with each call: answer it from its source's log, run it live, or turn
//! it down. Worked out from the two logs alone, with nothing kept in memory, so every copy
//! of the gateway decides the same way, before and after a crash.
//!
//! A call is matched to a source call of the same kind, name and arguments hash that no
//! earlier call in the fork has used. Before the fork step, every call must match one the
//! source made before that step; anything else means the agent has already gone another
//! way, and the fork is not a fork of that run any more. From the fork step on, the fork
//! runs live, and its tool mode says what happens to tool calls.

use std::collections::HashSet;

use ledger_store::{Event, EventKind, Fork, ToolMode};

/// A call arriving in a fork, as far as replay is concerned.
pub(crate) struct Asked<'a> {
    /// `CallRequest` or `ModelRequest`.
    pub kind: EventKind,
    /// The tool, or the model the agent named.
    pub name: &'a str,
    pub args_hash: &'a str,
    /// Whether the tool changes nothing, for a fork in `safe` mode.
    pub read_only: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Replay<'a> {
    /// Answer with what the source's call got: this request, and its final answer.
    Source {
        request: &'a Event,
        answer: &'a Event,
    },
    /// Run the call: before the fork step nothing does, after it the mode decides.
    Live,
    /// Do not run it, and say why, as a tool error the model can read.
    Refuse(String),
    /// The agent asked for something the source did not ask for before the fork step.
    Diverged(String),
}

/// The source call each of the fork's calls was answered from, recorded in its request as
/// `replayed_from`.
pub(crate) fn used(own: &[Event]) -> HashSet<i64> {
    own.iter()
        .filter(|e| e.kind.is_request())
        .filter_map(|e| e.payload["replayed_from"]["seq"].as_i64())
        .collect()
}

pub(crate) fn decide<'a>(
    fork: &Fork,
    source: &'a [Event],
    own: &[Event],
    asked: &Asked,
) -> Replay<'a> {
    let used = used(own);
    let unused = |e: &&Event| e.kind.is_request() && !used.contains(&e.seq);
    let same = |e: &&Event| {
        e.kind == asked.kind
            && e.tool == asked.name
            && e.args_hash.as_deref() == Some(asked.args_hash)
    };
    let replay = |request: &'a Event| match final_answer(source, request) {
        Some(answer) => Replay::Source { request, answer },
        None => Replay::Refuse(format!(
            "call #{} in session {} has no recorded answer to replay",
            request.seq, fork.source_session
        )),
    };

    let mut before = source
        .iter()
        .filter(|e| e.seq < fork.at_step)
        .filter(unused)
        .peekable();
    if before.peek().is_some() {
        return match before.find(same) {
            Some(request) => replay(request),
            None => Replay::Diverged(format!(
                "{} was not asked for in session {} before step {}, so this run has already \
                 gone another way and is not a fork of it",
                asked.name, fork.source_session, fork.at_step
            )),
        };
    }

    if asked.kind == EventKind::ModelRequest || fork.tools == ToolMode::Live {
        return Replay::Live;
    }
    let later = source
        .iter()
        .filter(|e| e.seq >= fork.at_step)
        .filter(unused)
        .find(same);
    match (later, fork.tools) {
        (Some(request), _) => replay(request),
        (None, ToolMode::Safe) if asked.read_only => Replay::Live,
        (None, mode) => Replay::Refuse(format!(
            "not run: this is a fork of session {} in {} mode, and {} with these arguments \
             was not called there{}",
            fork.source_session,
            mode.as_str(),
            asked.name,
            if mode == ToolMode::Safe {
                ", and it is not read-only"
            } else {
                ""
            }
        )),
    }
}

/// The source call's final answer, of the kind that answers its request.
fn final_answer<'a>(source: &'a [Event], request: &Event) -> Option<&'a Event> {
    let kind = match request.kind {
        EventKind::ModelRequest => EventKind::ModelResponse,
        _ => EventKind::CallResponse,
    };
    source.iter().find(|e| {
        e.kind == kind
            && e.request_seq == Some(request.seq)
            && e.outcome.is_some_and(|o| o.is_final())
    })
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use ledger_store::Outcome;
    use serde_json::{Value, json};

    use super::*;

    /// A session's log, built call by call.
    #[derive(Default)]
    struct Log(Vec<Event>);

    impl Log {
        fn push(
            &mut self,
            kind: EventKind,
            name: &str,
            hash: Option<&str>,
            link: Option<i64>,
            payload: Value,
        ) -> i64 {
            let seq = self.0.len() as i64 + 1;
            self.0.push(Event {
                session_id: "s".into(),
                seq,
                kind,
                request_seq: link,
                tool: name.into(),
                outcome: link.map(|_| Outcome::Ok),
                args_hash: hash.map(str::to_owned),
                fence_token: Some(1),
                payload,
                recorded_at: Utc::now(),
            });
            seq
        }

        /// A call and its answer.
        fn call(&mut self, kind: EventKind, name: &str, hash: &str) -> i64 {
            let seq = self.push(kind, name, Some(hash), None, json!({}));
            let answer = if kind == EventKind::ModelRequest {
                EventKind::ModelResponse
            } else {
                EventKind::CallResponse
            };
            self.push(answer, name, None, Some(seq), json!({}));
            seq
        }

        /// A call in the fork, answered from source call `from`.
        fn replayed(&mut self, kind: EventKind, name: &str, hash: &str, from: i64) {
            let payload = json!({ "replayed_from": { "session": "src", "seq": from } });
            let seq = self.push(kind, name, Some(hash), None, payload);
            self.push(EventKind::CallResponse, name, None, Some(seq), json!({}));
        }
    }

    const MODEL: EventKind = EventKind::ModelRequest;
    const TOOL: EventKind = EventKind::CallRequest;

    fn fork(at_step: i64, tools: ToolMode) -> Fork {
        Fork {
            session_id: "f".into(),
            source_session: "src".into(),
            at_step,
            model: None,
            system_prompt: None,
            tools,
        }
    }

    fn asked<'a>(kind: EventKind, name: &'a str, hash: &'a str) -> Asked<'a> {
        Asked {
            kind,
            name,
            args_hash: hash,
            read_only: false,
        }
    }

    /// model, write, model, send, model: the source run every test forks.
    fn source() -> Log {
        let mut log = Log::default();
        log.call(MODEL, "m", "m1"); // 1
        log.call(TOOL, "write", "w"); // 3
        log.call(MODEL, "m", "m2"); // 5
        log.call(TOOL, "send", "s"); // 7
        log.call(MODEL, "m", "m3"); // 9
        log
    }

    fn replayed_seq(replay: &Replay) -> Option<i64> {
        match replay {
            Replay::Source { request, .. } => Some(request.seq),
            _ => None,
        }
    }

    #[test]
    fn before_the_fork_step_each_call_is_answered_from_its_source_call() {
        let src = source();
        let f = fork(5, ToolMode::Safe);
        let mut own = Log::default();
        let first = decide(&f, &src.0, &own.0, &asked(MODEL, "m", "m1"));
        assert_eq!(replayed_seq(&first), Some(1));
        own.replayed(MODEL, "m", "m1", 1);
        let second = decide(&f, &src.0, &own.0, &asked(TOOL, "write", "w"));
        assert_eq!(replayed_seq(&second), Some(3));
        own.replayed(TOOL, "write", "w", 3);
        // Step 5 onwards is live: the next model call goes to the model.
        assert_eq!(
            decide(&f, &src.0, &own.0, &asked(MODEL, "m", "m2")),
            Replay::Live
        );
    }

    #[test]
    fn a_different_call_before_the_fork_step_has_diverged() {
        let src = source();
        let f = fork(5, ToolMode::Safe);
        let mut own = Log::default();
        let other_prompt = decide(&f, &src.0, &own.0, &asked(MODEL, "m", "other"));
        assert!(
            matches!(other_prompt, Replay::Diverged(_)),
            "{other_prompt:?}"
        );
        own.replayed(MODEL, "m", "m1", 1);
        // The same tool with other arguments, and a call the source made only later.
        for (name, hash) in [("write", "w2"), ("send", "s")] {
            let replay = decide(&f, &src.0, &own.0, &asked(TOOL, name, hash));
            assert!(matches!(replay, Replay::Diverged(_)), "{name}: {replay:?}");
        }
    }

    #[test]
    fn a_source_call_answers_once() {
        let mut src = Log::default();
        src.call(TOOL, "read", "r"); // 1
        src.call(TOOL, "read", "r"); // 3
        let f = fork(5, ToolMode::Replay);
        let mut own = Log::default();
        assert_eq!(
            replayed_seq(&decide(&f, &src.0, &own.0, &asked(TOOL, "read", "r"))),
            Some(1)
        );
        own.replayed(TOOL, "read", "r", 1);
        assert_eq!(
            replayed_seq(&decide(&f, &src.0, &own.0, &asked(TOOL, "read", "r"))),
            Some(3)
        );
        own.replayed(TOOL, "read", "r", 3);
        // Both used, and the fork step passed: a third read is not in the source.
        let third = decide(&f, &src.0, &own.0, &asked(TOOL, "read", "r"));
        assert!(matches!(third, Replay::Refuse(_)), "{third:?}");
    }

    #[test]
    fn after_the_fork_step_the_tool_mode_decides() {
        let src = source();
        let at_start = |tools| fork(1, tools);
        let send = asked(TOOL, "send", "s");
        let other_send = asked(TOOL, "send", "s2");
        let read = Asked {
            read_only: true,
            ..asked(TOOL, "read", "r")
        };

        // safe: a call the source made is answered from its log; a new read runs; a new
        // send does not.
        let safe = at_start(ToolMode::Safe);
        assert_eq!(replayed_seq(&decide(&safe, &src.0, &[], &send)), Some(7));
        assert_eq!(decide(&safe, &src.0, &[], &read), Replay::Live);
        assert!(matches!(
            decide(&safe, &src.0, &[], &other_send),
            Replay::Refuse(_)
        ));

        // replay: nothing runs.
        let replay = at_start(ToolMode::Replay);
        assert_eq!(replayed_seq(&decide(&replay, &src.0, &[], &send)), Some(7));
        assert!(matches!(
            decide(&replay, &src.0, &[], &read),
            Replay::Refuse(_)
        ));

        // live: everything runs, even what the source already did.
        let live = at_start(ToolMode::Live);
        assert_eq!(decide(&live, &src.0, &[], &send), Replay::Live);
        assert_eq!(decide(&live, &src.0, &[], &other_send), Replay::Live);

        // Model calls after the fork step always go to the model.
        assert_eq!(
            decide(&replay, &src.0, &[], &asked(MODEL, "m", "m1")),
            Replay::Live
        );
    }

    #[test]
    fn a_source_call_with_no_answer_cannot_be_replayed() {
        let mut src = Log::default();
        src.push(TOOL, "send", Some("s"), None, json!({}));
        let f = fork(2, ToolMode::Safe);
        let replay = decide(&f, &src.0, &[], &asked(TOOL, "send", "s"));
        assert!(matches!(replay, Replay::Refuse(_)), "{replay:?}");
    }

    #[test]
    fn a_fork_step_past_the_end_replays_the_whole_run() {
        let src = source();
        let f = fork(11, ToolMode::Safe);
        let mut own = Log::default();
        for (kind, name, hash, from) in [
            (MODEL, "m", "m1", 1),
            (TOOL, "write", "w", 3),
            (MODEL, "m", "m2", 5),
            (TOOL, "send", "s", 7),
            (MODEL, "m", "m3", 9),
        ] {
            assert_eq!(
                replayed_seq(&decide(&f, &src.0, &own.0, &asked(kind, name, hash))),
                Some(from)
            );
            own.replayed(kind, name, hash, from);
        }
        assert_eq!(used(&own.0), HashSet::from([1, 3, 5, 7, 9]));
    }
}
