//! Tells a retry from a new call, as a pure function of the session's log, so that every
//! copy of the gateway, and the simulator, decides the same way.
//!
//! A call is a retry only if it says which call it repeats:
//!
//! 1. **An explicit step.** A call naming a step that an earlier call named is a retry of
//!    it, if the tool and arguments match. If they do not, the step was reused, which is
//!    refused.
//! 2. **The same JSON-RPC id in the same transport session,** with the same tool and
//!    arguments. MCP forbids reusing an id within a session, so this is a retry.
//!
//! Anything else is a new call, even with arguments identical to an earlier one: an agent
//! may well mean to send the same email twice. The cost is that an agent which resends
//! with a new id and no step gets the tool run again.

use ledger_store::{Event, EventKind};
use serde_json::Value;

use crate::policy::EffectClass;

pub(crate) struct Incoming<'a> {
    pub tool: &'a str,
    pub args_hash: &'a str,
    pub rpc_id: &'a Value,
    pub transport: Option<&'a str>,
    pub step: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decision {
    New,
    /// A repeat of the call logged at this sequence number.
    Retry(i64),
    /// The step named a call with different arguments.
    StepReused(i64),
}

pub(crate) fn decide(events: &[Event], call: &Incoming) -> Decision {
    let matches = |seq: i64| {
        request(events, seq)
            .is_some_and(|r| r.tool == call.tool && r.args_hash.as_deref() == Some(call.args_hash))
    };

    if let Some(step) = call.step {
        let named = asks(events).find(|e| e.payload["step"].as_str() == Some(step));
        if let Some(seq) = named.and_then(call_of) {
            return if matches(seq) {
                Decision::Retry(seq)
            } else {
                Decision::StepReused(seq)
            };
        }
        return Decision::New;
    }

    if let Some(transport) = call.transport {
        let same_id = asks(events).rev().find(|e| {
            e.payload["transport"].as_str() == Some(transport)
                && &e.payload["rpc_id"] == call.rpc_id
        });
        if let Some(seq) = same_id.and_then(call_of)
            && matches(seq)
        {
            return Decision::Retry(seq);
        }
    }

    Decision::New
}

/// Requests and retries: every time an agent asked for something, oldest first.
fn asks(events: &[Event]) -> impl DoubleEndedIterator<Item = &Event> {
    events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::CallRequest | EventKind::CallRetry))
}

/// The call a request or retry belongs to.
fn call_of(event: &Event) -> Option<i64> {
    match event.kind {
        EventKind::CallRequest => Some(event.seq),
        EventKind::CallRetry => event.request_seq,
        EventKind::CallResponse | EventKind::ModelRequest | EventKind::ModelResponse => None,
    }
}

pub(crate) fn request(events: &[Event], seq: i64) -> Option<&Event> {
    events
        .iter()
        .find(|e| e.seq == seq && e.kind == EventKind::CallRequest)
}

pub(crate) fn final_answer(events: &[Event], seq: i64) -> Option<&Event> {
    events.iter().find(|e| {
        e.kind == EventKind::CallResponse
            && e.request_seq == Some(seq)
            && e.outcome.is_some_and(|o| o.is_final())
    })
}

/// The class a call was given when it was logged. A call logged without one is treated as
/// the least safe.
pub(crate) fn logged_class(request: &Event) -> EffectClass {
    serde_json::from_value(request.payload["effect"]["class"].clone())
        .unwrap_or(EffectClass::AtMostOnce)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use ledger_store::Outcome;
    use serde_json::json;

    use super::*;

    struct Log(Vec<Event>);

    impl Log {
        fn push(
            &mut self,
            kind: EventKind,
            link: Option<i64>,
            outcome: Option<Outcome>,
            payload: Value,
            hash: Option<&str>,
        ) -> i64 {
            let seq = self.0.len() as i64 + 1;
            self.0.push(Event {
                session_id: "s".to_owned(),
                seq,
                kind,
                request_seq: link,
                tool: "t".to_owned(),
                outcome,
                args_hash: hash.map(str::to_owned),
                fence_token: Some(1),
                payload,
                recorded_at: Utc::now(),
            });
            seq
        }

        fn request(&mut self, hash: &str, payload: Value) -> i64 {
            self.push(EventKind::CallRequest, None, None, payload, Some(hash))
        }

        fn answer(&mut self, seq: i64, outcome: Outcome) {
            self.push(
                EventKind::CallResponse,
                Some(seq),
                Some(outcome),
                json!({}),
                None,
            );
        }

        fn retry(&mut self, seq: i64, payload: Value) {
            self.push(EventKind::CallRetry, Some(seq), None, payload, None);
        }
    }

    fn call<'a>(hash: &'a str, rpc_id: &'a Value) -> Incoming<'a> {
        Incoming {
            tool: "t",
            args_hash: hash,
            rpc_id,
            transport: None,
            step: None,
        }
    }

    #[test]
    fn a_step_names_its_call() {
        let mut log = Log(Vec::new());
        let first = log.request("h", json!({"step": "1"}));
        log.answer(first, Outcome::Ok);
        log.request("h", json!({"step": "2"}));
        let id = json!(9);

        let mut again = call("h", &id);
        again.step = Some("1");
        assert_eq!(decide(&log.0, &again), Decision::Retry(first));

        let mut reused = call("other", &id);
        reused.step = Some("1");
        assert_eq!(decide(&log.0, &reused), Decision::StepReused(first));

        // A new step is a new call, even with arguments identical to an earlier one.
        let mut fresh = call("h", &id);
        fresh.step = Some("3");
        assert_eq!(decide(&log.0, &fresh), Decision::New);
    }

    #[test]
    fn a_step_found_on_a_retry_still_names_the_original_call() {
        let mut log = Log(Vec::new());
        let first = log.request("h", json!({}));
        log.retry(first, json!({"step": "7"}));
        let id = json!(1);
        let mut again = call("h", &id);
        again.step = Some("7");
        assert_eq!(decide(&log.0, &again), Decision::Retry(first));
    }

    #[test]
    fn the_same_id_in_the_same_transport_session_is_a_retry() {
        let mut log = Log(Vec::new());
        let first = log.request("h", json!({"transport": "x", "rpc_id": 4}));
        log.answer(first, Outcome::Ok);
        log.request("h2", json!({"transport": "x", "rpc_id": 5}));
        let four = json!(4);

        let mut again = call("h", &four);
        again.transport = Some("x");
        assert_eq!(decide(&log.0, &again), Decision::Retry(first));

        // Ids restart with each transport session.
        again.transport = Some("y");
        assert_eq!(decide(&log.0, &again), Decision::New);

        // An id reused for something else is not a retry of it.
        let mut different = call("h3", &four);
        different.transport = Some("x");
        assert_eq!(decide(&log.0, &different), Decision::New);
    }

    #[test]
    fn identical_arguments_alone_never_make_a_retry() {
        let mut log = Log(Vec::new());
        log.request("h", json!({"transport": "x", "rpc_id": 1}));
        let two = json!(2);
        let mut again = call("h", &two);
        again.transport = Some("x");
        // Unanswered or not, a new id with no step is a new call.
        assert_eq!(decide(&log.0, &again), Decision::New);
        log.answer(1, Outcome::Ok);
        assert_eq!(decide(&log.0, &again), Decision::New);
    }

    #[test]
    fn a_call_logged_without_a_class_counts_as_the_least_safe() {
        let mut log = Log(Vec::new());
        let seq = log.request("h", json!({}));
        assert_eq!(
            logged_class(request(&log.0, seq).unwrap()),
            EffectClass::AtMostOnce
        );
        let seq = log.request("h", json!({"effect": {"class": "idempotent"}}));
        assert_eq!(
            logged_class(request(&log.0, seq).unwrap()),
            EffectClass::Idempotent
        );
    }
}
