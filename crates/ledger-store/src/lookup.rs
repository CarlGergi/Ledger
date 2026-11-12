use serde_json::Value;

use crate::{Event, EventKind};

/// What identifies a call as a repeat of an earlier one: the step it names, or else its
/// JSON-RPC id within its transport session.
#[derive(Debug, Clone, PartialEq)]
pub struct CallKey {
    pub step: Option<String>,
    pub rpc_id: Value,
    pub transport: Option<String>,
}

/// The events that decide whether `key` repeats an earlier call, as
/// [`EventStore::call_lookup`](crate::EventStore::call_lookup) returns them, found here by
/// reading the whole log: the reference the stores are held to.
///
/// With a step, the oldest request or retry naming it. Without one, the newest request or
/// retry with the same id in the same transport session. Then the call it belongs to, and
/// every answer to that call. Ordered by sequence, each event once.
pub fn lookup_in(events: &[Event], key: &CallKey) -> Vec<Event> {
    let mut asks = events
        .iter()
        .filter(|e| matches!(e.kind, EventKind::CallRequest | EventKind::CallRetry));
    let ask = match (&key.step, &key.transport) {
        (Some(step), _) => asks.find(|e| e.payload["step"].as_str() == Some(step)),
        (None, Some(transport)) => asks.rev().find(|e| {
            e.payload["transport"].as_str() == Some(transport.as_str())
                && e.payload.get("rpc_id") == Some(&key.rpc_id)
        }),
        (None, None) => None,
    };
    let Some(ask) = ask else {
        return Vec::new();
    };
    let call = match ask.kind {
        EventKind::CallRequest => Some(ask.seq),
        _ => ask.request_seq,
    };
    events
        .iter()
        .filter(|e| {
            e.seq == ask.seq
                || Some(e.seq) == call
                || (e.kind == EventKind::CallResponse && e.request_seq == call)
        })
        .cloned()
        .collect()
}
