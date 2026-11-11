//! The invariants every run must keep, checked after it has settled. They compare the log
//! against what really happened: the tools' effects and what the agents were told.

use std::collections::{BTreeMap, HashMap};

use ledger_store::{Event, EventKind, EventStore, MemoryStore, Outcome};

use crate::faults::{Truth, truth};
use crate::world::World;
use crate::{Agent, Kind};

/// What a run did, for the summary.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub calls: usize,
    pub retries: usize,
    pub effects: usize,
    pub flagged: usize,
    pub takeovers: usize,
    /// At-most-once effects repeated for agents that resend with a new id and no step,
    /// which ledger cannot tell from a new call. Expected, and documented.
    pub unrecognised_resends: usize,
    /// At-most-once effects repeated by a tool that claims to be read-only and was not
    /// corrected. Expected: that is what the override file is for.
    pub uncorrected_lies: usize,
}

pub async fn check(world: &World, store: &MemoryStore, agents: &[Agent]) -> (Vec<String>, Stats) {
    let mut violations = Vec::new();
    let mut stats = Stats::default();
    let mut events_by_session = BTreeMap::new();
    for agent in agents {
        let events = store
            .session_events(&agent.session)
            .await
            .expect("memory store");
        well_formed(&agent.session, &events, &mut violations);
        events_by_session.insert(agent.session.clone(), events);
    }
    let state = world.state();
    let kinds: HashMap<&str, Kind> = agents
        .iter()
        .flat_map(|a| {
            a.intents
                .iter()
                .map(move |(_, intent)| (intent.as_str(), a.kind))
        })
        .collect();
    let tool_of: HashMap<&str, &str> = agents
        .iter()
        .flat_map(|a| {
            a.intents
                .iter()
                .map(|(tool, intent)| (intent.as_str(), tool.as_str()))
        })
        .collect();

    let all: Vec<&Event> = events_by_session.values().flatten().collect();
    let intent_of = |e: &Event| -> Option<String> {
        e.payload["params"]["arguments"]["intent"]
            .as_str()
            .map(str::to_owned)
    };
    let final_answer = |session: &str, seq: i64| {
        all.iter().copied().find(|e| {
            e.session_id == session
                && e.kind == EventKind::CallResponse
                && e.request_seq == Some(seq)
                && e.outcome.is_some_and(Outcome::is_final)
        })
    };

    stats.calls = all
        .iter()
        .filter(|e| e.kind == EventKind::CallRequest)
        .count();
    stats.retries = all
        .iter()
        .filter(|e| e.kind == EventKind::CallRetry)
        .count();
    stats.flagged = all
        .iter()
        .filter(|e| e.outcome == Some(Outcome::InDoubt))
        .count();
    stats.effects = state.effects.len();
    stats.takeovers = state
        .trace
        .iter()
        .filter(|l| l.contains(" took ") && !l.ends_with("token 1"))
        .count();

    // Nothing that is not safe to repeat stays open once a healthy copy has taken over.
    for request in all.iter().filter(|e| e.kind == EventKind::CallRequest) {
        let class = request.payload["effect"]["class"]
            .as_str()
            .unwrap_or("at_most_once");
        if class == "at_most_once" && final_answer(&request.session_id, request.seq).is_none() {
            violations.push(format!(
                "{} call #{} ({}) is not safe to repeat and was left with no final answer",
                request.session_id, request.seq, request.tool
            ));
        }
    }

    // Effects that must happen at most once, for every agent ledger can recognise.
    let mut runs: BTreeMap<&str, usize> = BTreeMap::new();
    for effect in &state.effects {
        if effect.intent != "sweep" {
            *runs.entry(effect.intent.as_str()).or_default() += 1;
        }
    }
    for (intent, count) in &runs {
        let tool = tool_of.get(intent).copied().unwrap_or_default();
        let unsafe_to_repeat = matches!(truth(tool), Truth::AtMostOnce | Truth::Keyed);
        if *count <= 1 || !unsafe_to_repeat {
            continue;
        }
        if kinds.get(intent) == Some(&Kind::NewId) {
            stats.unrecognised_resends += 1;
        } else if tool == "lie_uncorrected" {
            stats.uncorrected_lies += 1;
        } else {
            violations.push(format!("{intent} ({tool}) took effect {count} times"));
        }
    }

    // A call flagged as in doubt never runs again.
    for flag in all.iter().filter(|e| e.outcome == Some(Outcome::InDoubt)) {
        let call = flag.request_seq.expect("responses link to a call");
        let request = all
            .iter()
            .find(|e| e.session_id == flag.session_id && e.seq == call);
        let Some(intent) = request.and_then(|r| intent_of(r)) else {
            continue;
        };
        if kinds.get(intent.as_str()) == Some(&Kind::NewId) {
            continue;
        }
        let flagged_at = state
            .appended
            .iter()
            .find(|a| a.session == flag.session_id && a.seq == flag.seq)
            .map(|a| a.tick)
            .unwrap_or_default();
        if state
            .effects
            .iter()
            .any(|e| e.intent == intent && e.tick > flagged_at)
        {
            violations.push(format!(
                "{intent} ran after {} call #{call} was flagged as in doubt",
                flag.session_id
            ));
        }
    }

    // Everything an agent was told is in the log.
    for seen in &state.seen {
        let Ok(told) = &seen.answer else { continue };
        let logged = all.iter().any(|e| {
            e.kind == EventKind::CallResponse
                && matches!(e.outcome, Some(Outcome::Ok | Outcome::ToolError))
                && e.request_seq.is_some_and(|seq| {
                    all.iter().any(|r| {
                        r.session_id == e.session_id
                            && r.seq == seq
                            && intent_of(r).as_deref() == Some(seen.intent.as_str())
                    })
                })
                && e.payload["result"]["content"] == told["content"]
        });
        if !logged {
            violations.push(format!(
                "{} was answered with something the log does not hold",
                seen.intent
            ));
        }
    }

    // Every call an agent was told had an unrecorded outcome ends up settled or open for a
    // retry, never answered from nowhere: covered above when it is not safe to repeat.
    let lost: Vec<&str> = state
        .seen
        .iter()
        .filter(|s| s.answer == Err(ledger_gateway::error_codes::OUTCOME_NOT_RECORDED.0))
        .map(|s| s.intent.as_str())
        .collect();
    for intent in lost {
        let Some(&tool) = tool_of.get(intent) else {
            continue;
        };
        let unsafe_class = matches!(truth(tool), Truth::AtMostOnce) && tool != "lie_uncorrected";
        let recognised = kinds.get(intent) != Some(&Kind::NewId);
        if unsafe_class && recognised && runs.get(intent).copied().unwrap_or_default() > 1 {
            violations.push(format!(
                "{intent} ran again after its outcome went unrecorded"
            ));
        }
    }

    (violations, stats)
}

/// Gapless sequences, links to earlier requests of the same tool, at most one final answer
/// per call, and fencing tokens that never go backwards.
fn well_formed(session: &str, events: &[Event], violations: &mut Vec<String>) {
    let mut finals: HashMap<i64, usize> = HashMap::new();
    let mut token = 0;
    for (i, event) in events.iter().enumerate() {
        if event.seq != i as i64 + 1 {
            violations.push(format!("{session} has a gap before #{}", event.seq));
        }
        let fence = event.fence_token.unwrap_or_default();
        if fence < token {
            violations.push(format!(
                "{session} #{} was appended with token {fence} after token {token}",
                event.seq
            ));
        }
        token = token.max(fence);
        if event.kind == EventKind::CallRequest {
            continue;
        }
        let linked = event
            .request_seq
            .and_then(|seq| events.iter().find(|e| e.seq == seq));
        match linked {
            Some(request)
                if request.kind == EventKind::CallRequest
                    && request.seq < event.seq
                    && request.tool == event.tool => {}
            _ => violations.push(format!("{session} #{} links to no earlier call", event.seq)),
        }
        if event.kind == EventKind::CallResponse && event.outcome.is_some_and(Outcome::is_final) {
            let count = finals
                .entry(event.request_seq.unwrap_or_default())
                .or_default();
            *count += 1;
            if *count > 1 {
                violations.push(format!(
                    "{session} call #{} has two final answers",
                    event.request_seq.unwrap_or_default()
                ));
            }
        }
    }
}
