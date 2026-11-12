use serde::Serialize;

use crate::StoreError;

/// What a fork does with tool calls once it runs live, from its fork step on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolMode {
    /// A call the source made, with the same arguments, is answered from the source's
    /// log; a read-only call runs; anything else is refused. Nothing with effects runs.
    Safe,
    /// No tool runs: a call is answered from the source's log, or refused.
    Replay,
    /// Every tool call runs, effects and all.
    Live,
}

impl ToolMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Safe => "safe",
            Self::Replay => "replay",
            Self::Live => "live",
        }
    }

    pub fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "safe" => Ok(Self::Safe),
            "replay" => Ok(Self::Replay),
            "live" => Ok(Self::Live),
            other => Err(StoreError::Corrupt(format!("unknown tool mode {other:?}"))),
        }
    }
}

/// A session that starts as a copy of another: its calls before `at_step` are answered
/// from `source_session`'s log, and from there on it runs live.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Fork {
    pub session_id: String,
    pub source_session: String,
    /// The source's event sequence number to go live at. Calls the source requested at
    /// earlier events are replayed.
    pub at_step: i64,
    /// The model to ask once live, in place of the one the agent names.
    pub model: Option<String>,
    /// The system prompt to send once live, in place of the agent's.
    pub system_prompt: Option<String>,
    pub tools: ToolMode,
}
