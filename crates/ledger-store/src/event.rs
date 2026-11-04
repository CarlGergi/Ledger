use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    CallRequest,
    CallResponse,
}

impl EventKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CallRequest => "call_request",
            Self::CallResponse => "call_response",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "call_request" => Ok(Self::CallRequest),
            "call_response" => Ok(Self::CallResponse),
            other => Err(StoreError::Corrupt(format!("unknown event kind {other:?}"))),
        }
    }
}

/// How a forwarded call ended, as far as the gateway could observe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// The tool ran and reported success.
    Ok,
    /// The tool ran and reported failure (`isError: true`).
    ToolError,
    /// The tool server rejected the call with a JSON-RPC error.
    RpcError,
    /// No answer came back. The effect may or may not have happened.
    TransportError,
    /// No answer came back in time. The effect may or may not have happened.
    Timeout,
    /// The gateway had nowhere to send the call, so it was never forwarded.
    NotRouted,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ToolError => "tool_error",
            Self::RpcError => "rpc_error",
            Self::TransportError => "transport_error",
            Self::Timeout => "timeout",
            Self::NotRouted => "not_routed",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "ok" => Ok(Self::Ok),
            "tool_error" => Ok(Self::ToolError),
            "rpc_error" => Ok(Self::RpcError),
            "transport_error" => Ok(Self::TransportError),
            "timeout" => Ok(Self::Timeout),
            "not_routed" => Ok(Self::NotRouted),
            other => Err(StoreError::Corrupt(format!("unknown outcome {other:?}"))),
        }
    }
}

/// An event waiting to be appended. The store assigns its sequence number.
#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    pub session_id: String,
    pub kind: EventKind,
    pub request_seq: Option<i64>,
    pub tool: String,
    pub outcome: Option<Outcome>,
    pub payload: Value,
}

impl NewEvent {
    pub fn request(session_id: &str, tool: &str, payload: Value) -> Self {
        Self {
            session_id: session_id.to_owned(),
            kind: EventKind::CallRequest,
            request_seq: None,
            tool: tool.to_owned(),
            outcome: None,
            payload,
        }
    }

    pub fn response(
        session_id: &str,
        request_seq: i64,
        tool: &str,
        outcome: Outcome,
        payload: Value,
    ) -> Self {
        Self {
            session_id: session_id.to_owned(),
            kind: EventKind::CallResponse,
            request_seq: Some(request_seq),
            tool: tool.to_owned(),
            outcome: Some(outcome),
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Event {
    pub session_id: String,
    pub seq: i64,
    pub kind: EventKind,
    pub request_seq: Option<i64>,
    pub tool: String,
    pub outcome: Option<Outcome>,
    pub payload: Value,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub event_count: i64,
    pub created_at: DateTime<Utc>,
}
