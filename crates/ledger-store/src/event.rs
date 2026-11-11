use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::StoreError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    CallRequest,
    /// An agent asking again for a call already logged, which `request_seq` names.
    CallRetry,
    CallResponse,
    /// A request to a model, as the agent sent it.
    ModelRequest,
    /// The model's answer to the request `request_seq` names.
    ModelResponse,
}

impl EventKind {
    /// Whether the event opens a call, and so carries an arguments hash.
    pub fn is_request(self) -> bool {
        matches!(self, Self::CallRequest | Self::ModelRequest)
    }

    /// Whether the event answers a call, and so carries an outcome.
    pub fn is_response(self) -> bool {
        matches!(self, Self::CallResponse | Self::ModelResponse)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::CallRequest => "call_request",
            Self::CallRetry => "call_retry",
            Self::CallResponse => "call_response",
            Self::ModelRequest => "model_request",
            Self::ModelResponse => "model_response",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "call_request" => Ok(Self::CallRequest),
            "call_retry" => Ok(Self::CallRetry),
            "call_response" => Ok(Self::CallResponse),
            "model_request" => Ok(Self::ModelRequest),
            "model_response" => Ok(Self::ModelResponse),
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
    /// The gateway turned the call down before forwarding it.
    Refused,
    /// The tool may have run, and running it again is not safe. A person should check.
    InDoubt,
}

impl Outcome {
    /// Whether this answers the call for good. The others say only that no answer came
    /// back, so a later attempt may still record one.
    pub fn is_final(self) -> bool {
        !matches!(self, Self::TransportError | Self::Timeout)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ToolError => "tool_error",
            Self::RpcError => "rpc_error",
            Self::TransportError => "transport_error",
            Self::Timeout => "timeout",
            Self::NotRouted => "not_routed",
            Self::Refused => "refused",
            Self::InDoubt => "in_doubt",
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
            "refused" => Ok(Self::Refused),
            "in_doubt" => Ok(Self::InDoubt),
            other => Err(StoreError::Corrupt(format!("unknown outcome {other:?}"))),
        }
    }
}

/// A SHA-256 of the arguments in a canonical form: object keys sorted, no whitespace.
/// Arguments that differ only in key order or spacing hash the same; `1` and `1.0` do not.
pub fn args_hash(arguments: &Value) -> String {
    // serde_json keeps object keys sorted, so its compact output is already canonical.
    format!("{:x}", Sha256::digest(arguments.to_string()))
}

/// An event waiting to be appended. The store assigns its sequence number.
#[derive(Debug, Clone, PartialEq)]
pub struct NewEvent {
    pub session_id: String,
    pub kind: EventKind,
    pub request_seq: Option<i64>,
    pub tool: String,
    pub outcome: Option<Outcome>,
    /// For requests only, from [`args_hash`].
    pub args_hash: Option<String>,
    pub payload: Value,
}

impl NewEvent {
    pub fn request(session_id: &str, tool: &str, arguments: &Value, payload: Value) -> Self {
        Self {
            session_id: session_id.to_owned(),
            kind: EventKind::CallRequest,
            request_seq: None,
            tool: tool.to_owned(),
            outcome: None,
            args_hash: Some(args_hash(arguments)),
            payload,
        }
    }

    /// A request to a model. `body` is what the agent sent; the hash covers all of it.
    pub fn model_request(session_id: &str, model: &str, body: &Value, payload: Value) -> Self {
        Self {
            kind: EventKind::ModelRequest,
            ..Self::request(session_id, model, body, payload)
        }
    }

    pub fn model_response(
        session_id: &str,
        request_seq: i64,
        model: &str,
        outcome: Outcome,
        payload: Value,
    ) -> Self {
        Self {
            kind: EventKind::ModelResponse,
            ..Self::response(session_id, request_seq, model, outcome, payload)
        }
    }

    pub fn retry(session_id: &str, request_seq: i64, tool: &str, payload: Value) -> Self {
        Self {
            session_id: session_id.to_owned(),
            kind: EventKind::CallRetry,
            request_seq: Some(request_seq),
            tool: tool.to_owned(),
            outcome: None,
            args_hash: None,
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
            args_hash: None,
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
    pub args_hash: Option<String>,
    /// The token of the lease it was appended under. None for events from before leases.
    pub fence_token: Option<i64>,
    pub payload: Value,
    pub recorded_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SessionSummary {
    pub session_id: String,
    pub event_count: i64,
    pub created_at: DateTime<Utc>,
}
