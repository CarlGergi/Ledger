//! A stand-in for an OpenAI-compatible model, for tests and demos. Its reply depends only
//! on the model name and the conversation so far, so the same conversation always gets the
//! same reply, and a different model name can be made to behave differently on purpose.
//!
//! The `reporter-*` models act out one task: write the user's message to a report file,
//! email it, and say it is done, one tool call per turn.
//!
//! - `reporter-1` emails it with the subject "Daily report".
//! - `reporter-2` emails it with the subject "Daily report (v2)": same tools, other arguments.
//! - `reporter-3` reads the file back before emailing: another tool at the second turn.
//!
//! Three more exist for testing the proxy: `fail` answers 500 with an HTML page, `hang`
//! never answers, and any other name gets a 404 as from a real provider.

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// What the model received: each request's body and headers, in order.
#[derive(Clone, Default)]
pub struct Received(Arc<Mutex<Vec<(HeaderMap, Value)>>>);

impl Received {
    pub fn bodies(&self) -> Vec<Value> {
        self.lock().iter().map(|(_, body)| body.clone()).collect()
    }

    pub fn headers(&self) -> Vec<HeaderMap> {
        self.lock()
            .iter()
            .map(|(headers, _)| headers.clone())
            .collect()
    }

    pub fn count(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<(HeaderMap, Value)>> {
        self.0.lock().expect("lock poisoned")
    }
}

/// Serves the model at `/v1/chat/completions` until `shutdown` is cancelled.
pub async fn serve_model(
    listener: TcpListener,
    received: Received,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let app = Router::new()
        .route("/v1/chat/completions", post(complete))
        .with_state((received, shutdown.clone()));
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}

async fn complete(
    State((received, shutdown)): State<(Received, CancellationToken)>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<Value>,
) -> Response {
    received.lock().push((headers, body.clone()));
    let model = body["model"].as_str().unwrap_or_default();
    match model {
        "fail" => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/html")],
            "<h1>Internal Server Error</h1>",
        )
            .into_response(),
        "hang" => {
            shutdown.cancelled().await;
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        _ => match reply(&body) {
            Some(message) => {
                let completion = completion(&body, message);
                if body["stream"] == json!(true) {
                    stream(&completion).into_response()
                } else {
                    axum::Json(completion).into_response()
                }
            }
            None => (
                StatusCode::NOT_FOUND,
                axum::Json(json!({ "error": {
                    "message": format!("The model `{model}` does not exist"),
                    "type": "invalid_request_error",
                    "code": "model_not_found",
                }})),
            )
                .into_response(),
        },
    }
}

/// The assistant message a `reporter-*` model answers with, or None for an unknown model.
pub fn reply(body: &Value) -> Option<Value> {
    let model = body["model"].as_str().unwrap_or_default();
    let version = model.strip_prefix("reporter-")?;
    let messages = body["messages"].as_array().cloned().unwrap_or_default();
    let report = messages
        .iter()
        .find(|m| m["role"] == "user")
        .and_then(|m| m["content"].as_str())
        .unwrap_or_default()
        .to_owned();
    let turn = messages.iter().filter(|m| m["role"] == "tool").count();
    let write = (
        "files__write_file",
        json!({ "path": "reports/daily.md", "content": report }),
    );
    let read = ("files__read_file", json!({ "path": "reports/daily.md" }));
    let send = |subject: &str| {
        let arguments = json!({ "to": "team@example.com", "subject": subject, "body": report });
        ("email__send_email", arguments)
    };
    let plan = match version {
        "1" => vec![write, send("Daily report")],
        "2" => vec![write, send("Daily report (v2)")],
        "3" => vec![write, read, send("Daily report")],
        _ => return None,
    };
    Some(match plan.get(turn) {
        Some((tool, arguments)) => json!({
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "id": format!("call_{turn}"),
                "type": "function",
                "function": { "name": tool, "arguments": arguments.to_string() },
            }],
        }),
        None => json!({ "role": "assistant", "content": "Sent the daily report." }),
    })
}

fn completion(body: &Value, message: Value) -> Value {
    let prompt = body["messages"].to_string();
    let finish = if message["tool_calls"].is_null() {
        "stop"
    } else {
        "tool_calls"
    };
    json!({
        "id": format!("chatcmpl-{:.16x}", Sha256::digest(&prompt)),
        "object": "chat.completion",
        "created": 0,
        "model": body["model"],
        "choices": [{ "index": 0, "message": message, "finish_reason": finish }],
        "usage": {
            "prompt_tokens": prompt.len() / 4,
            "completion_tokens": message.to_string().len() / 4,
            "total_tokens": prompt.len() / 4 + message.to_string().len() / 4,
        },
    })
}

/// The completion as a stream of chunks: the role, the content or each tool call, the
/// finish reason, then `[DONE]`.
fn stream(completion: &Value) -> impl IntoResponse {
    let choice = &completion["choices"][0];
    let message = &choice["message"];
    let chunk = |delta: Value, finish: Value| {
        json!({
            "id": completion["id"],
            "object": "chat.completion.chunk",
            "created": 0,
            "model": completion["model"],
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }],
        })
    };
    let mut chunks = vec![chunk(json!({ "role": "assistant" }), Value::Null)];
    if let Some(content) = message["content"].as_str() {
        for word in content.split_inclusive(' ') {
            chunks.push(chunk(json!({ "content": word }), Value::Null));
        }
    }
    if let Some(calls) = message["tool_calls"].as_array() {
        for (index, call) in calls.iter().enumerate() {
            let mut call = call.clone();
            call["index"] = json!(index);
            chunks.push(chunk(json!({ "tool_calls": [call] }), Value::Null));
        }
    }
    chunks.push(chunk(json!({}), choice["finish_reason"].clone()));
    let mut text = String::new();
    for chunk in chunks {
        text.push_str(&format!("data: {chunk}\n\n"));
    }
    text.push_str("data: [DONE]\n\n");
    ([(header::CONTENT_TYPE, "text/event-stream")], text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ask(model: &str, tool_results: usize) -> Value {
        let mut messages = vec![json!({ "role": "user", "content": "All good." })];
        for _ in 0..tool_results {
            messages.push(json!({ "role": "tool", "content": "ok" }));
        }
        json!({ "model": model, "messages": messages })
    }

    fn tool_of(message: &Value) -> Option<(String, Value)> {
        let call = &message["tool_calls"][0]["function"];
        let arguments = serde_json::from_str(call["arguments"].as_str()?).ok()?;
        Some((call["name"].as_str()?.to_owned(), arguments))
    }

    #[test]
    fn reporters_write_then_email_then_finish() {
        let first = tool_of(&reply(&ask("reporter-1", 0)).unwrap()).unwrap();
        assert_eq!(first.0, "files__write_file");
        assert_eq!(first.1["content"], "All good.");
        let second = tool_of(&reply(&ask("reporter-1", 1)).unwrap()).unwrap();
        assert_eq!(second.0, "email__send_email");
        assert_eq!(second.1["subject"], "Daily report");
        let last = reply(&ask("reporter-1", 2)).unwrap();
        assert!(last["tool_calls"].is_null());
        assert_eq!(last["content"], "Sent the daily report.");
    }

    #[test]
    fn versions_differ_where_they_say() {
        let v2 = tool_of(&reply(&ask("reporter-2", 1)).unwrap()).unwrap();
        assert_eq!(v2.1["subject"], "Daily report (v2)");
        let v3 = tool_of(&reply(&ask("reporter-3", 1)).unwrap()).unwrap();
        assert_eq!(v3.0, "files__read_file");
        assert!(reply(&ask("gpt-unknown", 0)).is_none());
    }

    #[test]
    fn the_same_conversation_gets_the_same_completion() {
        let body = ask("reporter-1", 1);
        let a = completion(&body, reply(&body).unwrap());
        let b = completion(&body, reply(&body).unwrap());
        assert_eq!(a, b);
    }
}
