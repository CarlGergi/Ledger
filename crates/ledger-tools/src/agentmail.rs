//! Email through AgentMail (agentmail.to): an MCP tool server that sends real email from
//! an AgentMail inbox, and a stand-in for AgentMail's API, for tests and for trying the demo
//! without an account.
//!
//! `send_email` takes an `idempotency_key`, which it sends as AgentMail's `Idempotency-Key`
//! header, so a repeated key returns the first message and sends nothing. Ledger declares
//! tools with such an input as keyed, and fills the key in itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// AgentMail's API, up to and including the version.
pub const API_URL: &str = "https://api.agentmail.to/v0";

/// An AgentMail inbox to send from.
#[derive(Clone)]
pub struct AgentMail {
    client: reqwest::Client,
    api: String,
    key: String,
    inbox: String,
}

impl std::fmt::Debug for AgentMail {
    // The key stays out of debug output, and so out of logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentMail")
            .field("api", &self.api)
            .field("inbox", &self.inbox)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Sent {
    pub message_id: String,
    pub thread_id: String,
}

impl AgentMail {
    pub fn new(api: &str, key: String, inbox: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            api: api.trim_end_matches('/').to_owned(),
            key,
            inbox: inbox.to_owned(),
        }
    }

    /// Sends one message. With a key, a repeat of it sends nothing and returns the first.
    pub async fn send(
        &self,
        to: &str,
        subject: &str,
        text: &str,
        key: Option<&str>,
    ) -> Result<Sent, String> {
        let url = format!("{}/inboxes/{}/messages/send", self.api, self.inbox);
        let mut request = self
            .client
            .post(url)
            .bearer_auth(&self.key)
            .json(&json!({ "to": [to], "subject": subject, "text": text }));
        if let Some(key) = key {
            request = request.header("Idempotency-Key", agentmail_key(key));
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("AgentMail: {e}"))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            let message = body["message"].as_str().unwrap_or("no message");
            return Err(format!("AgentMail answered {status}: {message}"));
        }
        serde_json::from_value(body).map_err(|e| format!("AgentMail's answer: {e}"))
    }

    /// How many messages this inbox has sent with exactly this subject.
    pub async fn count_sent(&self, subject: &str) -> Result<usize, String> {
        let url = format!("{}/inboxes/{}/messages", self.api, self.inbox);
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.key)
            .query(&[("subject", subject), ("labels", "sent"), ("limit", "100")])
            .send()
            .await
            .map_err(|e| format!("AgentMail: {e}"))?;
        let status = response.status();
        let body: Value = response
            .json()
            .await
            .map_err(|e| format!("AgentMail: {e}"))?;
        if !status.is_success() {
            return Err(format!("AgentMail answered {status}: {body}"));
        }
        let messages = body["messages"].as_array().cloned().unwrap_or_default();
        Ok(messages.iter().filter(|m| m["subject"] == subject).count())
    }
}

/// AgentMail allows `A-Z a-z 0-9 - . _ ~` in a key, which Ledger's keys
/// (`ledger:<session>:<call>`) do not keep to. A digest maps each key to one that does,
/// the same every time.
pub fn agentmail_key(key: &str) -> String {
    format!("key-{:x}", Sha256::digest(key.as_bytes()))
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendEmailRequest {
    /// Recipient address.
    pub to: String,
    /// Subject line.
    pub subject: String,
    /// Plain-text message body.
    pub body: String,
    /// Sending again with the same key returns the first message and sends nothing.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

/// The MCP tool server: `send_email` through one AgentMail inbox.
#[derive(Debug, Clone)]
pub struct AgentMailServer {
    mail: AgentMail,
    tool_router: ToolRouter<Self>,
}

impl AgentMailServer {
    pub fn new(mail: AgentMail) -> Self {
        Self {
            mail,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl AgentMailServer {
    /// Send an email from the AgentMail inbox. Every call sends a new message, except a
    /// repeat of an earlier call's idempotency key, which returns that call's message.
    #[tool(annotations(
        title = "Send email",
        read_only_hint = false,
        destructive_hint = true,
        idempotent_hint = false,
        open_world_hint = true
    ))]
    async fn send_email(
        &self,
        Parameters(request): Parameters<SendEmailRequest>,
    ) -> Result<Json<Sent>, String> {
        let key = request.idempotency_key.as_deref();
        let sent = self
            .mail
            .send(&request.to, &request.subject, &request.body, key)
            .await?;
        Ok(Json(sent))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for AgentMailServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("ledger-agentmail", env!("CARGO_PKG_VERSION")),
        )
    }
}

/// A stand-in for the parts of AgentMail's API the demo uses, with its documented rules
/// for idempotency keys: a repeat with the same content returns the first message, a
/// repeat with other content is a 409, and a key outside `A-Z a-z 0-9 - . _ ~`, or longer
/// than 256 characters, is a 400.
#[derive(Clone)]
pub struct FakeAgentMail {
    key: String,
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    sent: Vec<Value>,
    /// Idempotency key to (what was sent, what came back).
    keys: HashMap<String, (Value, Value)>,
}

impl FakeAgentMail {
    /// Accepts only `key` as its API key.
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            state: Arc::default(),
        }
    }

    /// Every message sent, oldest first.
    pub fn sent(&self) -> Vec<Value> {
        self.state.lock().expect("lock poisoned").sent.clone()
    }

    /// Serves the API at `/v0` until `shutdown` is cancelled.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let app = Router::new()
            .route("/v0/inboxes/{inbox}/messages/send", post(fake_send))
            .route("/v0/inboxes/{inbox}/messages", get(fake_list))
            .with_state(self);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        headers.get("authorization").and_then(|v| v.to_str().ok())
            == Some(format!("Bearer {}", self.key).as_str())
    }
}

fn api_error(status: StatusCode, name: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({ "name": name, "message": message })),
    )
        .into_response()
}

fn valid_key(key: &str) -> bool {
    (1..=256).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~'))
}

async fn fake_send(
    State(api): State<FakeAgentMail>,
    Path(inbox): Path<String>,
    headers: HeaderMap,
    axum::Json(body): axum::Json<Value>,
) -> Response {
    if !api.authorized(&headers) {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "UnauthorizedError",
            "invalid API key",
        );
    }
    let key = headers
        .get("idempotency-key")
        .map(|v| v.to_str().unwrap_or_default().to_owned());
    if let Some(key) = &key
        && !valid_key(key)
    {
        return api_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            "invalid Idempotency-Key",
        );
    }
    let asked = json!({ "inbox": inbox, "body": body });
    let mut state = api.state.lock().expect("lock poisoned");
    if let Some(key) = &key
        && let Some((earlier, answer)) = state.keys.get(key)
    {
        if *earlier != asked {
            let message = "Idempotency-Key was already used for a different request";
            return api_error(StatusCode::CONFLICT, "ConflictError", message);
        }
        return axum::Json(answer.clone()).into_response();
    }
    let answer = json!({
        "message_id": format!("<{}@agentmail.to>", Uuid::new_v4()),
        "thread_id": Uuid::new_v4().to_string(),
    });
    let mut message = body.clone();
    message["inbox_id"] = json!(inbox);
    message["message_id"] = answer["message_id"].clone();
    message["labels"] = json!(["sent"]);
    state.sent.push(message);
    if let Some(key) = key {
        state.keys.insert(key, (asked, answer.clone()));
    }
    axum::Json(answer).into_response()
}

async fn fake_list(
    State(api): State<FakeAgentMail>,
    Path(inbox): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if !api.authorized(&headers) {
        return api_error(
            StatusCode::UNAUTHORIZED,
            "UnauthorizedError",
            "invalid API key",
        );
    }
    let state = api.state.lock().expect("lock poisoned");
    let messages: Vec<&Value> = state
        .sent
        .iter()
        .filter(|m| m["inbox_id"] == inbox.as_str())
        .filter(|m| {
            query.get("subject").is_none_or(|s| {
                m["subject"]
                    .as_str()
                    .unwrap_or_default()
                    .contains(s.as_str())
            })
        })
        .collect();
    axum::Json(json!({ "count": messages.len(), "messages": messages, "next_page_token": null }))
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fake() -> (FakeAgentMail, AgentMail) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}/v0", listener.local_addr().unwrap());
        let fake = FakeAgentMail::new("am-test-key");
        tokio::spawn(fake.clone().serve(listener, CancellationToken::new()));
        let mail = AgentMail::new(&api, "am-test-key".to_owned(), "demo@agentmail.to");
        (fake, mail)
    }

    #[test]
    fn ledger_keys_become_keys_agentmail_accepts() {
        let key = agentmail_key("ledger:run-1:7");
        assert!(valid_key(&key), "{key}");
        assert_eq!(key, agentmail_key("ledger:run-1:7"));
        assert_ne!(key, agentmail_key("ledger:run-1:8"));
        assert!(!valid_key("ledger:run-1:7"));
    }

    #[tokio::test]
    async fn without_a_key_every_send_sends() {
        let (fake, mail) = fake().await;
        let a = mail.send("to@example.com", "s", "t", None).await.unwrap();
        let b = mail.send("to@example.com", "s", "t", None).await.unwrap();
        assert_ne!(a, b);
        assert_eq!(fake.sent().len(), 2);
        assert_eq!(mail.count_sent("s").await.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_repeated_key_returns_the_first_message_and_sends_nothing() {
        let (fake, mail) = fake().await;
        let a = mail
            .send("to@example.com", "s", "t", Some("k1"))
            .await
            .unwrap();
        let b = mail
            .send("to@example.com", "s", "t", Some("k1"))
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_eq!(fake.sent().len(), 1);
        // The same key with other content is refused, as AgentMail documents.
        let other = mail
            .send("to@example.com", "s", "other", Some("k1"))
            .await
            .unwrap_err();
        assert!(other.contains("409"), "{other}");
    }

    #[tokio::test]
    async fn the_api_key_is_required_and_never_shown() {
        let (_fake, mail) = fake().await;
        let wrong = AgentMail {
            key: "wrong".into(),
            ..mail.clone()
        };
        assert!(
            wrong
                .send("to@example.com", "s", "t", None)
                .await
                .unwrap_err()
                .contains("401")
        );
        assert!(!format!("{mail:?}").contains("am-test-key"));
    }
}
