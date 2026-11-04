//! An email sender. Messages go to an outbox file, one JSON object per line, so
//! every send is a countable side effect.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct SentEmail {
    pub message_id: String,
    pub to: String,
    pub subject: String,
    pub body: String,
    pub sent_at: DateTime<Utc>,
}

/// The append-only file that stands in for a mail provider.
#[derive(Debug)]
pub struct Outbox {
    path: PathBuf,
    write_lock: Mutex<()>,
}

impl Outbox {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            write_lock: Mutex::new(()),
        }
    }

    pub async fn deliver(&self, email: &SentEmail) -> io::Result<()> {
        let mut line = serde_json::to_vec(email)?;
        line.push(b'\n');
        let _guard = self.write_lock.lock().await;
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(&line).await?;
        file.sync_data().await
    }

    pub async fn sent(&self) -> io::Result<Vec<SentEmail>> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(contents) => contents,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        contents
            .lines()
            .map(|line| serde_json::from_str(line).map_err(io::Error::from))
            .collect()
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SendEmailRequest {
    /// Recipient address.
    pub to: String,
    /// Subject line.
    pub subject: String,
    /// Plain-text message body.
    pub body: String,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct SentList {
    pub emails: Vec<SentEmail>,
}

#[derive(Debug, Clone)]
pub struct EmailServer {
    outbox: Arc<Outbox>,
    tool_router: ToolRouter<Self>,
}

impl EmailServer {
    pub fn new(outbox: Arc<Outbox>) -> Self {
        Self {
            outbox,
            tool_router: Self::tool_router(),
        }
    }
}

fn validate(request: &SendEmailRequest) -> Result<(), String> {
    let valid_address = request
        .to
        .split_once('@')
        .is_some_and(|(local, domain)| !local.is_empty() && !domain.is_empty());
    if !valid_address || request.to.contains(char::is_whitespace) {
        return Err(format!("invalid recipient address: {:?}", request.to));
    }
    if request.subject.contains(['\r', '\n']) {
        return Err("subject must not contain line breaks".to_owned());
    }
    Ok(())
}

#[tool_router]
impl EmailServer {
    /// Send an email. Every successful call delivers one new message.
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
    ) -> Result<Json<SentEmail>, String> {
        validate(&request)?;
        let email = SentEmail {
            message_id: Uuid::new_v4().to_string(),
            to: request.to,
            subject: request.subject,
            body: request.body,
            sent_at: Utc::now(),
        };
        self.outbox
            .deliver(&email)
            .await
            .map_err(|e| format!("delivery failed: {e}"))?;
        Ok(Json(email))
    }

    /// List every email sent so far, oldest first.
    #[tool(annotations(
        title = "List sent email",
        read_only_hint = true,
        open_world_hint = false
    ))]
    async fn list_sent(&self) -> Result<Json<SentList>, String> {
        let emails = self
            .outbox
            .sent()
            .await
            .map_err(|e| format!("could not read outbox: {e}"))?;
        Ok(Json(SentList { emails }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for EmailServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("ledger-email", env!("CARGO_PKG_VERSION")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(to: &str, subject: &str) -> SendEmailRequest {
        SendEmailRequest {
            to: to.to_owned(),
            subject: subject.to_owned(),
            body: "hello".to_owned(),
        }
    }

    fn server(dir: &tempfile::TempDir) -> (EmailServer, Arc<Outbox>) {
        let outbox = Arc::new(Outbox::new(dir.path().join("outbox.jsonl")));
        (EmailServer::new(outbox.clone()), outbox)
    }

    #[tokio::test]
    async fn each_send_delivers_one_new_message() {
        let dir = tempfile::tempdir().unwrap();
        let (server, outbox) = server(&dir);
        assert!(outbox.sent().await.unwrap().is_empty());

        for _ in 0..2 {
            server
                .send_email(Parameters(request("a@example.com", "hi")))
                .await
                .unwrap();
        }

        let sent = outbox.sent().await.unwrap();
        assert_eq!(sent.len(), 2);
        assert_ne!(sent[0].message_id, sent[1].message_id);
        assert_eq!(sent[0].to, "a@example.com");
    }

    #[tokio::test]
    async fn invalid_messages_are_not_delivered() {
        let dir = tempfile::tempdir().unwrap();
        let (server, outbox) = server(&dir);

        for bad in [
            request("not-an-address", "hi"),
            request("@example.com", "hi"),
            request("a@", "hi"),
            request("a b@example.com", "hi"),
            request("a@example.com", "hi\r\nBcc: victim@example.com"),
        ] {
            assert!(server.send_email(Parameters(bad)).await.is_err());
        }
        assert!(outbox.sent().await.unwrap().is_empty());
    }

    #[test]
    fn annotations_describe_the_effects() {
        let send = EmailServer::send_email_tool_attr().annotations.unwrap();
        assert_eq!(send.read_only_hint, Some(false));
        assert_eq!(send.destructive_hint, Some(true));
        assert_eq!(send.idempotent_hint, Some(false));

        let list = EmailServer::list_sent_tool_attr().annotations.unwrap();
        assert_eq!(list.read_only_hint, Some(true));
    }
}
