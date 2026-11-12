//! Email over SMTP: an MCP tool server whose `send_email` hands each message to an SMTP
//! server, such as Mailpit (a local mail catcher with a web inbox), and a minimal SMTP
//! server that keeps what it receives, for tests.
//!
//! SMTP has no idempotency key, so the tool server keeps one itself: a repeat of a key it
//! has sent returns the first message and sends nothing. The keys live as long as the
//! process. Ledger declares tools with an `idempotency_key` input as keyed, and fills the
//! key in itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lettre::message::{Mailbox, header::ContentType};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{Implementation, ServerCapabilities, ServerConfig};
use rmcp::schemars::{self, JsonSchema};
use rmcp::{ServerHandler, tool, tool_handler, tool_router};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Sent {
    pub message_id: String,
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

/// An SMTP server to send through, without TLS or sign-in, as a local mail catcher takes.
#[derive(Clone)]
pub struct Smtp {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    /// Keys sent, to the message each sent. Held across a send, so that two sends with one
    /// key cannot both go out.
    keys: Arc<tokio::sync::Mutex<HashMap<String, Sent>>>,
}

impl std::fmt::Debug for Smtp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Smtp")
            .field("from", &self.from.to_string())
            .finish_non_exhaustive()
    }
}

impl Smtp {
    pub fn new(host: &str, port: u16, from: &str) -> Result<Self, String> {
        let from = from.parse().map_err(|e| format!("from address: {e}"))?;
        let transport = AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(host)
            .port(port)
            .build();
        Ok(Self {
            transport,
            from,
            keys: Arc::default(),
        })
    }

    /// Sends one message. With a key this process has sent, sends nothing and returns the
    /// first message.
    pub async fn send(
        &self,
        to: &str,
        subject: &str,
        body: &str,
        key: Option<&str>,
    ) -> Result<Sent, String> {
        let mut keys = self.keys.lock().await;
        if let Some(sent) = key.and_then(|k| keys.get(k)) {
            return Ok(sent.clone());
        }
        let id = format!("{}@ledger.local", Uuid::new_v4());
        let message = Message::builder()
            .from(self.from.clone())
            .to(to.parse().map_err(|e| format!("to address: {e}"))?)
            .subject(subject)
            .message_id(Some(format!("<{id}>")))
            .header(ContentType::TEXT_PLAIN)
            .body(body.to_owned())
            .map_err(|e| format!("message: {e}"))?;
        self.transport
            .send(message)
            .await
            .map_err(|e| format!("SMTP: {e}"))?;
        let sent = Sent { message_id: id };
        if let Some(key) = key {
            keys.insert(key.to_owned(), sent.clone());
        }
        Ok(sent)
    }
}

/// The MCP tool server: `send_email` through one SMTP server.
#[derive(Debug, Clone)]
pub struct SmtpServer {
    smtp: Smtp,
    tool_router: ToolRouter<Self>,
}

impl SmtpServer {
    pub fn new(smtp: Smtp) -> Self {
        Self {
            smtp,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl SmtpServer {
    /// Send an email. Every call sends a new message, except a repeat of an earlier call's
    /// idempotency key, which returns that call's message.
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
            .smtp
            .send(&request.to, &request.subject, &request.body, key)
            .await?;
        Ok(Json(sent))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for SmtpServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("ledger-smtp", env!("CARGO_PKG_VERSION")),
        )
    }
}

/// One message as an SMTP server received it.
#[derive(Debug, Clone, PartialEq)]
pub struct Received {
    pub to: Vec<String>,
    /// Headers and body, as sent.
    pub data: String,
}

impl Received {
    /// The value of the first header with this name.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.data
            .lines()
            .take_while(|l| !l.is_empty())
            .find_map(|l| {
                let (n, v) = l.split_once(':')?;
                n.eq_ignore_ascii_case(name).then(|| v.trim())
            })
    }
}

/// The least of an SMTP server that a client sending plain messages needs, keeping each
/// message it accepts. For tests.
#[derive(Debug, Clone, Default)]
pub struct SmtpSink {
    received: Arc<Mutex<Vec<Received>>>,
}

impl SmtpSink {
    pub fn received(&self) -> Vec<Received> {
        self.received.lock().expect("lock poisoned").clone()
    }

    /// Accepts connections until `shutdown` is cancelled.
    pub async fn serve(self, listener: TcpListener, shutdown: CancellationToken) {
        loop {
            let stream = tokio::select! {
                () = shutdown.cancelled() => return,
                accepted = listener.accept() => match accepted {
                    Ok((stream, _)) => stream,
                    Err(_) => continue,
                },
            };
            let sink = self.clone();
            tokio::spawn(async move {
                let _ = sink.session(stream).await;
            });
        }
    }

    async fn session(&self, stream: tokio::net::TcpStream) -> std::io::Result<()> {
        let (read, mut write) = stream.into_split();
        let mut lines = BufReader::new(read).lines();
        write.write_all(b"220 sink ESMTP\r\n").await?;
        let mut to = Vec::new();
        while let Some(line) = lines.next_line().await? {
            let verb = line.get(..4).unwrap_or(&line).to_ascii_uppercase();
            let reply: &[u8] = match verb.as_str() {
                "EHLO" | "HELO" => b"250 sink\r\n",
                "MAIL" | "RSET" | "NOOP" => {
                    to.clear();
                    b"250 OK\r\n"
                }
                "RCPT" => {
                    to.push(
                        line.split_once(':')
                            .map_or("", |(_, a)| a)
                            .trim()
                            .to_owned(),
                    );
                    b"250 OK\r\n"
                }
                "DATA" => {
                    write.write_all(b"354 go on\r\n").await?;
                    let mut data = String::new();
                    while let Some(line) = lines.next_line().await? {
                        if line == "." {
                            break;
                        }
                        let line = line.strip_prefix('.').unwrap_or(&line);
                        data.push_str(line);
                        data.push('\n');
                    }
                    let message = Received {
                        to: std::mem::take(&mut to),
                        data,
                    };
                    self.received.lock().expect("lock poisoned").push(message);
                    b"250 OK\r\n"
                }
                "QUIT" => {
                    write.write_all(b"221 bye\r\n").await?;
                    return Ok(());
                }
                _ => b"502 not here\r\n",
            };
            write.write_all(reply).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn sink() -> (SmtpSink, Smtp) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let sink = SmtpSink::default();
        tokio::spawn(sink.clone().serve(listener, CancellationToken::new()));
        (
            sink,
            Smtp::new("127.0.0.1", port, "Ledger demo <demo@ledger.local>").unwrap(),
        )
    }

    #[tokio::test]
    async fn without_a_key_every_send_sends() {
        let (sink, smtp) = sink().await;
        let a = smtp
            .send("carl@example.com", "Hi", "Once.", None)
            .await
            .unwrap();
        let b = smtp
            .send("carl@example.com", "Hi", "Once.", None)
            .await
            .unwrap();
        assert_ne!(a, b);
        let received = sink.received();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].to, ["<carl@example.com>"]);
        assert_eq!(received[0].header("Subject"), Some("Hi"));
        let id = format!("<{}>", a.message_id);
        assert_eq!(received[0].header("Message-ID"), Some(id.as_str()));
        assert!(received[0].data.contains("Once."));
    }

    #[tokio::test]
    async fn a_repeated_key_returns_the_first_message_and_sends_nothing() {
        let (sink, smtp) = sink().await;
        let a = smtp
            .send("carl@example.com", "Hi", "t", Some("k1"))
            .await
            .unwrap();
        let b = smtp
            .send("carl@example.com", "Hi", "t", Some("k1"))
            .await
            .unwrap();
        let c = smtp
            .send("carl@example.com", "Hi", "t", Some("k2"))
            .await
            .unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(sink.received().len(), 2);
    }

    #[tokio::test]
    async fn a_send_that_fails_keeps_no_key() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let smtp = Smtp::new("127.0.0.1", port, "demo@ledger.local").unwrap();
        assert!(
            smtp.send("carl@example.com", "Hi", "t", Some("k1"))
                .await
                .is_err()
        );
        assert!(smtp.keys.lock().await.is_empty());
    }
}
