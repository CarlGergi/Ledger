//! The model side of the gateway: an OpenAI-compatible endpoint that logs each model call
//! in the agent's session and forwards it to the real model.
//!
//! The rule is the one tool calls follow: the request is logged before it is sent, the
//! reply is logged before the agent gets it, and what the agent gets is rebuilt from the
//! log. A streamed reply is therefore read to the end, logged, and then sent on, which
//! costs the agent its first-token latency but means the log never holds less than the
//! agent saw.

use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use serde_json::{Value, json};

/// Request headers that carry the agent's credentials for the model. They are passed on
/// when the gateway has no key of its own, and never logged.
const CREDENTIAL_HEADERS: [HeaderName; 2] =
    [header::AUTHORIZATION, HeaderName::from_static("api-key")];

/// Hosts accepted when none are configured, as for the MCP endpoint.
const LOOPBACK_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// An OpenAI-compatible chat completions endpoint.
#[derive(Debug, Clone)]
pub struct ModelUpstream {
    client: reqwest::Client,
    /// Up to and including `/v1`, as for an OpenAI client's base URL.
    base_url: String,
    api_key: Option<String>,
    timeout: Duration,
}

/// What came back from the model, before it is logged.
#[derive(Debug)]
pub(crate) struct Reply {
    pub status: StatusCode,
    pub content_type: Option<String>,
    pub body: Bytes,
}

impl ModelUpstream {
    /// `api_key`, when given, is sent instead of whatever credentials the agent sent.
    pub fn new(base_url: &str, api_key: Option<String>, timeout: Duration) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: base_url.trim_end_matches('/').to_owned(),
            api_key,
            timeout,
        }
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Sends a chat completion request and reads the whole reply, streamed or not.
    pub(crate) async fn send(
        &self,
        body: &Value,
        credentials: &HeaderMap,
    ) -> Result<Reply, reqwest::Error> {
        let mut request = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .json(body);
        match &self.api_key {
            Some(key) => request = request.bearer_auth(key),
            // Already reduced to credential headers by `credentials`, the one filter.
            None => request = request.headers(credentials.clone()),
        }
        let response = request.send().await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let body = response.bytes().await?;
        Ok(Reply {
            status,
            content_type,
            body,
        })
    }
}

/// Only the credential headers, so nothing else the agent sent travels further.
pub(crate) fn credentials(headers: &HeaderMap) -> HeaderMap {
    let mut kept = HeaderMap::new();
    for name in &CREDENTIAL_HEADERS {
        if let Some(value) = headers.get(name) {
            kept.insert(name.clone(), value.clone());
        }
    }
    kept
}

/// The reply as the log holds it: the status, and the body as JSON, as the data lines of
/// an event stream, or as text, whichever it is.
pub(crate) fn logged(reply: &Reply) -> Value {
    let text = String::from_utf8_lossy(&reply.body);
    let is_stream = reply
        .content_type
        .as_deref()
        .is_some_and(|t| t.starts_with("text/event-stream"));
    let body = if is_stream {
        json!({ "stream": stream_data(&text) })
    } else if let Ok(value) = serde_json::from_slice::<Value>(&reply.body) {
        json!({ "json": value })
    } else {
        json!({ "text": text })
    };
    json!({ "status": reply.status.as_u16(), "body": body })
}

/// The `data:` payloads of a server-sent event stream, in order.
fn stream_data(text: &str) -> Vec<String> {
    let mut events = Vec::new();
    for block in text.replace("\r\n", "\n").split("\n\n") {
        let data: Vec<&str> = block
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(|d| d.strip_prefix(' ').unwrap_or(d))
            .collect();
        if !data.is_empty() {
            events.push(data.join("\n"));
        }
    }
    events
}

/// The HTTP reply the agent gets, rebuilt from a logged reply.
pub(crate) fn rebuild(logged: &Value) -> (StatusCode, HeaderMap, Bytes) {
    let status = logged["status"]
        .as_u64()
        .and_then(|s| u16::try_from(s).ok())
        .and_then(|s| StatusCode::from_u16(s).ok())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let body = &logged["body"];
    let (content_type, bytes) = if let Some(events) = body["stream"].as_array() {
        let mut text = String::new();
        for event in events {
            for line in event.as_str().unwrap_or_default().split('\n') {
                text.push_str("data: ");
                text.push_str(line);
                text.push('\n');
            }
            text.push('\n');
        }
        ("text/event-stream", Bytes::from(text))
    } else if let Some(text) = body["text"].as_str() {
        ("text/plain; charset=utf-8", Bytes::from(text.to_owned()))
    } else {
        ("application/json", Bytes::from(body["json"].to_string()))
    };
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    (status, headers, bytes)
}

/// An error in the shape OpenAI clients expect, so they report it rather than choke on it.
pub(crate) fn error_body(code: i32, kind: &str, message: &str) -> Value {
    json!({ "error": { "message": message, "type": kind, "code": code } })
}

/// Whether a request's `Host` and `Origin` are ones the gateway serves, so that a web page
/// cannot use a gateway on the user's machine, and the model key it may hold, through DNS
/// rebinding. The same rule as the MCP endpoint.
pub(crate) fn host_allowed(headers: &HeaderMap, uri: &http::Uri, allowed: &[String]) -> bool {
    let allowed: Vec<String> = if allowed.is_empty() {
        LOOPBACK_HOSTS.iter().map(|h| (*h).to_owned()).collect()
    } else {
        allowed.to_vec()
    };
    let matches = |host: &str, port: Option<u16>| {
        allowed.iter().any(|a| {
            let (a_host, a_port) = match http::uri::Authority::try_from(a.as_str()) {
                Ok(auth) => (normalize(auth.host()), auth.port_u16()),
                Err(_) => (normalize(a), None),
            };
            a_host == normalize(host) && a_port.is_none_or(|p| Some(p) == port)
        })
    };
    let host = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| http::uri::Authority::try_from(h).ok())
        .or_else(|| uri.authority().cloned());
    let Some(host) = host else {
        return false;
    };
    if !matches(host.host(), host.port_u16()) {
        return false;
    }
    match headers.get(header::ORIGIN).map(|o| o.to_str()) {
        None => true,
        Some(Ok(origin)) => http::Uri::try_from(origin)
            .ok()
            .and_then(|uri| uri.authority().cloned())
            .is_some_and(|auth| matches(auth.host(), auth.port_u16())),
        Some(Err(_)) => false,
    }
}

fn normalize(host: &str) -> String {
    host.trim_matches(['[', ']']).to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reply(content_type: &str, body: &str) -> Reply {
        Reply {
            status: StatusCode::OK,
            content_type: Some(content_type.to_owned()),
            body: Bytes::from(body.to_owned()),
        }
    }

    #[test]
    fn a_json_reply_is_logged_and_rebuilt_as_it_came() {
        let logged = logged(&reply("application/json", r#"{"id":"x","choices":[]}"#));
        assert_eq!(logged["body"]["json"]["id"], "x");
        let (status, headers, body) = rebuild(&logged);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            json!({"id": "x", "choices": []})
        );
    }

    #[test]
    fn a_stream_is_logged_event_by_event_and_rebuilt() {
        let sent = "data: {\"a\":1}\n\n: keep-alive\n\ndata: {\"b\":2}\r\n\r\ndata: [DONE]\n\n";
        let logged = logged(&reply("text/event-stream; charset=utf-8", sent));
        assert_eq!(
            logged["body"]["stream"],
            json!(["{\"a\":1}", "{\"b\":2}", "[DONE]"])
        );
        let (_, headers, body) = rebuild(&logged);
        assert_eq!(headers[header::CONTENT_TYPE], "text/event-stream");
        assert_eq!(
            body,
            "data: {\"a\":1}\n\ndata: {\"b\":2}\n\ndata: [DONE]\n\n"
        );
        // Rebuilding what was rebuilt changes nothing.
        let again = logged_from(&body);
        assert_eq!(again["body"], logged["body"]);
    }

    fn logged_from(body: &Bytes) -> Value {
        logged(&reply(
            "text/event-stream",
            std::str::from_utf8(body).unwrap(),
        ))
    }

    #[test]
    fn a_reply_that_is_not_json_is_kept_as_text() {
        let mut bad = reply("text/html", "<h1>Bad Gateway</h1>");
        bad.status = StatusCode::BAD_GATEWAY;
        let logged = logged(&bad);
        assert_eq!(logged["status"], 502);
        let (status, _, body) = rebuild(&logged);
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body, "<h1>Bad Gateway</h1>");
    }

    #[test]
    fn only_credential_headers_are_kept() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer k"));
        headers.insert("api-key", HeaderValue::from_static("k2"));
        headers.insert("x-ledger-session", HeaderValue::from_static("s"));
        headers.insert(header::COOKIE, HeaderValue::from_static("c"));
        let kept = credentials(&headers);
        assert_eq!(kept.len(), 2);
        assert!(kept.contains_key(header::AUTHORIZATION) && kept.contains_key("api-key"));
    }

    #[test]
    fn hosts_follow_the_mcp_endpoint_rule() {
        let check = |host: &str, origin: Option<&str>, allowed: &[&str]| {
            let mut headers = HeaderMap::new();
            headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
            if let Some(origin) = origin {
                headers.insert(header::ORIGIN, HeaderValue::from_str(origin).unwrap());
            }
            let allowed: Vec<String> = allowed.iter().map(|a| (*a).to_owned()).collect();
            host_allowed(
                &headers,
                &http::Uri::from_static("/v1/chat/completions"),
                &allowed,
            )
        };
        assert!(check("127.0.0.1:7100", None, &[]));
        assert!(check("localhost:7100", Some("http://localhost:3000"), &[]));
        assert!(check("[::1]:7100", None, &[]));
        assert!(!check("evil.example:7100", None, &[]));
        assert!(!check("127.0.0.1:7100", Some("http://evil.example"), &[]));
        assert!(!check("127.0.0.1:7100", Some("null"), &[]));
        assert!(check("ledger.internal", None, &["ledger.internal"]));
        assert!(!check("127.0.0.1", None, &["ledger.internal"]));
        assert!(!check("ledger.internal:9", None, &["ledger.internal:8"]));
    }
}
