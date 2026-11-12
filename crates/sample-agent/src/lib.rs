//! Two agents that speak plain MCP to whatever server they are pointed at: a scripted one,
//! and one that asks an OpenAI-compatible model what to do next.

use std::collections::HashMap;

use anyhow::Context;
use http::{HeaderName, HeaderValue};
use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, CallToolResult, ClientConfig};
use rmcp::service::{RoleClient, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use serde_json::{Map, Value, json};

pub type Client = RunningService<RoleClient, ClientConfig>;

/// Connects as an ordinary MCP client, naming a session only if one is given.
pub async fn connect(url: &str, session: Option<&str>) -> anyhow::Result<Client> {
    let mut headers = HashMap::new();
    if let Some(session) = session {
        headers.insert(
            HeaderName::from_static("x-ledger-session"),
            HeaderValue::from_str(session).context("session id is not a valid header value")?,
        );
    }
    let config =
        StreamableHttpClientTransportConfig::with_uri(url.to_owned()).custom_headers(headers);
    let transport = StreamableHttpClientTransport::from_config(config);
    ClientConfig::default()
        .serve(transport)
        .await
        .with_context(|| format!("connecting to {url}"))
}

/// Builds a tool call from a JSON object of arguments.
pub fn call_params(tool: &str, arguments: Value) -> CallToolRequestParams {
    let arguments: Map<String, Value> = match arguments {
        Value::Object(map) => map,
        other => panic!("tool arguments must be an object, got {other}"),
    };
    CallToolRequestParams::new(tool.to_owned()).with_arguments(arguments)
}

#[derive(Debug, Clone)]
pub struct Step {
    pub tool: String,
    pub arguments: Value,
    pub result: CallToolResult,
}

/// Writes a report, reads it back, emails it, and checks that the email went out.
pub async fn run_script(client: &Client, report: &str) -> anyhow::Result<Vec<Step>> {
    let plan = [
        (
            "files__write_file",
            json!({ "path": "reports/daily.md", "content": report }),
        ),
        ("files__read_file", json!({ "path": "reports/daily.md" })),
        (
            "email__send_email",
            json!({ "to": "team@example.com", "subject": "Daily report", "body": report }),
        ),
        ("email__list_sent", json!({})),
    ];

    let mut steps = Vec::with_capacity(plan.len());
    for (tool, arguments) in plan {
        let result = client
            .call_tool(call_params(tool, arguments.clone()))
            .await
            .with_context(|| format!("calling {tool}"))?;
        anyhow::ensure!(result.is_error != Some(true), "{tool} failed: {result:?}");
        steps.push(Step {
            tool: tool.to_owned(),
            arguments,
            result,
        });
    }
    Ok(steps)
}

/// Turns past which the model-driven agent gives up, so a model that never stops cannot
/// keep it going.
pub const MAX_TURNS: usize = 16;

/// What the model-driven agent did: every message of the conversation, and the model's
/// last answer.
#[derive(Debug, Clone)]
pub struct Conversation {
    pub messages: Vec<Value>,
    pub answer: String,
}

/// Asks `model` what to do with `task`, offering it every tool the MCP server lists, runs
/// each tool call it asks for, and stops when it answers in words. Both the model and the
/// tools are reached through `model_url` and `client`, under `session` if one is given.
pub async fn run_model_agent(
    client: &Client,
    model_url: &str,
    session: Option<&str>,
    model: &str,
    task: &str,
) -> anyhow::Result<Conversation> {
    let tools: Vec<Value> = client
        .list_all_tools()
        .await?
        .into_iter()
        .map(|tool| {
            json!({
                "type": "function",
                "function": {
                    "name": tool.name,
                    "description": tool.description,
                    "parameters": tool.input_schema,
                },
            })
        })
        .collect();
    let mut messages = vec![
        json!({ "role": "system", "content": "You write and send the daily report." }),
        json!({ "role": "user", "content": task }),
    ];
    let http = reqwest::Client::new();
    let url = format!("{}/chat/completions", model_url.trim_end_matches('/'));

    for _ in 0..MAX_TURNS {
        let mut request = http.post(&url).json(&json!({
            "model": model,
            "messages": messages,
            "tools": tools,
        }));
        if let Some(session) = session {
            request = request.header("x-ledger-session", session);
        }
        let response = request.send().await.context("asking the model")?;
        let status = response.status();
        let completion: Value = response
            .json()
            .await
            .context("reading the model's answer")?;
        anyhow::ensure!(
            status.is_success(),
            "the model answered {status}: {completion}"
        );
        let message = completion["choices"][0]["message"].clone();
        messages.push(message.clone());

        let Some(calls) = message["tool_calls"].as_array().filter(|c| !c.is_empty()) else {
            let answer = message["content"].as_str().unwrap_or_default().to_owned();
            return Ok(Conversation { messages, answer });
        };
        for call in calls {
            let function = &call["function"];
            let tool = function["name"]
                .as_str()
                .context("a tool call with no name")?;
            let arguments: Value =
                serde_json::from_str(function["arguments"].as_str().unwrap_or("{}"))
                    .with_context(|| format!("arguments for {tool}"))?;
            let result = client
                .call_tool(call_params(tool, arguments))
                .await
                .with_context(|| format!("calling {tool}"))?;
            messages.push(json!({
                "role": "tool",
                "tool_call_id": call["id"],
                "content": serde_json::to_string(&result.content)?,
            }));
        }
    }
    anyhow::bail!("the model did not finish within {MAX_TURNS} turns")
}

/// How a retrying agent's email went.
#[derive(Debug, Clone)]
pub struct Delivery {
    /// Attempts made, the last one included.
    pub attempts: usize,
    /// The tool's answer to the last attempt, or why it failed.
    pub result: Result<CallToolResult, String>,
}

/// Sends one email the way many agents do: call the tool, and if no answer comes back, connect
/// again and send it again, up to `attempts` times. Every attempt names `step`, which is how a
/// gateway like Ledger can tell a resend from a new email; a tool server on its own cannot.
pub async fn send_until_answered(
    url: &str,
    session: Option<&str>,
    step: &str,
    tool: &str,
    arguments: Value,
    attempts: usize,
) -> Delivery {
    let mut last = Err("no attempt was made".to_owned());
    for attempt in 1..=attempts {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderName::from_static("x-ledger-step"),
            HeaderValue::from_str(step).expect("a valid step"),
        );
        if let Some(session) = session {
            headers.insert(
                HeaderName::from_static("x-ledger-session"),
                HeaderValue::from_str(session).expect("a valid session"),
            );
        }
        let config =
            StreamableHttpClientTransportConfig::with_uri(url.to_owned()).custom_headers(headers);
        let connected = ClientConfig::default()
            .serve(StreamableHttpClientTransport::from_config(config))
            .await;
        let client = match connected {
            Ok(client) => client,
            Err(e) => {
                last = Err(format!("connecting: {e}"));
                continue;
            }
        };
        last = client
            .call_tool(call_params(tool, arguments.clone()))
            .await
            .map_err(|e| e.to_string());
        let _ = client.cancel().await;
        if last.is_ok() {
            return Delivery {
                attempts: attempt,
                result: last,
            };
        }
    }
    Delivery {
        attempts,
        result: last,
    }
}

/// An agent that keeps one connection and retries a call until it is answered, reconnecting
/// only after a failure, as a long-running agent would. Each call names its step in its own
/// `_meta`, so one connection can carry many calls and a gateway can still tell a resend from
/// a new call.
pub struct RetryingAgent {
    url: String,
    session: Option<String>,
    client: Option<Client>,
}

impl RetryingAgent {
    pub fn new(url: &str, session: Option<&str>) -> Self {
        Self {
            url: url.to_owned(),
            session: session.map(str::to_owned),
            client: None,
        }
    }

    pub async fn call(
        &mut self,
        step: &str,
        tool: &str,
        arguments: Value,
        attempts: usize,
    ) -> Delivery {
        let mut last = Err("no attempt was made".to_owned());
        for attempt in 1..=attempts {
            if self.client.is_none() {
                match connect(&self.url, self.session.as_deref()).await {
                    Ok(client) => self.client = Some(client),
                    Err(e) => {
                        last = Err(format!("connecting: {e:#}"));
                        continue;
                    }
                }
            }
            let client = self.client.as_ref().expect("connected above");
            let mut meta = Map::new();
            meta.insert("ledger/step".to_owned(), Value::String(step.to_owned()));
            let mut params = call_params(tool, arguments.clone());
            params.meta = Some(meta.into());
            last = client.call_tool(params).await.map_err(|e| e.to_string());
            if last.is_ok() {
                return Delivery {
                    attempts: attempt,
                    result: last,
                };
            }
            // The connection may be broken: start a new one for the next attempt.
            if let Some(client) = self.client.take() {
                let _ = client.cancel().await;
            }
        }
        Delivery {
            attempts,
            result: last,
        }
    }

    pub async fn close(mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.cancel().await;
        }
    }
}
