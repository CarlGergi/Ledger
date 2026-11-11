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
