//! A scripted agent: it speaks plain MCP to whatever server it is pointed at.

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
