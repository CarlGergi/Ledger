//! Traces and metrics: one span per call, saying what the gateway decided and how the call
//! ended, joined to the agent's trace, and counters a dashboard can be built on.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{CALL_TIMEOUT, FaultyStore, spawn_gateway, spawn_model, spawn_tool_server, upstream};
use http::{HeaderName, HeaderValue};
use ledger_gateway::{Gateway, ModelUpstream, Telemetry, error_codes};
use ledger_store::{EventKind, EventStore, PgEventStore};
use ledger_testkit::{PATIENCE, TestDb};
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::files::FileServer;
use ledger_tools::model::Received;
use opentelemetry::{KeyValue, Value as OtelValue};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use rmcp::ServiceExt;
use rmcp::model::ClientConfig;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use sample_agent::{Client, call_params, run_model_agent};
use serde_json::json;

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

/// What a test sees of its gateway's telemetry.
struct Watch {
    spans: InMemorySpanExporter,
    metrics: InMemoryMetricExporter,
    meters: SdkMeterProvider,
    telemetry: Telemetry,
}

impl Watch {
    fn new() -> Self {
        let spans = InMemorySpanExporter::default();
        let tracers = SdkTracerProvider::builder()
            .with_simple_exporter(spans.clone())
            .build();
        let metrics = InMemoryMetricExporter::default();
        // Exports only when flushed: the interval is far longer than any test.
        let reader = PeriodicReader::builder(metrics.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let meters = SdkMeterProvider::builder().with_reader(reader).build();
        let telemetry = Telemetry::new(&tracers, &meters);
        Self {
            spans,
            metrics,
            meters,
            telemetry,
        }
    }

    fn spans(&self) -> Vec<SpanData> {
        self.spans.get_finished_spans().unwrap()
    }

    /// The total of a counter, or the number of a histogram's measurements, over the data
    /// points whose attributes include `with`.
    fn total(&self, name: &str, with: &[KeyValue]) -> u64 {
        self.meters.force_flush().unwrap();
        let exported = self.metrics.get_finished_metrics().unwrap();
        let Some(latest) = exported.last() else {
            return 0;
        };
        let matches = |attributes: Vec<&KeyValue>| with.iter().all(|kv| attributes.contains(&kv));
        let mut total = 0;
        for metric in latest.scope_metrics().flat_map(|s| s.metrics()) {
            if metric.name() != name {
                continue;
            }
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    for point in sum.data_points() {
                        if matches(point.attributes().collect()) {
                            total += point.value();
                        }
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        if matches(point.attributes().collect()) {
                            total += point.count();
                        }
                    }
                }
                other => panic!("unexpected data for {name}: {other:?}"),
            }
        }
        total
    }
}

fn attribute<'a>(span: &'a SpanData, key: &str) -> Option<&'a OtelValue> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| &kv.value)
}

fn attributes(span: &SpanData) -> HashMap<String, String> {
    span.attributes
        .iter()
        .map(|kv| (kv.key.to_string(), kv.value.to_string()))
        .collect()
}

/// Both tool servers and the scripted model behind a gateway with telemetry.
struct Stack {
    _db: TestDb,
    _files: tempfile::TempDir,
    store: Arc<dyn EventStore>,
    mcp: String,
    model: Received,
}

async fn stack(watch: &Watch, faulty: Option<&Arc<FaultyStore>>, db: TestDb) -> Stack {
    let pg: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let store: Arc<dyn EventStore> = match faulty {
        Some(faulty) => faulty.clone(),
        None => pg.clone(),
    };
    let (model_url, model) = spawn_model().await;
    let files = tempfile::tempdir().unwrap();
    let outbox = Arc::new(Outbox::new(files.path().join(".outbox.jsonl")));
    let email = spawn_tool_server(EmailServer::new(outbox)).await;
    let file_server = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
    let gateway = Gateway::new(
        store,
        vec![upstream("email", &email), upstream("files", &file_server)],
        CALL_TIMEOUT,
    )
    .with_model(ModelUpstream::new(&model_url, None, PATIENCE))
    .with_telemetry(watch.telemetry.clone());
    Stack {
        _db: db,
        _files: files,
        store: pg,
        mcp: spawn_gateway(gateway).await,
        model,
    }
}

/// An MCP client sending `headers` with every request.
async fn connect(url: &str, headers: &[(&'static str, &str)]) -> Client {
    let headers = headers
        .iter()
        .map(|(k, v)| {
            (
                HeaderName::from_static(k),
                HeaderValue::from_str(v).unwrap(),
            )
        })
        .collect();
    let config =
        StreamableHttpClientTransportConfig::with_uri(url.to_owned()).custom_headers(headers);
    ClientConfig::default()
        .serve(StreamableHttpClientTransport::from_config(config))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_call_to_a_tool_or_the_model_gets_one_span_describing_it() {
    let watch = Watch::new();
    let stack = stack(&watch, None, TestDb::new().await).await;
    let client = connect(&stack.mcp, &[("x-ledger-session", "s")]).await;
    let v1 = format!("{}/v1", stack.mcp.trim_end_matches("/mcp"));

    run_model_agent(&client, &v1, Some("s"), "reporter-1", "All good.")
        .await
        .unwrap();

    let spans = watch.spans();
    let names: Vec<&str> = spans.iter().map(|s| s.name.as_ref()).collect();
    assert_eq!(
        names,
        [
            "chat reporter-1",
            "tools/call files__write_file",
            "chat reporter-1",
            "tools/call email__send_email",
            "chat reporter-1",
        ]
    );
    let events = stack.store.session_events("s").await.unwrap();
    let requests: Vec<i64> = events
        .iter()
        .filter(|e| e.kind.is_request())
        .map(|e| e.seq)
        .collect();
    for (span, seq) in spans.iter().zip(requests) {
        let a = attributes(span);
        assert_eq!(a["ledger.session"], "s");
        assert_eq!(a["ledger.seq"], seq.to_string(), "{}", span.name);
        assert_eq!(a["ledger.decision"], "new");
        assert_eq!(a["ledger.outcome"], "ok");
        assert_eq!(span.span_kind, opentelemetry::trace::SpanKind::Server);
    }
    let send = &attributes(&spans[3]);
    assert_eq!(send["gen_ai.tool.name"], "email__send_email");
    assert_eq!(send["mcp.method.name"], "tools/call");
    assert_eq!(send["ledger.effect.class"], "keyed");
    let chat = &attributes(&spans[0]);
    assert_eq!(chat["gen_ai.request.model"], "reporter-1");
    assert_eq!(chat["http.response.status_code"], "200");
    assert!(chat["gen_ai.usage.input_tokens"].parse::<u64>().unwrap() > 0);
    // The forward is marked on the tool call's span.
    assert!(spans[1].events.iter().any(|e| e.name == "forwarded"));

    let model = KeyValue::new("ledger.call.kind", "model");
    let tool = KeyValue::new("ledger.call.kind", "tool");
    assert_eq!(watch.total("ledger.calls", std::slice::from_ref(&model)), 3);
    assert_eq!(watch.total("ledger.calls", std::slice::from_ref(&tool)), 2);
    assert_eq!(watch.total("ledger.call.duration", &[]), 5);
    // The session was new: taken once, from no one.
    let takeover = KeyValue::new("ledger.lease.takeover", false);
    assert_eq!(watch.total("ledger.leases", &[takeover]), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_agents_trace_runs_through_the_gateway_and_on_to_the_model() {
    let watch = Watch::new();
    let stack = stack(&watch, None, TestDb::new().await).await;
    let client = connect(
        &stack.mcp,
        &[("x-ledger-session", "s"), ("traceparent", TRACEPARENT)],
    )
    .await;

    client
        .call_tool(call_params("files__read_file", json!({ "path": "x" })))
        .await
        .unwrap();
    let v1 = format!("{}/v1/chat/completions", stack.mcp.trim_end_matches("/mcp"));
    let body = json!({ "model": "reporter-1", "messages": [{ "role": "user", "content": "hi" }] });
    reqwest::Client::new()
        .post(&v1)
        .header("x-ledger-session", "s")
        .header("traceparent", TRACEPARENT)
        .json(&body)
        .send()
        .await
        .unwrap();

    let spans = watch.spans();
    assert_eq!(spans.len(), 2);
    for span in &spans {
        assert_eq!(
            span.span_context.trace_id().to_string(),
            TRACE_ID,
            "{}",
            span.name
        );
        assert_eq!(span.parent_span_id.to_string(), "00f067aa0ba902b7");
    }
    // The model was asked as a child of the model call's span.
    let sent = stack.model.headers()[0]["traceparent"]
        .to_str()
        .unwrap()
        .to_owned();
    let chat = spans.iter().find(|s| s.name.starts_with("chat")).unwrap();
    assert_eq!(
        sent,
        format!("00-{TRACE_ID}-{}-01", chat.span_context.span_id())
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retry_answered_from_the_log_says_so() {
    let watch = Watch::new();
    let stack = stack(&watch, None, TestDb::new().await).await;
    let client = connect(
        &stack.mcp,
        &[("x-ledger-session", "s"), ("x-ledger-step", "1")],
    )
    .await;
    let read = || call_params("files__read_file", json!({ "path": "x" }));

    client.call_tool(read()).await.unwrap();
    client.call_tool(read()).await.unwrap();

    let decisions: Vec<String> = watch
        .spans()
        .iter()
        .map(|s| attribute(s, "ledger.decision").unwrap().to_string())
        .collect();
    assert_eq!(decisions, ["new", "retry_from_log"]);
    let retried = KeyValue::new("ledger.decision", "retry_from_log");
    assert_eq!(watch.total("ledger.calls", &[retried]), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_left_in_doubt_is_traced_and_counted() {
    let watch = Watch::new();
    let db = TestDb::new().await;
    let faulty = Arc::new(FaultyStore::new(Arc::new(PgEventStore::new(db.pool()))));
    let stack = stack(&watch, Some(&faulty), db).await;
    let client = connect(
        &stack.mcp,
        &[("x-ledger-session", "s"), ("x-ledger-step", "1")],
    )
    .await;
    let append = || {
        call_params(
            "files__append_file",
            json!({ "path": "log", "content": "x" }),
        )
    };

    // The append runs, but its outcome cannot be logged; asked again, it is in doubt.
    faulty.fail_appends_of(EventKind::CallResponse).await;
    let lost = client.call_tool(append()).await.unwrap_err();
    faulty.heal().await;
    let flagged = client.call_tool(append()).await.unwrap_err();

    for (error, code) in [
        (lost, error_codes::OUTCOME_NOT_RECORDED),
        (flagged, error_codes::OUTCOME_UNKNOWN),
    ] {
        match error {
            rmcp::ServiceError::McpError(e) => assert_eq!(e.code, code),
            other => panic!("{other}"),
        }
    }
    let spans = watch.spans();
    let outcome = |i: usize| attribute(&spans[i], "ledger.outcome").unwrap().to_string();
    assert_eq!(
        (outcome(0), outcome(1)),
        ("outcome_not_recorded".into(), "in_doubt".into())
    );
    assert_eq!(
        attribute(&spans[1], "ledger.decision").unwrap().to_string(),
        "retry_in_doubt"
    );
    assert_eq!(
        attribute(&spans[1], "error.type").unwrap().to_string(),
        "in_doubt"
    );
    assert_eq!(
        spans[1].status,
        opentelemetry::trace::Status::error("in_doubt")
    );
    let by_retry = KeyValue::new("ledger.flagged.by", "retry");
    assert_eq!(watch.total("ledger.in_doubt", &[by_retry]), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_takeover_and_a_fenced_append_are_counted() {
    let watch = Watch::new();
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let files = tempfile::tempdir().unwrap();
    let file_server = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
    let copy = |telemetry: Option<&Telemetry>| {
        let gateway = Gateway::new(
            store.clone(),
            vec![upstream("files", &file_server)],
            CALL_TIMEOUT,
        )
        .with_lease_ttl(Duration::ZERO);
        match telemetry {
            Some(t) => gateway.with_telemetry(t.clone()),
            None => gateway,
        }
    };
    let first = connect(
        &spawn_gateway(copy(Some(&watch.telemetry))).await,
        &[("x-ledger-session", "s")],
    )
    .await;
    let second = connect(
        &spawn_gateway(copy(None)).await,
        &[("x-ledger-session", "s")],
    )
    .await;
    let read = || call_params("files__read_file", json!({ "path": "x" }));

    first.call_tool(read()).await.unwrap();
    second.call_tool(read()).await.unwrap();
    // The first copy's lease is gone; its next append is fenced, and it takes the session back.
    let _ = first.call_tool(read()).await;
    first.call_tool(read()).await.unwrap();

    assert_eq!(watch.total("ledger.fenced", &[]), 1);
    let takeover = KeyValue::new("ledger.lease.takeover", true);
    assert_eq!(watch.total("ledger.leases", &[takeover]), 1);
}
