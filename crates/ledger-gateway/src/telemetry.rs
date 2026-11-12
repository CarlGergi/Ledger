//! Traces and metrics, through OpenTelemetry.
//!
//! Every call, to a tool or a model, gets one span, named and described by the
//! OpenTelemetry conventions for MCP and generative AI where they have a name for a thing,
//! and under `ledger.*` where they do not: the session, the call's number in the log, its
//! effect class, how the gateway decided to handle it, and how it ended. If the agent sent a
//! W3C `traceparent`, the span joins the agent's trace, and model calls pass it on.
//!
//! The gateway is handed its telemetry rather than reaching for a global one, so that each
//! test can watch its own gateway, and a gateway with none configured costs next to nothing.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use opentelemetry::global::BoxedTracer;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::{
    Span, SpanKind, Status, TraceContextExt, Tracer, TracerProvider, noop::NoopTracer,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;

/// The name the gateway's spans and metrics are reported under.
pub const SCOPE: &str = "ledger";

/// What a call is to: the two kinds of call the log holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallKind {
    Tool,
    Model,
}

impl CallKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::Model => "model",
        }
    }
}

#[derive(Clone)]
pub struct Telemetry {
    inner: Arc<Inner>,
}

struct Inner {
    tracer: BoxedTracer,
    calls: Counter<u64>,
    duration: Histogram<f64>,
    in_doubt: Counter<u64>,
    leases: Counter<u64>,
    fenced: Counter<u64>,
}

impl std::fmt::Debug for Telemetry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Telemetry")
    }
}

impl Telemetry {
    /// Reports through these providers.
    pub fn new<T>(tracer_provider: &T, meter_provider: &impl MeterProvider) -> Self
    where
        T: TracerProvider,
        T::Tracer: Send + Sync + 'static,
        <T::Tracer as Tracer>::Span: Send + Sync + 'static,
    {
        let tracer = BoxedTracer::new(Box::new(tracer_provider.tracer(SCOPE)));
        Self::with_parts(tracer, meter_provider.meter(SCOPE))
    }

    /// Reports nothing.
    pub fn none() -> Self {
        let meter = SdkMeterProvider::builder().build().meter(SCOPE);
        Self::with_parts(BoxedTracer::new(Box::new(NoopTracer::new())), meter)
    }

    fn with_parts(tracer: BoxedTracer, meter: Meter) -> Self {
        Self {
            inner: Arc::new(Inner {
                tracer,
                calls: meter
                    .u64_counter("ledger.calls")
                    .with_description("Calls handled, by kind, decision and outcome")
                    .build(),
                duration: meter
                    .f64_histogram("ledger.call.duration")
                    .with_unit("s")
                    .with_description("How long each call took, logging included")
                    .build(),
                in_doubt: meter
                    .u64_counter("ledger.in_doubt")
                    .with_description("Calls flagged for a person: they may have run")
                    .build(),
                leases: meter
                    .u64_counter("ledger.leases")
                    .with_description("Sessions taken by this copy, and whether from another")
                    .build(),
                fenced: meter
                    .u64_counter("ledger.fenced")
                    .with_description("Appends refused because another copy took the session")
                    .build(),
            }),
        }
    }

    /// Starts the span for one call, as a child of `traceparent` if the agent sent one.
    pub(crate) fn start_call(
        &self,
        kind: CallKind,
        name: &str,
        session: &str,
        traceparent: Option<&str>,
    ) -> CallSpan {
        let parent = traceparent.map(parent_context).unwrap_or_default();
        let (span_name, attributes) = match kind {
            CallKind::Tool => (
                format!("tools/call {name}"),
                vec![
                    KeyValue::new("mcp.method.name", "tools/call"),
                    KeyValue::new("gen_ai.operation.name", "execute_tool"),
                    KeyValue::new("gen_ai.tool.name", name.to_owned()),
                ],
            ),
            CallKind::Model => (
                format!("chat {name}"),
                vec![
                    KeyValue::new("gen_ai.operation.name", "chat"),
                    KeyValue::new("gen_ai.request.model", name.to_owned()),
                ],
            ),
        };
        let mut attributes = attributes;
        attributes.push(KeyValue::new("ledger.session", session.to_owned()));
        let builder = self
            .inner
            .tracer
            .span_builder(span_name)
            .with_kind(SpanKind::Server)
            .with_attributes(attributes);
        CallSpan {
            span: self.inner.tracer.build_with_context(builder, &parent),
            kind,
            decision: "new",
            started: Instant::now(),
        }
    }

    pub(crate) fn flagged_in_doubt(&self, kind: &'static str) {
        self.inner
            .in_doubt
            .add(1, &[KeyValue::new("ledger.flagged.by", kind)]);
    }

    pub(crate) fn lease_taken(&self, token: i64) {
        let from_another = token > 1;
        let attributes = [KeyValue::new("ledger.lease.takeover", from_another)];
        self.inner.leases.add(1, &attributes);
    }

    pub(crate) fn fenced(&self) {
        self.inner.fenced.add(1, &[]);
    }
}

/// The span of one call, open until the call has its answer.
pub(crate) struct CallSpan {
    span: opentelemetry::global::BoxedSpan,
    kind: CallKind,
    decision: &'static str,
    started: Instant,
}

impl CallSpan {
    /// How the gateway decided to handle the call: `new`, `retry_from_log`, `replayed`, ...
    pub(crate) fn decided(&mut self, decision: &'static str) {
        self.decision = decision;
    }

    pub(crate) fn set(&mut self, key: &'static str, value: impl Into<opentelemetry::Value>) {
        self.span.set_attribute(KeyValue::new(key, value));
    }

    pub(crate) fn event(&mut self, name: &'static str) {
        self.span.add_event(name, Vec::new());
    }

    /// The `traceparent` that makes a request a child of this span.
    pub(crate) fn traceparent(&self) -> Option<String> {
        let context = Context::current().with_remote_span_context(self.span.span_context().clone());
        let mut carrier = HashMap::new();
        TraceContextPropagator::new().inject_context(&context, &mut carrier);
        carrier.remove("traceparent")
    }

    /// Ends the span with what the agent got, and counts the call.
    pub(crate) fn finish(mut self, telemetry: &Telemetry, outcome: &'static str) {
        self.span
            .set_attribute(KeyValue::new("ledger.decision", self.decision));
        self.span
            .set_attribute(KeyValue::new("ledger.outcome", outcome));
        if outcome != "ok" {
            self.span
                .set_attribute(KeyValue::new("error.type", outcome));
            self.span.set_status(Status::error(outcome));
        }
        self.span.end();
        let kind = KeyValue::new("ledger.call.kind", self.kind.as_str());
        let outcome = KeyValue::new("ledger.outcome", outcome);
        let decision = KeyValue::new("ledger.decision", self.decision);
        let inner = &telemetry.inner;
        inner
            .calls
            .add(1, &[kind.clone(), decision, outcome.clone()]);
        inner
            .duration
            .record(self.started.elapsed().as_secs_f64(), &[kind, outcome]);
    }
}

fn parent_context(traceparent: &str) -> Context {
    let carrier = HashMap::from([("traceparent".to_owned(), traceparent.to_owned())]);
    TraceContextPropagator::new().extract(&carrier)
}

#[cfg(test)]
mod tests {
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    use super::*;

    const PARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn a_call_joins_the_agents_trace_and_passes_it_on() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = Telemetry::new(&provider, &SdkMeterProvider::builder().build());

        let mut span = telemetry.start_call(CallKind::Model, "m", "s", Some(PARENT));
        let onward = span.traceparent().unwrap();
        span.decided("replayed");
        span.finish(&telemetry, "ok");

        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.name, "chat m");
        assert_eq!(
            span.span_context.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(span.parent_span_id.to_string(), "00f067aa0ba902b7");
        // What is passed on names this span as the parent.
        assert!(
            onward.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
            "{onward}"
        );
        assert!(
            onward.contains(&span.span_context.span_id().to_string()),
            "{onward}"
        );
    }

    #[test]
    fn a_traceparent_that_is_not_one_starts_a_new_trace() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let telemetry = Telemetry::new(&provider, &SdkMeterProvider::builder().build());
        telemetry
            .start_call(CallKind::Tool, "t", "s", Some("not a traceparent"))
            .finish(&telemetry, "ok");
        let span = &exporter.get_finished_spans().unwrap()[0];
        assert!(span.span_context.is_valid());
        assert_eq!(span.parent_span_id, opentelemetry::trace::SpanId::INVALID);
    }

    #[test]
    fn no_telemetry_costs_a_call_nothing_it_can_notice() {
        let telemetry = Telemetry::none();
        let mut span = telemetry.start_call(CallKind::Tool, "t", "s", None);
        span.set("ledger.seq", 1);
        span.finish(&telemetry, "ok");
    }
}
