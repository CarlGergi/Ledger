use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use ledger_gateway::regress::{self, Suite};
use ledger_gateway::{
    Config, Gateway, McpUpstream, ModelUpstream, Telemetry, TelemetryConfig, Upstream,
};
use ledger_store::{Event, EventKind, EventStore, Fork, PgEventStore, ToolMode};
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// One-shot commands have nothing waiting on them, and can always be interrupted.
const READ_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Parser)]
#[command(name = "ledger", about = "Crash-safe MCP gateway for AI agents")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the gateway.
    Serve {
        #[arg(long, default_value = "config/ledger.toml")]
        config: PathBuf,
    },
    /// List recorded sessions.
    Sessions {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        /// Print sessions as JSON lines.
        #[arg(long)]
        json: bool,
    },
    /// Print a session's events in order.
    Log {
        session: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
        /// Print full events as JSON lines.
        #[arg(long)]
        json: bool,
    },
    /// List calls flagged for a person: they may have run, and are not safe to run again.
    InDoubt {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Start a session that replays another up to a step, then runs live. Prints its id:
    /// run the agent again under that session, through the gateway.
    Fork {
        /// The session to fork.
        source: String,
        /// The source's event to go live at; calls requested before it are answered
        /// from the source's log. One more than the source's last event replays it all.
        #[arg(long)]
        at: i64,
        /// The model to ask once live, in place of the one the agent names.
        #[arg(long)]
        model: Option<String>,
        /// A file whose text replaces the agent's system prompt once live.
        #[arg(long)]
        system_prompt_file: Option<PathBuf>,
        /// What tool calls do once live: `safe` answers calls the source made from its
        /// log and runs only read-only ones, `replay` runs none, `live` runs them all.
        #[arg(long, default_value = "safe", value_parser = ["safe", "replay", "live"])]
        tools: String,
        /// The new session's id. Defaults to `<source>.fork-<8 hex digits>`.
        #[arg(long = "as")]
        session: Option<String>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Start a replay of a session on another model, with every tool call answered from its
    /// log. Prints the replay's id: run the agent again under it, then `ledger compare`.
    Replay {
        source: String,
        #[arg(long)]
        model: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Compare a replay, or any fork, with the session it came from: where they first act
    /// differently, and what each cost.
    Compare {
        source: String,
        replay: String,
        /// Print the report as JSON.
        #[arg(long)]
        json: bool,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Replay every session in a suite on a new model, running the suite's agent command for
    /// each against a running gateway, and report where behaviour changed.
    Regress {
        suite: PathBuf,
        /// Replay on this model instead of the suite's.
        #[arg(long)]
        model: Option<String>,
        /// How long one agent run may take.
        #[arg(long, default_value_t = 600)]
        timeout_secs: u64,
        /// Exit with failure if any run diverged or its agent failed.
        #[arg(long)]
        fail_on_divergence: bool,
        /// Print one JSON report per run.
        #[arg(long)]
        json: bool,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        Command::Serve { config } => serve(Config::load(&config)?).await,
        Command::Sessions { database_url, json } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            for session in store.sessions().await? {
                if json {
                    println!("{}", serde_json::to_string(&session)?);
                    continue;
                }
                println!(
                    "{}  {:>6} events  {}",
                    session.created_at.to_rfc3339(),
                    session.event_count,
                    session.session_id
                );
            }
            Ok(())
        }
        Command::InDoubt { database_url } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            for flag in store.in_doubt().await? {
                println!(
                    "{}  {}  call #{}  {}  {}",
                    flag.recorded_at.to_rfc3339(),
                    flag.session_id,
                    flag.request_seq.unwrap_or_default(),
                    flag.tool,
                    flag.payload["reason"].as_str().unwrap_or_default()
                );
            }
            Ok(())
        }
        Command::Fork {
            source,
            at,
            model,
            system_prompt_file,
            tools,
            session,
            database_url,
        } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            let events = store.session_events(&source).await?;
            anyhow::ensure!(!events.is_empty(), "no events for session {source:?}");
            let last = events.len() as i64;
            anyhow::ensure!(
                (1..=last + 1).contains(&at),
                "--at must be from 1 to {}: session {source:?} has {last} events",
                last + 1
            );
            let system_prompt = system_prompt_file
                .map(|path| {
                    std::fs::read_to_string(&path)
                        .with_context(|| format!("reading {}", path.display()))
                })
                .transpose()?;
            let session = session.unwrap_or_else(|| {
                let id = uuid::Uuid::new_v4().simple().to_string();
                format!("{source}.fork-{}", &id[..8])
            });
            let fork = Fork {
                session_id: session.clone(),
                source_session: source,
                at_step: at,
                model,
                system_prompt,
                tools: ToolMode::parse(&tools)?,
            };
            store.create_fork(&fork).await?;
            println!("{session}");
            Ok(())
        }
        Command::Replay {
            source,
            model,
            database_url,
        } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            println!("{}", regress::start_replay(&store, &source, &model).await?);
            Ok(())
        }
        Command::Compare {
            source,
            replay,
            json,
            database_url,
        } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            let report = regress::report(&store, &source, &replay).await?;
            if json {
                println!("{}", serde_json::to_string(&report)?);
            } else {
                print_report(&report);
            }
            Ok(())
        }
        Command::Regress {
            suite,
            model,
            timeout_secs,
            fail_on_divergence,
            json,
            database_url,
        } => {
            let mut suite = Suite::load(&suite)?;
            if let Some(model) = model {
                suite.model = model;
            }
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            let timeout = Duration::from_secs(timeout_secs);
            let (mut same, mut diverged, mut failed) = (0, 0, 0);
            for run in &suite.runs {
                let replay = regress::start_replay(&store, &run.session, &suite.model).await?;
                let agent = run_agent(&suite.command(run, &replay), timeout).await;
                let report = regress::report(&store, &run.session, &replay).await?;
                match (&agent, report.same()) {
                    (Err(_), _) => failed += 1,
                    (Ok(()), true) => same += 1,
                    (Ok(()), false) => diverged += 1,
                }
                if json {
                    let mut line = serde_json::to_value(&report)?;
                    line["agent_error"] =
                        serde_json::json!(agent.as_ref().err().map(|e| format!("{e:#}")));
                    println!("{line}");
                    continue;
                }
                print_report(&report);
                if let Err(e) = &agent {
                    println!("  the agent failed: {e:#}");
                }
                println!();
            }
            if !json {
                println!(
                    "{} runs on {}: {same} same, {diverged} diverged, {failed} failed",
                    suite.runs.len(),
                    suite.model
                );
            }
            anyhow::ensure!(
                !fail_on_divergence || diverged + failed == 0,
                "{diverged} runs diverged and {failed} failed"
            );
            Ok(())
        }
        Command::Log {
            session,
            database_url,
            json,
        } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            let events = store.session_events(&session).await?;
            let fork = store.fork(&session).await?;
            if let (Some(fork), false) = (&fork, json) {
                println!(
                    "fork of {} at step {}, tools {}{}",
                    fork.source_session,
                    fork.at_step,
                    fork.tools.as_str(),
                    fork.model
                        .as_ref()
                        .map(|m| format!(", model {m}"))
                        .unwrap_or_default()
                );
            }
            anyhow::ensure!(
                !events.is_empty() || fork.is_some(),
                "no events for session {session:?}"
            );
            for event in &events {
                if json {
                    println!("{}", serde_json::to_string(event)?);
                } else {
                    println!("{}", summary(event));
                }
            }
            Ok(())
        }
    }
}

async fn serve(config: Config) -> anyhow::Result<()> {
    let policy = config.effect_policy()?;
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .or_else(|| config.database_url.clone())
        .context("set DATABASE_URL or database_url in the config")?;
    let store = PgEventStore::connect(&database_url, config.call_timeout())
        .await
        .context("connecting to the event store")?;
    store.migrate().await.context("migrating the event store")?;

    let upstreams = config
        .upstreams
        .iter()
        .map(|u| {
            Arc::new(McpUpstream::new(&u.name, &u.url, config.connect_timeout()))
                as Arc<dyn Upstream>
        })
        .collect();
    let mut gateway =
        Gateway::with_policy(Arc::new(store), upstreams, config.call_timeout(), policy)
            .with_lease_ttl(config.lease_ttl());
    let providers = telemetry_providers(config.telemetry.as_ref())?;
    if let Some((traces, metrics)) = &providers {
        gateway = gateway.with_telemetry(Telemetry::new(traces, metrics));
    }
    if let Some(model) = &config.model {
        gateway = gateway.with_model(ModelUpstream::new(
            &model.url,
            model.api_key()?,
            model.timeout(),
        ));
    }

    // Ctrl-C, or SIGTERM as container platforms send: stop being ready and turn new calls
    // away, but keep serving until calls already running have logged their answers, so
    // that health checks get a clear answer meanwhile. Then stop.
    let grace = config.shutdown_grace();
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        let gateway = gateway.clone();
        async move {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            tracing::info!("shutting down");
            gateway.begin_shutdown();
            if !gateway.drain(grace).await {
                tracing::warn!(
                    calls = gateway.active_calls(),
                    "calls still running after {grace:?}; their answers will not be logged"
                );
            }
            gateway.release_leases().await;
            shutdown.cancel();
        }
    });

    let listener = TcpListener::bind(&config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    let address = listener.local_addr()?;
    tracing::info!("gateway on http://{address}/mcp");
    if config.model.is_some() {
        tracing::info!("model proxy on http://{address}/v1/chat/completions");
    }
    ledger_gateway::serve(gateway, listener, config.allowed_hosts, shutdown).await?;
    if let Some((traces, metrics)) = providers {
        // Sends what is still buffered. Blocking, so off the async threads.
        tokio::task::spawn_blocking(move || {
            for result in [traces.shutdown(), metrics.shutdown()] {
                if let Err(e) = result {
                    tracing::warn!(error = %e, "could not flush telemetry");
                }
            }
        })
        .await?;
    }
    Ok(())
}

/// OTLP/HTTP exporters for traces and metrics: to the configured collector, or where
/// `OTEL_EXPORTER_OTLP_ENDPOINT` says if nothing is configured, or none if neither is.
fn telemetry_providers(
    config: Option<&TelemetryConfig>,
) -> anyhow::Result<Option<(SdkTracerProvider, SdkMeterProvider)>> {
    let from_env = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some();
    if config.is_none() && !from_env {
        return Ok(None);
    }
    let service = config
        .map_or("ledger", |c| c.service_name.as_str())
        .to_owned();
    let resource = Resource::builder().with_service_name(service).build();
    let mut spans = SpanExporter::builder().with_http();
    let mut metrics = MetricExporter::builder().with_http();
    if let Some(config) = config {
        let base = config.otlp_endpoint.trim_end_matches('/');
        spans = spans.with_endpoint(format!("{base}/v1/traces"));
        metrics = metrics.with_endpoint(format!("{base}/v1/metrics"));
    }
    let interval = config.map_or(60, |c| c.metrics_interval_secs);
    let reader = PeriodicReader::builder(metrics.build()?)
        .with_interval(Duration::from_secs(interval))
        .build();
    let traces = SdkTracerProvider::builder()
        .with_batch_exporter(spans.build()?)
        .with_resource(resource.clone())
        .build();
    let metrics = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();
    Ok(Some((traces, metrics)))
}

fn print_report(report: &ledger_gateway::compare::Report) {
    println!(
        "{} -> {}: {}",
        report.source,
        report.replay,
        report.summary()
    );
    for (who, cost) in [
        ("source", &report.source_cost),
        ("replay", &report.replay_cost),
    ] {
        println!(
            "  {who}: {} model calls, {} tokens; tools: {} run, {} from the log, {} refused",
            cost.model_calls, cost.tokens, cost.tools_run, cost.tools_from_log, cost.tools_refused
        );
    }
}

/// Runs an agent to the end, keeping its output to report if it fails.
async fn run_agent(command: &[String], timeout: Duration) -> anyhow::Result<()> {
    let (program, args) = command.split_first().context("an empty agent command")?;
    let child = tokio::process::Command::new(program)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("starting {program}"))?;
    let output = tokio::time::timeout(timeout, child.wait_with_output())
        .await
        .map_err(|_| anyhow::anyhow!("{program} did not finish within {timeout:?}"))??;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let tail: Vec<&str> = stderr.lines().rev().take(5).collect();
    let tail: Vec<&str> = tail.into_iter().rev().collect();
    anyhow::bail!(
        "{program} exited with {}: {}",
        output.status,
        tail.join(" / ")
    )
}

fn summary(event: &Event) -> String {
    let what = match (event.kind, event.request_seq, event.outcome) {
        (EventKind::ModelResponse, Some(request), Some(outcome)) => {
            format!("model reply to #{request} ({})", outcome.as_str())
        }
        (_, Some(request), Some(outcome)) => {
            format!("response to #{request} ({})", outcome.as_str())
        }
        (EventKind::CallRetry, Some(request), None) => format!("retry of #{request}"),
        _ if !event.payload["replayed_from"].is_null() => {
            let from = &event.payload["replayed_from"];
            let what = match event.kind {
                EventKind::ModelRequest => "model request",
                _ => "request",
            };
            let source = from["session"].as_str().unwrap_or_default();
            format!("{what}, replayed from {source} #{}", from["seq"])
        }
        (EventKind::ModelRequest, _, _) => "model request".to_owned(),
        _ => match event.payload["effect"]["class"].as_str() {
            Some(class) => format!("request ({class})"),
            None => "request".to_owned(),
        },
    };
    format!(
        "{:>5}  {}  {}  {what}",
        event.seq,
        event.recorded_at.format("%H:%M:%S%.3f"),
        event.tool
    )
}
