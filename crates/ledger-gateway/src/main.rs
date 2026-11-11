use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use ledger_gateway::{Config, Gateway, McpUpstream, ModelUpstream, Upstream};
use ledger_store::{Event, EventKind, EventStore, PgEventStore};
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
        Command::Log {
            session,
            database_url,
            json,
        } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            let events = store.session_events(&session).await?;
            anyhow::ensure!(!events.is_empty(), "no events for session {session:?}");
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
    if let Some(model) = &config.model {
        gateway = gateway.with_model(ModelUpstream::new(
            &model.url,
            model.api_key()?,
            model.timeout(),
        ));
    }

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
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
    Ok(())
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
