use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::{Parser, Subcommand};
use ledger_gateway::{Config, Gateway, McpUpstream, Upstream};
use ledger_store::{Event, EventStore, PgEventStore};
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
        Command::Sessions { database_url } => {
            let store = PgEventStore::connect(&database_url, READ_TIMEOUT).await?;
            for session in store.sessions().await? {
                println!(
                    "{}  {:>6} events  {}",
                    session.created_at.to_rfc3339(),
                    session.event_count,
                    session.session_id
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
    let gateway = Gateway::new(Arc::new(store), upstreams, config.call_timeout());

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
    tracing::info!("gateway on http://{}/mcp", listener.local_addr()?);
    ledger_gateway::serve(gateway, listener, config.allowed_hosts, shutdown).await?;
    Ok(())
}

fn summary(event: &Event) -> String {
    let what = match (event.request_seq, event.outcome) {
        (Some(request), Some(outcome)) => {
            format!("response to #{request} ({})", outcome.as_str())
        }
        _ => "request".to_owned(),
    };
    format!(
        "{:>5}  {}  {}  {what}",
        event.seq,
        event.recorded_at.format("%H:%M:%S%.3f"),
        event.tool
    )
}
