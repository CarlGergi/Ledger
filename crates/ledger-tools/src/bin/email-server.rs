use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use ledger_tools::email::{EmailServer, Outbox};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// MCP email sender that delivers to an outbox file.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7101")]
    listen: String,
    #[arg(long, default_value = "var/outbox.jsonl")]
    outbox: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    if let Some(parent) = args.outbox.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });

    let listener = TcpListener::bind(&args.listen).await?;
    println!("email server on http://{}/mcp", listener.local_addr()?);
    let server = EmailServer::new(Arc::new(Outbox::new(args.outbox)));
    ledger_tools::http::serve_mcp(server, listener, shutdown).await?;
    Ok(())
}
