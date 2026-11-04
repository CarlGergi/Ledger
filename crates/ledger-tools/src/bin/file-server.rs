use std::path::PathBuf;

use clap::Parser;
use ledger_tools::files::FileServer;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

/// MCP file writer confined to a root directory.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7102")]
    listen: String,
    #[arg(long, default_value = "var/files")]
    root: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    tokio::fs::create_dir_all(&args.root).await?;

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });

    let listener = TcpListener::bind(&args.listen).await?;
    println!("file server on http://{}/mcp", listener.local_addr()?);
    let server = FileServer::new(&args.root)?;
    ledger_tools::http::serve_mcp(server, listener, shutdown).await?;
    Ok(())
}
