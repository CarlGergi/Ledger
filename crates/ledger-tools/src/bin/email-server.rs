use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use ledger_tools::email::{EmailServer, Outbox};

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
    let args = Args::parse();
    if let Some(parent) = args.outbox.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let server = EmailServer::new(Arc::new(Outbox::new(args.outbox)));
    ledger_tools::http::run("email server", server, &args.listen).await
}
