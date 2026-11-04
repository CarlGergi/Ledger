use std::path::PathBuf;

use clap::Parser;
use ledger_tools::files::FileServer;

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
    let args = Args::parse();
    tokio::fs::create_dir_all(&args.root).await?;
    let server = FileServer::new(&args.root)?;
    ledger_tools::http::run("file server", server, &args.listen).await
}
