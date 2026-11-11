use clap::Parser;
use ledger_tools::model::{Received, serve_model};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// A scripted, OpenAI-compatible model: `reporter-1`, `reporter-2` and `reporter-3`.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7103")]
    listen: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });
    let listener = TcpListener::bind(&args.listen).await?;
    println!("model server on http://{}/v1", listener.local_addr()?);
    serve_model(listener, Received::default(), shutdown).await?;
    Ok(())
}
