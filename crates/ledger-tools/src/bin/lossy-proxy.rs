use clap::Parser;
use ledger_tools::lossy::LossyProxy;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// Forwards HTTP to a target, but loses the answers to its first tool calls after the
/// tool has run, as a dropped connection would.
#[derive(Parser)]
struct Args {
    #[arg(long)]
    listen: String,
    /// Scheme, host and port to forward to.
    #[arg(long)]
    target: String,
    /// How many tool calls' answers to lose.
    #[arg(long, default_value_t = 1)]
    lose: usize,
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
    println!(
        "lossy proxy on http://{} to {}",
        listener.local_addr()?,
        args.target
    );
    LossyProxy::new(&args.target, args.lose)
        .serve(listener, shutdown)
        .await?;
    Ok(())
}
