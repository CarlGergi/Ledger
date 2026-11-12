use clap::Parser;
use ledger_tools::agentmail::FakeAgentMail;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// A stand-in for AgentMail's API at /v0, accepting the key in AGENTMAIL_API_KEY.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7110")]
    listen: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let key = std::env::var("AGENTMAIL_API_KEY").unwrap_or_else(|_| "am-demo-key".to_owned());
    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        }
    });
    let listener = TcpListener::bind(&args.listen).await?;
    println!("fake AgentMail on http://{}/v0", listener.local_addr()?);
    FakeAgentMail::new(&key).serve(listener, shutdown).await?;
    Ok(())
}
