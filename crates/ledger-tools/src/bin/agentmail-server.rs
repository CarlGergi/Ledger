use anyhow::Context;
use clap::Parser;
use ledger_tools::agentmail::{API_URL, AgentMail, AgentMailServer};

/// MCP server whose `send_email` sends real email from an AgentMail inbox. The API key
/// comes from AGENTMAIL_API_KEY, never from the command line.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7111")]
    listen: String,
    /// The inbox to send from, such as `you@agentmail.to`.
    #[arg(long)]
    inbox: String,
    /// AgentMail's API, up to and including `/v0`.
    #[arg(long, default_value = API_URL)]
    api: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let key = std::env::var("AGENTMAIL_API_KEY").context("set AGENTMAIL_API_KEY")?;
    let server = AgentMailServer::new(AgentMail::new(&args.api, key, &args.inbox));
    ledger_tools::http::run("agentmail server", server, &args.listen).await
}
