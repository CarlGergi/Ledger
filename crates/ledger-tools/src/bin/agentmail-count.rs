use anyhow::Context;
use clap::Parser;
use ledger_tools::agentmail::{API_URL, AgentMail};

/// Prints how many messages an AgentMail inbox has sent with exactly this subject. The API
/// key comes from AGENTMAIL_API_KEY.
#[derive(Parser)]
struct Args {
    #[arg(long)]
    inbox: String,
    #[arg(long)]
    subject: String,
    #[arg(long, default_value = API_URL)]
    api: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let key = std::env::var("AGENTMAIL_API_KEY").context("set AGENTMAIL_API_KEY")?;
    let mail = AgentMail::new(&args.api, key, &args.inbox);
    let count = mail
        .count_sent(&args.subject)
        .await
        .map_err(anyhow::Error::msg)?;
    println!("{count}");
    Ok(())
}
