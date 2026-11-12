use clap::Parser;
use serde_json::json;

/// Sends one email through an MCP server, connecting again and resending if no answer
/// comes back, as many agents do.
#[derive(Parser)]
struct Args {
    /// The MCP server: the tool server itself, or Ledger.
    #[arg(long)]
    url: String,
    /// The session to record under, when going through Ledger.
    #[arg(long)]
    session: Option<String>,
    /// The tool: `send_email` on the tool server, `agentmail__send_email` through Ledger.
    #[arg(long)]
    tool: String,
    #[arg(long)]
    to: String,
    #[arg(long)]
    subject: String,
    #[arg(long, default_value = "Sent by the Ledger demo.")]
    body: String,
    #[arg(long, default_value_t = 3)]
    attempts: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let email = json!({ "to": args.to, "subject": args.subject, "body": args.body });
    let delivery = sample_agent::send_until_answered(
        &args.url,
        args.session.as_deref(),
        "1",
        &args.tool,
        email,
        args.attempts,
    )
    .await;
    match delivery.result {
        Ok(_) => println!("answered after {} attempts", delivery.attempts),
        Err(e) => anyhow::bail!("no answer after {} attempts: {e}", delivery.attempts),
    }
    Ok(())
}
