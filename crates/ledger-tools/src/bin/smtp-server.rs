use clap::Parser;
use ledger_tools::smtp::{Smtp, SmtpServer};

/// MCP server whose `send_email` sends through an SMTP server, such as Mailpit.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7117")]
    listen: String,
    /// The SMTP server's host, reached without TLS or sign-in.
    #[arg(long, default_value = "127.0.0.1")]
    smtp_host: String,
    #[arg(long, default_value_t = 1025)]
    smtp_port: u16,
    #[arg(long, default_value = "Ledger demo <agent@ledger.local>")]
    from: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let smtp =
        Smtp::new(&args.smtp_host, args.smtp_port, &args.from).map_err(anyhow::Error::msg)?;
    ledger_tools::http::run("smtp server", SmtpServer::new(smtp), &args.listen).await
}
