use clap::Parser;

/// Run a scripted agent against an MCP gateway.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7100/mcp")]
    gateway: String,
    /// Session to record under. Without it, the connection gets its own session.
    #[arg(long)]
    session: Option<String>,
    #[arg(long, default_value = "All systems nominal.")]
    report: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let client = sample_agent::connect(&args.gateway, args.session.as_deref()).await?;

    let tools = client.list_all_tools().await?;
    println!("{} tools available", tools.len());
    for tool in &tools {
        println!("  {}", tool.name);
    }

    for step in sample_agent::run_script(&client, &args.report).await? {
        println!("called {} with {}", step.tool, step.arguments);
    }
    client.cancel().await?;
    Ok(())
}
