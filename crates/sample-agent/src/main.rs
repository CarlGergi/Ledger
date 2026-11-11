use clap::Parser;

/// Run an agent against an MCP gateway: a fixed script, or, with `--model`, whatever an
/// OpenAI-compatible model decides.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:7100/mcp")]
    gateway: String,
    /// Session to record under. Without it, the connection gets its own session.
    #[arg(long)]
    session: Option<String>,
    #[arg(long, default_value = "All systems nominal.")]
    report: String,
    /// Let this model decide what to do, asked through `--model-url`.
    #[arg(long)]
    model: Option<String>,
    /// The model's base URL, up to and including `/v1`: usually the gateway's.
    #[arg(long, default_value = "http://127.0.0.1:7100/v1")]
    model_url: String,
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

    if let Some(model) = &args.model {
        let session = args.session.as_deref();
        let conversation =
            sample_agent::run_model_agent(&client, &args.model_url, session, model, &args.report)
                .await?;
        for message in &conversation.messages[2..] {
            println!("{message}");
        }
        println!("{model}: {}", conversation.answer);
    } else {
        for step in sample_agent::run_script(&client, &args.report).await? {
            println!("called {} with {}", step.tool, step.arguments);
        }
    }
    client.cancel().await?;
    Ok(())
}
