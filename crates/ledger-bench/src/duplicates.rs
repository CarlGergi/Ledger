//! How many side effects run twice when answers are lost, with and without Ledger.
//!
//! One agent sends N emails, or appends N lines, one call each, and retries a call until it
//! is answered, up to a limit, as agents commonly do. A proxy loses each tool call's answer
//! with probability p, after the tool has run. Effects are counted where they happen, in the
//! email outbox and the appended file, never from what the agent believes.
//!
//! Arrangements:
//!   direct          agent -> lossy -> tool            the usual setup, without Ledger
//!   ledger-to-agent agent -> lossy -> Ledger -> tool  answer lost on the way to the agent
//!   ledger-from-tool agent -> Ledger -> lossy -> tool answer lost on the way from the tool
//!
//! Tools: email (keyed: takes an idempotency key) and append (at most once: has none).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ledger_gateway::Gateway;
use ledger_store::{EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::TestDb;
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::files::FileServer;
use ledger_tools::http::serve_mcp;
use ledger_tools::lossy::LossyProxy;
use sample_agent::RetryingAgent;
use serde::Serialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Args {
    /// Calls per arrangement.
    #[arg(long, default_value_t = 1000)]
    calls: usize,
    /// Probability that a tool call's answer is lost after the tool ran.
    #[arg(long, default_value_t = 0.1)]
    loss: f64,
    /// Seeds the losses, so a run repeats exactly.
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// Attempts per call before the agent gives up.
    #[arg(long, default_value_t = 5)]
    attempts: usize,
    /// Print one JSON object per arrangement instead of a table.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Arrangement {
    Direct,
    LedgerToAgent,
    LedgerFromTool,
}

impl Arrangement {
    fn name(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::LedgerToAgent => "ledger, answer lost to the agent",
            Self::LedgerFromTool => "ledger, answer lost from the tool",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Tool {
    /// send_email, which takes an idempotency key.
    Email,
    /// append_file, which has no key and runs at most once through Ledger.
    Append,
}

#[derive(Debug, Default, Serialize)]
struct Result {
    arrangement: String,
    tool: String,
    calls: usize,
    loss: f64,
    answers_lost: usize,
    /// Effects that happened, counted at the tool.
    effects: usize,
    /// Effects beyond one per call.
    duplicates: usize,
    /// Calls with no effect at all.
    missing: usize,
    /// Calls whose agent got an answer in the end.
    answered: usize,
    /// Calls whose agent gave up, told the outcome is unknown.
    gave_up: usize,
    /// Through Ledger: retries answered from the log, without running anything.
    from_log: usize,
    /// Through Ledger: calls run again with the same idempotency key.
    rerun_with_key: usize,
    /// Through Ledger: calls flagged in doubt for a person.
    flagged: usize,
}

async fn spawn<F, Fut>(serve: F) -> String
where
    F: FnOnce(TcpListener) -> Fut,
    Fut: Future<Output = std::io::Result<()>> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("address"));
    tokio::spawn(serve(listener));
    url
}

async fn run(args: &Args, arrangement: Arrangement, tool: Tool) -> anyhow::Result<Result> {
    let dir = tempfile::tempdir()?;
    let outbox = Arc::new(Outbox::new(dir.path().join("outbox.jsonl")));
    let shutdown = CancellationToken::new();
    let tool_url = match tool {
        Tool::Email => {
            let server = EmailServer::new(outbox.clone());
            spawn(|l| serve_mcp(server, l, shutdown.clone())).await
        }
        Tool::Append => {
            let server = FileServer::new(dir.path())?;
            spawn(|l| serve_mcp(server, l, shutdown.clone())).await
        }
    };
    let proxy_to = |target: &str| LossyProxy::random(target, args.loss, args.seed);
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let gateway = |upstream: &str| {
        let upstream = Arc::new(ledger_gateway::McpUpstream::new(
            "tools",
            format!("{upstream}/mcp"),
            Duration::from_secs(120),
        )) as Arc<dyn ledger_gateway::Upstream>;
        Gateway::new(store.clone(), vec![upstream], Duration::from_secs(120))
    };
    let serve_gateway = |g: Gateway| {
        let shutdown = shutdown.clone();
        move |l| ledger_gateway::serve(g, l, Vec::new(), shutdown)
    };

    let (proxy, agent_url, prefix) = match arrangement {
        Arrangement::Direct => {
            let proxy = proxy_to(&tool_url);
            let url = spawn(|l| proxy.clone().serve(l, shutdown.clone())).await;
            (proxy, url, "")
        }
        Arrangement::LedgerToAgent => {
            let ledger = spawn(serve_gateway(gateway(&tool_url))).await;
            let proxy = proxy_to(&ledger);
            let url = spawn(|l| proxy.clone().serve(l, shutdown.clone())).await;
            (proxy, url, "tools__")
        }
        Arrangement::LedgerFromTool => {
            let proxy = proxy_to(&tool_url);
            let lossy = spawn(|l| proxy.clone().serve(l, shutdown.clone())).await;
            let url = spawn(serve_gateway(gateway(&lossy))).await;
            (proxy, url, "tools__")
        }
    };
    let through_ledger = arrangement != Arrangement::Direct;
    let session = through_ledger.then_some("bench");
    let (name, mut result) = match tool {
        Tool::Email => ("send_email", Result::default()),
        Tool::Append => ("append_file", Result::default()),
    };
    let url = format!("{agent_url}/mcp");
    let mut agent = RetryingAgent::new(&url, session);
    for i in 0..args.calls {
        let arguments = match tool {
            Tool::Email => {
                json!({ "to": "team@example.com", "subject": format!("call {i}"), "body": "x" })
            }
            Tool::Append => json!({ "path": "effects.log", "content": format!("call {i}\n") }),
        };
        let step = i.to_string();
        let tool_name = format!("{prefix}{name}");
        let delivery = agent
            .call(&step, &tool_name, arguments, args.attempts)
            .await;
        match delivery.result {
            Ok(_) => result.answered += 1,
            Err(_) => result.gave_up += 1,
        }
    }

    agent.close().await;

    // Count the effects where they happened.
    let mut per_call: HashMap<String, usize> = HashMap::new();
    match tool {
        Tool::Email => {
            for email in outbox.sent().await? {
                *per_call.entry(email.subject).or_default() += 1;
            }
        }
        Tool::Append => {
            let text = std::fs::read_to_string(dir.path().join("effects.log")).unwrap_or_default();
            for line in text.lines() {
                *per_call.entry(line.to_owned()).or_default() += 1;
            }
        }
    }
    result.effects = per_call.values().sum();
    result.duplicates = per_call.values().map(|n| n - 1).sum();
    result.missing = args.calls - per_call.len();
    result.answers_lost = proxy.lost();
    if through_ledger {
        let events = store.session_events("bench").await?;
        let final_before = |call: Option<i64>, before: i64| {
            events.iter().any(|e| {
                e.kind == EventKind::CallResponse
                    && e.request_seq == call
                    && e.seq < before
                    && e.outcome.is_some_and(Outcome::is_final)
            })
        };
        // A retry that arrived once its call had a final answer got that answer from the log.
        result.from_log = events
            .iter()
            .filter(|e| e.kind == EventKind::CallRetry && final_before(e.request_seq, e.seq))
            .count();
        // A keyed call whose first try lost its answer, and that then succeeded, ran again
        // with the same key.
        result.rerun_with_key = events
            .iter()
            .filter(|e| e.kind == EventKind::CallResponse && e.outcome == Some(Outcome::Ok))
            .filter(|e| e.payload["idempotency_key"].is_string())
            .filter(|e| {
                events.iter().any(|r| {
                    r.request_seq == e.request_seq && r.outcome == Some(Outcome::TransportError)
                })
            })
            .count();
        result.flagged = events
            .iter()
            .filter(|e| e.outcome == Some(Outcome::InDoubt))
            .count();
    }
    shutdown.cancel();
    result.arrangement = arrangement.name().to_owned();
    result.tool = match tool {
        Tool::Email => "email (keyed)".to_owned(),
        Tool::Append => "append (no key)".to_owned(),
    };
    result.calls = args.calls;
    result.loss = args.loss;
    Ok(result)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let runs = [
        (Arrangement::Direct, Tool::Email),
        (Arrangement::LedgerToAgent, Tool::Email),
        (Arrangement::LedgerFromTool, Tool::Email),
        (Arrangement::Direct, Tool::Append),
        (Arrangement::LedgerToAgent, Tool::Append),
        (Arrangement::LedgerFromTool, Tool::Append),
    ];
    if !args.json {
        println!(
            "{} calls each, {:.0}% of answers lost after the tool ran (seed {}), up to {} attempts\n",
            args.calls,
            args.loss * 100.0,
            args.seed,
            args.attempts
        );
        println!(
            "| Arrangement | Tool | Answers lost | Effects | Duplicates | Missing | Answered | Gave up | From the log | Re-run with key | Flagged in doubt |"
        );
        println!("| --- | --- | --: | --: | --: | --: | --: | --: | --: | --: | --: |");
    }
    for (arrangement, tool) in runs {
        let r = run(&args, arrangement, tool).await?;
        if args.json {
            println!("{}", serde_json::to_string(&r)?);
        } else {
            println!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                r.arrangement,
                r.tool,
                r.answers_lost,
                r.effects,
                r.duplicates,
                r.missing,
                r.answered,
                r.gave_up,
                r.from_log,
                r.rerun_with_key,
                r.flagged
            );
        }
    }
    Ok(())
}
