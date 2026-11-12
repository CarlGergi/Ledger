//! Drives a deployed gateway from outside, for the cloud demo. Expects the example file
//! tool server behind it as `files`.
//!
//!   remote measure --url https://.../mcp   latency per call, against the network's own
//!                                          round trip; and a retried append that writes once
//!   remote wait --url ... --session s --step 2
//!                                          calls until answered, and says how long it took
//!
//! The MCP client opens a connection per request, so each call pays for a TCP and TLS
//! handshake. The baseline does too: `/healthz` over a new connection each time, which
//! touches neither the log nor a tool. The difference is what the gateway, its two commits
//! to PostgreSQL and the tool server add.

use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use sample_agent::{Delivery, RetryingAgent};
use serde_json::{Value, json};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Mode,
}

#[derive(Subcommand)]
enum Mode {
    Measure {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 200)]
        calls: usize,
    },
    Wait {
        #[arg(long)]
        url: String,
        #[arg(long)]
        session: String,
        #[arg(long)]
        step: String,
        /// Give up after this many seconds.
        #[arg(long, default_value_t = 300)]
        patience: u64,
    },
}

/// Nearest-rank percentile of sorted durations.
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty() && (0.0..=100.0).contains(&p));
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

fn answered(delivery: &Delivery) -> bool {
    matches!(&delivery.result, Ok(r) if r.is_error != Some(true))
}

fn text(delivery: &Delivery) -> String {
    let Ok(result) = &delivery.result else {
        return String::new();
    };
    let value = serde_json::to_value(result).unwrap_or(Value::Null);
    value["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

fn row(name: &str, mut times: Vec<Duration>) -> String {
    times.sort();
    format!(
        "| {name} | {} | {} | {} | {} | {} |",
        times.len(),
        ms(percentile(&times, 50.0)),
        ms(percentile(&times, 90.0)),
        ms(percentile(&times, 99.0)),
        ms(*times.last().expect("measured")),
    )
}

async fn measure(url: &str, calls: usize) {
    let base = url.trim_end_matches("/mcp");
    // A new connection per request, as the MCP client makes.
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(0)
        .build()
        .expect("HTTP client");
    let mut health = Vec::new();
    for _ in 0..calls {
        let started = Instant::now();
        let status = http
            .get(format!("{base}/healthz"))
            .send()
            .await
            .expect("healthz")
            .status();
        assert!(status.is_success(), "healthz answered {status}");
        health.push(started.elapsed());
    }

    let session = format!("remote-{}", uuid::Uuid::new_v4());
    let mut agent = RetryingAgent::new(url, Some(&session));
    let mut reads = Vec::new();
    // The first call also opens the MCP session; it is not counted.
    let first = agent
        .call(
            "0",
            "files__append_file",
            json!({ "path": "remote.txt", "content": "" }),
            1,
        )
        .await;
    assert!(answered(&first), "{:?}", first.result);
    for step in 1..=calls {
        let started = Instant::now();
        let delivery = agent
            .call(
                &step.to_string(),
                "files__read_file",
                json!({ "path": "remote.txt" }),
                1,
            )
            .await;
        assert!(answered(&delivery), "{:?}", delivery.result);
        reads.push(started.elapsed());
    }

    // An append, and the agent asking again for the same step, as after a lost answer.
    let append = json!({ "path": "remote.txt", "content": "appended once\n" });
    let started = Instant::now();
    let original = agent
        .call("append", "files__append_file", append.clone(), 1)
        .await;
    let original_time = started.elapsed();
    assert!(answered(&original), "{:?}", original.result);
    let started = Instant::now();
    let retry = agent.call("append", "files__append_file", append, 1).await;
    let retry_time = started.elapsed();
    assert!(answered(&retry), "{:?}", retry.result);
    let read = agent
        .call(
            "read back",
            "files__read_file",
            json!({ "path": "remote.txt" }),
            1,
        )
        .await;
    let lines = text(&read).matches("appended once").count();
    agent.close().await;

    println!("Session {session}, {calls} calls of each kind, one at a time.");
    println!();
    println!("| Request | n | p50 ms | p90 ms | p99 ms | max ms |");
    println!("| --- | --: | --: | --: | --: | --: |");
    println!("{}", row("`/healthz`, new connection each", health));
    println!("{}", row("`files__read_file` through Ledger", reads));
    println!();
    println!("| The append | ms | Same answer | Lines in the file |");
    println!("| --- | --: | --- | --: |");
    println!("| First call | {} | | |", ms(original_time));
    println!(
        "| Retry, same step | {} | {} | {lines} |",
        ms(retry_time),
        if text(&original) == text(&retry) {
            "yes"
        } else {
            "no"
        }
    );
}

async fn wait(url: &str, session: &str, step: &str, patience: Duration) {
    let started = Instant::now();
    let mut agent = RetryingAgent::new(url, Some(session));
    let mut refusals = 0;
    loop {
        let delivery = agent
            .call(step, "files__read_file", json!({ "path": "remote.txt" }), 1)
            .await;
        // Any answer from the tool counts, even an error: a copy that refuses the session
        // answers with a JSON-RPC error instead. After a restart the file may be gone.
        if delivery.result.is_ok() {
            break;
        }
        refusals += 1;
        if started.elapsed() > patience {
            eprintln!("no answer after {patience:?}: {:?}", delivery.result);
            std::process::exit(1);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    agent.close().await;
    println!(
        "answered after {:.2} s and {refusals} failed attempts",
        started.elapsed().as_secs_f64()
    );
}

#[tokio::main]
async fn main() {
    match Args::parse().command {
        Mode::Measure { url, calls } => measure(&url, calls).await,
        Mode::Wait {
            url,
            session,
            step,
            patience,
        } => wait(&url, &session, &step, Duration::from_secs(patience)).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_nearest_rank() {
        let d: Vec<Duration> = (1..=10).map(Duration::from_millis).collect();
        assert_eq!(percentile(&d, 50.0), Duration::from_millis(5));
        assert_eq!(percentile(&d, 90.0), Duration::from_millis(9));
        assert_eq!(percentile(&d, 99.0), Duration::from_millis(10));
    }
}
