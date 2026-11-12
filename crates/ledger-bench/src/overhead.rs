//! What Ledger costs per call: latency added (p50, p90, p99, max) and calls per second, for
//! the same tool call made straight to the tool server and through Ledger.
//!
//! The tool is a real read-only `read_file`, so Ledger's share includes everything it does
//! for a call: deciding what the call is, two commits to PostgreSQL, and forwarding.
//!
//! Timing depends on everything else the machine is doing, so this refuses to measure
//! unless told the machine is quiet, and prints the machine and its load with every result.

use std::sync::Arc;
use std::time::{Duration, Instant};

mod ports;

use clap::Parser;
use ledger_gateway::{Gateway, McpUpstream, Upstream};
use ledger_store::{EventStore, PgEventStore};
use ledger_testkit::TestDb;
use ledger_tools::files::FileServer;
use ledger_tools::http::serve_mcp;
use sample_agent::{Client, call_params, connect};
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Args {
    /// Calls measured one at a time, per arrangement, after the warm-up.
    #[arg(long, default_value_t = 1000)]
    calls: usize,
    #[arg(long, default_value_t = 100)]
    warmup: usize,
    /// Clients calling at once, for calls per second.
    #[arg(long, default_value_t = 16)]
    clients: usize,
    /// Calls each client makes in the throughput run. Kept small: the MCP HTTP client opens
    /// a connection per request, and macOS runs out of local ports to one address well
    /// before 16,000 connections at full speed.
    #[arg(long, default_value_t = 100)]
    calls_per_client: usize,
    /// Required to measure: timings from a busy machine mislead.
    #[arg(long)]
    machine_is_quiet: bool,
    /// Checks the setup with one call through each arrangement, and measures nothing.
    #[arg(long)]
    dry_run: bool,
    /// Also writes every sample to this file, as JSON, for the figures.
    #[arg(long)]
    json_out: Option<std::path::PathBuf>,
}

/// Percentiles of a set of durations, nearest-rank.
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    assert!(!sorted.is_empty() && (0.0..=100.0).contains(&p));
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

fn ms(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64() * 1000.0)
}

fn machine() -> String {
    let sysctl = |name: &str| {
        std::process::Command::new("sysctl")
            .args(["-n", name])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default()
    };
    format!(
        "{}, {}, {} cores; load {}",
        sysctl("hw.model"),
        sysctl("machdep.cpu.brand_string"),
        sysctl("hw.ncpu"),
        sysctl("vm.loadavg")
    )
}

async fn read(client: &Client) {
    client
        .call_tool(call_params(
            &CALL_NAME.with(|n| n.borrow().clone()),
            json!({ "path": "f.txt" }),
        ))
        .await
        .unwrap_or_else(|e| {
            let e = e.to_string();
            if ports::out_of_ports(&e) {
                panic!("the machine ran out of local ports, so this run is invalid: {e}");
            }
            panic!("the call failed: {e}")
        });
}

thread_local! {
    static CALL_NAME: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
}

struct Measured {
    sorted: Vec<Duration>,
    /// Median of the session's first 100 calls, and of its last 100: whether a call costs
    /// more once its session is long.
    early: Duration,
    late: Duration,
    per_second: f64,
}

async fn measure(url: &str, tool: &str, args: &Args) -> Measured {
    ports::let_ports_drain().await;
    CALL_NAME.with(|n| *n.borrow_mut() = tool.to_owned());
    // Warm up in a session of its own, so that the measured calls are a fresh session's
    // first to last.
    let warmup = connect(url, Some("overhead-warmup"))
        .await
        .expect("connect");
    for _ in 0..args.warmup {
        read(&warmup).await;
    }
    warmup.cancel().await.ok();
    let client = connect(url, Some("overhead")).await.expect("connect");
    let mut times = Vec::with_capacity(args.calls);
    for _ in 0..args.calls {
        let started = Instant::now();
        read(&client).await;
        times.push(started.elapsed());
    }
    let median_of = |slice: &[Duration]| {
        let mut slice = slice.to_vec();
        slice.sort();
        percentile(&slice, 50.0)
    };
    let window = times.len().min(100);
    let early = median_of(&times[..window]);
    let late = median_of(&times[times.len() - window..]);
    times.sort();
    client.cancel().await.ok();

    ports::let_ports_drain().await;
    let started = Instant::now();
    let mut clients = Vec::new();
    for c in 0..args.clients {
        let url = url.to_owned();
        let tool = tool.to_owned();
        let calls = args.calls_per_client;
        clients.push(tokio::spawn(async move {
            let client = connect(&url, Some(&format!("overhead-{c}")))
                .await
                .expect("connect");
            for _ in 0..calls {
                client
                    .call_tool(call_params(&tool, json!({ "path": "f.txt" })))
                    .await
                    .expect("the call failed");
            }
        }));
    }
    for client in clients {
        client.await.expect("a client failed");
    }
    let total = (args.clients * args.calls_per_client) as f64;
    Measured {
        sorted: times,
        early,
        late,
        per_second: total / started.elapsed().as_secs_f64(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = Args::parse();
    anyhow::ensure!(
        args.machine_is_quiet || args.dry_run,
        "timings from a busy machine mislead: pass --machine-is-quiet once it is, or --dry-run"
    );
    if args.dry_run {
        (args.calls, args.warmup, args.clients, args.calls_per_client) = (1, 0, 1, 1);
    }
    let dir = tempfile::tempdir()?;
    std::fs::write(dir.path().join("f.txt"), "a small file")?;
    let shutdown = CancellationToken::new();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let tool = format!("http://{}/mcp", listener.local_addr()?);
    tokio::spawn(serve_mcp(
        FileServer::new(dir.path())?,
        listener,
        shutdown.clone(),
    ));

    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let upstream =
        Arc::new(McpUpstream::new("files", &tool, Duration::from_secs(60))) as Arc<dyn Upstream>;
    let gateway = Gateway::new(store, vec![upstream], Duration::from_secs(60));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let ledger = format!("http://{}/mcp", listener.local_addr()?);
    tokio::spawn(ledger_gateway::serve(
        gateway,
        listener,
        Vec::new(),
        shutdown.clone(),
    ));

    // Each call opens a TCP connection: one straight to the tool, two through Ledger.
    let before = machine();
    let direct = measure(&tool, "read_file", &args).await;
    let through = measure(&ledger, "files__read_file", &args).await;
    let after = machine();
    shutdown.cancel();

    if args.dry_run {
        println!("dry run: one call straight to the tool and one through Ledger both worked");
        return Ok(());
    }
    println!("Machine at start: {before}");
    println!("Machine at end:   {after}");
    println!(
        "{} calls one at a time after {} to warm up; {} clients x {} calls for calls per second",
        args.calls, args.warmup, args.clients, args.calls_per_client
    );
    println!(
        "Every call includes opening a TCP connection, as the MCP HTTP client does: one straight to the tool, two through Ledger.\n"
    );
    println!("| | p50 ms | p90 ms | p99 ms | max ms | calls/s |");
    println!("| --- | --: | --: | --: | --: | --: |");
    for (name, m) in [
        ("straight to the tool", &direct),
        ("through Ledger", &through),
    ] {
        println!(
            "| {name} | {} | {} | {} | {} | {:.0} |",
            ms(percentile(&m.sorted, 50.0)),
            ms(percentile(&m.sorted, 90.0)),
            ms(percentile(&m.sorted, 99.0)),
            ms(*m.sorted.last().expect("measured")),
            m.per_second
        );
    }
    println!(
        "\nThrough Ledger, median of a session's first 100 calls: {} ms; of calls {} to {}: {} ms",
        ms(through.early),
        args.calls.saturating_sub(99).max(1),
        args.calls,
        ms(through.late)
    );
    if let Some(path) = &args.json_out {
        let samples = |m: &Measured| -> Vec<f64> {
            m.sorted.iter().map(|d| d.as_secs_f64() * 1000.0).collect()
        };
        let record = json!({
            "machine_before": before,
            "machine_after": after,
            "calls": args.calls,
            "warmup": args.warmup,
            "clients": args.clients,
            "calls_per_client": args.calls_per_client,
            "direct_ms": samples(&direct),
            "ledger_ms": samples(&through),
            "ledger_early_median_ms": through.early.as_secs_f64() * 1000.0,
            "ledger_late_median_ms": through.late.as_secs_f64() * 1000.0,
            "direct_per_second": direct.per_second,
            "ledger_per_second": through.per_second,
        });
        std::fs::write(path, serde_json::to_string_pretty(&record)?)?;
    }
    let added = |p| percentile(&through.sorted, p).saturating_sub(percentile(&direct.sorted, p));
    println!(
        "\nAdded by Ledger: {} ms at p50, {} ms at p99",
        ms(added(50.0)),
        ms(added(99.0))
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_are_nearest_rank() {
        let d: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
        assert_eq!(percentile(&d, 50.0), Duration::from_millis(50));
        assert_eq!(percentile(&d, 99.0), Duration::from_millis(99));
        assert_eq!(percentile(&d, 100.0), Duration::from_millis(100));
        assert_eq!(percentile(&d, 0.0), Duration::from_millis(1));
        assert_eq!(percentile(&d[..1], 99.0), Duration::from_millis(1));
    }
}
