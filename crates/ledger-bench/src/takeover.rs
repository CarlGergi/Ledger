//! How long a session waits after the copy serving it stops: two `ledger serve` processes
//! share one database; an agent calls through copy A, A is stopped, and the agent calls
//! through copy B until B answers.
//!
//! Killed (SIGKILL), A leaves its lease to expire: B refuses the session (-32050, not
//! recorded) until then, and every logged event renews the lease, so the wait is about
//! `lease_secs` after A's last event. Stopped (SIGTERM), A releases its leases on the way
//! out, and B can take the session at once. The killed numbers are seconds, so the
//! machine's load barely moves them; the machine and its load are printed anyway.
//!
//! Run `cargo build -p ledger-gateway` first: this starts the `ledger` binary next to it.

use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use clap::Parser;
use ledger_testkit::ReservedPort;
use ledger_testkit::TestDb;
use ledger_tools::files::FileServer;
use ledger_tools::http::serve_mcp;
use sample_agent::RetryingAgent;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
struct Args {
    /// Lease lengths to measure, in seconds.
    #[arg(long, value_delimiter = ',', default_value = "1,5,30")]
    lease_secs: Vec<u64>,
    /// Kills per lease length.
    #[arg(long, default_value_t = 10)]
    trials: usize,
    /// Calls through A before it is killed.
    #[arg(long, default_value_t = 20)]
    calls: usize,
    /// How often the agent tries B, in milliseconds: the resolution of each measurement.
    #[arg(long, default_value_t = 10)]
    poll_ms: u64,
    /// Also writes every trial to this file, as JSON, for the figures.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

/// How A is stopped.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Stop {
    /// SIGKILL: no draining, no last words.
    Kill,
    /// SIGTERM: drains, releases its leases, exits.
    Term,
}

impl Stop {
    fn describe(self) -> &'static str {
        match self {
            Stop::Kill => "killed (SIGKILL)",
            Stop::Term => "stopped (SIGTERM)",
        }
    }
}

/// One stop: how long from A's exit, and from its last logged event, until B answered.
#[derive(Debug, Clone, Copy)]
struct Trial {
    after_kill: Duration,
    after_last_event: Duration,
    refusals: usize,
}

/// Median, smallest and largest.
fn summary(values: &[Duration]) -> (Duration, Duration, Duration) {
    assert!(!values.is_empty());
    let mut sorted = values.to_vec();
    sorted.sort();
    let n = sorted.len();
    let median = if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2
    };
    (median, sorted[0], sorted[n - 1])
}

fn secs(d: Duration) -> String {
    format!("{:.3}", d.as_secs_f64())
}

fn machine() -> String {
    let sysctl = |name: &str| {
        Command::new("sysctl")
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

/// A `ledger serve` process, killed when dropped.
struct Copy {
    child: Child,
    url: String,
}

impl Copy {
    fn start(
        ledger: &Path,
        dir: &Path,
        name: &str,
        database: &str,
        tool: &str,
        lease: u64,
    ) -> Self {
        let port = ReservedPort::claim();
        let addr = port.addr();
        let config = dir.join(format!("{name}.toml"));
        std::fs::write(
            &config,
            format!(
                "listen = \"{addr}\"\nlease_secs = {lease}\n\n[[upstream]]\nname = \"files\"\nurl = \"{tool}\"\n"
            ),
        )
        .expect("writing the config");
        drop(port);
        let log = std::fs::File::create(dir.join(format!("{name}.log"))).expect("log file");
        let child = Command::new(ledger)
            .args(["serve", "--config"])
            .arg(&config)
            .env("DATABASE_URL", database)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .expect("starting ledger");
        let started = Instant::now();
        while TcpStream::connect(addr).is_err() {
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "{name} did not start; see {}",
                dir.join(format!("{name}.log")).display()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        Self {
            child,
            url: format!("http://{addr}/mcp"),
        }
    }

    /// Stops the copy, and returns once it has exited.
    fn stop(mut self, how: Stop) {
        match how {
            Stop::Kill => self.child.kill().expect("killing the copy"),
            Stop::Term => {
                let pid = self.child.id().to_string();
                let sent = Command::new("kill").args(["-TERM", &pid]).status();
                assert!(sent.expect("running kill").success(), "SIGTERM failed");
            }
        }
        let status = self.child.wait().expect("reaping the copy");
        assert!(
            how == Stop::Kill || status.success(),
            "stopped badly: {status}"
        );
    }
}

impl Drop for Copy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn answered(delivery: &sample_agent::Delivery) -> bool {
    matches!(&delivery.result, Ok(r) if r.is_error != Some(true))
}

async fn trial(args: &Args, a: Copy, how: Stop, b_url: &str, session: &str) -> Trial {
    let read = || json!({ "path": "f.txt" });
    let mut agent = RetryingAgent::new(&a.url, Some(session));
    let mut last_event = Instant::now();
    for step in 1..=args.calls {
        let delivery = agent
            .call(&step.to_string(), "files__read_file", read(), 1)
            .await;
        assert!(answered(&delivery), "through A: {:?}", delivery.result);
        // The call's answer was logged before it reached the agent: A's last event is no
        // later than this.
        last_event = Instant::now();
    }
    a.stop(how);
    let killed = Instant::now();
    drop(agent);

    let mut agent = RetryingAgent::new(b_url, Some(session));
    let step = (args.calls + 1).to_string();
    let mut refusals = 0;
    loop {
        let delivery = agent.call(&step, "files__read_file", read(), 1).await;
        if answered(&delivery) {
            break;
        }
        refusals += 1;
        assert!(
            killed.elapsed() < Duration::from_secs(600),
            "B never took over: {:?}",
            delivery.result
        );
        tokio::time::sleep(Duration::from_millis(args.poll_ms)).await;
    }
    let done = Instant::now();
    agent.close().await;
    Trial {
        after_kill: done - killed,
        after_last_event: done - last_event,
        refusals,
    }
}

fn ledger_binary() -> PathBuf {
    let exe = std::env::current_exe().expect("this binary's path");
    let ledger = exe.with_file_name("ledger");
    assert!(
        ledger.exists(),
        "{} is missing: run cargo build -p ledger-gateway with the same profile first",
        ledger.display()
    );
    ledger
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ledger = ledger_binary();
    let db = TestDb::new().await;
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(dir.path().join("f.txt"), "x").expect("writing the file");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("binding");
    let tool = format!("http://{}/mcp", listener.local_addr().expect("address"));
    let files = FileServer::new(dir.path()).expect("file server");
    tokio::spawn(serve_mcp(files, listener, CancellationToken::new()));

    println!("Machine: {}", machine());
    println!(
        "{} stops of each kind per lease length, {} calls through A before each, B tried every {} ms.",
        args.trials, args.calls, args.poll_ms
    );
    println!();
    println!(
        "| How A stopped | lease_secs | A's exit to B's answer, median | min | max | A's last event to B's answer, median | min | max |"
    );
    println!("| --- | --: | --: | --: | --: | --: | --: | --: |");
    let mut records = Vec::new();
    for &lease in &args.lease_secs {
        let b = Copy::start(
            &ledger,
            dir.path(),
            &format!("b-{lease}"),
            db.url(),
            &tool,
            lease,
        );
        for how in [Stop::Kill, Stop::Term] {
            let mut trials = Vec::new();
            for t in 0..args.trials {
                let name = format!("a-{lease}-{how:?}-{t}");
                let a = Copy::start(&ledger, dir.path(), &name, db.url(), &tool, lease);
                let session = format!("takeover-{lease}-{how:?}-{t}");
                let result = trial(&args, a, how, &b.url, &session).await;
                eprintln!(
                    "lease {lease}s, {} {}: {} s after it exited, {} s after its last event, {} refusals",
                    how.describe(),
                    t + 1,
                    secs(result.after_kill),
                    secs(result.after_last_event),
                    result.refusals
                );
                trials.push(result);
                records.push(json!({
                    "lease_secs": lease,
                    "stop": format!("{how:?}").to_lowercase(),
                    "after_exit_s": result.after_kill.as_secs_f64(),
                    "after_last_event_s": result.after_last_event.as_secs_f64(),
                    "refusals": result.refusals,
                }));
            }
            let (km, kmin, kmax) =
                summary(&trials.iter().map(|t| t.after_kill).collect::<Vec<_>>());
            let (lm, lmin, lmax) = summary(
                &trials
                    .iter()
                    .map(|t| t.after_last_event)
                    .collect::<Vec<_>>(),
            );
            println!(
                "| {} | {lease} | {} | {} | {} | {} | {} | {} |",
                how.describe(),
                secs(km),
                secs(kmin),
                secs(kmax),
                secs(lm),
                secs(lmin),
                secs(lmax)
            );
        }
        b.stop(Stop::Kill);
    }
    if let Some(path) = &args.json_out {
        let record = json!({ "machine": machine(), "poll_ms": args.poll_ms, "trials": records });
        std::fs::write(path, serde_json::to_string_pretty(&record).expect("JSON"))
            .expect("writing the JSON");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_summary_is_median_min_and_max() {
        let s = |v: &[u64]| {
            summary(
                &v.iter()
                    .map(|&ms| Duration::from_millis(ms))
                    .collect::<Vec<_>>(),
            )
        };
        let ms = Duration::from_millis;
        assert_eq!(s(&[3, 1, 2]), (ms(2), ms(1), ms(3)));
        assert_eq!(s(&[4, 1, 2, 3]), (ms(2) + ms(1) / 2, ms(1), ms(4)));
        assert_eq!(s(&[7]), (ms(7), ms(7), ms(7)));
    }
}
