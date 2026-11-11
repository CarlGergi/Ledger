//! Runs the real `ledger` binary, with its config file and migrations, as a process.

mod common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{Probe, eventually, spawn_tool_server};
use ledger_store::{EventKind, EventStore, Outcome, PgEventStore};
use ledger_testkit::{PATIENCE, ReservedPort, TestDb};
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::files::FileServer;
use sample_agent::{call_params, connect, run_script};
use serde_json::json;

const LEDGER: &str = env!("CARGO_BIN_EXE_ledger");

struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_ledger(config: &std::path::Path, database_url: &str) -> KillOnDrop {
    KillOnDrop(
        Command::new(LEDGER)
            .args(["serve", "--config"])
            .arg(config)
            .env("DATABASE_URL", database_url)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    )
}

fn ledger_output(args: &[&str], database_url: &str) -> (bool, String) {
    let output = Command::new(LEDGER)
        .args(args)
        .env("DATABASE_URL", database_url)
        .output()
        .unwrap();
    (
        output.status.success(),
        String::from_utf8(output.stdout).unwrap(),
    )
}

async fn wait_for_listener(addr: SocketAddr) {
    eventually(|| async move { tokio::net::TcpStream::connect(addr).await.is_ok() }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_binary_serves_records_and_survives_being_killed() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let dir = tempfile::tempdir().unwrap();
    let outbox = Arc::new(Outbox::new(dir.path().join("outbox.jsonl")));
    let email_url = spawn_tool_server(EmailServer::new(outbox.clone())).await;
    let files_url = spawn_tool_server(FileServer::new(dir.path()).unwrap()).await;

    let port = ReservedPort::claim();
    let addr = port.addr();
    let patience = PATIENCE.as_secs();
    let config = dir.path().join("ledger.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            listen = "{addr}"
            call_timeout_secs = {patience}
            connect_timeout_secs = {patience}
            lease_secs = 0
            [[upstream]]
            name = "email"
            url = "{email_url}"
            [[upstream]]
            name = "files"
            url = "{files_url}"
            "#
        ),
    )
    .unwrap();

    let db_url = db.url();
    let url = format!("http://{addr}/mcp");

    let mut ledger = start_ledger(&config, db_url);
    wait_for_listener(addr).await;
    let agent = connect(&url, Some("run-1")).await.unwrap();
    let steps = run_script(&agent, "first").await.unwrap();

    // SIGKILL: no shutdown code runs. Everything already answered must still be there.
    ledger.0.kill().unwrap();
    ledger.0.wait().unwrap();
    drop(agent);

    let store = PgEventStore::new(pool.clone());
    let before = store.session_events("run-1").await.unwrap();
    assert_eq!(before.len(), steps.len() * 2);

    let _ledger = start_ledger(&config, db_url);
    wait_for_listener(addr).await;
    let agent = connect(&url, Some("run-1")).await.unwrap();
    run_script(&agent, "second").await.unwrap();

    let after = store.session_events("run-1").await.unwrap();
    assert_eq!(after.len(), steps.len() * 4);
    assert_eq!(&after[..before.len()], &before[..]);
    let seqs: Vec<i64> = after.iter().map(|e| e.seq).collect();
    assert_eq!(seqs, (1..=after.len() as i64).collect::<Vec<_>>());
    assert!(
        after
            .iter()
            .filter(|e| e.kind == EventKind::CallResponse)
            .all(|e| e.outcome == Some(Outcome::Ok))
    );
    assert_eq!(outbox.sent().await.unwrap().len(), 2);

    let (ok, listed) = ledger_output(&["sessions", "--json"], db_url);
    assert!(ok);
    let listed: serde_json::Value = serde_json::from_str(listed.lines().next().unwrap()).unwrap();
    assert_eq!(listed["session_id"], "run-1");
    assert_eq!(listed["event_count"], after.len());

    let (ok, sessions) = ledger_output(&["sessions"], db_url);
    assert!(ok);
    assert!(sessions.contains("run-1"), "{sessions}");
    assert!(
        sessions.contains(&format!("{} events", after.len())),
        "{sessions}"
    );

    let (ok, log) = ledger_output(&["log", "run-1"], db_url);
    assert!(ok);
    let lines: Vec<&str> = log.lines().collect();
    assert_eq!(lines.len(), after.len());
    assert!(lines[0].contains("files__write_file"), "{}", lines[0]);
    assert!(lines[0].contains("request (idempotent)"), "{}", lines[0]);
    assert!(lines[1].contains("response to #1 (ok)"), "{}", lines[1]);

    let (ok, json_log) = ledger_output(&["log", "run-1", "--json"], db_url);
    assert!(ok);
    let first: serde_json::Value = serde_json::from_str(json_log.lines().next().unwrap()).unwrap();
    assert_eq!(first["seq"], 1);
    assert_eq!(first["kind"], "call_request");

    let (ok, _) = ledger_output(&["log", "no-such-session"], db_url);
    assert!(!ok);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crash_mid_call_leaves_the_request_on_record() {
    let db = TestDb::new().await;
    let pool = db.pool();
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(pool.clone()));
    let dir = tempfile::tempdir().unwrap();
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;

    let port = ReservedPort::claim();
    let addr = port.addr();
    let patience = PATIENCE.as_secs();
    let config = dir.path().join("ledger.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            listen = "{addr}"
            call_timeout_secs = {patience}
            connect_timeout_secs = {patience}
            lease_secs = 0
            [[upstream]]
            name = "probe"
            url = "{probe_url}"
            "#
        ),
    )
    .unwrap();

    let db_url = db.url();
    let url = format!("http://{addr}/mcp");
    let mut ledger = start_ledger(&config, db_url);
    wait_for_listener(addr).await;

    let agent = connect(&url, Some("s")).await.unwrap();
    let in_flight =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;

    ledger.0.kill().unwrap();
    ledger.0.wait().unwrap();
    in_flight.abort();
    probe.release();

    // The tool ran, nobody recorded how it ended, and the log says exactly that.
    let events = store.session_events("s").await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].kind, EventKind::CallRequest);
    assert_eq!(events[0].tool, "probe__wait");

    // The next copy carries on from there without a gap. Taking the session over, it
    // finds the interrupted call in doubt and, since it is not safe to repeat, flags it
    // for a person rather than ever running it again.
    let _ledger = start_ledger(&config, db_url);
    wait_for_listener(addr).await;
    let agent = connect(&url, Some("s")).await.unwrap();
    agent
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap();

    let events = store.session_events("s").await.unwrap();
    let shape: Vec<_> = events
        .iter()
        .map(|e| (e.seq, e.kind, e.request_seq, e.outcome))
        .collect();
    assert_eq!(
        shape,
        [
            (1, EventKind::CallRequest, None, None),
            (2, EventKind::CallResponse, Some(1), Some(Outcome::InDoubt)),
            (3, EventKind::CallRequest, None, None),
            (4, EventKind::CallResponse, Some(3), Some(Outcome::Ok)),
        ]
    );
    // The interrupted call and `observe`: the interrupted one never ran again.
    assert_eq!(probe.started.load(Ordering::SeqCst), 2);

    let (ok, flagged) = ledger_output(&["in-doubt"], db_url);
    assert!(ok);
    assert!(
        flagged.contains("call #1") && flagged.contains("probe__wait"),
        "{flagged}"
    );
}

#[test]
fn a_bad_override_file_stops_the_binary_with_a_useful_message() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ledger.toml");
    std::fs::write(
        &config,
        r#"
        listen = "127.0.0.1:0"
        effect_overrides = "effects.toml"
        [[upstream]]
        name = "email"
        url = "http://127.0.0.1:1/mcp"
        "#,
    )
    .unwrap();
    std::fs::write(
        dir.path().join("effects.toml"),
        "[tool.emial__send]\nclass = \"keyed\"",
    )
    .unwrap();

    let mut ledger = KillOnDrop(
        Command::new(LEDGER)
            .args(["serve", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );

    // Started the wrong way, it would serve forever rather than fail this test.
    let deadline = Instant::now() + PATIENCE;
    let status = loop {
        if let Some(status) = ledger.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "the binary is still running");
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(!status.success());
    let mut stderr = String::new();
    let mut pipe = ledger.0.stderr.take().unwrap();
    std::io::Read::read_to_string(&mut pipe, &mut stderr).unwrap();
    assert!(stderr.contains("emial__send"), "{stderr}");
}

#[test]
fn a_bad_config_stops_the_binary_with_a_useful_message() {
    let output = Command::new(LEDGER)
        .args(["serve", "--config", "/nonexistent/ledger.toml"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("/nonexistent/ledger.toml"), "{stderr}");
}
