//! Runs the real `ledger` binary, with its config file and migrations, as a process.

mod common;

use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{Probe, eventually, spawn_tool_server};
use ledger_store::{EventKind, EventStore, NewEvent, Outcome, PgEventStore, ToolMode};
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

#[tokio::test(flavor = "multi_thread")]
async fn the_fork_command_starts_a_fork_that_the_log_names() {
    let db = TestDb::new().await;
    let store = PgEventStore::new(db.pool());
    let lease = store.acquire_lease("src", "test", PATIENCE).await.unwrap();
    for _ in 0..2 {
        let request = NewEvent::request("src", "t", &json!({}), json!({}));
        store.append(request, &lease).await.unwrap();
    }
    let db_url = db.url();
    let prompt = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(prompt.path(), "Be brief.").unwrap();

    let (ok, named) = ledger_output(
        &[
            "fork",
            "src",
            "--at",
            "3",
            "--model",
            "m2",
            "--tools",
            "replay",
            "--as",
            "f1",
            "--system-prompt-file",
            prompt.path().to_str().unwrap(),
        ],
        db_url,
    );
    assert!(ok);
    assert_eq!(named, "f1\n");
    let fork = store.fork("f1").await.unwrap().unwrap();
    assert_eq!((fork.at_step, fork.model.as_deref()), (3, Some("m2")));
    assert_eq!(fork.system_prompt.as_deref(), Some("Be brief."));
    assert_eq!(fork.tools, ToolMode::Replay);

    let (ok, generated) = ledger_output(&["fork", "src", "--at", "1"], db_url);
    assert!(ok);
    assert!(generated.trim().starts_with("src.fork-"), "{generated}");
    assert_eq!(
        store.fork(generated.trim()).await.unwrap().unwrap().tools,
        ToolMode::Safe
    );

    let (ok, log) = ledger_output(&["log", "f1"], db_url);
    assert!(ok);
    assert_eq!(log, "fork of src at step 3, tools replay, model m2\n");

    let refused = |args: &[&str]| {
        let output = Command::new(LEDGER)
            .args(args)
            .env("DATABASE_URL", db_url)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        String::from_utf8(output.stderr).unwrap()
    };
    assert!(refused(&["fork", "src", "--at", "4"]).contains("from 1 to 3"));
    assert!(refused(&["fork", "src", "--at", "0"]).contains("from 1 to 3"));
    assert!(refused(&["fork", "missing", "--at", "1"]).contains("no events"));
    assert!(refused(&["fork", "src", "--at", "1", "--as", "f1"]).contains("already exists"));
    assert!(refused(&["fork", "src", "--at", "1", "--tools", "all"]).contains("safe"));
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_and_compare_report_where_a_new_model_diverges() {
    let stack = common::Stack::start().await;

    let (ok, replay) = ledger_output(&["replay", "src", "--model", "reporter-2"], stack.db_url());
    assert!(ok);
    let replay = replay.trim();
    stack.run(replay, common::REPORT).await.unwrap();

    let (ok, text) = ledger_output(&["compare", "src", replay], stack.db_url());
    assert!(ok);
    let first = text.lines().next().unwrap();
    assert!(
        first.starts_with(&format!(
            "src -> {replay}: diverges at step 2: email__send_email"
        )),
        "{text}"
    );
    assert!(text.contains("replay: 3 model calls"), "{text}");
    assert!(
        text.contains("tools: 0 run, 1 from the log, 1 refused"),
        "{text}"
    );
    let (ok, json) = ledger_output(&["compare", "src", replay, "--json"], stack.db_url());
    assert!(ok);
    let report: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(report["divergence"]["step"], 2);
    assert_eq!(report["replay_cost"]["tools_run"], 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn regress_runs_the_suites_agent_for_each_session_and_counts_the_outcomes() {
    let stack = common::Stack::start().await;
    let dir = tempfile::tempdir().unwrap();
    let suite = |name: &str, agent: &str| {
        let path = dir.path().join(format!("{name}.toml"));
        let text = format!(
            "model = \"reporter-2\"\nagent = [\"sh\", \"-c\", {agent:?}, \"agent\", \"{{session}}\"]\n\
             [[run]]\nsession = \"src\"\n[[run]]\nsession = \"src\"\nargs = [\"extra\"]\n"
        );
        std::fs::write(&path, text).unwrap();
        path.to_str().unwrap().to_owned()
    };
    let regress = |path: &str, extra: &[&str]| {
        let output = Command::new(LEDGER)
            .args(["regress", path])
            .args(extra)
            .env("DATABASE_URL", stack.db_url())
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        (output.status.success(), stdout)
    };

    // An agent that does nothing: each replay records nothing, so it diverges at once.
    // It is given the replay's session, and each run's own arguments.
    let args = dir.path().join("args");
    let echo = suite("echo", &format!("echo \"$@\" >> '{}'", args.display()));
    let (ok, out) = regress(&echo, &[]);
    assert!(ok, "{out}");
    assert!(
        out.ends_with("2 runs on reporter-2: 0 same, 2 diverged, 0 failed\n"),
        "{out}"
    );
    let given = std::fs::read_to_string(&args).unwrap();
    let given: Vec<&str> = given.lines().collect();
    assert_eq!(given.len(), 2, "{given:?}");
    assert!(
        given[0].starts_with("src.replay-") && !given[0].contains(' '),
        "{given:?}"
    );
    assert!(
        given[1].starts_with("src.replay-") && given[1].ends_with(" extra"),
        "{given:?}"
    );
    let (ok, _) = regress(&echo, &["--fail-on-divergence"]);
    assert!(!ok);

    let failing = suite("failing", "echo broken >&2; exit 3");
    let (ok, out) = regress(&failing, &[]);
    assert!(ok, "{out}");
    assert!(
        out.contains("the agent failed: sh exited with exit status: 3: broken"),
        "{out}"
    );
    assert!(out.ends_with("0 same, 0 diverged, 2 failed\n"), "{out}");

    // An agent that never finishes is stopped at the limit. Only it gets a short one: it
    // never finishes, so the limit is reached however loaded the machine is.
    let (ok, out) = regress(&suite("sleeping", "sleep 600"), &["--timeout-secs", "1"]);
    assert!(ok, "{out}");
    assert!(out.contains("did not finish within 1s"), "{out}");

    // Another model on the command line wins over the suite's.
    let (ok, out) = regress(&echo, &["--model", "reporter-3"]);
    assert!(ok);
    assert!(
        out.ends_with("2 runs on reporter-3: 0 same, 2 diverged, 0 failed\n"),
        "{out}"
    );
}

/// A stand-in OTLP/HTTP collector: every request's path and body, in order.
async fn spawn_collector() -> (String, Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>) {
    let received = Arc::new(std::sync::Mutex::new(Vec::new()));
    let app = axum::Router::new().fallback({
        let received = received.clone();
        move |uri: axum::http::Uri, body: axum::body::Bytes| {
            let received = received.clone();
            async move {
                received
                    .lock()
                    .unwrap()
                    .push((uri.path().to_owned(), body.to_vec()));
                axum::http::StatusCode::OK
            }
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await });
    (url, received)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_binary_sends_its_traces_and_metrics_to_the_configured_collector() {
    let db = TestDb::new().await;
    let dir = tempfile::tempdir().unwrap();
    let files_url = spawn_tool_server(FileServer::new(dir.path()).unwrap()).await;
    let (collector, received) = spawn_collector().await;
    let port = ReservedPort::claim();
    let addr = port.addr();
    let config = dir.path().join("ledger.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            listen = "{addr}"
            [[upstream]]
            name = "files"
            url = "{files_url}"
            [telemetry]
            otlp_endpoint = "{collector}"
            service_name = "ledger-under-test"
            "#
        ),
    )
    .unwrap();

    let mut ledger = start_ledger(&config, db.url());
    wait_for_listener(addr).await;
    let agent = connect(&format!("http://{addr}/mcp"), Some("run-1"))
        .await
        .unwrap();
    agent
        .call_tool(call_params("files__read_file", json!({ "path": "x" })))
        .await
        .unwrap();
    drop(agent);

    // Ctrl-C: the gateway stops, and sends what it still holds before exiting.
    let pid = ledger.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-INT", &pid])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + PATIENCE;
    let status = loop {
        if let Some(status) = ledger.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "the binary did not stop");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(status.success(), "{status}");

    let received = received.lock().unwrap();
    let body_of = |path: &str| {
        received
            .iter()
            .filter(|(p, _)| p == path)
            .flat_map(|(_, body)| body.clone())
            .collect::<Vec<u8>>()
    };
    let contains = |body: &[u8], text: &str| body.windows(text.len()).any(|w| w == text.as_bytes());
    let traces = body_of("/v1/traces");
    assert!(
        contains(&traces, "tools/call files__read_file"),
        "{received:?}"
    );
    assert!(contains(&traces, "ledger-under-test"));
    assert!(contains(&traces, "run-1"));
    assert!(contains(&body_of("/v1/metrics"), "ledger.calls"));
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_lets_a_running_call_finish_and_log_its_answer() {
    let db = TestDb::new().await;
    let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
    let probe = Probe::new(store.clone(), "s");
    let probe_url = spawn_tool_server(probe.clone()).await;
    let port = ReservedPort::claim();
    let addr = port.addr();
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ledger.toml");
    let patience = PATIENCE.as_secs();
    std::fs::write(
        &config,
        format!(
            r#"
            listen = "{addr}"
            call_timeout_secs = {patience}
            shutdown_grace_secs = {patience}
            [[upstream]]
            name = "probe"
            url = "{probe_url}"
            "#
        ),
    )
    .unwrap();
    let mut ledger = start_ledger(&config, db.url());
    wait_for_listener(addr).await;
    let ready = format!("http://{addr}/readyz");
    assert_eq!(reqwest::get(&ready).await.unwrap().status(), 200);
    assert_eq!(
        reqwest::get(format!("http://{addr}/healthz"))
            .await
            .unwrap()
            .status(),
        200
    );

    let agent = connect(&format!("http://{addr}/mcp"), Some("s"))
        .await
        .unwrap();
    let call =
        tokio::spawn(async move { agent.call_tool(call_params("probe__wait", json!({}))).await });
    probe.wait_until_started().await;

    // Told to stop with a call still running: it says it is not ready, turns new calls
    // away, and waits for the running one.
    let pid = ledger.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    eventually(|| async { reqwest::get(&ready).await.unwrap().status() == 503 }).await;
    let late = connect(&format!("http://{addr}/mcp"), Some("late"))
        .await
        .unwrap();
    // A tool that answers at once, so that if the call were taken, the test fails rather
    // than waits on it.
    let refused = late
        .call_tool(call_params("probe__observe", json!({})))
        .await
        .unwrap_err();
    match refused {
        rmcp::ServiceError::McpError(e) => {
            assert_eq!(e.code, ledger_gateway::error_codes::NOT_RECORDED)
        }
        other => panic!("{other}"),
    }
    drop(late);
    assert!(
        ledger.0.try_wait().unwrap().is_none(),
        "it exited with a call running"
    );

    probe.release();
    let deadline = Instant::now() + PATIENCE;
    let status = loop {
        if let Some(status) = ledger.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "the binary did not stop");
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert!(status.success(), "{status}");
    // The answer may not reach the agent: the gateway stops once it is logged, and closes
    // the agent's connection. A retry gets it from the log. The client would wait forever.
    call.abort();

    let events = store.session_events("s").await.unwrap();
    let kinds: Vec<EventKind> = events.iter().map(|e| e.kind).collect();
    assert_eq!(kinds, [EventKind::CallRequest, EventKind::CallResponse]);
    assert_eq!(events[1].outcome, Some(Outcome::Ok));
    assert!(store.session_events("late").await.unwrap().is_empty());
}
