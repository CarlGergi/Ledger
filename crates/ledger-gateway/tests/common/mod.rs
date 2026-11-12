#![allow(dead_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use ledger_gateway::ModelUpstream;
use ledger_gateway::{Gateway, McpUpstream, Upstream};
use ledger_store::{
    Event, EventKind, EventStore, Fork, Lease, NewEvent, PgEventStore, SessionSummary, StoreError,
    ToolMode,
};
use ledger_testkit::PATIENCE;
use ledger_testkit::TestDb;
use ledger_tools::email::{EmailServer, Outbox};
use ledger_tools::files::FileServer;
use ledger_tools::http::serve_mcp;
use ledger_tools::model::Received;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, ListToolsResult,
    PaginatedRequestParams, RequestMetaObject, ServerCapabilities, ServerConfig, Tool,
    ToolAnnotations,
};
use rmcp::service::{RequestContext, RoleServer};
use rmcp::{ErrorData, ServerHandler};
use sample_agent::{Conversation, connect, run_model_agent};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

pub const CALL_TIMEOUT: Duration = PATIENCE;

pub fn upstream(name: &str, url: &str) -> Arc<dyn Upstream> {
    Arc::new(McpUpstream::new(name, url, PATIENCE))
}

/// Starts a tool server on an ephemeral port and returns its URL.
pub async fn spawn_tool_server<S>(handler: S) -> String
where
    S: ServerHandler + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(serve_mcp(handler, listener, CancellationToken::new()));
    url
}

/// Starts the scripted model on an ephemeral port: its base URL, up to `/v1`, and what it
/// received.
pub async fn spawn_model() -> (String, ledger_tools::model::Received) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let received = ledger_tools::model::Received::default();
    tokio::spawn(ledger_tools::model::serve_model(
        listener,
        received.clone(),
        CancellationToken::new(),
    ));
    (url, received)
}

/// Starts a gateway on an ephemeral port and returns its URL.
pub async fn spawn_gateway(gateway: Gateway) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    tokio::spawn(ledger_gateway::serve(
        gateway,
        listener,
        Vec::new(),
        CancellationToken::new(),
    ));
    url
}

/// The real tool servers behind a real gateway.
pub struct Harness {
    pub url: String,
    pub outbox: Arc<Outbox>,
    pub files: tempfile::TempDir,
    pub email_url: String,
    pub files_url: String,
}

impl Harness {
    pub async fn start(store: Arc<dyn EventStore>) -> Self {
        let files = tempfile::tempdir().unwrap();
        let outbox = Arc::new(Outbox::new(files.path().join(".outbox.jsonl")));
        let email_url = spawn_tool_server(EmailServer::new(outbox.clone())).await;
        let files_url = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
        let gateway = Gateway::new(
            store,
            vec![upstream("email", &email_url), upstream("files", &files_url)],
            CALL_TIMEOUT,
        );
        let url = spawn_gateway(gateway).await;
        Self {
            url,
            outbox,
            files,
            email_url,
            files_url,
        }
    }
}

/// A store that can be told to refuse appends of a given kind.
pub struct FaultyStore {
    inner: Arc<dyn EventStore>,
    fail_kind: Mutex<Option<EventKind>>,
}

impl FaultyStore {
    pub fn new(inner: Arc<dyn EventStore>) -> Self {
        Self {
            inner,
            fail_kind: Mutex::new(None),
        }
    }

    pub async fn fail_appends_of(&self, kind: EventKind) {
        *self.fail_kind.lock().await = Some(kind);
    }

    pub async fn heal(&self) {
        *self.fail_kind.lock().await = None;
    }
}

#[async_trait]
impl EventStore for FaultyStore {
    async fn acquire_lease(
        &self,
        session_id: &str,
        owner: &str,
        ttl: Duration,
    ) -> Result<Lease, StoreError> {
        self.inner.acquire_lease(session_id, owner, ttl).await
    }

    async fn append(&self, event: NewEvent, lease: &Lease) -> Result<Event, StoreError> {
        if *self.fail_kind.lock().await == Some(event.kind) {
            return Err(StoreError::Database(sqlx::Error::PoolTimedOut));
        }
        self.inner.append(event, lease).await
    }

    async fn renew_lease(&self, lease: &Lease) -> Result<(), StoreError> {
        self.inner.renew_lease(lease).await
    }

    async fn session_events(&self, session_id: &str) -> Result<Vec<Event>, StoreError> {
        self.inner.session_events(session_id).await
    }

    async fn in_doubt(&self) -> Result<Vec<Event>, StoreError> {
        self.inner.in_doubt().await
    }

    async fn sessions(&self) -> Result<Vec<SessionSummary>, StoreError> {
        self.inner.sessions().await
    }

    async fn create_fork(&self, fork: &Fork) -> Result<(), StoreError> {
        self.inner.create_fork(fork).await
    }

    async fn fork(&self, session_id: &str) -> Result<Option<Fork>, StoreError> {
        self.inner.fork(session_id).await
    }
}

/// A tool server that lets a test watch and steer what happens during a call.
///
/// - Every call records the `_meta` that actually arrived.
/// - `observe` records what the log held at the moment the tool ran.
/// - `wait` runs only once the test calls `release`.
/// - `reject` answers with a JSON-RPC error.
/// - Every tool is listed with `annotations` and `input_schema`, and `listings` counts
///   the listings.
/// - `received` holds the arguments of every call, in order.
#[derive(Clone)]
pub struct Probe {
    pub store: Arc<dyn EventStore>,
    pub session: String,
    pub observed: Arc<Mutex<Option<Vec<Event>>>>,
    pub received_meta: Arc<Mutex<Option<RequestMetaObject>>>,
    pub stall_listing: bool,
    pub annotations: Option<ToolAnnotations>,
    pub input_schema: serde_json::Value,
    pub listings: Arc<AtomicUsize>,
    pub received: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    pub gate: Arc<Semaphore>,
    pub started: Arc<AtomicUsize>,
    pub finished: Arc<AtomicUsize>,
}

impl Probe {
    pub fn new(store: Arc<dyn EventStore>, session: &str) -> Self {
        Self {
            store,
            session: session.to_owned(),
            observed: Arc::default(),
            received_meta: Arc::default(),
            stall_listing: false,
            annotations: None,
            input_schema: serde_json::json!({"type": "object"}),
            listings: Arc::default(),
            received: Arc::default(),
            gate: Arc::new(Semaphore::new(0)),
            started: Arc::default(),
            finished: Arc::default(),
        }
    }

    pub fn release(&self) {
        self.gate.add_permits(1);
    }

    pub async fn wait_until_started(&self) {
        eventually(|| async { self.started.load(Ordering::SeqCst) > 0 }).await;
    }
}

impl ServerHandler for Probe {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        self.listings.fetch_add(1, Ordering::SeqCst);
        if self.stall_listing {
            std::future::pending::<()>().await;
        }
        let schema = Arc::new(self.input_schema.as_object().unwrap().clone());
        let tools = ["observe", "wait", "reject"]
            .into_iter()
            .map(|name| {
                let mut tool = Tool::new(name, "probe", schema.clone());
                tool.annotations = self.annotations.clone();
                tool
            })
            .collect();
        Ok(ListToolsResult::with_all_items(tools))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let arguments = serde_json::json!(request.arguments);
        self.received.lock().unwrap().push(arguments);
        *self.received_meta.lock().await = Some(context.meta);
        match request.name.as_ref() {
            "observe" => {
                let events = self.store.session_events(&self.session).await.unwrap();
                *self.observed.lock().await = Some(events);
            }
            "wait" => {
                let _permit = self.gate.acquire().await.unwrap();
            }
            "reject" => return Err(ErrorData::invalid_params("probe says no", None)),
            other => panic!("unexpected tool {other}"),
        }
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(CallToolResult::success(vec![ContentBlock::text("done")]).into())
    }
}

/// Waits for something that happens after a response has returned.
pub async fn eventually<F, Fut>(check: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + PATIENCE;
    while Instant::now() < deadline {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition was not met in time");
}

pub const REPORT: &str = "All good.";

/// The scripted model and both tool servers behind one gateway, with one source run of
/// `reporter-1` recorded in session `src`: model, write, model, send, model, ten events.
pub struct Stack {
    _db: TestDb,
    pub store: Arc<dyn EventStore>,
    pub mcp: String,
    pub model: Received,
    pub outbox: Arc<Outbox>,
    _files: tempfile::TempDir,
}

impl Stack {
    pub async fn start() -> Self {
        let db = TestDb::new().await;
        let store: Arc<dyn EventStore> = Arc::new(PgEventStore::new(db.pool()));
        let (model_url, model) = spawn_model().await;
        let files = tempfile::tempdir().unwrap();
        let outbox = Arc::new(Outbox::new(files.path().join(".outbox.jsonl")));
        let email = spawn_tool_server(EmailServer::new(outbox.clone())).await;
        let file_server = spawn_tool_server(FileServer::new(files.path()).unwrap()).await;
        let gateway = Gateway::new(
            store.clone(),
            vec![upstream("email", &email), upstream("files", &file_server)],
            CALL_TIMEOUT,
        )
        .with_model(ModelUpstream::new(&model_url, None, PATIENCE));
        let mcp = spawn_gateway(gateway).await;
        let stack = Self {
            _db: db,
            store,
            mcp,
            model,
            outbox,
            _files: files,
        };
        let source = stack.run("src", REPORT).await.unwrap();
        assert_eq!(source.answer, "Sent the daily report.");
        assert_eq!(stack.events("src").await.len(), 10);
        stack
    }

    /// The agent, run under `session` through the gateway, model calls included.
    pub async fn run(&self, session: &str, report: &str) -> anyhow::Result<Conversation> {
        let client = connect(&self.mcp, Some(session)).await?;
        let gateway_v1 = format!("{}/v1", self.mcp.trim_end_matches("/mcp"));
        run_model_agent(&client, &gateway_v1, Some(session), "reporter-1", report).await
    }

    pub async fn fork(&self, at_step: i64, model: Option<&str>, tools: ToolMode) -> String {
        self.fork_with(at_step, model, None, tools).await
    }

    pub async fn fork_with(
        &self,
        at_step: i64,
        model: Option<&str>,
        system_prompt: Option<&str>,
        tools: ToolMode,
    ) -> String {
        let session = format!("src.fork-{at_step}-{}", tools.as_str());
        let fork = Fork {
            session_id: session.clone(),
            source_session: "src".into(),
            at_step,
            model: model.map(str::to_owned),
            system_prompt: system_prompt.map(str::to_owned),
            tools,
        };
        self.store.create_fork(&fork).await.unwrap();
        session
    }

    pub async fn events(&self, session: &str) -> Vec<Event> {
        self.store.session_events(session).await.unwrap()
    }

    /// Model calls the model actually received after the source run.
    pub fn later_model_bodies(&self) -> Vec<Value> {
        self.model.bodies()[3..].to_vec()
    }
}
