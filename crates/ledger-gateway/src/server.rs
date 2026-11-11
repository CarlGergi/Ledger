use std::io;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use rmcp::model::ErrorCode;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::Gateway;
use crate::gateway::{ModelCall, ModelFailure};
use crate::model;
use crate::session::{resolve_session, validate};

/// The most a model request body may hold. Long conversations with images can be large.
const MAX_MODEL_REQUEST: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct ModelRoutes {
    gateway: Gateway,
    allowed_hosts: Arc<Vec<String>>,
}

/// Serves the gateway until `shutdown` is cancelled: MCP at `/mcp`, and OpenAI-compatible
/// chat completions at `/v1/chat/completions`, naming the session in the
/// `x-ledger-session` header, or at `/s/<session>/v1/chat/completions` for clients that
/// can only set a base URL. An empty `allowed_hosts` accepts loopback hosts only.
pub async fn serve(
    gateway: Gateway,
    listener: TcpListener,
    allowed_hosts: Vec<String>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut config =
        StreamableHttpServerConfig::default().with_cancellation_token(shutdown.child_token());
    if !allowed_hosts.is_empty() {
        config = config.with_allowed_hosts(allowed_hosts.clone());
    }
    let routes = ModelRoutes {
        gateway: gateway.clone(),
        allowed_hosts: Arc::new(allowed_hosts),
    };
    let service = StreamableHttpService::new(
        move || Ok(gateway.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let app = Router::new()
        .route("/v1/chat/completions", post(chat))
        .route("/s/{session}/v1/chat/completions", post(chat_in_session))
        .with_state(routes)
        .nest_service("/mcp", service);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}

async fn chat(State(routes): State<ModelRoutes>, request: Request) -> Response {
    forward(routes, None, request).await
}

async fn chat_in_session(
    State(routes): State<ModelRoutes>,
    Path(session): Path<String>,
    request: Request,
) -> Response {
    forward(routes, Some(session), request).await
}

async fn forward(routes: ModelRoutes, session: Option<String>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    if !model::host_allowed(&parts.headers, &parts.uri, &routes.allowed_hosts) {
        tracing::warn!("rejected a model call with a disallowed Host or Origin");
        return refuse(StatusCode::FORBIDDEN, "Host or Origin is not allowed");
    }
    let session = match session {
        Some(session) => validate(&session).map(|()| session),
        None => resolve_session(&parts),
    };
    let session = match session {
        Ok(session) => session,
        Err(e) => return refuse(StatusCode::BAD_REQUEST, &e.message),
    };
    let bytes = match axum::body::to_bytes(body, MAX_MODEL_REQUEST).await {
        Ok(bytes) => bytes,
        Err(e) => return refuse(StatusCode::PAYLOAD_TOO_LARGE, &e.to_string()),
    };
    let body = match serde_json::from_slice(&bytes) {
        Ok(body @ Value::Object(_)) => body,
        _ => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "the request body must be a JSON object",
            );
        }
    };
    let call = ModelCall {
        session,
        body,
        credentials: model::credentials(&parts.headers),
    };
    match routes.gateway.model_call(call).await {
        Ok(logged) => model::rebuild(&logged).into_response(),
        Err(ModelFailure { status, body }) => (status, axum::Json(body)).into_response(),
    }
}

/// A request turned away before anything was logged or sent.
fn refuse(status: StatusCode, message: &str) -> Response {
    let body = model::error_body(ErrorCode::INVALID_REQUEST.0, "invalid_request", message);
    (status, axum::Json(body)).into_response()
}
