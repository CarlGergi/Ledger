//! An HTTP proxy that loses answers: the failure that makes retries dangerous.
//!
//! It passes everything through to its target, except some `tools/call` requests: the
//! first few, or each one with a set probability. Those it forwards, waits until the tool
//! has answered, and then tells the client the connection failed. The tool ran; the client
//! cannot know it did.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use futures::TryStreamExt;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct LossyProxy {
    client: reqwest::Client,
    target: String,
    to_lose: Arc<AtomicUsize>,
    /// Lose each answer with this probability, drawn from this generator, instead.
    chance: Option<(f64, Arc<Mutex<ChaCha8Rng>>)>,
    lost: Arc<AtomicUsize>,
}

impl LossyProxy {
    /// Forwards to `target` (scheme, host and port), losing the answers to the first
    /// `lose` tool calls.
    pub fn new(target: &str, lose: usize) -> Self {
        Self {
            client: reqwest::Client::new(),
            target: target.trim_end_matches('/').to_owned(),
            to_lose: Arc::new(AtomicUsize::new(lose)),
            chance: None,
            lost: Arc::default(),
        }
    }

    /// Forwards to `target`, losing each tool call's answer with probability `p`. The same
    /// seed loses the same answers, given the same calls in the same order.
    pub fn random(target: &str, p: f64, seed: u64) -> Self {
        let rng = Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(seed)));
        Self {
            chance: Some((p, rng)),
            ..Self::new(target, 0)
        }
    }

    /// How many answers it has lost so far.
    pub fn lost(&self) -> usize {
        self.lost.load(Ordering::SeqCst)
    }

    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let app = Router::new().fallback(forward).with_state(self);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    }

    fn take_one_to_lose(&self) -> bool {
        if let Some((p, rng)) = &self.chance {
            return rng.lock().expect("lock poisoned").random_bool(*p);
        }
        self.to_lose
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }
}

fn is_tool_call(body: &[u8]) -> bool {
    serde_json::from_slice::<Value>(body).is_ok_and(|m| m["method"] == "tools/call")
}

async fn forward(State(proxy): State<LossyProxy>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let Ok(body) = axum::body::to_bytes(body, 64 * 1024 * 1024).await else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let mut headers = parts.headers.clone();
    headers.remove(header::HOST);
    headers.remove(header::CONTENT_LENGTH);
    let lose = is_tool_call(&body) && proxy.take_one_to_lose();
    let sent = proxy
        .client
        .request(parts.method, format!("{}{path}", proxy.target))
        .headers(headers)
        .body(body)
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(e) => return (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    };
    if lose {
        // Let the tool finish, then drop what it said.
        let _ = response.bytes().await;
        proxy.lost.fetch_add(1, Ordering::SeqCst);
        return (StatusCode::BAD_GATEWAY, "connection lost").into_response();
    }
    let status = response.status();
    let mut headers = HeaderMap::new();
    for (name, value) in response.headers() {
        if name != header::TRANSFER_ENCODING && name != header::CONTENT_LENGTH {
            headers.append(name.clone(), value.clone());
        }
    }
    let stream = response
        .bytes_stream()
        .map_ok(Bytes::from)
        .map_err(std::io::Error::other);
    (status, headers, Body::from_stream(stream)).into_response()
}
