//! A gateway that agents connect to as if it were their MCP tool server. It records
//! every tool call in the event log and forwards it to the real server. It can also stand
//! in for an OpenAI-compatible model, logging each model call in the same session.

mod config;
mod dedup;
mod fork;
mod gateway;
mod model;
mod policy;
mod server;
mod session;
mod upstream;

pub use config::{Config, ModelConfig, UpstreamConfig};
pub use gateway::{
    Arrival, DEFAULT_LEASE_TTL, Gateway, ModelAnswer, ModelCall, ModelFailure, error_codes,
};
pub use model::ModelUpstream;
pub use policy::EffectPolicy;
pub use server::serve;
pub use session::{SESSION_HEADER, SESSION_QUERY_PARAM, STEP_HEADER, STEP_META_KEY};
pub use upstream::{McpUpstream, Upstream, UpstreamError};
