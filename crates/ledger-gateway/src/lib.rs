//! A gateway that agents connect to as if it were their MCP tool server. It records
//! every tool call in the event log and forwards it to the real server.

mod config;
mod gateway;
mod policy;
mod server;
mod session;
mod upstream;

pub use config::{Config, UpstreamConfig};
pub use gateway::{DEFAULT_LEASE_TTL, Gateway, error_codes};
pub use policy::EffectPolicy;
pub use server::serve;
pub use session::{SESSION_HEADER, SESSION_QUERY_PARAM};
pub use upstream::{McpUpstream, Upstream, UpstreamError};
