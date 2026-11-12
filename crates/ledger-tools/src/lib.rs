//! Example MCP tool servers: an email sender, a file writer, and email through AgentMail,
//! with a stand-in for AgentMail's API. Also a scripted stand-in for an OpenAI-compatible
//! model, and a proxy that loses answers, for showing what retries do.

pub mod agentmail;
pub mod email;
pub mod files;
pub mod http;
pub mod lossy;
pub mod model;
