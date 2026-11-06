//! What tests need in order to run alongside each other on a busy machine.

mod database;
mod port;

use std::time::Duration;

pub use database::TestDb;
pub use port::{RESERVED_PORTS, ReservedPort};

/// The limit on any real work in a test, far beyond what a loaded machine needs. Only a
/// failing test reaches it. Shorter limits belong only on things that never answer, where
/// the limit firing late cannot change the result.
pub const PATIENCE: Duration = Duration::from_secs(120);
