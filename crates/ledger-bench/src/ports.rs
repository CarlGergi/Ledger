//! Local ports. The MCP HTTP client opens a connection per request, so a benchmark making
//! thousands of calls can run out of local ports to one address.

use std::time::{Duration, Instant};

fn time_wait() -> usize {
    std::process::Command::new("netstat")
        .args(["-an", "-p", "tcp"])
        .output()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| l.contains("TIME_WAIT"))
                .count()
        })
        .unwrap_or(0)
}

async fn wait_below(limit: usize) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while time_wait() > limit && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Waits until few sockets are left in TIME_WAIT. The MCP HTTP client opens a connection
/// per request, and macOS keeps each closed one for about 30 seconds, so phases run back
/// to back would run out of the ~16,000 local ports. Gives up waiting after two minutes.
pub async fn let_ports_drain() {
    wait_below(1000).await;
}

/// All TCP sockets on the machine, TIME_WAIT included: a sysctl, cheap enough to read
/// before every call, where netstat takes a fraction of a second.
fn sockets() -> usize {
    std::process::Command::new("sysctl")
        .args(["-n", "net.inet.tcp.pcbcount"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(0)
}

/// Waits while the machine holds more than 8,000 TCP sockets, half its ~16,000 local
/// ports: for pacing a run that opens connections faster than they drain. Gives up
/// waiting after two minutes; a run that still runs out is then refused, not counted.
pub async fn keep_ports_free() {
    let deadline = Instant::now() + Duration::from_secs(120);
    while sockets() > 8000 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Whether an error is the machine running out of local ports (EADDRNOTAVAIL), not
/// anything the code under test did.
pub fn out_of_ports(error: &str) -> bool {
    error.contains("os error 49") || error.contains("Can't assign requested address")
}

/// Whether the agent could not reach a server on this machine that is up: only the
/// machine can cause that (reqwest's message hides the cause, which is usually
/// EADDRNOTAVAIL), so the run is invalid.
pub fn could_not_connect(error: &str) -> bool {
    error.starts_with("connecting:") || out_of_ports(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_out_of_ports_is_recognised() {
        assert!(out_of_ports(
            "connecting: error sending request: Can't assign requested address (os error 49)"
        ));
        assert!(!out_of_ports("connection closed before message completed"));
        assert!(could_not_connect(
            "connecting: connecting to http://127.0.0.1:5/mcp: Send message error Transport"
        ));
        assert!(!could_not_connect(
            "Mcp error: -32052: may already have run"
        ));
    }
}
