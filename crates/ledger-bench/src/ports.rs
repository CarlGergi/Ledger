//! Local ports. The MCP HTTP client opens a connection per request, so a benchmark making
//! thousands of calls can run out of local ports to one address.

use std::time::{Duration, Instant};

/// Waits until few sockets are left in TIME_WAIT. The MCP HTTP client opens a connection
/// per request, and macOS keeps each closed one for about 30 seconds, so phases run back
/// to back would run out of the ~16,000 local ports. Gives up waiting after two minutes.
pub async fn let_ports_drain() {
    let waiting = || {
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
    };
    let deadline = Instant::now() + Duration::from_secs(120);
    while waiting() > 1000 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Whether an error is the machine running out of local ports (EADDRNOTAVAIL), not
/// anything the code under test did.
pub fn out_of_ports(error: &str) -> bool {
    error.contains("os error 49") || error.contains("Can't assign requested address")
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
    }
}
