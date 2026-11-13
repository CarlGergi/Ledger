//! Local ports. The MCP HTTP client opens a connection per request, so a benchmark making
//! thousands of calls can run out of local ports to one address.

use std::sync::Once;
use std::time::{Duration, Instant};

/// All TCP sockets on the machine, TIME_WAIT included, from a macOS sysctl cheap enough to
/// read before every call. None where it cannot be read. Not netstat: started from a
/// program rather than a shell, it printed nothing on the Mac these ran on, and a count
/// of zero let every wait pass at once.
fn sockets() -> Option<usize> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "net.inet.tcp.pcbcount"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Waits while the machine holds more than `limit` TCP sockets, for at most two minutes.
/// Says so, once, if it cannot count them, rather than pretending there are none.
async fn wait_below(limit: usize, poll: Duration) {
    static UNCOUNTED: Once = Once::new();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let Some(count) = sockets() else {
            UNCOUNTED.call_once(|| {
                eprintln!("cannot count TCP sockets here, so not waiting for local ports")
            });
            return;
        };
        if count <= limit || Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(poll).await;
    }
}

/// Waits until few sockets are left, TIME_WAIT included. The MCP HTTP client opens a
/// connection per request, and macOS keeps each closed one for about 30 seconds, so
/// phases run back to back would run out of the ~16,000 local ports.
pub async fn let_ports_drain() {
    wait_below(1000, Duration::from_secs(1)).await;
}

/// Waits while the machine holds more than 8,000 TCP sockets, half its ~16,000 local
/// ports: for pacing a run that opens connections faster than they drain. A run that
/// still runs out is then refused, not counted.
pub async fn keep_ports_free() {
    wait_below(8000, Duration::from_millis(200)).await;
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
