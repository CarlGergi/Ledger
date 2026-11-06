//! A TCP port that stays a test's own, even while nothing is listening on it.
//!
//! A test that stops a server and brings it back at the same address cannot rely on the
//! kernel's port 0. Once released, that port can go to any other socket on the machine,
//! including another test's server or the source port of an outgoing connection. Keeping
//! the listener open across the restart does not help either, because clients would then
//! connect and hang instead of being refused, which is not what a dead server looks like.
//!
//! So these ports come from below the range the kernel hands out by itself, which starts
//! at 32768 on Linux and 49152 on macOS. Tests keep out of each other's way, across
//! processes, by holding a listener on a sibling port for as long as they own the port.
//! The kernel allows only one holder, and frees it if the process dies.
//!
//! A process that spawns children must not bind a reserved port itself. A child being
//! spawned holds all of its parent's open sockets until it execs, so a listener the parent
//! has closed can keep answering, and keep the port busy, for a moment after that.

use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::ops::Range;

use uuid::Uuid;

const PORTS: Range<u16> = 21000..25000;
const CLAIM_OFFSET: u16 = 4000;

/// Every port these reservations touch, for checking against what a machine hands out.
pub const RESERVED_PORTS: Range<u16> = PORTS.start..PORTS.end + CLAIM_OFFSET;

/// Owned until dropped. Nothing listens on the port unless the test binds it, so until
/// then connections to it are refused.
pub struct ReservedPort {
    port: u16,
    _claim: TcpListener,
}

impl ReservedPort {
    pub fn claim() -> Self {
        // Starting anywhere keeps overlapping test runs from contending for the same ports.
        let start = (Uuid::new_v4().as_u128() % PORTS.len() as u128) as usize;
        PORTS
            .cycle()
            .skip(start)
            .take(PORTS.len())
            .find_map(Self::try_claim)
            .expect("every reserved port is taken")
    }

    fn try_claim(port: u16) -> Option<Self> {
        let claim = TcpListener::bind((Ipv4Addr::LOCALHOST, port + CLAIM_OFFSET)).ok()?;
        let reserved = Self {
            port,
            _claim: claim,
        };
        // Something other than these tests may be using the port itself. Connecting finds
        // that out without opening a listener on the port, which see above.
        match TcpStream::connect(reserved.addr()) {
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => Some(reserved),
            _ => None,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.port))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_port_has_one_owner() {
        let held = ReservedPort::claim();
        assert!(ReservedPort::try_claim(held.port).is_none());
    }

    #[test]
    fn a_port_with_something_else_on_it_is_not_claimed() {
        let held = ReservedPort::claim();
        let port = held.port;
        let _listener = TcpListener::bind(held.addr()).unwrap();
        drop(held);
        assert!(ReservedPort::try_claim(port).is_none());
    }

    #[test]
    fn claims_do_not_overlap() {
        let held: Vec<_> = (0..50).map(|_| ReservedPort::claim()).collect();
        let mut ports: Vec<_> = held.iter().map(|p| p.port).collect();
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(ports.len(), held.len());
    }

    #[test]
    fn a_server_can_come_and_go_while_the_port_stays_reserved() {
        let port = ReservedPort::claim();
        let refused = || TcpStream::connect(port.addr()).unwrap_err().kind();
        assert_eq!(refused(), ErrorKind::ConnectionRefused);

        let first = TcpListener::bind(port.addr()).unwrap();
        TcpStream::connect(port.addr()).unwrap();
        drop(first);
        assert_eq!(refused(), ErrorKind::ConnectionRefused);

        let second = TcpListener::bind(port.addr()).unwrap();
        TcpStream::connect(port.addr()).unwrap();
        drop(second);
    }
}
