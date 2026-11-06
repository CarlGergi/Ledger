//! In a test binary of its own, because it runs child processes, which must not happen
//! alongside the tests that bind reserved ports. See the `port` module.

use ledger_testkit::RESERVED_PORTS;

/// The ranges this machine says it hands out, for port 0 and for outgoing connections.
#[cfg(target_os = "linux")]
fn ephemeral_ranges() -> Vec<(u16, u16)> {
    let range = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let mut bounds = range.split_whitespace().map(|n| n.parse().unwrap());
    vec![(bounds.next().unwrap(), bounds.next().unwrap())]
}

#[cfg(target_os = "macos")]
fn ephemeral_ranges() -> Vec<(u16, u16)> {
    let sysctl = |name: &str| -> u16 {
        let output = std::process::Command::new("sysctl")
            .args(["-n", name])
            .output()
            .unwrap();
        let value = String::from_utf8(output.stdout).unwrap();
        value.trim().parse().unwrap()
    };
    vec![
        (
            sysctl("net.inet.ip.portrange.first"),
            sysctl("net.inet.ip.portrange.last"),
        ),
        (
            sysctl("net.inet.ip.portrange.hifirst"),
            sysctl("net.inet.ip.portrange.hilast"),
        ),
    ]
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn the_kernel_never_hands_out_a_reserved_port() {
    for (first, last) in ephemeral_ranges() {
        assert!(
            RESERVED_PORTS.end <= first || last < RESERVED_PORTS.start,
            "this machine hands out ports {first} to {last}, which overlaps the ports \
             these tests reserve, {RESERVED_PORTS:?}"
        );
    }
}
