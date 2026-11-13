//! The simulator's own checks: that seeds keep every invariant, and replay exactly. The
//! full run of 10,000 seeds is `make simulate`.

#[test]
fn the_first_seeds_keep_every_invariant() {
    for seed in 0..300 {
        let report = ledger_sim::run(seed);
        assert!(
            report.violations.is_empty(),
            "seed {seed}: {:?}\nreplay: cargo run --release -p ledger-sim -- --seed {seed} --trace",
            report.violations
        );
    }
}

#[test]
fn a_seed_replays_exactly() {
    for seed in [0, 7, 42] {
        let first = ledger_sim::run(seed);
        let again = ledger_sim::run(seed);
        assert_eq!(first.trace, again.trace, "seed {seed} did not replay");
        assert_eq!(first.stats, again.stats);
        assert!(
            first.trace.len() > 20,
            "seed {seed} did too little to mean anything"
        );
    }
}

#[test]
fn different_seeds_do_different_things() {
    assert_ne!(
        ledger_sim::run(1).fingerprint(),
        ledger_sim::run(2).fingerprint()
    );
}

#[test]
fn the_runs_exercise_every_fault() {
    let mut trace = Vec::new();
    for seed in 0..100 {
        trace.extend(ledger_sim::run(seed).trace);
    }
    for fault in [
        "chaos: kill",
        "chaos: pause",
        "killed just after a write",
        "lost the acknowledgement of a write",
        "write failed",
        "connection refused",
        "hangs",
        "answer lost",
        "lost on the way",
        "duplicated to",
        "network: answer to",
        "refused: the session has been taken over",
        "resent late",
    ] {
        assert!(
            trace.iter().any(|line| line.contains(fault)),
            "no run in 100 seeds exercised: {fault}"
        );
    }
}
