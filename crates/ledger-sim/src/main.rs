//! Runs simulation seeds: `ledger-sim [--seeds N] [--from S]` runs N seeds from S, and
//! `ledger-sim --seed S --trace` replays one seed and prints everything that happened.

use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let value = |flag: &str| {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .map(|v| {
                v.parse::<u64>()
                    .unwrap_or_else(|_| panic!("{flag} takes a number"))
            })
    };

    if let Some(seed) = value("--seed") {
        let report = ledger_sim::run(seed);
        if args.iter().any(|a| a == "--trace") {
            for line in &report.trace {
                println!("{line}");
            }
        }
        println!("seed {seed}: {:?}", report.stats);
        for violation in &report.violations {
            println!("  VIOLATION {violation}");
        }
        return if report.violations.is_empty() {
            ExitCode::SUCCESS
        } else {
            ExitCode::FAILURE
        };
    }

    let from = value("--from").unwrap_or(0);
    let count = value("--seeds").unwrap_or(10_000);
    let mut failed = Vec::new();
    let mut total = ledger_sim::Stats::default();
    for seed in from..from + count {
        let report = ledger_sim::run(seed);
        let s = report.stats;
        total.calls += s.calls;
        total.retries += s.retries;
        total.effects += s.effects;
        total.flagged += s.flagged;
        total.takeovers += s.takeovers;
        total.unrecognised_resends += s.unrecognised_resends;
        total.uncorrected_lies += s.uncorrected_lies;
        if !report.violations.is_empty() {
            println!("seed {seed} FAILED: {}", report.violations[0]);
            failed.push(seed);
        }
    }
    println!(
        "{count} seeds from {from}: {} failed. {} calls, {} retries, {} tool effects, {} \
         takeovers, {} calls flagged in doubt. Expected repeats: {} from agents resending \
         with a new id, {} from an uncorrected lying tool.",
        failed.len(),
        total.calls,
        total.retries,
        total.effects,
        total.takeovers,
        total.flagged,
        total.unrecognised_resends,
        total.uncorrected_lies
    );
    if let Some(seed) = failed.first() {
        println!("replay: cargo run --release -p ledger-sim -- --seed {seed} --trace");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
