#!/usr/bin/env python3
"""Runs the whole test suite over and over, under the conditions that have made it flaky
before. Fails if any run fails, if PostgreSQL resets, or if a test database is left behind.

Failed runs keep their full output in target/soak, since the text of a failure that
happens once in a hundred runs cannot be recovered afterwards. The machine and its load
are printed with the results, because they say what the timings mean.

The CPU-starved runs sit at the lowest priorities, so that they compete with each other
rather than with anything else on the machine.

Usage: soak.py [--scale N], where N multiplies the number of runs.
"""

import argparse
import datetime
import os
import platform
import re
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "target" / "soak"
RESET = re.compile(r"terminating any other active server processes|all server processes terminated")
PANIC = re.compile(r"^thread '([^']+)'[^\n]* panicked at [^\n]*\n([^\n]*)", re.M)
CONNECTIONS = "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'"
TEST_DATABASES = "SELECT datname FROM pg_database WHERE datname LIKE 'ledger\\_test\\_%'"
SAMPLE_INTERVAL_SECS = 0.25
SERVER_RECOVERY_SECS = 120
# A test drops its database before its run ends, so one from these runs never needs this.
# It is for the databases of anything else testing against the same server just now.
OTHER_RUNS_SECS = 300
LOWEST_PRIORITY = 19


@dataclass
class Condition:
    name: str
    instances: int
    runs_each: int
    starved: bool = False


@dataclass
class Result:
    condition: Condition
    seconds: list[float] = field(default_factory=list)
    failures: int = 0
    peak_connections: int = 0
    resets: int = 0
    leftover_databases: int = 0
    load_before: float = 0.0
    load_after: float = 0.0


def query(sql: str) -> list[str] | None:
    """Returns None if the server cannot answer, as it cannot just after a reset."""
    done = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "ledger", "-d", "ledger", "-Atc", sql],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    return done.stdout.split() if done.returncode == 0 else None


def test_databases() -> set[str]:
    deadline = time.monotonic() + SERVER_RECOVERY_SECS
    while time.monotonic() < deadline:
        names = query(TEST_DATABASES)
        if names is not None:
            return set(names)
        time.sleep(1)
    raise SystemExit(f"PostgreSQL did not answer for {SERVER_RECOVERY_SECS}s")


def leftover_databases(before: set[str]) -> set[str]:
    deadline = time.monotonic() + OTHER_RUNS_SECS
    while (left := test_databases() - before) and time.monotonic() < deadline:
        time.sleep(1)
    return left


def count_resets(since: datetime.datetime) -> int:
    logs = subprocess.run(
        ["docker", "compose", "logs", "--no-log-prefix", "--since", since.isoformat(), "postgres"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return len(RESET.findall(logs.stdout + logs.stderr))


def machine() -> str:
    def sysctl(name: str) -> str:
        return subprocess.run(["sysctl", "-n", name], capture_output=True, text=True).stdout.strip()

    if platform.system() == "Darwin":
        name = f"{sysctl('hw.model')}, {sysctl('machdep.cpu.brand_string')}"
        memory = int(sysctl("hw.memsize")) // 2**30
    else:
        name = platform.machine()
        memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2**30
    return f"{name}, {os.cpu_count()} cores, {memory} GB, {platform.system()} {platform.release()}"


def own_build(instance: int) -> dict[str, str]:
    """Overlapping runs need separate build directories, because even a `cargo test` with
    nothing to build removes and relinks the binaries that another run's tests are starting."""
    return dict(os.environ, CARGO_TARGET_DIR=str(LOGS / f"build-{instance}"))


def run_suite(result: Result, lock: threading.Lock, instance: int, label: str) -> None:
    command = ["cargo", "test", "--workspace", "--no-fail-fast"]
    if result.condition.starved:
        command = ["nice", "-n", str(LOWEST_PRIORITY), *command]
    started = time.monotonic()
    done = subprocess.run(command, cwd=ROOT, env=own_build(instance), capture_output=True, text=True)
    elapsed = time.monotonic() - started
    with lock:
        result.seconds.append(elapsed)
        if done.returncode == 0:
            return
        result.failures += 1
    output = done.stdout + done.stderr
    log = LOGS / f"{label}.log"
    log.write_text(output)
    for test, message in PANIC.findall(output) or [("(no panic)", f"exit {done.returncode}")]:
        print(f"FAILED  {label}: {test}: {message[:200]}", flush=True)
    print(f"        full output in {log}", flush=True)


def soak(condition: Condition, scale: float) -> Result:
    result = Result(condition, load_before=os.getloadavg()[0])
    runs = max(1, round(condition.runs_each * scale))
    lock = threading.Lock()
    since = datetime.datetime.now(datetime.timezone.utc)
    databases_before = test_databases()

    finished = threading.Event()

    def sample_connections() -> None:
        while not finished.is_set():
            sample = query(CONNECTIONS)
            if sample:
                result.peak_connections = max(result.peak_connections, int(sample[0]))
            finished.wait(SAMPLE_INTERVAL_SECS)

    def instance(number: int) -> None:
        slug = re.sub(r"\W+", "-", condition.name)
        for run in range(1, runs + 1):
            run_suite(result, lock, number, f"{slug}-{number}-{run}")

    # Two for each core, one step above the tests, so the tests always wait for them.
    busy = [
        subprocess.Popen(["nice", "-n", str(LOWEST_PRIORITY - 1), sys.executable, "-c", "while True: pass"])
        for _ in range((os.cpu_count() or 4) * 2 if condition.starved else 0)
    ]
    sampler = threading.Thread(target=sample_connections)
    sampler.start()
    try:
        threads = [threading.Thread(target=instance, args=(n,)) for n in range(1, condition.instances + 1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        finished.set()
        sampler.join()
        for process in busy:
            process.kill()
            process.wait()

    result.load_after = os.getloadavg()[0]
    result.resets = count_resets(since)
    result.leftover_databases = len(leftover_databases(databases_before))
    return result


def print_table(results: list[Result]) -> None:
    print("| Condition | Suite runs | Failed | Median | Slowest | Peak connections | PostgreSQL resets | Load, start to end |")
    print("| --- | --: | --: | --: | --: | --: | --: | --: |")
    for r in results:
        print(
            f"| {r.condition.name} | {len(r.seconds)} | {r.failures} | {statistics.median(r.seconds):.1f}s "
            f"| {max(r.seconds):.1f}s | {r.peak_connections} | {r.resets} "
            f"| {r.load_before:.1f} to {r.load_after:.1f} |"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=float, default=1.0)
    args = parser.parse_args()

    LOGS.mkdir(parents=True, exist_ok=True)
    conditions = [
        Condition("One run at a time", instances=1, runs_each=40),
        Condition("Three runs at once", instances=3, runs_each=60),
        Condition("Starved of CPU", instances=1, runs_each=10, starved=True),
    ]
    for instance in range(1, max(c.instances for c in conditions) + 1):
        build = ["cargo", "test", "--workspace", "--no-run"]
        if subprocess.run(build, cwd=ROOT, env=own_build(instance)).returncode != 0:
            return 1

    results = []
    for condition in conditions:
        print(f"{condition.name}...", flush=True)
        results.append(soak(condition, args.scale))

    print(f"\nMachine: {machine()}\n")
    print_table(results)

    failures = sum(r.failures for r in results)
    resets = sum(r.resets for r in results)
    leftovers = sum(r.leftover_databases for r in results)
    print(f"\n{failures} failed runs, {resets} PostgreSQL resets, {leftovers} test databases left behind")
    return 1 if failures or resets or leftovers else 0


if __name__ == "__main__":
    sys.exit(main())
