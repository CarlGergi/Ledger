#!/usr/bin/env python3
"""Breaks each guarantee Ledger makes, one at a time, and checks that the test meant
to protect it notices. A guarantee whose test still passes is not really tested.

Works on a throwaway copy of the source, so an interrupted run cannot leave a fault
behind. Usage: breakage.py [--list] [name-filter]
"""

import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATEWAY = "crates/ledger-gateway/src/gateway.rs"
MIGRATION = "crates/ledger-store/migrations/0001_event_log.sql"
STORE = "crates/ledger-store/src/postgres.rs"
TEST_TIMEOUT_SECS = 300


@dataclass
class Fault:
    name: str
    caught_by: str
    path: str
    edits: list[tuple[str, str]]


FAULTS = [
    Fault(
        "tool runs before the request is logged",
        "the_request_is_committed_before_the_tool_runs",
        GATEWAY,
        [
            (
                """        let request = store
            .append(NewEvent::request(session, &tool, request_payload))""",
                """        let forwarded = self.forward(params).await;
        let request = store
            .append(NewEvent::request(session, &tool, request_payload))""",
            ),
            ("        let forwarded = self.forward(params).await;\n        let (outcome,",
             "        let (outcome,"),
        ],
    ),
    Fault(
        "result released even though it was not logged",
        "if_the_response_cannot_be_logged_the_agent_does_not_get_it",
        GATEWAY,
        [
            (
                """                    "the tool may have run, but ledger could not record the outcome",
                    None,
                )
            })?;""",
                """                    "the tool may have run, but ledger could not record the outcome",
                    None,
                )
            })
            .ok();""",
            )
        ],
    ),
    Fault(
        "response never logged",
        "every_call_is_logged_in_order",
        GATEWAY,
        [
            (
                """            .append(NewEvent::response(
                session,
                request.seq,
                &tool,
                outcome,
                response_payload,
            ))
            .await""",
                """            .session_events(session)
            .await
            .map(|_| {
                let _ = (&outcome, &response_payload);
            })""",
            )
        ],
    ),
    Fault(
        "cancelling the call also abandons recording its outcome",
        "an_agent_cancelling_a_call_does_not_lose_the_outcome",
        GATEWAY,
        [
            (
                """        tokio::spawn(async move {
            gateway
                .record_and_forward(&session, rpc_id, meta, request)
                .await
        })
        .await
        .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?""",
                """        let _ = ErrorCode::INTERNAL_ERROR;
        tokio::select! {
            r = gateway.record_and_forward(&session, rpc_id, meta, request) => r,
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("cancelled", None)),
        }""",
            )
        ],
    ),
    Fault(
        "annotations dropped from the tool listing",
        "tools_pass_through_with_their_annotations_and_schemas",
        GATEWAY,
        [
            (
                "                    tool.name = Cow::Owned(prefixed(upstream, &tool.name));\n",
                "                    tool.name = Cow::Owned(prefixed(upstream, &tool.name));\n"
                "                    tool.annotations = None;\n",
            )
        ],
    ),
    Fault(
        "discover answered like a server from the newest revision",
        "a_client_preferring_the_newest_protocol_still_gets_a_session",
        GATEWAY,
        [
            (
                "        Err(ErrorData::method_not_found::<DiscoverRequestMethod>())",
                """        let _ = ErrorData::method_not_found::<DiscoverRequestMethod>();
        Ok(DiscoverResult::from_server_info(
            self.supported_protocol_versions().into_owned(),
            self.get_info(),
        ))""",
            )
        ],
    ),
    Fault(
        "listing tools is effectively unbounded",
        "a_tool_server_that_stalls_on_listing_cannot_stall_the_gateway",
        GATEWAY,
        [
            (
                "tokio::time::timeout(timeout, upstream.list_tools())",
                "tokio::time::timeout(timeout * 500, upstream.list_tools())",
            )
        ],
    ),
    Fault(
        "the agent's _meta is forwarded to tools",
        "the_agents_meta_is_logged_but_not_forwarded",
        GATEWAY,
        [
            (
                "        params.name = Cow::Owned(tool.to_owned());\n",
                "        params.name = Cow::Owned(tool.to_owned());\n"
                '        params.meta = serde_json::from_value(json!({"example.com/trace": "abc"})).ok();\n',
            )
        ],
    ),
    Fault(
        "the agent's _meta is left out of the log",
        "the_agents_meta_is_logged_but_not_forwarded",
        GATEWAY,
        [
            (
                'json!({ "rpc_id": rpc_id, "meta": meta, "params": params })',
                'json!({ "rpc_id": rpc_id, "params": params })',
            )
        ],
    ),
    Fault(
        "append-only triggers removed",
        "the_log_cannot_be_rewritten",
        MIGRATION,
        [
            (
                """CREATE TRIGGER events_no_update_or_delete
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();
""",
                "",
            )
        ],
    ),
    Fault(
        "payload column is JSONB",
        "payloads_round_trip_exactly",
        MIGRATION,
        [("    payload     JSON NOT NULL,", "    payload     JSONB NOT NULL,")],
    ),
    Fault(
        "sequence numbers skip",
        "sequences_start_at_one_and_have_no_gaps",
        STORE,
        [("SET last_seq = sessions.last_seq + 1", "SET last_seq = sessions.last_seq + 2")],
    ),
    Fault(
        "a call may be given two responses",
        "a_call_cannot_have_two_responses",
        MIGRATION,
        [
            (
                """CREATE UNIQUE INDEX events_one_response_per_request
    ON events (session_id, request_seq)
    WHERE request_seq IS NOT NULL;
""",
                "",
            )
        ],
    ),
]


def run_tests(copy: Path) -> tuple[bool, dict[str, str], str]:
    """Returns whether the suite finished, each failed test's panic, and any compile error."""
    env = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / "target" / "breakage"))
    try:
        result = subprocess.run(
            ["cargo", "test", "--workspace", "--tests", "--no-fail-fast"],
            cwd=copy,
            env=env,
            capture_output=True,
            text=True,
            timeout=TEST_TIMEOUT_SECS,
        )
    except subprocess.TimeoutExpired:
        return False, {}, ""
    output = result.stdout + result.stderr
    failed = dict.fromkeys(re.findall(r"^test (\S+) \.\.\. FAILED", output, re.M), "")
    for name, panic in re.findall(r"^thread '([^']+)'[^\n]* panicked at [^\n]*\n([^\n]*)", output, re.M):
        if name in failed:
            failed[name] = panic
    compile_error = ""
    if "could not compile" in output:
        compile_error = next(line for line in output.splitlines() if line.startswith("error"))
    return True, failed, compile_error


def check(fault: Fault, copy: Path) -> tuple[bool, str]:
    target = copy / fault.path
    original = target.read_text()
    broken = original
    for old, new in fault.edits:
        if old not in broken:
            return False, "the source has changed, so this fault no longer applies"
        broken = broken.replace(old, new)
    target.write_text(broken)
    try:
        finished, failed, compile_error = run_tests(copy)
    finally:
        target.write_text(original)

    if compile_error:
        return False, f"the fault does not compile: {compile_error}"
    if not finished:
        return False, f"the tests hung for {TEST_TIMEOUT_SECS}s instead of failing"
    if fault.caught_by not in failed:
        others = f" (these did: {failed})" if failed else ""
        return False, f"{fault.caught_by} still passes{others}"
    # Every other failure is printed in full: one that makes no sense for this fault is
    # a problem with the suite, and is impossible to diagnose after the fact.
    others = "".join(f"\n        also failed: {n}: {p}" for n, p in sorted(failed.items()) if n != fault.caught_by)
    return True, fault.caught_by + others


def copy_source(copy: Path) -> None:
    """Copies what git would see: tracked files plus untracked ones it does not ignore."""
    listed = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    for name in filter(None, listed.split("\0")):
        source = ROOT / name
        if source.is_file():
            (copy / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, copy / name)


def main() -> int:
    args = sys.argv[1:]
    if "--list" in args:
        for fault in FAULTS:
            print(f"{fault.name}\n    caught by {fault.caught_by}")
        return 0
    faults = [f for f in FAULTS if not args or args[0] in f.name]

    with tempfile.TemporaryDirectory(prefix="ledger-breakage-") as temp:
        copy = Path(temp) / "src"
        copy_source(copy)

        print("baseline: the suite must pass with no fault applied", flush=True)
        finished, failed, compile_error = run_tests(copy)
        if not finished or failed or compile_error:
            print(f"baseline is not green: {compile_error or sorted(failed) or 'hung'}")
            return 1

        problems = 0
        for fault in faults:
            caught, detail = check(fault, copy)
            problems += not caught
            print(f"{'CAUGHT' if caught else 'MISSED'}  {fault.name}\n        {detail}", flush=True)

    print(f"\n{len(faults) - problems} of {len(faults)} faults caught")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
