#!/usr/bin/env python3
"""Breaks each guarantee Ledger makes, one at a time, and checks that the test meant
to protect it notices. A guarantee whose test still passes is not really tested.

Works on a throwaway copy of the source, so an interrupted run cannot leave a fault
behind. Usage: breakage.py [--list] [name-filter]
"""

import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GATEWAY = "crates/ledger-gateway/src/gateway.rs"
GATEWAY_MAIN = "crates/ledger-gateway/src/main.rs"
POLICY = "crates/ledger-gateway/src/policy.rs"
MIGRATION = "crates/ledger-store/migrations/0001_event_log.sql"
LEASES = "crates/ledger-store/migrations/0002_leases.sql"
EVENT = "crates/ledger-store/src/event.rs"
STORE = "crates/ledger-store/src/postgres.rs"
PORT = "crates/ledger-testkit/src/port.rs"
UPSTREAM = "crates/ledger-gateway/src/upstream.rs"
TEST_TIMEOUT_SECS = 1800


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
                "                tool.name = Cow::Owned(prefixed(name, &tool.name));\n",
                "                tool.name = Cow::Owned(prefixed(name, &tool.name));\n"
                "                tool.annotations = None;\n",
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
                "tokio::time::timeout(self.inner.call_timeout, upstream.list_tools())",
                "tokio::time::timeout(self.inner.call_timeout * 1000, upstream.list_tools())",
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
                '"meta": meta, "effect": effect, ',
                '"effect": effect, ',
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
    Fault(
        "connecting to a tool server has no limit of its own",
        "a_connection_attempt_gives_up_on_its_own",
        UPSTREAM,
        [
            (
                "tokio::time::timeout(self.connect_timeout, client.serve(transport))",
                "tokio::time::timeout(self.connect_timeout * 5000, client.serve(transport))",
            )
        ],
    ),
    Fault(
        "reserved test ports overlap the ones the kernel hands out",
        "the_kernel_never_hands_out_a_reserved_port",
        PORT,
        [("const PORTS: Range<u16> = 21000..25000;", "const PORTS: Range<u16> = 50000..54000;")],
    ),
    Fault(
        "a reserved test port ignores something already on it",
        "a_port_with_something_else_on_it_is_not_claimed",
        PORT,
        [("            _ => None,\n", "            _ => Some(reserved),\n")],
    ),
    Fault(
        "a reserved test port does not stay claimed",
        "a_port_has_one_owner",
        PORT,
        [("            _claim: claim,", "            _claim: TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).ok()?,")],
    ),
    Fault(
        "effect policy ignores the override file",
        "a_wrong_annotation_is_believed_until_the_override_file_corrects_it",
        POLICY,
        [
            (
                "if let Some(entry) = self.overrides.get(name) {",
                'if let Some(entry) = self.overrides.get(name).filter(|_| name.is_empty()) {',
            )
        ],
    ),
    Fault(
        "effect policy treats an unannotated tool as idempotent",
        "an_unannotated_tool_gets_the_safest_class",
        POLICY,
        [
            (
                "let idempotent = annotations.is_some_and(|a| a.idempotent_hint == Some(true));",
                "let idempotent = annotations.is_none_or(|a| a.idempotent_hint != Some(false));",
            )
        ],
    ),
    Fault(
        "effect policy treats an unknown tool as read-only",
        "a_tool_its_server_does_not_list_gets_the_safest_class",
        POLICY,
        [
            (
                """                class: EffectClass::AtMostOnce,
                source: EffectSource::Unlisted,""",
                """                class: EffectClass::ReadOnly,
                source: EffectSource::Unlisted,""",
            )
        ],
    ),
    Fault(
        "effect policy lets read-only outrank destructive",
        "a_read_only_tool_that_is_also_destructive_is_not_read_only",
        POLICY,
        [(" && a.destructive_hint != Some(true)", "")],
    ),
    Fault(
        "effect policy accepts an override for an unknown upstream",
        "an_override_for_an_unknown_upstream_is_refused",
        POLICY,
        [("upstreams.contains(&upstream),", "upstreams.contains(&upstream) || !upstream.is_empty(),")],
    ),
    Fault(
        "effect policy keeps tools a server stopped listing",
        "the_latest_listing_decides_and_a_failed_one_changes_nothing",
        GATEWAY,
        [("        catalog.retain(|known, _| !known.starts_with(&own_prefix));\n", "        let _ = own_prefix;\n")],
    ),
    Fault(
        "effect policy never asks a server about a tool it has not seen",
        "a_tool_server_is_asked_once_rather_than_on_every_call",
        GATEWAY,
        [("            self.list_upstream(upstream).await;\n        }\n        policy", "            let _ = upstream;\n        }\n        policy")],
    ),
    Fault(
        "effect policy lists tools again on every call",
        "a_tool_server_is_asked_once_rather_than_on_every_call",
        GATEWAY,
        [("            && self.listed(name).is_none()\n", "")],
    ),
    Fault(
        "effect policy decisions are left out of the log",
        "the_real_tools_are_logged_with_the_class_their_annotations_give",
        GATEWAY,
        [('"effect": effect, ', "")],
    ),
    Fault(
        "effect policy file errors do not stop startup",
        "a_bad_override_file_stops_the_binary_with_a_useful_message",
        GATEWAY_MAIN,
        [("let policy = config.effect_policy()?;", "let policy = config.effect_policy().unwrap_or_default();")],
    ),
    Fault(
        "fencing: an append ignores its token",
        "an_append_with_a_stale_token_is_rejected",
        STORE,
        [("        WHERE session_id = $1 AND token = $8\n", "        WHERE session_id = $1\n")],
    ),
    Fault(
        "fencing: a takeover keeps the old token",
        "an_append_with_a_stale_token_is_rejected",
        STORE,
        [("token = leases.token + 1,", "token = leases.token,")],
    ),
    Fault(
        "fencing: a lease can be taken before it expires",
        "a_lease_that_has_not_expired_cannot_be_taken",
        STORE,
        [("        WHERE leases.expires_at <= now() OR leases.owner = excluded.owner\n", "")],
    ),
    Fault(
        "fencing: appending does not extend the lease",
        "appending_extends_the_lease",
        STORE,
        [("UPDATE leases SET expires_at = now() + make_interval(secs => $9)", "UPDATE leases SET expires_at = expires_at + make_interval(secs => 0 * $9)")],
    ),
    Fault(
        "fencing: a fenced append uses up a sequence number",
        "an_append_with_a_stale_token_is_rejected",
        STORE,
        [
            (
                """    ), bumped AS (
        INSERT INTO sessions (session_id, last_seq) SELECT $1, 1 FROM fence
        ON CONFLICT (session_id) DO UPDATE SET last_seq = sessions.last_seq + 1
        RETURNING last_seq
    )
    INSERT INTO events
        (session_id, seq, kind, request_seq, tool, outcome, payload, args_hash, fence_token)
    SELECT $1, last_seq, $2, $3, $4, $5, $6::text::json, $7, $8 FROM bumped""",
                """    ), bumped AS (
        INSERT INTO sessions (session_id, last_seq) VALUES ($1, 1)
        ON CONFLICT (session_id) DO UPDATE SET last_seq = sessions.last_seq + 1
        RETURNING last_seq
    )
    INSERT INTO events
        (session_id, seq, kind, request_seq, tool, outcome, payload, args_hash, fence_token)
    SELECT $1, last_seq, $2, $3, $4, $5, $6::text::json, $7, $8 FROM bumped, fence""",
            )
        ],
    ),
    Fault(
        "fencing: events need no fence token",
        "the_schema_refuses_malformed_events",
        LEASES,
        [("    ADD CONSTRAINT events_carry_a_fence_token CHECK (fence_token IS NOT NULL) NOT VALID,\n", "")],
    ),
    Fault(
        "fencing: the arguments hash ignores the arguments",
        "the_arguments_hash_ignores_key_order_and_nothing_else",
        EVENT,
        [('format!("{:x}", Sha256::digest(arguments.to_string()))', 'format!("{:x}", Sha256::digest(arguments.is_object().to_string()))')],
    ),
    Fault(
        "fencing: the gateway takes a session another copy holds",
        "a_copy_cannot_record_in_a_session_another_copy_holds",
        STORE,
        [("        WHERE leases.expires_at <= now() OR leases.owner = excluded.owner\n", "")],
    ),
]


def run_tests(copy: Path) -> tuple[bool, dict[str, str], str]:
    """Returns whether the suite finished, each failed test's panic, and any compile error."""
    env = dict(os.environ, CARGO_TARGET_DIR=str(ROOT / "target" / "breakage"))
    # In a process group of its own, so that a timeout also ends the test binaries, which
    # would otherwise carry on by themselves, holding their databases.
    tests = subprocess.Popen(
        ["cargo", "test", "--workspace", "--tests", "--no-fail-fast"],
        cwd=copy,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        output, _ = tests.communicate(timeout=TEST_TIMEOUT_SECS)
    except subprocess.TimeoutExpired:
        os.killpg(tests.pid, signal.SIGKILL)
        tests.wait()
        return False, {}, ""
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
    # Unit tests report as `module::tests::name`.
    failed = {name.split("::")[-1]: panic for name, panic in failed.items()}
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
