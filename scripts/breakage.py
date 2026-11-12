#!/usr/bin/env python3
"""Breaks each guarantee Ledger makes, one at a time, and checks that the test meant
to protect it notices. A guarantee whose test still passes is not really tested.

Works on a throwaway copy of the source, so an interrupted run cannot leave a fault
behind. Usage: breakage.py [--list | --check] [name-filter]

--check only confirms that every fault still applies: each edit's text appears exactly
once in its file. Text that appears twice would break more than the fault says, and text
that has gone would leave the fault testing nothing. It takes a second, so lint runs it.
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
DEDUP = "crates/ledger-gateway/src/dedup.rs"
POLICY = "crates/ledger-gateway/src/policy.rs"
MIGRATION = "crates/ledger-store/migrations/0001_event_log.sql"
LEASES = "crates/ledger-store/migrations/0002_leases.sql"
RETRIES = "crates/ledger-store/migrations/0003_retries.sql"
MODEL_CALLS = "crates/ledger-store/migrations/0004_model_calls.sql"
MODEL = "crates/ledger-gateway/src/model.rs"
FORK = "crates/ledger-gateway/src/fork.rs"
COMPARE = "crates/ledger-gateway/src/compare.rs"
REGRESS = "crates/ledger-gateway/src/regress.rs"
EVENT = "crates/ledger-store/src/event.rs"
MEMORY = "crates/ledger-store/src/memory.rs"
SIM_TEST = "the_first_seeds_keep_every_invariant"
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
                "        let guard = lock.lock().await;\n",
                "        let early = self.forward(arrival.params.clone()).await;\n        let guard = lock.lock().await;\n",
            ),
            (
                "        let forwarded = self.forward(params).await;\n",
                "        let forwarded = { drop(params); Forwarded::NotRouted };\n",
            ),
        ],
    ),
    Fault(
        "result released even though it was not logged",
        "if_the_response_cannot_be_logged_the_agent_does_not_get_it",
        GATEWAY,
        [
            (
                """            return Err(ErrorData::new(
                error_codes::OUTCOME_NOT_RECORDED,
                "the tool may have run, but ledger could not record the outcome",
                None,
            ));""",
                """            let _ = ErrorData::new(
                error_codes::OUTCOME_NOT_RECORDED,
                "the tool may have run, but ledger could not record the outcome",
                None,
            );""",
            )
        ],
    ),
    Fault(
        "response never logged",
        "every_call_is_logged_in_order",
        GATEWAY,
        [
            (
                "        if let Err(e) = self.append(response).await {\n            tracing::error!(session, tool, seq,",
                "        if let Err(e) = Ok::<(), StoreError>(drop(response)) {\n            tracing::error!(session, tool, seq,",
            )
        ],
    ),
    Fault(
        "cancelling the call also abandons recording its outcome",
        "an_agent_cancelling_a_call_does_not_lose_the_outcome",
        GATEWAY,
        [
            (
                "        self.call(arrival).await\n    }\n}",
                """        tokio::select! {
            answer = self.handle(arrival) => answer.1,
            _ = context.ct.cancelled() => Err(ErrorData::internal_error("cancelled", None)),
        }
    }
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
        [('            "meta": arrival.meta,\n', "")],
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
        "a call may be given two final answers",
        "a_retry_links_to_its_call_and_a_call_gets_one_final_answer",
        RETRIES,
        [
            (
                """CREATE UNIQUE INDEX events_one_final_response_per_request
    ON events (session_id, request_seq)
    WHERE kind = 'call_response' AND outcome NOT IN ('transport_error', 'timeout');
""",
                "",
            )
        ],
    ),
    Fault(
        "dedup: a timeout counts as a final answer",
        "a_retry_links_to_its_call_and_a_call_gets_one_final_answer",
        EVENT,
        [("!matches!(self, Self::TransportError | Self::Timeout)", "!matches!(self, Self::TransportError)")],
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
                """            None => Effect {
                class: EffectClass::AtMostOnce,
                source: EffectSource::Unlisted,""",
                """            None => Effect {
                class: EffectClass::ReadOnly,
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
        [('                payload["effect"] = json!(effect);\n', "                let _ = &effect;\n")],
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
    Fault(
        "dedup: an unsafe call in doubt runs again on a retry",
        "an_unsafe_call_in_doubt_is_flagged_and_never_runs_again",
        GATEWAY,
        [("            EffectClass::ReadOnly | EffectClass::Idempotent => {\n", "            EffectClass::ReadOnly | EffectClass::Idempotent | EffectClass::AtMostOnce => {\n")],
    ),
    Fault(
        "dedup: taking a session over leaves open calls unflagged",
        "a_takeover_flags_an_open_unsafe_call_and_its_late_outcome_is_not_logged",
        GATEWAY,
        [("        self.flag_open_calls(&lease).await?;\n", "")],
    ),
    Fault(
        "dedup: a call whose outcome was not logged stays in flight",
        "an_unsafe_call_in_doubt_is_flagged_and_never_runs_again",
        GATEWAY,
        [
            (
                """                    .in_flight
                    .lock()
                    .expect("lock poisoned")
                    .remove(&id);""",
                """                    .in_flight
                    .lock()
                    .expect("lock poisoned")
                    .get(&id);""",
            )
        ],
    ),
    Fault(
        "dedup: a keyed retry gets a new key",
        "a_keyed_call_whose_outcome_was_lost_runs_again_with_the_same_key",
        GATEWAY,
        [("let key = Some((param, key_for(session, request.seq)));", "let key = Some((param, key_for(session, request.seq + 1)));")],
    ),
    Fault(
        "dedup: no heartbeat during a call",
        "a_call_longer_than_the_lease_keeps_the_session",
        GATEWAY,
        [("        if ttl.is_zero() {\n", "        if ttl.is_zero() || ttl > Duration::ZERO {\n")],
    ),
    Fault(
        "dedup: identical arguments alone make a retry",
        "identical_calls_without_a_step_all_run",
        DEDUP,
        [("&& &e.payload[\"rpc_id\"] == call.rpc_id", "|| true")],
    ),
    Fault(
        "dedup: a step reused for other arguments is accepted",
        "a_step_reused_for_other_arguments_is_refused",
        DEDUP,
        [("            return if matches(seq) {\n", "            return if matches(seq) || true {\n")],
    ),
    Fault(
        "dedup: the same id in the same transport session is not a retry",
        "the_same_id_in_the_same_transport_session_is_a_retry",
        DEDUP,
        [("            return Decision::Retry(seq);\n        }\n    }\n", "            let _ = seq;\n        }\n    }\n")],
    ),
    Fault(
        "dedup: agents see the idempotency key",
        "tools_pass_through_with_their_annotations_and_schemas",
        GATEWAY,
        [("        if let Some(param) = effect.key_param {\n", "        if let Some(param) = effect.key_param.filter(|_| false) {\n")],
    ),
    Fault(
        "dedup: an agent's own key is kept",
        "an_agents_own_key_is_replaced",
        GATEWAY,
        [(".insert(param.clone(), Value::String(key.clone()));", ".entry(param.clone()).or_insert(Value::String(key.clone()));")],
    ),
    Fault(
        "conformance: the memory store ignores fencing",
        "an_append_with_a_stale_token_is_rejected",
        MEMORY,
        [("            Some(row) if row.token == lease.token => {}\n            _ => return Err(StoreError::Fenced),", "            Some(_) => {}\n            _ => return Err(StoreError::Fenced),")],
    ),
    Fault(
        "conformance: the memory store accepts a second final answer",
        "a_retry_links_to_its_call_and_a_call_gets_one_final_answer",
        MEMORY,
        [("        if answered {\n", "        if answered && false {\n")],
    ),
    Fault(
        "conformance: the memory store renews on a refused append",
        "a_refused_append_does_not_extend_the_lease",
        MEMORY,
        [
            (
                "        check(&event, session, seq)?;\n",
                """        if let Err(refused) = check(&event, session, seq) {
            if let Some(row) = state.leases.get_mut(&event.session_id) {
                row.expires_at = expiry(now, lease.ttl);
            }
            return Err(refused);
        }
""",
            )
        ],
    ),
    # The simulator must catch real bugs, not merely run without complaint.
    Fault(
        "sim: an unsafe call in doubt runs again",
        SIM_TEST,
        GATEWAY,
        [("            EffectClass::ReadOnly | EffectClass::Idempotent => {\n", "            EffectClass::ReadOnly | EffectClass::Idempotent | EffectClass::AtMostOnce => {\n")],
    ),
    Fault(
        "sim: a takeover leaves open calls unflagged",
        SIM_TEST,
        GATEWAY,
        [("        self.flag_open_calls(&lease).await?;\n", "")],
    ),
    Fault(
        "sim: a keyed retry gets a new key",
        SIM_TEST,
        GATEWAY,
        [("let key = Some((param, key_for(session, request.seq)));", "let key = Some((param, key_for(session, request.seq + 1)));")],
    ),
    Fault(
        "sim: a step does not identify a retry",
        SIM_TEST,
        DEDUP,
        [("    if let Some(step) = call.step {\n", "    if let Some(step) = call.step.filter(|_| false) {\n")],
    ),
    Fault(
        "sim: the store ignores fencing",
        SIM_TEST,
        MEMORY,
        [("            Some(row) if row.token == lease.token => {}\n            _ => return Err(StoreError::Fenced),", "            Some(_) => {}\n            _ => return Err(StoreError::Fenced),")],
    ),
    Fault(
        "sim: a result is released though not logged",
        SIM_TEST,
        GATEWAY,
        [
            (
                """            return Err(ErrorData::new(
                error_codes::OUTCOME_NOT_RECORDED,
                "the tool may have run, but ledger could not record the outcome",
                None,
            ));""",
                """            let _ = ErrorData::new(
                error_codes::OUTCOME_NOT_RECORDED,
                "the tool may have run, but ledger could not record the outcome",
                None,
            );""",
            )
        ],
    ),
    Fault(
        "sim: a lying tool's override is ignored",
        SIM_TEST,
        POLICY,
        [("if let Some(entry) = self.overrides.get(name) {", "if let Some(entry) = self.overrides.get(name).filter(|_| name.is_empty()) {")],
    ),
    Fault(
        "model: the model is asked before the request is logged",
        "a_model_call_that_cannot_be_logged_is_not_sent",
        GATEWAY,
        [("        let seq = self.append(request).await.map_err(not_recorded)?.seq;\n        drop(guard);", "        let _early = upstream.send(&sent, &call.credentials).await;\n        let seq = self.append(request).await.map_err(not_recorded)?.seq;\n        drop(guard);")],
    ),
    Fault(
        "model: a reply reaches the agent though it was not logged",
        "a_reply_that_cannot_be_logged_does_not_reach_the_agent",
        GATEWAY,
        [("            tracing::error!(session, model = name, seq, error = %e, \"could not record model reply\");\n            return Err(", "            tracing::error!(session, model = name, seq, error = %e, \"could not record model reply\");\n            return Ok(logged);\n            #[allow(unreachable_code)]\n            return Err(")],
    ),
    Fault(
        "model: the agent's key is logged",
        "credentials_reach_the_model_but_never_the_log",
        GATEWAY,
        [("        let mut payload = json!({ \"request\": call.body });", "        let mut payload = json!({ \"request\": call.body, \"auth\": format!(\"{:?}\", call.credentials) });")],
    ),
    Fault(
        "model: every header the agent sent is passed on",
        "credentials_reach_the_model_but_never_the_log",
        MODEL,
        [("pub(crate) fn credentials(headers: &HeaderMap) -> HeaderMap {\n", "pub(crate) fn credentials(headers: &HeaderMap) -> HeaderMap {\n    #[allow(clippy::needless_return)]\n    return headers.clone();\n    #[allow(unreachable_code)]\n")],
    ),
    Fault(
        "model: a stream is logged without its last event",
        "a_streamed_reply_is_logged_event_by_event_and_delivered_whole",
        MODEL,
        [("        if !data.is_empty() {\n            events.push(data.join(\"\\n\"));", "        if !data.is_empty() && data[0] != \"[DONE]\" {\n            events.push(data.join(\"\\n\"));")],
    ),
    Fault(
        "model: any Host is accepted",
        "a_disallowed_host_or_origin_is_refused_before_anything_is_logged",
        MODEL,
        [("    if !matches(host.host(), host.port_u16()) {\n        return false;", "    if !matches(host.host(), host.port_u16()) {\n        return true;")],
    ),
    Fault(
        "model: a takeover flags open model calls",
        "a_takeover_leaves_an_open_model_call_alone",
        GATEWAY,
        [("            e.kind == EventKind::CallRequest\n                && dedup::final_answer(&events, e.seq).is_none()", "            e.kind.is_request()\n                && dedup::final_answer(&events, e.seq).is_none()")],
    ),
    Fault(
        "model: a failed model call is logged as a success",
        "a_model_error_is_logged_and_passed_on",
        GATEWAY,
        [("                } else {\n                    Outcome::RpcError\n                };", "                } else {\n                    Outcome::Ok\n                };")],
    ),
    Fault(
        "model: a timeout counts as the model's final answer",
        "model_calls_share_the_session_sequence_and_get_one_final_answer",
        MODEL_CALLS,
        [("    WHERE kind = 'model_response' AND outcome NOT IN ('transport_error', 'timeout');", "    WHERE kind = 'model_response';")],
    ),
    Fault(
        "model: the schema lets a model request go unhashed",
        "the_schema_refuses_malformed_model_calls",
        MODEL_CALLS,
        [("        CHECK ((kind IN ('call_request', 'model_request')) = (args_hash IS NOT NULL)) NOT VALID;", "        CHECK ((kind = 'call_request') = (args_hash IS NOT NULL) OR kind = 'model_request') NOT VALID;")],
    ),
    Fault(
        "conformance: the memory store lets a model request go unhashed",
        "the_schema_refuses_malformed_model_calls",
        MEMORY,
        [("event.kind.is_request() != event.args_hash.is_some()", "(event.kind == EventKind::CallRequest && event.args_hash.is_none())")],
    ),
    Fault(
        "fork: calls before the fork step run instead of replaying",
        "a_fork_past_the_end_replays_the_whole_run_and_runs_nothing",
        FORK,
        [("    if before.peek().is_some() {", "    if before.peek().is_some() && fork.at_step < 0 {")],
    ),
    Fault(
        "fork: a call that went another way before the fork step is let through",
        "a_fork_whose_agent_goes_another_way_before_the_step_is_refused",
        FORK,
        [("            None => Replay::Diverged(format!(", "            None if fork.at_step > 0 => return Replay::Live,\n            None => Replay::Diverged(format!(")],
    ),
    Fault(
        "fork: a source call can answer more than once",
        "a_fork_goes_live_at_its_step_on_the_new_model_and_runs_no_new_effects",
        FORK,
        [("        .filter_map(|e| e.payload[\"replayed_from\"][\"seq\"].as_i64())", "        .filter_map(|e| e.payload[\"replayed_from\"][\"seq\"].as_i64().filter(|_| false))")],
    ),
    Fault(
        "fork: safe mode runs a tool with effects",
        "a_fork_goes_live_at_its_step_on_the_new_model_and_runs_no_new_effects",
        FORK,
        [("        (None, ToolMode::Safe) if asked.read_only => Replay::Live,", "        (None, ToolMode::Safe) => Replay::Live,")],
    ),
    Fault(
        "fork: replay mode runs a read",
        "in_replay_mode_nothing_new_runs_not_even_a_read",
        FORK,
        [("        (None, ToolMode::Safe) if asked.read_only => Replay::Live,", "        (None, ToolMode::Safe | ToolMode::Replay) if asked.read_only => Replay::Live,")],
    ),
    Fault(
        "fork: live mode answers from the log instead of running",
        "in_live_mode_a_fork_runs_its_tool_calls_for_real",
        FORK,
        [("    if asked.kind == EventKind::ModelRequest || fork.tools == ToolMode::Live {", "    if asked.kind == EventKind::ModelRequest {")],
    ),
    Fault(
        "fork: the fork's model is not used",
        "a_fork_goes_live_at_its_step_on_the_new_model_and_runs_no_new_effects",
        GATEWAY,
        [("                        sent[\"model\"] = json!(model);", "                        let _ = model;")],
    ),
    Fault(
        "fork: the fork's system prompt is not used",
        "a_new_system_prompt_is_sent_and_logged_beside_the_agents_request",
        GATEWAY,
        [("                        set_system_prompt(&mut sent, prompt);", "                        let _ = prompt;")],
    ),
    Fault(
        "fork: a live model call is hashed as sent, not as the agent asked",
        "a_new_system_prompt_is_sent_and_logged_beside_the_agents_request",
        GATEWAY,
        [("        let request = NewEvent::model_request(session, &name, &call.body, payload);", "        let request = NewEvent::model_request(session, &name, &sent, payload);")],
    ),
    Fault(
        "fork: the gateway forgets that a session is a fork",
        "a_fork_past_the_end_replays_the_whole_run_and_runs_nothing",
        GATEWAY,
        [("                forks.entry(session).or_insert(None);", "                forks.insert(session, None);")],
    ),
    Fault(
        "fork: a started session can become a fork",
        "a_fork_cannot_take_a_started_session_or_name_a_missing_source",
        STORE,
        [("        ON CONFLICT (session_id) DO NOTHING", "        ON CONFLICT (session_id) DO UPDATE SET last_seq = sessions.last_seq")],
    ),
    Fault(
        "conformance: the memory store lets a started session become a fork",
        "a_fork_cannot_take_a_started_session_or_name_a_missing_source",
        MEMORY,
        [("        if state.sessions.contains_key(&fork.session_id) {\n            return Err(StoreError::SessionExists", "        if state.sessions.contains_key(&fork.session_id) && fork.at_step < 0 {\n            return Err(StoreError::SessionExists")],
    ),
    Fault(
        "replay: a replay runs its tool calls",
        "a_model_that_emails_differently_diverges_at_the_email",
        REGRESS,
        [("        tools: ToolMode::Replay,", "        tools: ToolMode::Live,")],
    ),
    Fault(
        "replay: a replay keeps the recorded model",
        "a_model_that_emails_differently_diverges_at_the_email",
        REGRESS,
        [("        model: Some(model.to_owned()),", "        model: Some(model.to_owned()).filter(|_| false),")],
    ),
    Fault(
        "replay: tool calls are compared by name alone",
        "a_model_that_emails_differently_diverges_at_the_email",
        COMPARE,
        [("            ) => a == b && x == y,", "            ) => a == b && (x == y || true),")],
    ),
    Fault(
        "replay: the final answer is not compared",
        "another_answer_diverges_at_the_end",
        COMPARE,
        [("        && message[\"tool_calls\"].as_array().is_none_or(Vec::is_empty)", "        && message[\"tool_calls\"].as_array().is_none_or(Vec::is_empty)\n        && reply.seq < 0")],
    ),
    Fault(
        "replay: calls answered from the log count as run",
        "a_replay_on_the_same_model_acts_the_same_and_runs_no_tool",
        COMPARE,
        [("                _ if !event.payload[\"replayed_from\"].is_null() => cost.tools_from_log += 1,", "                _ if event.payload[\"replayed_from\"].is_null() && false => cost.tools_from_log += 1,")],
    ),
    Fault(
        "replay: an agent that fails counts as the same",
        "regress_runs_the_suites_agent_for_each_session_and_counts_the_outcomes",
        GATEWAY_MAIN,
        [("                    (Err(_), _) => failed += 1,", "                    (Err(_), _) => same += 1,")],
    ),
    Fault(
        "replay: --fail-on-divergence is ignored",
        "regress_runs_the_suites_agent_for_each_session_and_counts_the_outcomes",
        GATEWAY_MAIN,
        [("                !fail_on_divergence || diverged + failed == 0,", "                !fail_on_divergence || diverged + failed < usize::MAX,")],
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
    names = list(filter(None, listed.split("\0")))
    if not names:
        sys.exit(f"git lists no files under {ROOT}: is it a git work tree of its own?")
    for name in names:
        source = ROOT / name
        if source.is_file():
            (copy / name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, copy / name)


def stale_edits() -> list[str]:
    """Faults whose edit text is missing from, or appears more than once in, its file."""
    problems = []
    for fault in FAULTS:
        text = (ROOT / fault.path).read_text()
        for old, _ in fault.edits:
            count = text.count(old)
            if count != 1:
                problems.append(f"{fault.name}: edit text found {count} times in {fault.path}")
    return problems


def main() -> int:
    args = sys.argv[1:]
    if "--check" in args:
        problems = stale_edits()
        for problem in problems:
            print(problem)
        print(f"{len(FAULTS)} faults, {len(problems)} that no longer apply cleanly")
        return 1 if problems else 0
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
