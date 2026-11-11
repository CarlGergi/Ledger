import assert from "node:assert/strict";
import { test } from "node:test";

import { LedgerEvent, calls, description, inspect, isFinal, label, replayAt, status } from "../src/model";

let next = 0;
function event(kind: LedgerEvent["kind"], fields: Partial<LedgerEvent> = {}): LedgerEvent {
  next += 1;
  return {
    session_id: "s",
    seq: next,
    kind,
    request_seq: null,
    tool: "files__append_file",
    outcome: null,
    args_hash: kind === "call_request" ? "a".repeat(64) : null,
    fence_token: 1,
    payload: {},
    recorded_at: `2026-01-01T00:00:0${next}Z`,
    ...fields,
  };
}

/** A session: one call answered at once, and one retried after a timeout and settled. */
function session(): LedgerEvent[] {
  next = 0;
  return [
    event("call_request", {
      payload: { effect: { class: "at_most_once" }, params: { arguments: { content: "x" } }, step: "1" },
    }),
    event("call_response", { request_seq: 1, outcome: "ok", payload: { result: { content: [] } } }),
    event("call_request", { tool: "email__send_email", payload: { effect: { class: "keyed" } } }),
    event("call_response", { request_seq: 3, tool: "email__send_email", outcome: "timeout" }),
    event("call_retry", { request_seq: 3, tool: "email__send_email", payload: { step: "2" } }),
    event("call_response", {
      request_seq: 3,
      tool: "email__send_email",
      outcome: "ok",
      payload: { idempotency_key: "ledger:s:3", result: { content: ["sent"] } },
    }),
  ];
}

test("events group into calls, in the order the calls were made", () => {
  const grouped = calls(session());
  assert.deepEqual(
    grouped.map((c) => [c.seq, c.retries.length, c.responses.length]),
    [
      [1, 0, 1],
      [3, 1, 2],
    ],
  );
});

test("only a final outcome settles a call", () => {
  assert.equal(isFinal("ok"), true);
  assert.equal(isFinal("in_doubt"), true);
  assert.equal(isFinal("timeout"), false);
  assert.equal(isFinal("transport_error"), false);

  const events = session();
  assert.deepEqual(calls(events).map(status), ["ok", "ok"]);
  // Before the retry settled it, the second call was open, with only a timeout heard.
  assert.equal(status(calls(events.slice(0, 4))[1]), "open (timeout)");
  assert.equal(status(calls(events.slice(0, 3))[1]), "open");
});

test("a flagged call shows as in doubt", () => {
  next = 0;
  const events = [event("call_request"), event("call_response", { request_seq: 1, outcome: "in_doubt" })];
  assert.equal(status(calls(events)[0]), "in_doubt");
});

test("descriptions give the status, retries and effect class", () => {
  const [first, second] = calls(session());
  assert.equal(description(first), "ok  [at_most_once]");
  assert.equal(description(second), "ok, 1 retry  [keyed]");
});

test("inspecting a call shows its arguments, every attempt, and its answer", () => {
  const second = calls(session())[1];
  const view = inspect(second) as any;
  assert.equal(view.call, 3);
  assert.equal(view.status, "ok");
  assert.deepEqual(
    view.attempts.map((a: any) => [a.outcome, a.idempotency_key]),
    [
      ["timeout", null],
      ["ok", "ledger:s:3"],
    ],
  );
  assert.equal(view.retries[0].step, "2");
  assert.deepEqual(view.answer, { content: ["sent"] });
  assert.deepEqual((inspect(calls(session())[0]) as any).arguments, { content: "x" });
});

test("replay shows the session as it stood at each step", () => {
  const events = session();
  const third = replayAt(events, 3);
  assert.match(third, /step 3 of 6/);
  assert.match(third, /#1 files__append_file\s+ok/);
  assert.match(third, /#3 email__send_email\s+open\s/);
  const last = replayAt(events, 6);
  assert.match(last, /#3 email__send_email\s+ok, 1 retry/);
  // Steps outside the session clamp to its ends.
  assert.match(replayAt(events, 0), /step 1 of 6/);
  assert.match(replayAt(events, 99), /step 6 of 6/);
  assert.match(replayAt([], 1), /no events/);
});

test("model calls join the timeline, with what the model chose", () => {
  next = 0;
  const asked = { model: "reporter-1", messages: [{ role: "user", content: "hi" }], tools: [{ function: { name: "files__write_file" } }] };
  const chose = { role: "assistant", tool_calls: [{ function: { name: "files__write_file" } }] };
  const events = [
    event("model_request", { tool: "reporter-1", payload: { request: asked } }),
    event("model_response", {
      request_seq: 1,
      tool: "reporter-1",
      outcome: "ok",
      payload: { status: 200, body: { json: { choices: [{ message: chose }], usage: { total_tokens: 9 } } } },
    }),
    event("call_request", { payload: { effect: { class: "at_most_once" } } }),
    event("model_request", { tool: "reporter-1", payload: { request: asked } }),
    event("model_response", { request_seq: 4, tool: "reporter-1", outcome: "timeout", payload: { error: "slow" } }),
  ];
  const grouped = calls(events);
  assert.deepEqual(
    grouped.map((c) => [c.seq, c.kind]),
    [
      [1, "model"],
      [3, "tool"],
      [4, "model"],
    ],
  );
  assert.equal(label(grouped[0]), "#1 model reporter-1");
  assert.equal(description(grouped[0]), "ok, calls files__write_file  [model]");
  assert.equal(description(grouped[2]), "open (timeout)  [model]");
  const view = inspect(grouped[0]) as any;
  assert.equal(view.model, "reporter-1");
  assert.deepEqual(view.answer, chose);
  assert.deepEqual(view.tools_offered, ["files__write_file"]);
  assert.equal(view.usage.total_tokens, 9);
  assert.match(replayAt(events, 2), /model's answer to call #1: ok/);
  assert.match(replayAt(events, 2), /#1 model reporter-1\s+ok, calls files__write_file/);
});
