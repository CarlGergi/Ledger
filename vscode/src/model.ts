// The log as Ledger records it, and the views of it the extension shows. Nothing here
// touches VS Code, so all of it can be tested on its own.

export type EventKind = "call_request" | "call_retry" | "call_response";

export interface LedgerEvent {
  session_id: string;
  seq: number;
  kind: EventKind;
  request_seq: number | null;
  tool: string;
  outcome: string | null;
  args_hash: string | null;
  fence_token: number | null;
  payload: any;
  recorded_at: string;
}

export interface SessionSummary {
  session_id: string;
  event_count: number;
  created_at: string;
}

/** One call: its request, every retry of it, and every answer it got. */
export interface Call {
  seq: number;
  tool: string;
  request: LedgerEvent;
  retries: LedgerEvent[];
  responses: LedgerEvent[];
}

/** Outcomes that say only that no answer came back; a later attempt may still settle the call. */
const UNSETTLED = new Set(["transport_error", "timeout"]);

export function isFinal(outcome: string | null): boolean {
  return outcome !== null && !UNSETTLED.has(outcome);
}

/** Groups a session's events into calls, in the order they were made. */
export function calls(events: LedgerEvent[]): Call[] {
  const bySeq = new Map<number, Call>();
  for (const event of events) {
    if (event.kind === "call_request") {
      bySeq.set(event.seq, { seq: event.seq, tool: event.tool, request: event, retries: [], responses: [] });
      continue;
    }
    const call = event.request_seq === null ? undefined : bySeq.get(event.request_seq);
    if (!call) {
      continue;
    }
    if (event.kind === "call_retry") {
      call.retries.push(event);
    } else {
      call.responses.push(event);
    }
  }
  return [...bySeq.values()];
}

/** The call's final answer, if it has one. */
export function finalAnswer(call: Call): LedgerEvent | undefined {
  return call.responses.find((r) => isFinal(r.outcome));
}

/**
 * Where the call stands: its final outcome, or "open" with the last thing heard. A call
 * flagged in_doubt is final, and the one status a person has to act on.
 */
export function status(call: Call): string {
  const settled = finalAnswer(call);
  if (settled) {
    return settled.outcome!;
  }
  const last = call.responses[call.responses.length - 1];
  return last ? `open (${last.outcome})` : "open";
}

export function label(call: Call): string {
  return `#${call.seq} ${call.tool}`;
}

export function description(call: Call): string {
  const retries = call.retries.length;
  const tail = retries === 0 ? "" : retries === 1 ? ", 1 retry" : `, ${retries} retries`;
  const effect = call.request.payload?.effect?.class;
  return `${status(call)}${tail}${effect ? `  [${effect}]` : ""}`;
}

/** Everything about one call, arranged for reading. */
export function inspect(call: Call): object {
  const params = call.request.payload?.params ?? {};
  const answer = finalAnswer(call);
  return {
    call: call.seq,
    tool: call.tool,
    status: status(call),
    effect: call.request.payload?.effect ?? null,
    step: call.request.payload?.step ?? null,
    arguments: params.arguments ?? {},
    requested_at: call.request.recorded_at,
    fence_token: call.request.fence_token,
    retries: call.retries.map((r) => ({
      event: r.seq,
      at: r.recorded_at,
      step: r.payload?.step ?? null,
      rpc_id: r.payload?.rpc_id ?? null,
    })),
    attempts: call.responses.map((r) => ({
      event: r.seq,
      at: r.recorded_at,
      outcome: r.outcome,
      idempotency_key: r.payload?.idempotency_key ?? null,
    })),
    answer: answer
      ? answer.payload?.result ?? answer.payload?.error ?? answer.payload?.reason ?? answer.payload
      : null,
  };
}

export function inspectText(call: Call): string {
  return JSON.stringify(inspect(call), null, 2) + "\n";
}

function describeEvent(event: LedgerEvent): string {
  switch (event.kind) {
    case "call_request":
      return `call #${event.seq} to ${event.tool} with ${JSON.stringify(event.payload?.params?.arguments ?? {})}`;
    case "call_retry":
      return `retry of call #${event.request_seq}`;
    case "call_response":
      return `answer to call #${event.request_seq}: ${event.outcome}`;
  }
}

/**
 * The session as it stood after its first `step` events: the event at that step, and every
 * call as it was then. Steps run from 1 to the number of events.
 */
export function replayAt(events: LedgerEvent[], step: number): string {
  if (events.length === 0) {
    return "This session has no events.\n";
  }
  const at = Math.min(Math.max(step, 1), events.length);
  const seen = events.slice(0, at);
  const current = seen[seen.length - 1];
  const lines = [
    `Session ${current.session_id}, step ${at} of ${events.length}  (Alt+Left / Alt+Right to move)`,
    "",
    `Now:  ${current.recorded_at}  ${describeEvent(current)}`,
    "",
    "Calls so far:",
  ];
  for (const call of calls(seen)) {
    lines.push(`  ${label(call).padEnd(32)} ${description(call)}`);
  }
  lines.push("", "This event:", JSON.stringify(current, null, 2));
  return lines.join("\n") + "\n";
}
