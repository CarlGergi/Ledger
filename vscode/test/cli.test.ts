// Runs the CLI reader against a stand-in `ledger` command, which records how it was
// called and answers from fixed output.

import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { events, sessions } from "../src/cli";

function standIn(script: string): { command: string; calls: string } {
  const dir = mkdtempSync(join(tmpdir(), "ledger-cli-test-"));
  const command = join(dir, "ledger");
  const calls = join(dir, "calls");
  writeFileSync(command, `#!/bin/sh\necho "$* DATABASE_URL=$DATABASE_URL" >> '${calls}'\n${script}\n`);
  chmodSync(command, 0o755);
  return { command, calls };
}

test("sessions are read as JSON lines from `ledger sessions --json`", async () => {
  const stand = standIn(
    `echo '{"session_id":"a","event_count":2,"created_at":"t"}'\n` +
      `echo '{"session_id":"b","event_count":4,"created_at":"t"}'`,
  );
  const listed = await sessions({ command: stand.command, databaseUrl: "postgres://x/y" });
  assert.deepEqual(
    listed.map((s) => [s.session_id, s.event_count]),
    [
      ["a", 2],
      ["b", 4],
    ],
  );
  assert.equal(readFileSync(stand.calls, "utf8"), "sessions --json DATABASE_URL=postgres://x/y\n");
});

test("a session's events come from `ledger log <id> --json`", async () => {
  const stand = standIn(`echo '{"session_id":"a","seq":1,"kind":"call_request","request_seq":null}'`);
  const read = await events({ command: stand.command }, "a b");
  assert.equal(read.length, 1);
  assert.equal(read[0].kind, "call_request");
  assert.match(readFileSync(stand.calls, "utf8"), /^log a b --json /);
});

test("a failing command reports what it said", async () => {
  const stand = standIn(`echo 'no events for session "nope"' >&2\nexit 1`);
  await assert.rejects(events({ command: stand.command }, "nope"), /no events for session "nope"/);
});

test("a missing command is reported, not hung on", async () => {
  await assert.rejects(sessions({ command: "/nonexistent/ledger" }), /ledger sessions --json failed/);
});
