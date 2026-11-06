// Drives the gateway with the official TypeScript MCP client, configured only with a
// URL: it knows nothing about Ledger. Then checks what Ledger recorded about it.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";

const url = new URL(process.env.LEDGER_URL ?? "http://127.0.0.1:7100/mcp");
const transport = new StreamableHTTPClientTransport(url);
const client = new Client({ name: "interop-check", version: "1.0.0" });
// Far beyond what a loaded machine needs, so that only a real failure reaches it.
const patience = { timeout: 120_000 };
await client.connect(transport, patience);

const { tools } = await client.listTools(undefined, patience);
const byName = new Map(tools.map((tool) => [tool.name, tool]));
assert.ok(byName.has("email__send_email"), "email tools are listed");
assert.ok(byName.has("files__write_file"), "file tools are listed");
assert.equal(byName.get("email__send_email").annotations.destructiveHint, true);
assert.equal(byName.get("email__send_email").annotations.idempotentHint, false);
assert.equal(byName.get("files__read_file").annotations.readOnlyHint, true);

const stamp = new Date().toISOString();
const calls = [
  ["files__write_file", { path: "interop/note.txt", content: stamp }],
  ["files__read_file", { path: "interop/note.txt" }],
  ["email__send_email", { to: "team@example.com", subject: "interop", body: stamp }],
];
const results = [];
for (const [name, args] of calls) {
  const result = await client.callTool({ name, arguments: args }, undefined, patience);
  assert.notEqual(result.isError, true, `${name} failed`);
  results.push(result);
}
assert.equal(results[1].structuredContent.content, stamp, "the file reads back");

const refused = await client
  .callTool({ name: "files__write_file", arguments: { path: "../escape", content: "x" } }, undefined, patience)
  .then((result) => result.isError);
assert.equal(refused, true, "tool errors reach the client as tool errors");

const session = transport.sessionId;
assert.ok(session, "the client was given a transport session");
const log = execFileSync("target/debug/ledger", ["log", session, "--json"], { encoding: "utf8" })
  .trim()
  .split("\n")
  .map((line) => JSON.parse(line));

assert.equal(log.length, (calls.length + 1) * 2);
log.forEach((event, index) => {
  assert.equal(event.seq, index + 1);
  assert.equal(event.kind, index % 2 === 0 ? "call_request" : "call_response");
});
calls.forEach(([name, args], index) => {
  const [request, response] = [log[index * 2], log[index * 2 + 1]];
  assert.equal(request.tool, name);
  assert.deepEqual(request.payload.params.arguments, args);
  assert.equal(response.outcome, "ok");
  assert.deepEqual(response.payload.result.structuredContent, results[index].structuredContent);
});
assert.equal(log.at(-1).outcome, "tool_error");

await client.close();
console.log(`ok: ${log.length} events recorded in order under session ${session}`);
console.log(`protocol version negotiated: ${transport.protocolVersion}`);
