# Ledger

A crash-safe gateway and time-travel debugger for AI agents. Ledger sits between an agent and the tools it calls through [MCP](https://modelcontextprotocol.io). The agent connects to Ledger as if it were its tool server and needs no changes. Ledger records every call in PostgreSQL before forwarding it.

**Status:** the gateway and the event log are working. Retry policy, deduplication, and failover between gateway copies are next; see [Roadmap](#roadmap).

## Quick start

You need Rust 1.99 or newer, Docker, and for the optional checks, Node.js and Python 3.

```bash
make demo
```

This starts PostgreSQL, two example tool servers, and the gateway. It then runs a sample agent that writes a file, reads it back, and sends an email, and prints what was recorded:

```text
    1  00:13:01.482  files__write_file  request
    2  00:13:01.497  files__write_file  response to #1 (ok)
    3  00:13:01.501  files__read_file  request
    4  00:13:01.509  files__read_file  response to #3 (ok)
```

To point your own agent at it, run `make stack` and give the agent this MCP server URL:

```text
http://127.0.0.1:7100/mcp
```

## How it works

```text
agent  --MCP-->  Ledger  --MCP-->  tool servers
                   |
               PostgreSQL
            (append-only log)
```

For each `tools/call`, the gateway does three things in order:

1. It commits the request to the log. If that fails, the call is not forwarded.
2. It forwards the call to the tool server, exactly once.
3. It commits the response to the log, then returns it to the agent. If that fails, the agent gets an error instead of the result.

So a tool cannot run without a record of the request, and nothing reaches the agent that the log does not also hold. Everything the agent saw can be replayed later.

The log enforces this in the schema, not only in the gateway:

- Each session's sequence numbers run 1, 2, 3, ... with no gaps, even under concurrent calls.
- `(session, sequence)` is unique, and a call can have only one recorded response.
- Updates, deletes, and truncates are rejected.

### What happens when something fails

| Failure | What the agent sees | What the log shows |
| --- | --- | --- |
| The database rejects the request | Error `-32050`; the tool does not run | Nothing |
| The tool reports failure | The tool's own error result | `tool_error` |
| The tool server rejects the call | The server's JSON-RPC error, unchanged | `rpc_error` |
| The tool server is unreachable | Error `-32052`, outcome unknown | `transport_error` |
| The tool server does not answer in time | Error `-32052`, outcome unknown | `timeout` |
| The tool name has no matching server | Invalid params error | `not_routed` |
| The database rejects the response | Error `-32051`; the tool may have run | A request with no response |
| The gateway is killed mid-call | Connection error | A request with no response |
| The agent cancels or hangs up mid-call | Nothing further | The outcome, recorded when the tool finishes |

A request with no response means the outcome is in doubt, and the log says so rather than guessing.

### Sessions

Calls are grouped into sessions, chosen in this order:

1. The `x-ledger-session` header.
2. The `session` URL parameter, as in `/mcp?session=run-42`.
3. The MCP transport session, which gives an unmodified agent one log per connection.

Only the first two survive a reconnect, because a transport session ends with its connection. Session IDs can use letters, digits, and `- _ . :`, up to 128 characters.

The newest MCP revision (2026-07-28) has no transport sessions. So Ledger answers `server/discover` the way an older server does, which sends clients back to the `initialize` handshake.

### Tool names

Agents see each tool as `<server>__<tool>`, such as `email__send_email`. Schemas and annotations pass through unchanged.

## Configuration

The gateway reads a TOML file; see [`config/ledger.toml`](config/ledger.toml).

| Key | Default | Meaning |
| --- | --- | --- |
| `listen` | `127.0.0.1:7100` | Address to serve on |
| `database_url` | none | Used when `DATABASE_URL` is not set |
| `call_timeout_secs` | `60` | How long to wait for a tool server |
| `allowed_hosts` | loopback only | `Host` values to accept |
| `[[upstream]]` | none | A tool server: `name` and `url` |

## Inspecting the log

```bash
make sessions
```

```bash
make log SESSION=<id>
```

`ledger log <id> --json` prints full events, including arguments and results, one per line.

## Testing

| Command | What it does |
| --- | --- |
| `make test` | Runs every test against real PostgreSQL |
| `make breakage` | Breaks each guarantee in turn and checks that the test protecting it fails |
| `make interop` | Drives the gateway with the official TypeScript MCP client, configured only with a URL |
| `make reproduce` | Runs all of the above, plus lint |

Each database test gets its own randomly named database, so test runs can overlap on one server. A killed test run can leave databases behind; `make db-reset` clears them, along with all recorded sessions.

## Layout

| Path | Contents |
| --- | --- |
| `crates/ledger-store` | Event model, the `EventStore` trait, PostgreSQL store, migrations |
| `crates/ledger-gateway` | The gateway and the `ledger` command |
| `crates/ledger-testkit` | A private, automatically dropped database for each test |
| `crates/ledger-tools` | Example tool servers: email and files |
| `crates/sample-agent` | A scripted MCP client |
| `interop/typescript` | Interoperability check with the official client |
| `scripts` | The breakage check and a helper that runs the full stack |

## Current limits

- **No retries or deduplication yet.** A call an agent retries runs again.
- **No authentication.** Keep the gateway on loopback or behind something that authenticates.
- **Tools only.** Resources and prompts are not passed through, and neither are progress notifications.
- **The agent's `_meta` is logged but not forwarded,** because it describes the agent's exchange with the gateway.
- **Clients that only speak MCP 2026-07-28** cannot connect, because they have no handshake to fall back to.

## Roadmap

1. ~~Pass-through gateway and event log~~
2. Effect policy from MCP annotations, deduplication, idempotency keys, leases with fencing tokens
3. Deterministic simulation testing with fault injection
4. VS Code extension: timeline, inspect, replay
5. Model proxy, forking a session at any step, regression replay against a new model
6. Benchmarks, OpenTelemetry, Azure deployment, a real email demo
