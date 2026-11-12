# Ledger

A crash-safe gateway and time-travel debugger for AI agents. Ledger sits between an agent and the tools it calls through [MCP](https://modelcontextprotocol.io). The agent connects to Ledger as if it were its tool server and needs no changes. Ledger records every call in PostgreSQL before forwarding it.

**Status:** the gateway, retries and failover between gateway copies, the simulation, the VS Code extension, a proxy that logs model calls, and forking a session are working. Regression replay against a new model is next; see [Roadmap](#roadmap).

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
| `call_timeout_secs` | `60` | How long a call waits for its tool, or for a database connection |
| `connect_timeout_secs` | `5` | How long a tool server has to accept a new connection |
| `lease_secs` | `30` | How long a session stays with this copy after its last append; see below |
| `effect_overrides` | none | File correcting what tools claim about their effects, relative to this one |
| `allowed_hosts` | loopback only | `Host` values to accept |
| `[[upstream]]` | none | A tool server: `name` and `url` |
| `[model]` | none | An OpenAI-compatible model to proxy: `url`, `api_key_env`, `timeout_secs` (default `300`); see [Model calls](#model-calls) |

## Inspecting the log

```bash
make sessions
```

```bash
make log SESSION=<id>
```

`ledger log <id> --json` prints full events, including arguments and results, one per line, and `ledger sessions --json` lists sessions the same way. `ledger in-doubt` lists every call flagged for a person (see below). `ledger fork` starts a fork of a session; see [Forking a session](#forking-a-session).

## VS Code extension

`vscode/` holds an extension that reads the log through the `ledger` command:

- **Timeline:** a Ledger view in the activity bar lists sessions, and under each one its calls, each with its status, retries and effect class. Calls flagged as in doubt show a warning icon.
- **Inspect:** selecting a call opens it as JSON: arguments, step, effect class, every retry and attempt with its idempotency key, and the answer.
- **Replay:** the play button on a session steps through it one event at a time, showing every call as it stood at that point. Alt+Right and Alt+Left move forward and back.

To install it:

```bash
make vscode-package
```

Then run **Extensions: Install from VSIX** on `vscode/out/ledger.vsix`. Set `ledger.command` to the `ledger` binary (for example the full path of `target/debug/ledger`), and `ledger.databaseUrl` if `DATABASE_URL` is not set where VS Code runs.

`make vscode` type-checks it against the VS Code API and tests its logic and its use of the `ledger` command, outside VS Code. Nothing tests it inside a running VS Code yet.

## Effect classes

Each tool gets a class that says how safely its calls can be repeated, and each request in the log records the class it was given and why.

| Class | Meaning | A tool gets it by |
| --- | --- | --- |
| `read_only` | Changes nothing | `readOnlyHint: true`, unless it also says `destructiveHint: true` |
| `idempotent` | Repeating it with the same arguments has no further effect | `idempotentHint: true` |
| `keyed` | Repeats with the same key take effect once | Declaring an `idempotency_key` input |
| `at_most_once` | Nothing makes a second run safe | Anything else, including having no annotations or not being listed |

Annotations are only claims made by the tool server, so a file named by `effect_overrides` in the config can correct them, and it always wins. `config/effects.toml` shows the format. A tool named after a server that is not configured stops the gateway at startup, because a typo there would silently leave a tool uncorrected. A misspelled tool name on a real server cannot be caught that way.

The gateway learns annotations from listings, whether an agent asked for them or the gateway needed them for a call. A server that is unreachable or stalled when asked costs the call up to `call_timeout_secs` of extra waiting.

The class decides what happens when a call may have to run twice; see the next section.

## Retries and calls in doubt

An agent that resends a call is told what happened the first time, rather than having the tool run again. A call counts as a retry only if it identifies the call it repeats, and its arguments match (compared by a SHA-256 of the arguments with object keys sorted):

1. **It names the same step,** in the `x-ledger-step` header or as `ledger/step` in its `_meta`, with the same tool and arguments. The same step with different arguments is refused.
2. **It has the same JSON-RPC id in the same transport session,** with the same tool and arguments. MCP forbids reusing an id, so a client doing so is resending.

A retry is logged, and then:

- **If the call is still running in this copy,** the retry waits for its answer.
- **If the call has a final answer,** the retry gets it from the log.
- **Otherwise the call is in doubt:** it was forwarded, and nobody recorded how it ended, so the tool may or may not have run. Ledger never simply runs it again. Instead it goes by the call's class:
  - **`read_only` and `idempotent`** calls run again.
  - **`keyed`** calls run again with the same idempotency key.
  - **Anything else** is answered with `-32052` and logged as `in_doubt`, flagging it for a person. It will never run again. `ledger in-doubt` lists flagged calls.

A copy that takes a session over treats every call left without a final answer as in doubt. Calls that are not safe to repeat are flagged straight away. Safe and keyed ones stay open for a retry to settle. If the old copy then finishes the call, it cannot log the outcome, so its agent gets `-32051` and the call stays in doubt. A copy whose response fails to log for any other reason leaves the call in doubt in the same way.

### Idempotency keys

A keyed tool declares an `idempotency_key` input, or another input named by `key_param` in the override file. The gateway fills it in with `ledger:<session>:<call>`, the same on every attempt at one call and different for every other call. It replaces any key the agent sent, and hides the input from the tool listing, so a model never chooses one. The key is logged with each response.

The demo's `send_email` takes a key: a repeated key returns the first message and sends nothing. `append_file` takes none, and is at most once.

**The trade-off.** Identical arguments on their own never make a retry, because an agent may well mean to send the same email twice, and both must go. So a retry is recognised only by its step or its request id. An agent that resends with a new id and no step gets the tool run again. Agents that number their steps, or resend with the same id, get exactly one run. The exception is after a takeover: any call left without an answer that is not safe to repeat is flagged as in doubt before anything else happens in the session, whatever the agent sends next.

## Several gateway copies

Copies of the gateway can share one database. Each session belongs to one copy at a time, through a lease in the database: a copy takes a session on its first call, and every append extends the lease by `lease_secs`. Another copy can take the session over only once the lease has expired, and taking it over gives it a higher fencing token.

Every append carries its copy's token, and the statement that writes the event also checks it. So a copy that was paused, perhaps for longer than its lease, cannot append once another copy has taken over, however sure it is that it still owns the session. Its calls fail with `-32050` before they reach a tool. Lease expiry uses the database's clock, the one clock every copy shares.

While a call runs, the copy renews its lease every quarter of `lease_secs`, so a call longer than the lease keeps the session.

Fencing protects the log, not the outside world. A copy that logged a request and then paused can still forward it when it wakes. What keeps that safe is the in-doubt rule above: the copy that takes over never runs such a call again unless its class says that is safe.

## Model calls

The gateway can also stand in for an OpenAI-compatible model, so one log holds what the model was asked, what it answered, and the tool calls that followed, in the order they happened. Point the agent's model client at the gateway instead of the provider:

```text
http://127.0.0.1:7100/v1                 with the x-ledger-session header
http://127.0.0.1:7100/s/<session>/v1     for clients that can only set a base URL
```

The session is the one the agent's tool calls use. `POST /chat/completions` is proxied to `[model] url`. A model call follows the tool-call rule:

1. The request is logged as a `model_request`, with a SHA-256 of the whole body. If that fails, the model is not asked, and the agent gets HTTP 503 with error `-32050`.
2. The request goes to the model once, with the lease renewed meanwhile.
3. The reply is logged as a `model_response`. If that fails, the agent gets HTTP 503 with `-32051` instead of the reply.
4. The agent gets the reply rebuilt from what was logged.

| What happened | What the agent gets | What the log shows |
| --- | --- | --- |
| The model answered | The answer | `ok`, with the status and body |
| The model returned an HTTP error | The same status and body | `rpc_error`, with the status and body |
| The model could not be reached | HTTP 502, error `-32052` | `transport_error` |
| The model did not answer within `timeout_secs` | HTTP 504, error `-32052` | `timeout` |

**Streaming.** A streamed reply (`"stream": true`) is read to the end and logged event by event, and only then sent to the agent as the same event stream. The log never holds less than the agent saw, at the cost of the agent receiving the first token only when the last one arrives.

**Credentials.** With `api_key_env` set, the gateway sends that key and ignores the agent's. Without it, the agent's `Authorization` or `api-key` header is passed on. No other header is passed on, and no key is ever logged. Requests whose `Host` or `Origin` is not allowed are refused, by the same rule as `/mcp`, so a web page cannot use the gateway's key through DNS rebinding.

**Model calls have no effects,** so they are never deduplicated, never in doubt, and never flagged when a copy takes a session over. An agent that asks again makes a new model call.

`make demo-model` runs the demo task decided by a scripted model, `reporter-1`, served by `model-server` on port 7103. Its replies depend only on the conversation, which keeps the tests repeatable. `reporter-2` and `reporter-3` differ from it on purpose, for testing replay against a new model.

## Forking a session

A fork runs an agent again from any step of a recorded session, on another model or with another system prompt, to see what would have changed. Everything before the step is answered from the recorded session's log, so the agent gets there exactly as it did the first time, and nothing runs twice.

```bash
ledger fork <session> --at <step> [--model <name>] [--system-prompt-file <file>] [--tools safe|replay|live] [--as <id>]
```

This prints the new session's id. Run the agent again under that session, through the gateway, for its tools and its model calls alike. `<step>` is a sequence number in the source's log (`ledger log`, or the step counter in the extension's replay). One past the last event replays the whole run.

How the fork answers each call:

- **Before the step,** each call, to a tool or to the model, is matched to a call the source made before the step, with the same kind, name and arguments. It gets that call's recorded answer, and nothing is forwarded. Each source call answers once. A call with no match means the agent has already gone another way, so the fork is not a fork of that run: a tool call gets error `-32053`, and a model call gets HTTP 409 with the same code.
- **From the step on,** model calls go to the model, as `--model` if given, with `--system-prompt-file` in place of the agent's system message if given. Tool calls depend on `--tools`:

| `--tools` | A call the source made, with the same arguments | A read-only call it did not make | Any other call |
| --- | --- | --- | --- |
| `safe` (default) | Answered from the source's log | Runs | Not run: the agent gets a tool error saying why |
| `replay` | Answered from the source's log | Not run | Not run |
| `live` | Runs | Runs | Runs |

So by default a fork can explore what a new model would do without sending a second email. Everything it does is logged in the fork's own session: answers taken from the source carry `replayed_from`, calls turned down are logged as `refused`, and live model calls record both the request the agent made and what was sent. `ledger log` starts with what the session is a fork of.

Which source calls have been used is read from the fork's own log on every call, so a fork carries on correctly after a crash or on another gateway copy. A session can become a fork only before it has recorded anything: `ledger fork --as` refuses a session that exists.

## Simulation

`make simulate` runs 10,000 seeds of a deterministic simulation. Each seed sets up three gateway copies sharing one store, a tool server, and two to five agents, all in one thread. Every random choice comes from one generator seeded by the seed, and every timer runs in simulated time. So a seed replays exactly, and a failing one prints the command that replays it with a full trace:

```bash
cargo run --release -p ledger-sim -- --seed <n> --trace
```

The gateway is the real one, driven through `Gateway::call`. The store is the in-memory store, which `crates/ledger-store/tests/conformance.rs` holds to the PostgreSQL store: one suite runs against both, in CI too, so any difference between them fails a test. The HTTP and MCP transport layer is outside the simulator, and so is the model proxy. The integration tests cover both, against real servers.

**Faults injected,** at rates each seed scales by a factor from 0.5 to 4:

- copies killed at any point, including just after a write and before learning it succeeded
- copies paused for longer than their lease, then woken still believing they own their sessions
- writes that fail, and writes that commit but whose acknowledgement is lost
- requests lost, delayed or duplicated on the way to a copy, and answers lost on the way back
- tool servers that refuse connections, hang, or run the tool and lose the answer
- a tool that claims to be read-only but is not, corrected by the override file, and another that is not corrected

Agents come in three kinds: ones that number their steps, ones that resend with the same request id, and ones that resend with a new id and no step.

**Invariants checked after every run,** once a healthy copy has taken every session over:

1. Every session's log is gapless, every answer and retry links to an earlier call of the same tool, no call has two final answers, and fencing tokens never go backwards along the log.
2. Nothing that is not safe to repeat is left without a final answer.
3. No at-most-once or keyed effect happens twice, for agents ledger can recognise.
4. A call flagged as in doubt never runs again.
5. Every answer an agent was given is in the log.
6. A call whose outcome went unrecorded (`-32051`) never runs again if it is not safe to repeat.

Repeats from agents that resend with a new id, and from the uncorrected lying tool, are counted rather than treated as failures. They are the documented limits: ledger cannot tell such a resend from a new call, and cannot know a tool lies unless told.

`make breakage` also checks the simulator itself, by injecting seven bugs into the gateway and store and requiring the seed test to catch each.

Results of `make simulate`, which takes a few seconds:

| Seeds | Failed | Calls | Retries | Tool effects | Takeovers | Flagged in doubt | Repeats from new-id resends | Repeats from the uncorrected lie |
| --: | --: | --: | --: | --: | --: | --: | --: | --: |
| 10,000 | 0 | 197,532 | 21,573 | 187,885 | 106,046 | 9,570 | 3,384 | 752 |

The simulator has not found a bug in Ledger itself. What it did find was a blind spot of its own: its step-numbering agents never reconnected, so rule 2 caught their retries whatever rule 1 did. Its own check against injected bugs is what showed this.

## Testing

| Command | What it does |
| --- | --- |
| `make test` | Runs every test against real PostgreSQL |
| `make breakage` | Breaks each guarantee in turn and checks that the test protecting it fails |
| `make simulate` | Runs 10,000 seeds of the deterministic simulation |
| `make interop` | Drives the gateway with the official TypeScript MCP client, configured only with a URL |
| `make vscode` | Type-checks and tests the VS Code extension |
| `make reproduce` | Runs all of the above, plus lint |
| `make soak` | Repeats the whole suite under load, and fails on any failed run, PostgreSQL reset, or leftover database. About 30 minutes on a busy machine, so it is not part of `make reproduce` |

Each database test gets its own randomly named database, so test runs can overlap on one server, as long as each has its own `CARGO_TARGET_DIR`: even with nothing to build, `cargo test` relinks the binaries that another run's tests may be starting. A killed test run can leave databases behind; `make db-reset` clears them, along with all recorded sessions.

A test that restarts a server reserves its port from below the range the kernel hands out by itself, so while the server is down, connections are refused and nothing else can be given the port.

No test needs real work to finish inside a limit a loaded machine could miss. Limits on real work are 120 seconds, which only a failing test reaches, and short limits appear only on things that never answer, where firing late cannot change the result.

### Flakiness

The suite once failed intermittently. Each cause was traced and fixed, with no retries or sleeps to hide it:

1. **The test harness could deadlock.** `#[sqlx::test]` gives all of a test binary's pools one shared budget of connection permits, and a pool only returns its share once its last clone is dropped. Dropping a test's database needs a permit from that same budget, so finished tests that left a server running could wait on each other until sqlx's 30-second acquire timeout. Tests now get their databases from `ledger-testkit`, where every pool has its own small cap.
2. **Overlapping test runs destroyed each other's databases.** `#[sqlx::test]` names a database after the test's path, so a second run on the same server dropped the first run's databases. `ledger-testkit` names each one randomly.
3. **The Docker health check reset PostgreSQL.** A health check that timed out left its `pg_isready` running. As PID 1, PostgreSQL adopted that orphan, took it for one of its own backends, and on its non-zero exit restarted every connection as if after a crash. The container now runs under an init process, which adopts orphans instead.

Running out of connections looked like the obvious cause, and measuring showed it was not: the peak stays far below the server's limit of 300.

`make soak` repeats the suite under those conditions, and prints the machine and its load with the results. The CPU-starved runs sit at the lowest priority, beneath two busy processes per core, so everything else on the machine comes first. This run shared the machine with other projects' builds:

Machine: Mac17,7, Apple M5 Max, 18 cores, 36 GB, Darwin 27.0.0

| Condition | Suite runs | Failed | Median | Slowest | Peak connections | PostgreSQL resets | Load, start to end |
| --- | --: | --: | --: | --: | --: | --: | --: |
| One run at a time | 40 | 0 | 5.4s | 12.6s | 31 | 0 | 13.1 to 8.3 |
| Three runs at once | 180 | 0 | 12.6s | 20.2s | 92 | 0 | 5.2 to 11.1 |
| Starved of CPU | 10 | 0 | 41.1s | 128.6s | 49 | 0 | 5.3 to 151.1 |

0 failed runs, 0 PostgreSQL resets, 0 test databases left behind.

## Layout

| Path | Contents |
| --- | --- |
| `crates/ledger-store` | Event model, the `EventStore` trait, PostgreSQL and in-memory stores, migrations, the conformance suite |
| `crates/ledger-gateway` | The gateway and the `ledger` command |
| `crates/ledger-sim` | The deterministic simulation |
| `crates/ledger-testkit` | A private, automatically dropped database and reserved ports for each test |
| `crates/ledger-tools` | Example tool servers, email and files, and a scripted OpenAI-compatible model |
| `crates/sample-agent` | MCP clients: a scripted one, and one that a model drives |
| `vscode` | The VS Code extension |
| `interop/typescript` | Interoperability check with the official client |
| `scripts` | The breakage check, the soak, and a helper that runs the full stack |
| `.github/workflows` | CI: lint and every test, against PostgreSQL |

## Current limits

- **Retry detection reads the whole session's log** for each call, which is fine for sessions of thousands of calls, not millions.
- **An agent that resends with a new request id and no step is not recognised as retrying,** so the tool runs again. See "The trade-off" above.
- **A crashed copy's sessions wait for its leases.** A restarted copy cannot record in them for up to `lease_secs`, because it cannot tell itself apart from a paused original.
- **No authentication.** Keep the gateway on loopback or behind something that authenticates.
- **Tools only.** Resources and prompts are not passed through, and neither are progress notifications.
- **A fork needs an agent that asks the same things when given the same answers.** One whose requests carry timestamps, random ids or anything else that changes between runs goes another way at its first call, and the fork refuses it with `-32053` rather than pretend to replay it.
- **Chat completions only.** The model proxy handles `POST /chat/completions`, not the Responses API, embeddings, or `GET /models`.
- **Streamed replies arrive all at once,** because each is logged in full before it is sent, so the agent loses streaming's early first token. The alternative keeps both: log each chunk as an event of its own, then forward it, so the agent streams and the log still leads by one chunk. A copy that died mid-stream would leave a reply with no end, which replay would have to treat as cut off. Planned only after milestone 6, if there is time.
- **The agent's `_meta` is logged but not forwarded,** because it describes the agent's exchange with the gateway.
- **Clients that only speak MCP 2026-07-28** cannot connect, because they have no handshake to fall back to.

## Roadmap

1. ~~Pass-through gateway and event log~~
2. ~~Effect policy from MCP annotations, deduplication, idempotency keys, leases with fencing tokens~~
3. ~~Deterministic simulation testing with fault injection~~
4. ~~VS Code extension: timeline, inspect, replay~~
5. Model proxy, forking a session at any step, regression replay against a new model
6. Benchmarks, OpenTelemetry, Azure deployment, a real email demo
