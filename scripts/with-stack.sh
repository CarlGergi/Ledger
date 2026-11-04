#!/usr/bin/env bash
# Runs a command with the tool servers and the gateway up, then stops them.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --quiet --workspace
export DATABASE_URL="${DATABASE_URL:-postgres://ledger:ledger@127.0.0.1:55432/ledger}"
mkdir -p var

listening() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

for port in 7100 7101 7102; do
    if listening "$port"; then
        echo "port $port is already in use; is another stack running?" >&2
        exit 1
    fi
done

pids=()
cleanup() {
    kill "${pids[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

target/debug/email-server --outbox var/outbox.jsonl >var/email.log 2>&1 &
pids+=($!)
target/debug/file-server --root var/files >var/files.log 2>&1 &
pids+=($!)
target/debug/ledger serve --config config/ledger.toml >var/ledger.log 2>&1 &
gateway=$!
pids+=("$gateway")

for _ in $(seq 1 100); do
    kill -0 "$gateway" 2>/dev/null || break
    if listening 7100; then
        "$@"
        exit $?
    fi
    sleep 0.1
done
echo "the gateway did not start; see var/ledger.log" >&2
exit 1
