#!/usr/bin/env bash
# The email demo: one email, an answer lost after it was sent, and an agent that retries.
#
#   1. Straight to the tool, the retry sends the email again.
#   2. Through Ledger, with the answer lost on its way to the agent: sent once.
#   3. Through Ledger, with the answer lost on its way from the tool: sent once.
#
# With AGENTMAIL_API_KEY, AGENTMAIL_INBOX (an inbox you own) and AGENTMAIL_TO (an address
# you read) set, this sends real email through AgentMail: four messages in all. Without
# them, it uses a local stand-in for AgentMail's API, which keeps AgentMail's rules for
# idempotency keys. --local always uses the stand-in, whatever is set.
set -euo pipefail
local_only=false
[[ "${1:-}" == "--local" ]] && local_only=true
cd "$(dirname "$0")/.."

cargo build --quiet --workspace
export DATABASE_URL="${DATABASE_URL:-postgres://ledger:ledger@127.0.0.1:55432/ledger}"
dir=var/agentmail-demo
mkdir -p "$dir"

pids=()
cleanup() {
    kill "${pids[@]}" 2>/dev/null || true
    wait 2>/dev/null || true
}
trap cleanup EXIT

listening() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

wait_for() {
    for _ in $(seq 1 1200); do
        listening "$1" && return 0
        sleep 0.1
    done
    echo "nothing is listening on port $1; see $dir/*.log" >&2
    exit 1
}

for port in 7110 7111 7112 7113 7114 7115 7116; do
    if listening "$port"; then
        echo "port $port is already in use" >&2
        exit 1
    fi
done

if [[ $local_only == false && -n "${AGENTMAIL_API_KEY:-}" && -n "${AGENTMAIL_INBOX:-}" && -n "${AGENTMAIL_TO:-}" ]]; then
    api=https://api.agentmail.to/v0
    inbox=$AGENTMAIL_INBOX
    to=$AGENTMAIL_TO
    echo "Sending real email from $inbox to $to through AgentMail."
else
    export AGENTMAIL_API_KEY=am-demo-key
    api=http://127.0.0.1:7110/v0
    inbox=demo@agentmail.local
    to=carl@example.com
    target/debug/fake-agentmail --listen 127.0.0.1:7110 >"$dir/fake-agentmail.log" 2>&1 &
    pids+=($!)
    wait_for 7110
    echo "Using a local stand-in for AgentMail's API."
fi

target/debug/agentmail-server --listen 127.0.0.1:7111 --inbox "$inbox" --api "$api" >"$dir/agentmail-server.log" 2>&1 &
pids+=($!)

# A gateway in front of the AgentMail tool server at $2, listening on $1.
gateway() {
    cat >"$dir/ledger-$1.toml" <<EOF
listen = "127.0.0.1:$1"
call_timeout_secs = 60
[[upstream]]
name = "agentmail"
url = "http://127.0.0.1:$2/mcp"
EOF
    target/debug/ledger serve --config "$dir/ledger-$1.toml" >"$dir/ledger-$1.log" 2>&1 &
    pids+=($!)
}

proxy() {
    target/debug/lossy-proxy --listen "127.0.0.1:$1" --target "http://127.0.0.1:$2" --lose 1 >"$dir/proxy-$1.log" 2>&1 &
    pids+=($!)
}

proxy 7112 7111 # 1: agent -> lossy -> tool
gateway 7113 7111
proxy 7114 7113 # 2: agent -> lossy -> Ledger -> tool
proxy 7115 7111
gateway 7116 7115 # 3: agent -> Ledger -> lossy -> tool
for port in 7111 7112 7113 7114 7115 7116; do wait_for "$port"; done

stamp=$(date +%s)
run() {
    local name=$1 url=$2 tool=$3 session=$4
    local subject="Ledger demo $stamp: $name"
    local args=(--url "$url" --tool "$tool" --to "$to" --subject "$subject")
    [[ -n "$session" ]] && args+=(--session "$session")
    local agent
    agent=$(target/debug/email-agent "${args[@]}" 2>&1) || agent="failed: $agent"
    local sent
    sent=$(target/debug/agentmail-count --api "$api" --inbox "$inbox" --subject "$subject")
    printf '| %s | %s | %s |\n' "$name" "$agent" "$sent"
}

echo
echo "| Run | The agent | Emails sent |"
echo "| --- | --- | --: |"
run "straight to the tool" http://127.0.0.1:7112/mcp send_email ""
run "through Ledger, answer lost on the way to the agent" http://127.0.0.1:7114/mcp agentmail__send_email "agentmail-demo-$stamp-a"
run "through Ledger, answer lost on the way from the tool" http://127.0.0.1:7116/mcp agentmail__send_email "agentmail-demo-$stamp-b"
