#!/usr/bin/env bash
# The email demo, into Mailpit: one email, an answer lost after it was sent, and an agent
# that retries.
#
#   1. Straight to the tool, the retry sends the email again.
#   2. Through Ledger, with the answer lost on its way to the agent: sent once.
#   3. Through Ledger, with the answer lost on its way from the tool: sent once.
#
# Mailpit (docker compose) catches the mail: SMTP on 1025, an inbox at
# http://127.0.0.1:8025. Nothing leaves this machine.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --quiet --workspace
export DATABASE_URL="${DATABASE_URL:-postgres://ledger:ledger@127.0.0.1:55432/ledger}"
mailpit=${MAILPIT_URL:-http://127.0.0.1:8025}
dir=var/email-demo
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

curl -sf "$mailpit/api/v1/info" >/dev/null || {
    echo "Mailpit is not answering at $mailpit; run make db-up" >&2
    exit 1
}
for port in 7117 7118 7119 7120 7121 7122; do
    if listening "$port"; then
        echo "port $port is already in use" >&2
        exit 1
    fi
done

target/debug/smtp-server --listen 127.0.0.1:7117 >"$dir/smtp-server.log" 2>&1 &
pids+=($!)

# A gateway in front of the email tool server at $2, listening on $1.
gateway() {
    cat >"$dir/ledger-$1.toml" <<EOF
listen = "127.0.0.1:$1"
call_timeout_secs = 60
[[upstream]]
name = "email"
url = "http://127.0.0.1:$2/mcp"
EOF
    target/debug/ledger serve --config "$dir/ledger-$1.toml" >"$dir/ledger-$1.log" 2>&1 &
    pids+=($!)
}

proxy() {
    target/debug/lossy-proxy --listen "127.0.0.1:$1" --target "http://127.0.0.1:$2" --lose 1 >"$dir/proxy-$1.log" 2>&1 &
    pids+=($!)
}

proxy 7118 7117 # 1: agent -> lossy -> tool
gateway 7119 7117
proxy 7120 7119 # 2: agent -> lossy -> Ledger -> tool
proxy 7121 7117
gateway 7122 7121 # 3: agent -> Ledger -> lossy -> tool
for port in 7117 7118 7119 7120 7121 7122; do wait_for "$port"; done

stamp=$(date +%s)
# How many messages Mailpit holds with exactly this subject.
count() {
    curl -sf -G "$mailpit/api/v1/search" --data-urlencode "query=subject:\"$1\"" --data-urlencode limit=100 |
        jq --arg s "$1" '[.messages[] | select(.Subject == $s)] | length'
}
run() {
    local name=$1 url=$2 tool=$3 session=$4
    local subject="Run $stamp: $name"
    local args=(--url "$url" --tool "$tool" --to carl@example.com --subject "$subject")
    [[ -n "$session" ]] && args+=(--session "$session")
    local agent
    agent=$(target/debug/email-agent "${args[@]}" 2>&1) || agent="failed: $agent"
    printf '| %s | %s | %s |\n' "$name" "$agent" "$(count "$subject")"
}

out=results/email-demo
mkdir -p "$out"
echo "Mail goes to Mailpit; open $mailpit and search for $stamp."
echo
{
    echo "| Run | The agent | Emails in the inbox |"
    echo "| --- | --- | --: |"
    run "straight to the tool" http://127.0.0.1:7118/mcp send_email ""
    run "through Ledger, answer lost on the way to the agent" http://127.0.0.1:7120/mcp email__send_email "email-demo-$stamp-a"
    run "through Ledger, answer lost on the way from the tool" http://127.0.0.1:7122/mcp email__send_email "email-demo-$stamp-b"
} | tee "$out/table.md"

# The inbox, as Mailpit shows it, when Chrome is installed to take the picture.
chrome=${CHROME:-"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"}
if [[ -x "$chrome" ]]; then
    "$chrome" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=2 \
        --window-size=1280,300 --virtual-time-budget=8000 \
        --screenshot="$PWD/$out/mailpit-inbox.png" "$mailpit/search?q=$stamp" >/dev/null 2>&1
    echo "The inbox: $out/mailpit-inbox.png"
fi
