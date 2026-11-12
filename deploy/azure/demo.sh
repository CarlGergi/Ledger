#!/usr/bin/env bash
# Deploys Ledger to Azure with the example file tool server, measures it from this
# machine, restarts it mid-session, and keeps everything in results/azure/. Costs money:
# see the README's "Deploying to Azure". Delete it all afterwards with teardown.sh.
#
#   deploy/azure/demo.sh
#
# Uses LEDGER_RESOURCE_GROUP (default ledger-demo) and LEDGER_NAME_PREFIX (default
# ledgerdemo). The database password is generated here and never written to results/.
set -euo pipefail
cd "$(dirname "$0")/../.."

export LEDGER_RESOURCE_GROUP=${LEDGER_RESOURCE_GROUP:-ledger-demo}
export LEDGER_NAME_PREFIX=${LEDGER_NAME_PREFIX:-ledgerdemo}
export LEDGER_EXAMPLE_FILES=true
out=results/azure
mkdir -p "$out"

empty=$(mktemp)
trap 'rm -f "$empty"' EXIT
echo '[]' >"$empty"
export LEDGER_UPSTREAMS=$empty
LEDGER_POSTGRES_PASSWORD="L$(openssl rand -hex 16)!"
export LEDGER_POSTGRES_PASSWORD

cargo build --quiet --release -p ledger-bench --bin remote

stamp() { date -u +%Y-%m-%dT%H:%M:%SZ; }
timings=$out/timings.md
echo "| Step | Started (UTC) | Seconds |" >"$timings"
echo "| --- | --- | --: |" >>"$timings"
# Runs a step, logging its output to $out/<name>.log and its time to timings.md.
step() {
    local name=$1
    shift
    local started
    started=$(stamp)
    local t0=$SECONDS
    echo "== $name"
    "$@" 2>&1 | tee "$out/$name.log"
    echo "| $name | $started | $((SECONDS - t0)) |" >>"$timings"
}

az account show --query '{subscription: name, state: state}' -o table | tee "$out/subscription.log"

step deploy deploy/azure/deploy.sh deploy
url=$(grep '^MCP endpoint:' "$out/deploy.log" | awk '{print $3}')
base=${url%/mcp}
echo "$url" >"$out/endpoint.txt"

ready() {
    for _ in $(seq 1 600); do
        [[ $(curl -s -o /dev/null -w '%{http_code}' "$base/readyz") == 200 ]] && return 0
        sleep 1
    done
    echo "never ready" >&2
    return 1
}
step ready ready

step measure target/release/remote measure --url "$url" --calls 200

# Restart with a session open: the old copy drains and releases its lease on SIGTERM, so
# the session's next call does not wait for the lease to expire.
session="restart-$(date +%s)"
target/release/remote wait --url "$url" --session "$session" --step 1 | tee "$out/before-restart.log"
app="$LEDGER_NAME_PREFIX-ledger"
revision=$(az containerapp revision list -g "$LEDGER_RESOURCE_GROUP" -n "$app" --query '[0].name' -o tsv)
step restart az containerapp revision restart -g "$LEDGER_RESOURCE_GROUP" -n "$app" --revision "$revision"
step after-restart target/release/remote wait --url "$url" --session "$session" --step 2

# What the copies logged, and what Application Insights received.
sleep 120 # ingestion delay before telemetry can be queried
az containerapp logs show -g "$LEDGER_RESOURCE_GROUP" -n "$app" --container ledger --tail 300 \
    --format text >"$out/container.log" 2>&1 || true
insights=$(grep '^Traces:' "$out/deploy.log" | awk '{print $NF}')
az monitor app-insights query -g "$LEDGER_RESOURCE_GROUP" --app "$insights" --analytics-query \
    'union requests, dependencies, traces | summarize count() by itemType, name | order by count_ desc | take 30' \
    -o json >"$out/insights.json" 2>&1 || true
az resource list -g "$LEDGER_RESOURCE_GROUP" --query '[].{name: name, type: type, location: location}' \
    -o table >"$out/resources.log"

echo
cat "$timings"
echo "Everything is in $out. Delete it all with deploy/azure/teardown.sh."
