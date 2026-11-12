#!/usr/bin/env bash
# Deploys Ledger to Azure with the example file tool server, measures it from this
# machine, restarts it mid-session, and keeps everything in results/azure/. Costs money:
# see the README's "Deploying to Azure". Delete it all afterwards with teardown.sh.
#
#   deploy/azure/demo.sh
#
# Uses LEDGER_RESOURCE_GROUP (default ledger-demo), LEDGER_NAME_PREFIX (default
# ledgerdemo) and LEDGER_TEMPLATE (default students, the variant that runs on Azure for
# Students). The database password is generated here and never written anywhere.
set -euo pipefail
cd "$(dirname "$0")/../.."

export LEDGER_RESOURCE_GROUP=${LEDGER_RESOURCE_GROUP:-ledger-demo}
export LEDGER_NAME_PREFIX=${LEDGER_NAME_PREFIX:-ledgerdemo}
export LEDGER_EXAMPLE_FILES=true
export LEDGER_TEMPLATE=${LEDGER_TEMPLATE:-students}
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

# Stop and start the app with a session open, five times. Stopping sends the copy SIGTERM:
# it drains and releases its lease, so the next copy answers the session at once instead
# of refusing it until the lease (30 s) expires. (Express environments cannot restart a
# revision; stop and start is the same SIGTERM.)
app="$LEDGER_NAME_PREFIX-ledger"
id=$(az containerapp show -g "$LEDGER_RESOURCE_GROUP" -n "$app" --query id -o tsv)
now() { python3 -c 'import time; print(time.time())'; }
restarts=$out/restarts.md
{
    echo "| Trial | Last answer before the stop to first answer after the start (s) | Stop took (s) | Start to first answer (s) | Refusals |"
    echo "| --: | --: | --: | --: | --: |"
} >"$restarts"
for trial in 1 2 3 4 5; do
    session="restart-$trial-$(date +%s)"
    target/release/remote wait --url "$url" --session "$session" --step 1 >/dev/null
    last=$(now)
    az rest --method post --url "https://management.azure.com$id/stop?api-version=2025-07-01" -o none
    until [[ $(az containerapp show -g "$LEDGER_RESOURCE_GROUP" -n "$app" --query properties.runningStatus -o tsv) == Stopped ]]; do
        sleep 1
    done
    stopped=$(now)
    az rest --method post --url "https://management.azure.com$id/start?api-version=2025-07-01" -o none
    answer=$(target/release/remote wait --url "$url" --session "$session" --step 2 --patience 600)
    done_at=$(now)
    refusals=$(sed -E 's/.* and ([0-9]+) failed.*/\1/' <<<"$answer")
    python3 -c "print('| $trial | %.1f | %.1f | %.1f | $refusals |' % ($done_at - $last, $stopped - $last, $done_at - $stopped))" |
        tee -a "$restarts"
    sleep 20
done

# Each copy's start and shutdown, from Log Analytics, once ingested.
sleep 120
workspace=$(grep '^Logs:' "$out/deploy.log" | awk '{print $NF}')
if [[ -n $workspace ]]; then
    customer=$(az monitor log-analytics workspace show -g "$LEDGER_RESOURCE_GROUP" -n "$workspace" --query customerId -o tsv)
    az monitor log-analytics query -w "$customer" -o json --analytics-query \
        "ContainerAppConsoleLogs_CL | where Log_s contains 'shutting' or Log_s contains 'gateway on' or Log_s contains 'still running' | project TimeGenerated, ContainerGroupId_g, Log_s | order by TimeGenerated asc" |
        jq -r '.[] | "\(.TimeGenerated)  copy \(.ContainerGroupId_g[0:8])  \(.Log_s | gsub("\u001b\\[[0-9;]*m"; "") | sub("^\\S+\\s+"; ""))"' \
            >"$out/copies.log"
fi
az resource list -g "$LEDGER_RESOURCE_GROUP" --query '[].{name: name, type: type, location: location}' \
    -o table >"$out/resources.log"

echo
cat "$timings"
echo "Everything is in $out. Delete it all with deploy/azure/teardown.sh."
