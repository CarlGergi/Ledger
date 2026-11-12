#!/usr/bin/env bash
# Deletes the demo's resource group and everything in it, waits until it is gone, and
# checks that nothing it made is left in the subscription.
#
#   deploy/azure/teardown.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
group=${LEDGER_RESOURCE_GROUP:-ledger-demo}
out=results/azure
mkdir -p "$out"

{
    echo "Deleting $group at $(date -u +%Y-%m-%dT%H:%M:%SZ)."
    t0=$SECONDS
    if [[ $(az group exists --name "$group") == true ]]; then
        az group delete --name "$group" --yes
    fi
    echo "Deleted in $((SECONDS - t0)) s."
    echo
    echo "az group exists --name $group: $(az group exists --name "$group")"
    echo
    echo "Resource groups left in the subscription:"
    az group list --query '[].{name: name, location: location}' -o table
    echo
    echo "Resources left in the subscription:"
    az resource list --query '[].{name: name, type: type, group: resourceGroup}' -o table
    echo "(end of list)"
} 2>&1 | tee "$out/teardown.log"
