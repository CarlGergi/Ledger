#!/usr/bin/env bash
# Deploys Ledger to Azure Container Apps, with Azure Database for PostgreSQL and
# Application Insights. Nothing here runs unless you run it, signed in with `az login`.
#
#   deploy/azure/deploy.sh preview   # compile, and show what would change; creates nothing
#   deploy/azure/deploy.sh deploy    # build the image in Azure and deploy
#
# Settings, from the environment:
#   LEDGER_RESOURCE_GROUP   resource group to create or use (default: ledger)
#   LEDGER_LOCATION         Azure region (default: canadacentral)
#   LEDGER_NAME_PREFIX      3 to 16 lowercase letters and digits (default: ledger)
#   LEDGER_UPSTREAMS        file with the tool servers, JSON: [{"name": ..., "url": ...}]
#   LEDGER_MODEL_URL        OpenAI-compatible base URL to proxy, optional
#   LEDGER_POSTGRES_PASSWORD  the database password; never stored by this script
set -euo pipefail
cd "$(dirname "$0")/../.."

mode=${1:-}
if [[ $mode != preview && $mode != deploy ]]; then
    sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
fi

group=${LEDGER_RESOURCE_GROUP:-ledger}
location=${LEDGER_LOCATION:-canadacentral}
prefix=${LEDGER_NAME_PREFIX:-ledger}
upstreams=${LEDGER_UPSTREAMS:?set LEDGER_UPSTREAMS to a JSON file of tool servers}
: "${LEDGER_POSTGRES_PASSWORD:?set LEDGER_POSTGRES_PASSWORD}"
model_url=${LEDGER_MODEL_URL:-}

command -v az >/dev/null || { echo "install the Azure CLI first" >&2; exit 1; }

# The password goes to az in a file only you can read, not on a command line any process
# can see, and the file is removed on exit.
secrets=$(mktemp)
trap 'rm -f "$secrets"' EXIT
chmod 600 "$secrets"
jq -n '{postgresPassword: {value: env.LEDGER_POSTGRES_PASSWORD}}' >"$secrets"
az account show --query '{subscription: name, user: user.name}' -o table \
    || { echo "sign in with az login first" >&2; exit 1; }

echo "Resource group $group in $location, prefix $prefix."
az bicep build --file deploy/azure/registry.bicep --stdout >/dev/null
az bicep build --file deploy/azure/main.bicep --stdout >/dev/null
echo "Both templates compile."

if [[ $mode == preview ]]; then
    if [[ $(az group exists --name "$group") != true ]]; then
        echo "$group does not exist: deploy would create it, a registry, and everything in main.bicep."
        exit 0
    fi
    az deployment group what-if --resource-group "$group" \
        --template-file deploy/azure/registry.bicep --parameters namePrefix="$prefix"
    registry_name=$(az acr list --resource-group "$group" --query "[?starts_with(name, '${prefix}acr')].name | [0]" -o tsv)
    if [[ -z $registry_name ]]; then
        echo "No registry yet: deploy would create it, then everything in main.bicep."
        exit 0
    fi
    az deployment group what-if --resource-group "$group" \
        --template-file deploy/azure/main.bicep \
        --parameters namePrefix="$prefix" registryName="$registry_name" \
        image="$(az acr show --name "$registry_name" --query loginServer -o tsv)/ledger:$(git rev-parse --short HEAD)" \
        upstreams=@"$upstreams" modelUrl="$model_url" \
        --parameters @"$secrets"
    exit 0
fi

az group create --name "$group" --location "$location" -o none

registry=$(az deployment group create --resource-group "$group" \
    --template-file deploy/azure/registry.bicep \
    --parameters namePrefix="$prefix" \
    --query properties.outputs -o json)
registry_name=$(jq -r .name.value <<<"$registry")
login_server=$(jq -r .loginServer.value <<<"$registry")
image="$login_server/ledger:$(git rev-parse --short HEAD)"

params=(
    --resource-group "$group"
    --template-file deploy/azure/main.bicep
    --parameters namePrefix="$prefix" registryName="$registry_name" image="$image"
    upstreams=@"$upstreams" modelUrl="$model_url"
    --parameters @"$secrets"
)

echo "Building $image in Azure."
az acr build --registry "$registry_name" --image "${image#*/}" .

outputs=$(az deployment group create "${params[@]}" --query properties.outputs -o json)
echo
echo "MCP endpoint:   $(jq -r .mcpUrl.value <<<"$outputs")"
echo "Model endpoint: $(jq -r .modelUrl.value <<<"$outputs")"
echo "Traces:         Application Insights $(jq -r .insightsName.value <<<"$outputs")"
