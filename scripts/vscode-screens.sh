#!/usr/bin/env bash
# Records three demo sessions with the real gateway, tool servers and scripted model, then
# opens them in VS Code with the extension installed and captures the screenshots and the
# GIF in docs/images, cropped to the Ledger view and what it opens. The whole windows are
# kept in target/vscode-screens/originals.
#
#   scripts/vscode-screens.sh
#
# Needs VS Code: set CODE_APP to the .app if it is not in /Applications. It runs with a
# profile of its own under target/vscode-screens, so your settings and extensions are not
# touched, and it opens a window, so run it on a machine you are not using. The sessions go
# to a database of their own, ledger_screens, recreated on each run.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ${1:-} == record ]]; then
    # Inside scripts/with-stack.sh: the gateway on 7100 with the email server, the file
    # server and the scripted model. A second gateway on 7130 reaches the file server
    # through a proxy that loses the first answer (7132), and the agent reaches that
    # gateway once through a proxy that loses the first answer too (7131).
    dir=target/vscode-screens
    cat >"$dir/ledger-7130.toml" <<EOF
listen = "127.0.0.1:7130"
[[upstream]]
name = "email"
url = "http://127.0.0.1:7101/mcp"
[[upstream]]
name = "files"
url = "http://127.0.0.1:7132/mcp"
EOF
    pids=()
    trap 'kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true' EXIT
    target/debug/lossy-proxy --listen 127.0.0.1:7132 --target http://127.0.0.1:7102 >"$dir/proxy-7132.log" 2>&1 &
    pids+=($!)
    target/debug/ledger serve --config "$dir/ledger-7130.toml" >"$dir/ledger-7130.log" 2>&1 &
    pids+=($!)
    target/debug/lossy-proxy --listen 127.0.0.1:7131 --target http://127.0.0.1:7130 >"$dir/proxy-7131.log" 2>&1 &
    pids+=($!)
    for port in 7130 7131 7132; do
        until (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do sleep 0.1; done
    done

    node scripts/vscode-screens.mjs record --session retries-demo \
        --direct http://127.0.0.1:7130/mcp --lossy http://127.0.0.1:7131/mcp
    target/debug/sample-agent --session report-demo --model reporter-1 --report "All systems nominal."
    # Fork the run where the model decides what to send, onto a model that words the email
    # differently: everything before is answered from the log, and the new email is
    # turned down rather than sent a second time.
    fork=$(target/debug/ledger fork report-demo --at 5 --model reporter-2)
    target/debug/sample-agent --session "$fork" --model reporter-1 --report "All systems nominal."
    exit 0
fi

app=${CODE_APP:-/Applications/Visual Studio Code.app}
if [[ ! -x "$app/Contents/MacOS/Electron" ]]; then
    echo "VS Code is not at $app; set CODE_APP to it" >&2
    exit 1
fi
base=${DATABASE_URL:-postgres://ledger:ledger@127.0.0.1:55432/ledger}
docker compose up -d --wait >/dev/null
docker compose exec -T postgres psql -q -U ledger -d ledger \
    -c "DROP DATABASE IF EXISTS ledger_screens WITH (FORCE)" -c "CREATE DATABASE ledger_screens"
export DATABASE_URL="${base%/*}/ledger_screens"

dir=target/vscode-screens
rm -rf "$dir"
mkdir -p "$dir/profile/User" "$dir/extensions" "$dir/ledger" "$dir/shots"
make --no-print-directory vscode-package >/dev/null
scripts/with-stack.sh "$0" record

"$app/Contents/Resources/app/bin/code" --user-data-dir "$dir/profile" --extensions-dir "$dir/extensions" \
    --install-extension vscode/out/ledger.vsix >/dev/null
# A plain light window: no status bar, so nothing but the editor, the views and Ledger.
cat >"$dir/profile/User/settings.json" <<EOF
{
  "ledger.command": "$PWD/target/debug/ledger",
  "ledger.databaseUrl": "$DATABASE_URL",
  "workbench.colorTheme": "Default Light Modern",
  "workbench.startupEditor": "none",
  "workbench.statusBar.visible": false,
  "workbench.tips.enabled": false,
  "workbench.editor.enablePreview": false,
  "window.commandCenter": false,
  "chat.commandCenter.enabled": false,
  "editor.minimap.enabled": false,
  "editor.wordWrap": "bounded",
  "editor.wordWrapColumn": 72,
  "git.enabled": false,
  "git.openRepositoryInParentFolders": "never",
  "telemetry.telemetryLevel": "off",
  "update.mode": "none"
}
EOF
port=9339
"$app/Contents/MacOS/Electron" --remote-debugging-port=$port --user-data-dir "$dir/profile" \
    --extensions-dir "$dir/extensions" --disable-workspace-trust --skip-release-notes \
    --new-window "$dir/ledger" >"$dir/vscode.log" 2>&1 &
code_pid=$!
trap 'kill $code_pid 2>/dev/null || true' EXIT
for _ in $(seq 1 600); do
    curl -sf "http://127.0.0.1:$port/json/list" | grep -q workbench && break
    sleep 0.1
done
node scripts/vscode-screens.mjs drive --port $port --out "$dir/shots" --originals "$dir/originals"
# Only the cropped images go into the repository; the whole windows stay in target/.
mkdir -p docs/images
cp "$dir"/shots/* docs/images/
ls -la "$dir"/shots
