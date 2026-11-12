#!/usr/bin/env bash
# Records three runs of the model-driven demo agent on reporter-1, then replays all three
# on each scripted model with `ledger regress`. Run it with the stack up, as `make regress`
# does: scripts/with-stack.sh scripts/regress.sh
set -euo pipefail
cd "$(dirname "$0")/.."

stamp=$(date +%s)
reports=("All systems nominal." "Two warnings, both resolved." "Disk at 91 percent.")
suite=var/regress-suite.toml

cat >"$suite" <<'EOF'
model = "reporter-1"
agent = ["target/debug/sample-agent", "--session", "{session}", "--model", "reporter-1"]
EOF

for i in 0 1 2; do
    session="regress-$stamp-$i"
    target/debug/sample-agent --session "$session" --model reporter-1 --report "${reports[$i]}" >/dev/null
    printf '\n[[run]]\nsession = "%s"\nargs = ["--report", "%s"]\n' "$session" "${reports[$i]}" >>"$suite"
done

for model in reporter-1 reporter-2 reporter-3; do
    echo "== replayed on $model"
    target/debug/ledger regress "$suite" --model "$model"
    echo
done
