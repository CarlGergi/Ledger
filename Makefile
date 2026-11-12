.PHONY: help db-up db-down db-reset lint test breakage simulate interop regress vscode vscode-package soak demo demo-model demo-agentmail agentmail-local demo-email bench-duplicates bench-overhead bench-takeover stack sessions log reproduce clean

help:
	@echo "make db-up      start PostgreSQL"
	@echo "make db-down    stop PostgreSQL"
	@echo "make db-reset   delete all recorded sessions and test leftovers"
	@echo "make lint       check formatting and run clippy"
	@echo "make test       run every test"
	@echo "make breakage   break each guarantee and check that a test notices"
	@echo "make simulate   run 10,000 seeds of the deterministic simulation"
	@echo "make interop    drive the gateway with the official TypeScript MCP client"
	@echo "make vscode     type-check and test the VS Code extension"
	@echo "make vscode-package  build vscode/out/ledger.vsix to install"
	@echo "make regress    replay three recorded runs on each scripted model and report changes"
	@echo "make bench-duplicates  count duplicate side effects with and without Ledger"
	@echo "make bench-overhead    latency and throughput Ledger adds; only on a quiet machine"
	@echo "make bench-takeover    how long a session waits after its copy is killed or stopped, about 7 minutes"
	@echo "make soak       repeat the test suite under load, about 30 minutes; not part of reproduce"
	@echo "make demo       run the sample agent and print its log"
	@echo "make demo-email  one email, a lost answer, a retry: two in Mailpit's inbox, then one with Ledger"
	@echo "make demo-agentmail  the same through AgentMail, or its local stand-in"
	@echo "make stack      run the gateway and tool servers until interrupted"
	@echo "make sessions   list recorded sessions"
	@echo "make log SESSION=<id>  print a session's events"
	@echo "make reproduce  run all of the above checks"

db-up:
	docker compose up -d --wait

db-down:
	docker compose down

db-reset:
	docker compose down --volumes
	docker compose up -d --wait

lint:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets -- -D warnings
	python3 scripts/breakage.py --check

test: db-up
	cargo test --workspace

breakage: db-up
	python3 scripts/breakage.py

simulate:
	cargo run --quiet --release -p ledger-sim -- --seeds 10000

vscode:
	npm ci --silent --prefix vscode
	npm test --prefix vscode

vscode-package:
	npm ci --silent --prefix vscode
	npm run --silent package --prefix vscode

interop: db-up
	npm ci --silent --prefix interop/typescript
	scripts/with-stack.sh node interop/typescript/client.mjs

soak: db-up
	python3 scripts/soak.py

demo: db-up
	scripts/with-stack.sh sh -c 'session=demo-$$(date +%s); \
		target/debug/sample-agent --session $$session && target/debug/ledger log $$session'

regress: db-up
	scripts/with-stack.sh scripts/regress.sh

# The same task, decided by the scripted model reporter-1 through the gateway's model proxy.
demo-model: db-up
	scripts/with-stack.sh sh -c 'session=demo-$$(date +%s); \
		target/debug/sample-agent --session $$session --model reporter-1 >/dev/null && target/debug/ledger log $$session'

demo-email: db-up
	scripts/email-demo.sh

demo-agentmail: db-up
	scripts/agentmail-demo.sh

# The same, always against the local stand-in for AgentMail, so it sends no real email.
agentmail-local: db-up
	scripts/agentmail-demo.sh --local

bench-duplicates: db-up
	for loss in 0.1 0.3; do \
		cargo run --quiet --release -p ledger-bench --bin duplicates -- --calls 1000 --loss $$loss --seed 1 || exit 1; \
		echo; done

# Timings mislead on a busy machine; the binary refuses to measure without the flag.
bench-takeover: db-up
	cargo build --quiet --release -p ledger-gateway -p ledger-bench
	target/release/takeover

bench-overhead: db-up
	cargo run --quiet --release -p ledger-bench --bin overhead -- --machine-is-quiet

stack: db-up
	scripts/with-stack.sh sh -c 'echo "gateway on http://127.0.0.1:7100/mcp; Ctrl-C to stop"; while :; do sleep 3600; done'

sessions:
	cargo run --quiet --package ledger-gateway --bin ledger -- sessions

log:
	cargo run --quiet --package ledger-gateway --bin ledger -- log $(SESSION)

reproduce: lint test breakage simulate interop regress demo-email agentmail-local bench-duplicates bench-takeover vscode

clean:
	cargo clean
	rm -rf var interop/typescript/node_modules vscode/node_modules vscode/out
