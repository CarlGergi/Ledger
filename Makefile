.PHONY: help db-up db-down db-reset lint test breakage interop demo stack sessions log reproduce clean

help:
	@echo "make db-up      start PostgreSQL"
	@echo "make db-down    stop PostgreSQL"
	@echo "make db-reset   delete all recorded sessions and test leftovers"
	@echo "make lint       check formatting and run clippy"
	@echo "make test       run every test"
	@echo "make breakage   break each guarantee and check that a test notices"
	@echo "make interop    drive the gateway with the official TypeScript MCP client"
	@echo "make demo       run the sample agent and print its log"
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

test: db-up
	cargo test --workspace

breakage: db-up
	python3 scripts/breakage.py

interop: db-up
	npm ci --silent --prefix interop/typescript
	scripts/with-stack.sh node interop/typescript/client.mjs

demo: db-up
	scripts/with-stack.sh sh -c 'session=demo-$$(date +%s); \
		target/debug/sample-agent --session $$session && target/debug/ledger log $$session'

stack: db-up
	scripts/with-stack.sh sh -c 'echo "gateway on http://127.0.0.1:7100/mcp; Ctrl-C to stop"; while :; do sleep 3600; done'

sessions:
	cargo run --quiet --package ledger-gateway --bin ledger -- sessions

log:
	cargo run --quiet --package ledger-gateway --bin ledger -- log $(SESSION)

reproduce: lint test breakage interop

clean:
	cargo clean
	rm -rf var interop/typescript/node_modules
