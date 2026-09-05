.PHONY: build run run-debug run-node-1 run-node-2 run-node-3 lint local-cli run-presentation logs-presentation stop-presentation

PRESENTATION := -p hes-presentation -f docker-compose.presentation.yaml

build: ## Build the server
	cargo build

run: ## run the server (in-memory DB, no metrics, single-node, no RPC, test mode: connections a few minutes ahead)
	cargo run --features debug-session-start -- --disble-metrics --database=in-memory --disable-cluster --disable-rpc --test-mode

run-debug: ## Run with debug-level logging for communication (backdoor + codec). Use RUST_LOG to override.
	RUST_LOG=info,backdoor=debug,common=debug cargo run --features debug-session-start -- --disble-metrics --database=in-memory --disable-cluster --disable-rpc --test-mode

run-node-1: ## Run node 1 (seed node, postgres on localhost, metrics enabled)
	cargo run -- --config configs/node-1.yaml

run-node-2: ## Run node 2 (joins via node-1, postgres on 100.86.94.38)
	cargo run -- --config configs/node-2.yaml

run-node-3: ## Run node 3 (joins via node-1, separate ports, postgres on 100.86.94.38)
	cargo run -- --config configs/node-3.yaml

mock-device: ## Run a mock device that registers and handles periodic sessions (use with make run)
	cargo run -p mock_device -- --backdoor-ip 127.0.0.1

local-cli: ## Connect to local HES via interactive CLI (127.0.0.1:6600)
	cargo run -p hes-cli

clean: ## clean the targets
	cargo clean

lint: ## Linter check
	cargo clippy --all-targets --all-features -- -D warnings

test: ## run tests
	cargo test --all

run-docker-metrics: ## run server in docker with metrics
	docker compose build
	docker compose up -d

stop-docker-metrics: ## stop docker with metrics
	docker compose stop

run-presentation: ## demo stack in docker: single node, in-memory DB, metrics, test mode, no RPC
	docker compose $(PRESENTATION) build
	docker compose $(PRESENTATION) up -d
	@echo ""
	@echo "HES listo: backdoor udp/6565, metricas :6464, prometheus :9090, grafana :6969 (admin/admin)"
	@echo "Logs: make logs-presentation | Parar: make stop-presentation"

logs-presentation: ## follow the presentation stack logs
	docker compose $(PRESENTATION) logs -f app

stop-presentation: ## stop and remove the presentation stack
	docker compose $(PRESENTATION) down --remove-orphans

clean-docker: ## clean docker containers, networks, volumes and images
	docker compose down -v --remove-orphans
	docker rmi $(docker images -q headend_app) 2>/dev/null || true
