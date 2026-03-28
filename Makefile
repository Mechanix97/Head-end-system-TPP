.PHONY: build run run-node-1 run-node-2 run-node-3 lint local-cli

build: ## Build the server
	cargo build

run: ## run the server (in-memory DB, no metrics, single-node)
	cargo run -- --disble-metrics --database=in-memory --disable-cluster

run-node-1: ## Run node 1 (seed node, postgres on localhost, metrics enabled)
	cargo run -- --config configs/node-1.yaml

run-node-2: ## Run node 2 (joins via node-1, postgres on 100.86.94.38)
	cargo run -- --config configs/node-2.yaml

run-node-3: ## Run node 3 (joins via node-1, separate ports, postgres on 100.86.94.38)
	cargo run -- --config configs/node-3.yaml

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

clean-docker: ## clean docker containers, networks, volumes and images
	docker compose down -v --remove-orphans
	docker rmi $(docker images -q headend_app) 2>/dev/null || true
