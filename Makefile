.PHONY: build run lint

build: ## Build the server
	cargo build

run: ## run the server
	cargo run -- --no-metrics --database=in-memory

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
