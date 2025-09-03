.PHONY: build run lint

build: ## Build the server
	cargo build

run: ## run the server
	cargo run

clean: ## clean the targets
	cargo clean

lint: ## Linter check
	cargo clippy --all-targets --all-features -- -D warnings

run-docker-metrics: ## run server in docker with metrics
	docker compose build
	docker compose up -d

stop-docker-metrics: ## stop docker with metrics
	docker compose down
