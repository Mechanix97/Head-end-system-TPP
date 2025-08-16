.PHONY: build run lint

build: ## Build the server
	cargo build

run: ## run the server
	cargo run

lint: ## Linter check
	cargo clippy --all-targets --all-features -- -D warnings
