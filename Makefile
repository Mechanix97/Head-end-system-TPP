.PHONY: build lint

build: ## Build the server
	cargo build

lint: ## Linter check
	cargo clippy --all-targets --all-features -- -D warnings
