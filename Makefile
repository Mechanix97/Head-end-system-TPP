.PHONY: build run lint

build: ## Build the server
	cargo build

run: ## run the server
	cargo run

clean: ## clean the targets
	cargo clean

lint: ## Linter check
	cargo clippy --all-targets --all-features -- -D warnings

run-prometheus:
	docker run -d -p 9090:9090 --name prometheus -v ${PWD}\metrics\prometheus.yml:/etc/prometheus/prometheus.yml prom/prometheus
