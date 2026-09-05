FROM rust:1.88 AS chef
WORKDIR /app
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS planner
WORKDIR /app
COPY --from=chef /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

FROM rust:1.88 AS builder
WORKDIR /app
# Optional cargo features (e.g. debug-session-start). Empty by default so the
# plain `docker compose build` keeps producing the same binary as before.
ARG CARGO_FEATURES=""
COPY . .
COPY --from=planner /app/target target
RUN if [ -n "$CARGO_FEATURES" ]; then cargo build --release --features "$CARGO_FEATURES"; else cargo build --release; fi

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/hes /app/hes
EXPOSE 6464
EXPOSE 6565
EXPOSE 6600
CMD ["./hes"]
