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
COPY . .
COPY --from=planner /app/target target
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/hes /app/hes
EXPOSE 6464
EXPOSE 6565
CMD ["./hes"]
