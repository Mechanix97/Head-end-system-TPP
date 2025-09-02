# Etapa 1: cargo chef (caching de dependencias)
FROM rust:1.88 as chef
WORKDIR /app
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Etapa 2: construir dependencias
FROM chef as planner
WORKDIR /app
COPY --from=chef /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Etapa 3: construir la app
FROM rust:1.88 as builder
WORKDIR /app
COPY . .
COPY --from=planner /app/target target
RUN cargo build --release

# Etapa final: imagen ligera
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/hes /app/hes
EXPOSE 8000
CMD ["./hes"]
