FROM rust:1.84-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/forevex /usr/local/bin/forevex
ENV RUST_LOG=info
EXPOSE 3000
ENTRYPOINT ["forevex"]
CMD ["serve"]
