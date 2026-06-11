# Multi-stage build: compile statically against the workspace, ship a slim
# runtime image containing only the kalpakdb binary.

FROM rust:1.96-slim-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p kalpakdb

FROM debian:bookworm-slim
RUN useradd --system --home /var/lib/kalpakdb kalpakdb \
    && mkdir -p /var/lib/kalpakdb \
    && chown kalpakdb:kalpakdb /var/lib/kalpakdb
COPY --from=builder /build/target/release/kalpakdb /usr/local/bin/kalpakdb
USER kalpakdb
VOLUME ["/var/lib/kalpakdb"]
EXPOSE 7411
ENTRYPOINT ["kalpakdb"]
CMD ["serve", "/var/lib/kalpakdb", "--addr", "0.0.0.0:7411"]
