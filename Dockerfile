# The gateway, as a container. The migrations are compiled into the binary.
#
#   docker build -t ledger .
#   docker run -v ./ledger.toml:/etc/ledger/ledger.toml -e DATABASE_URL=... -p 7100:7100 ledger
#
# The config must listen on 0.0.0.0, and name the host clients use in allowed_hosts.
FROM rust:1.99-slim-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p ledger-gateway --bin ledger \
    && cargo build --release --locked -p ledger-tools --bin file-server

FROM debian:trixie-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home ledger
COPY --from=build /src/target/release/ledger /usr/local/bin/ledger
# The example file tool server, for trying a deployment with no tool servers of its own.
COPY --from=build /src/target/release/file-server /usr/local/bin/file-server
USER ledger
EXPOSE 7100
ENTRYPOINT ["ledger", "serve", "--config", "/etc/ledger/ledger.toml"]
