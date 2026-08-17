FROM rust:1.97.1-bookworm AS build

WORKDIR /src
COPY Cargo.toml rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 app
USER app
COPY --from=build /src/target/release/espg /usr/local/bin/espg
EXPOSE 3000
ENV PORT=3000
ENTRYPOINT ["espg"]
