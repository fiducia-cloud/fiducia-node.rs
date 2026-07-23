# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node.
#
# The crate has path dependencies on sibling Fiducia crates, so the build stage
# clones those siblings before compiling. This keeps the local path-dependency
# workflow intact while producing a self-contained image.
FROM rust:1.97.1-slim-bookworm@sha256:99e09cb2284e2ddbb73a995deee3e91783fd04d177602ccf6eab326d778ee777 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=543b4ea3b3bba28b66c15a97a27514488d2ccce3
ARG INTERFACES_REF=6e20a3f4df2e52b99a0ad6add83d4528262b5dbc
RUN git init fiducia-routing.rs \
    && cd fiducia-routing.rs \
    && git remote add origin https://github.com/fiducia-cloud/fiducia-routing.rs.git \
    && git fetch --depth 1 origin "$ROUTING_REF" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "$ROUTING_REF"
RUN git init fiducia-interfaces \
    && cd fiducia-interfaces \
    && git remote add origin https://github.com/fiducia-cloud/fiducia-interfaces.git \
    && git fetch --depth 1 origin "$INTERFACES_REF" \
    && git checkout --detach FETCH_HEAD \
    && test "$(git rev-parse HEAD)" = "$INTERFACES_REF"
COPY . fiducia-node.rs
WORKDIR /build/fiducia-node.rs
RUN cargo build --release --locked && strip target/release/fiducia-node

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa
COPY --from=build --chown=65532:65532 /build/fiducia-node.rs/target/release/fiducia-node /usr/local/bin/fiducia-node
EXPOSE 8090 9090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-node"]
