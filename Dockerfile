# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node.
#
# The crate has path dependencies on sibling Fiducia crates, so the build stage
# clones those siblings before compiling. This keeps the local path-dependency
# workflow intact while producing a self-contained image.
FROM rust:1.97.0-slim-bookworm@sha256:cfbb0e0ef7a73e736386bfa346f1cb0503c6d162969dc9426fb37834f3f64c25 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=543b4ea3b3bba28b66c15a97a27514488d2ccce3
ARG INTERFACES_REF=487e470c45ab5851e8f6f3b1dc048fe067fbf408
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

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:ce0d66bc0f64aae46e6a03add867b07f42cc7b8799c949c2e898057b7f75a151
COPY --from=build --chown=65532:65532 /build/fiducia-node.rs/target/release/fiducia-node /usr/local/bin/fiducia-node
EXPOSE 8090 9090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-node"]
