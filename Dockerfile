# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node.
#
# The crate has path dependencies on sibling Fiducia crates, so the build stage
# clones those siblings before compiling. This keeps the local path-dependency
# workflow intact while producing a self-contained image.
FROM rust:1-slim-bookworm AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=main
ARG INTERFACES_REF=main
RUN git clone --depth 1 --branch "$ROUTING_REF" \
    https://github.com/fiducia-cloud/fiducia-routing.rs.git fiducia-routing.rs
RUN git clone --depth 1 --branch "$INTERFACES_REF" \
    https://github.com/fiducia-cloud/fiducia-interfaces.git fiducia-interfaces
COPY . fiducia-node.rs
WORKDIR /build/fiducia-node.rs
RUN cargo build --release && strip target/release/fiducia-node

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build --chown=65532:65532 /build/fiducia-node.rs/target/release/fiducia-node /usr/local/bin/fiducia-node
EXPOSE 8090 9090
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fiducia-node"]
