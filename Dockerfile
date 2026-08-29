# syntax=docker/dockerfile:1
# Multi-stage build for fiducia-node.
#
# The crate has path dependencies on sibling Fiducia crates, so the build stage
# clones those siblings before compiling. This keeps the local path-dependency
# workflow intact while producing a self-contained image.
FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates
WORKDIR /build
ARG ROUTING_REF=c694bc5c58587bec12989a347e926c0040aacada
ARG INTERFACES_REF=ee8fe09f846f5a776d156c0b0d0d15582c8bd539
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

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:9dac0a79194e45a7da0158a9c6da57b217585af0786db3845d1f0ec1a0dd182f
COPY --from=build --chown=65532:65532 /build/fiducia-node.rs/target/release/fiducia-node /usr/local/bin/fiducia-node
EXPOSE 8090 9090
USER 65532:65532
# --- sops: this final stage has no shell (distroless/scratch), so runtime
# decryption cannot run inside the container. Inject secrets HOST-SIDE at
# `docker run` instead — never at build, never as --build-arg:
#     just env-docker-run prod <image>        # decrypts env/enc/prod.env.enc
#                                             # and passes --env-file, no plaintext on disk
# or render a platform secret from the same ciphertext. See env/README.md.
ENTRYPOINT ["/usr/local/bin/fiducia-node"]
