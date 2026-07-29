# Nix development contract

The root flake is the canonical entrypoint, while `.nix/flake.nix` remains as a compatibility entrypoint for older local workflows:

```sh
nix develop
nix develop -c agent-check
nix run .#agent-check
nix flake check --show-trace

# Backward compatible:
nix develop ./.nix
```

## Standalone-clone behavior

`Cargo.toml` intentionally uses sibling path dependencies. A plain clone of this repository therefore cannot compile unless `fiducia-routing.rs` and `fiducia-interfaces` exist beside it.

`agent-check` solves that without mutating the checkout. It creates an ignored workspace under `.cache/nix-agent/workspaces/`, copies the current repository into it, and fetches the exact sibling commits already pinned by the Dockerfile:

- `fiducia-routing.rs`: `c694bc5c58587bec12989a347e926c0040aacada`
- `fiducia-interfaces`: `bd718cd72d72aa330534f3688f8fb1ce90c19d10`

The `vendor/flags-2-env` helper is fetched at the exact commit recorded by the repository's stage-0 gitlink. The agent script reads that pin from the Git index, so updating the submodule does not require duplicating its SHA in Nix code.

It then runs the CLI flag contract, formatting, Clippy with warnings denied, all
tests, and `cargo audit` using Rust 1.95.0 from `rust-toolchain.toml`. The same
locked shell supplies Quint 0.32.0, Java 21, and Node.js for the union-lock
formal model. Rustup, Cargo, build outputs, and other caches stay below
`.cache/` unless explicitly overridden.

Diagnostic subcommands are available for `preflight`, `rust`, `bootstrap`,
`flags`, `fmt`, `clippy`, `test`, and `audit`. Formal verification uses
`formal`, `formal-typecheck`, `formal-test`, `formal-simulate`, `formal-mbt`,
`formal-verify`, `formal-verify-deep`, and `formal-refinement`. The default
no-argument command runs the complete non-formal repository contract.
Rust test concurrency defaults to four workers so the same command remains
stable on developer machines with lower per-process resource limits; set
`RUST_TEST_THREADS` explicitly to override it. On macOS, Rust final links use
the platform Clang driver so panic unwinding behaves correctly; compilers,
headers, libraries, and all repository tools still come from the locked Nix
shell.

## Toolchain drift

The repository and existing CI pin Rust 1.95.0. The current Docker builder pins Rust 1.97.1. This Nix baseline follows the repository toolchain and records the Docker mismatch rather than silently choosing a third contract. A follow-up change should align the Docker builder only after release and dependency compatibility are verified.

## Docker and OCI policy

The existing Dockerfile remains authoritative. It already uses a digest-pinned multi-stage builder and a digest-pinned, non-root distroless runtime. Do not replace it with a Nix-built image until an OCI candidate demonstrates parity for:

- the release binary and dynamic-library closure;
- UID/GID 65532 and filesystem permissions;
- CA certificates, ports 8090/9090, and the `fiducia-node` entrypoint;
- image size, layer composition, startup, health, and shutdown behavior;
- SBOM, provenance, signature, and vulnerability results.

A future `packages.<system>.oci` may be added with `dockerTools.buildLayeredImage`, but it must be tested alongside the Dockerfile before becoming a deployment source.
