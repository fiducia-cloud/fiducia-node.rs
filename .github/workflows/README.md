# .github/workflows — GitHub Actions pipelines

CI/CD workflows for fiducia-node. Every third-party action is referenced by its
full commit SHA.

- `ci.yml` — on push/PR: audit the CLI contract, format, run strict clippy,
  execute all locked tests, and scan advisories. It checks out the interface and
  routing siblings at the same immutable commits used by the container build,
  uses Rust 1.95.0, and installs cargo-audit 0.21.2 from its locked graph.
- `docker.yml` — on merge to `main`: build and push the non-root container image
  with immutable sibling refs, an SBOM, and maximum provenance attestations.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.

The image workflow publishes only the immutable commit-SHA tag. Kubeconfig
credentials and rollout logic belong only to `fiducia-monorepo`.

## Security baseline

Every executable workflow uses explicit least-privilege permissions, immutable
third-party action or container references, non-persisted checkout credentials,
concurrency control, and a job timeout. The main CI workflow validates this
directory with the digest-pinned actionlint container. Environment mutation is
forbidden unless this README documents a repository-specific platform exception.
