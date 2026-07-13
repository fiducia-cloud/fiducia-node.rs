# .github/workflows — GitHub Actions pipelines

CI/CD workflows for fiducia-node. Every third-party action is referenced by its
full commit SHA.

- `ci.yml` — on push/PR: audit the CLI contract, format, run strict clippy,
  execute all locked tests, and scan advisories. It checks out the interface and
  routing siblings at the same immutable commits used by the container build,
  uses Rust 1.95.0, and installs cargo-audit 0.21.2 from its locked graph.
- `docker.yml` — on merge to `main`: build and push the non-root container image
  with immutable sibling refs, an SBOM, and maximum provenance attestations.
- `deploy-test.yml` — secret-gated deploy to the `fiducia-test` namespace;
  a no-op (validation only) when `KUBE_CONFIG_TEST` is absent, but a configured
  deployment fails the job if `kubectl` cannot update the target.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.
