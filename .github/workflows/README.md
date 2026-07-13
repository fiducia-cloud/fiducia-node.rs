# .github/workflows — GitHub Actions pipelines

CI/CD workflows for fiducia-node. Every third-party action is referenced by its
full commit SHA.

- `ci.yml` — on push/PR: audit the CLI contract, format, run strict clippy,
  execute all locked tests, and scan advisories. It checks out the interface and
  routing siblings at the same immutable commits used by the container build,
  uses Rust 1.95.0, and installs cargo-audit 0.21.2 from its locked graph.
- `docker.yml` — on merge to `main`: build and push the non-root container image
  with immutable sibling refs, an SBOM, and maximum provenance attestations.
- `deploy-test.yml` — fail-closed deploy to the `fiducia-test` namespace. It
  requires a nonblank base64-encoded `KUBE_CONFIG_TEST`, installs it mode 0600,
  updates the immutable commit-tagged image, and waits for the deployment
  rollout to succeed. Missing credentials, invalid kubeconfig data, update
  errors, and rollout failures all fail the job.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.
