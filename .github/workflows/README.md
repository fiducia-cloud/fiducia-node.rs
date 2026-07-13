# .github/workflows — GitHub Actions pipelines

CI/CD workflows for fiducia-node:

- `ci.yml` — on push/PR: build, rustfmt, clippy, and test the crate.
- `docker.yml` — on merge to `main`: build and push the container image.
- `deploy-test.yml` — secret-gated deploy to the `fiducia-test` namespace;
  a no-op (validation only) when `KUBE_CONFIG_TEST` is absent.
- `cli-flags.yml` — audits `.cli-flags.toml` with the pinned `flags2env`
  submodule whenever the CLI flag schema, scripts, or submodule change.
