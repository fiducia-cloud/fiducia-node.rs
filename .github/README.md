# .github — repository automation

GitHub-native configuration for this repo: CI/CD workflows (in `workflows/`)
and `dependabot.yml`, which opens weekly dependency-update PRs for Cargo crates
GitHub Actions, and Docker base images. Actions and sibling Fiducia repositories
are pinned to full commit SHAs in workflows; Dependabot is the review path for
intentional updates. These files are consumed by GitHub, not by the node at
runtime.
