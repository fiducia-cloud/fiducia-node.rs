# .nix — reproducible dev environment

The Nix flake that defines a pinned, reproducible development shell (Rust
toolchain and tooling) for the repo. `flake.nix` declares the environment and
`flake.lock` pins its inputs. The repo-root `./shell` wrapper and `.envrc`
(direnv) both enter this shell via `nix develop ./.nix`, so contributors get an
identical toolchain without polluting their host system.
