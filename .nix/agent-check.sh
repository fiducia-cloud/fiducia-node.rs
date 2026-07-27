#!/usr/bin/env bash
set -euo pipefail

export CI="${CI:-1}"
export NO_COLOR="${NO_COLOR:-1}"
export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

cache_root="${NIX_AGENT_CACHE_ROOT:-$repo_root/.cache/nix-agent}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-$cache_root/xdg}"
export RUSTUP_HOME="${RUSTUP_HOME:-$cache_root/rustup}"
export CARGO_HOME="${CARGO_HOME:-$cache_root/cargo}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$cache_root/cargo-target}"
mkdir -p "$XDG_CACHE_HOME" "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR"

git diff --check
nixfmt --check flake.nix .nix/flake.nix .nix/dev-shell.nix
shellcheck .nix/agent-check.sh
shfmt -d .nix/agent-check.sh

workflows=()
while IFS= read -r -d '' workflow; do
  workflows+=("$workflow")
done < <(find .github/workflows -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)
if ((${#workflows[@]} > 0)); then
  actionlint "${workflows[@]}"
fi

nix flake check --show-trace
nix flake check ./.nix --show-trace

rustup set profile minimal
rustup toolchain install 1.95.0 --profile minimal --component rustfmt --component clippy
export RUSTUP_TOOLCHAIN=1.95.0
rustc --version
cargo --version

routing_ref="c694bc5c58587bec12989a347e926c0040aacada"
interfaces_ref="2c5c806174e067fbe83ad48b724366323ba390a2"
workspace_root="$cache_root/workspaces/fiducia-node-${routing_ref:0:12}-${interfaces_ref:0:12}"
node_checkout="$workspace_root/fiducia-node.rs"

mkdir -p "$node_checkout"
rsync \
  --archive \
  --delete \
  --exclude '/.git/' \
  --exclude '/.cache/' \
  --exclude '/target/' \
  "$repo_root/" \
  "$node_checkout/"

ensure_checkout() {
  local destination="$1"
  local url="$2"
  local reference="$3"
  local actual

  mkdir -p "$destination"
  if [ ! -d "$destination/.git" ]; then
    git -C "$destination" init --quiet
    git -C "$destination" remote add origin "$url"
  else
    git -C "$destination" remote set-url origin "$url"
  fi

  git -C "$destination" fetch --depth 1 origin "$reference"
  git -C "$destination" switch --detach --force FETCH_HEAD
  actual="$(git -C "$destination" rev-parse HEAD)"
  if [ "$actual" != "$reference" ]; then
    printf 'expected %s at %s, found %s\n' "$reference" "$destination" "$actual" >&2
    return 1
  fi
}

ensure_checkout \
  "$workspace_root/fiducia-routing.rs" \
  "https://github.com/fiducia-cloud/fiducia-routing.rs.git" \
  "$routing_ref"
ensure_checkout \
  "$workspace_root/fiducia-interfaces" \
  "https://github.com/fiducia-cloud/fiducia-interfaces.git" \
  "$interfaces_ref"

cd "$node_checkout"
make -B -C vendor/flags-2-env all
vendor/flags-2-env/build/flags2env audit .cli-flags.toml
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
cargo audit
