{ pkgs, agentCheck }:
let
  shellPackages =
    (with pkgs; [
      age
      sops
      actionlint
      binutils
      cacert
      cargo-audit
      coreutils
      findutils
      gcc
      git
      gnumake
      jdk21_headless
      jq
      nixfmt
      nodejs_22
      pkg-config
      quint
      rsync
      rust-analyzer
      rustup
      shellcheck
      shfmt
    ])
    ++ [ agentCheck ];
in
pkgs.mkShell {
  packages = shellPackages;

  LANG = if pkgs.stdenv.hostPlatform.isDarwin then "en_US.UTF-8" else "C.UTF-8";
  LC_ALL = if pkgs.stdenv.hostPlatform.isDarwin then "en_US.UTF-8" else "C.UTF-8";
  RUST_BACKTRACE = "1";

  shellHook = ''

    # sops age key for env/enc/*.env.enc — see env/README.md
    if [ -z "''${SOPS_AGE_KEY_FILE:-}" ]; then
      for _k in "''${XDG_CONFIG_HOME:-$HOME/.config}/sops/age/keys.txt" \
                "$HOME/Library/Application Support/sops/age/keys.txt"; do
        if [ -f "$_k" ]; then export SOPS_AGE_KEY_FILE="$_k"; break; fi
      done
      unset _k
    fi
    export NIX_DEV_SHELL=fiducia-node
    export NIX_AGENT_CACHE_ROOT="''${NIX_AGENT_CACHE_ROOT:-$PWD/.cache/nix-agent}"
    export RUSTUP_HOME="''${RUSTUP_HOME:-$NIX_AGENT_CACHE_ROOT/rustup}"
    export CARGO_HOME="''${CARGO_HOME:-$NIX_AGENT_CACHE_ROOT/cargo}"
    export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$PWD/.cache/cargo-target}"
    mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR"
  '';
}
