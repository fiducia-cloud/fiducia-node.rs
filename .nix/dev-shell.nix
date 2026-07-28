{ pkgs, agentCheck }:
let
  shellPackages =
    (with pkgs; [
      actionlint
      binutils
      cacert
      cargo-audit
      findutils
      gcc
      git
      gnumake
      jq
      nixfmt-rfc-style
      pkg-config
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
    export NIX_DEV_SHELL=fiducia-node
    export NIX_AGENT_CACHE_ROOT="''${NIX_AGENT_CACHE_ROOT:-$PWD/.cache/nix-agent}"
    export RUSTUP_HOME="''${RUSTUP_HOME:-$NIX_AGENT_CACHE_ROOT/rustup}"
    export CARGO_HOME="''${CARGO_HOME:-$NIX_AGENT_CACHE_ROOT/cargo}"
    export CARGO_TARGET_DIR="''${CARGO_TARGET_DIR:-$PWD/.cache/cargo-target}"
    mkdir -p "$RUSTUP_HOME" "$CARGO_HOME" "$CARGO_TARGET_DIR"
  '';
}
