{
  description = "Compatibility flake for the fiducia-node agent environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs =
    { self, nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      pkgsFor = system: import nixpkgs { inherit system; };
    in
    {
      formatter = forAllSystems (system: (pkgsFor system).nixfmt-rfc-style);

      packages = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
          agentCheck = pkgs.writeShellApplication {
            name = "agent-check";
            runtimeInputs = with pkgs; [
              actionlint
              binutils
              cacert
              cargo-audit
              findutils
              gcc
              git
              gnumake
              nix
              nixfmt-rfc-style
              rsync
              rustup
              shellcheck
              shfmt
            ];
            text = builtins.readFile ./agent-check.sh;
          };
        in
        {
          inherit agentCheck;
          default = agentCheck;
        }
      );

      apps = forAllSystems (system: {
        "agent-check" = {
          type = "app";
          program = "${self.packages.${system}.agentCheck}/bin/agent-check";
        };
        default = self.apps.${system}."agent-check";
      });

      checks = forAllSystems (system: {
        agentCheck = self.packages.${system}.agentCheck;
      });

      devShells = forAllSystems (
        system:
        let
          pkgs = pkgsFor system;
        in
        {
          default = import ./dev-shell.nix {
            inherit pkgs;
            agentCheck = self.packages.${system}.agentCheck;
          };
        }
      );
    };
}
