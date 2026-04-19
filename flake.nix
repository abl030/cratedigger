{
  description = "Cratedigger / Soularr — quality-obsessed music acquisition pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
      });
    in {
      packages = forAllSystems ({ pkgs, ... }: {
        slskd-api = pkgs.callPackage ./nix/slskd-api.nix { };
      });

      devShells = forAllSystems ({ pkgs, ... }: {
        default = import ./nix/shell.nix { inherit pkgs; };
      });
    };
}
