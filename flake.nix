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

      linuxSystems = [ "x86_64-linux" "aarch64-linux" ];
      forLinux = f: nixpkgs.lib.genAttrs linuxSystems (system: f {
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

      nixosModules.default = ./nix/module.nix;

      checks = forLinux ({ pkgs, system }: {
        # Boots a NixOS VM with the upstream module enabled against an
        # ephemeral postgres + a stubbed slskd. Verifies: migrator runs,
        # config.ini is rendered correctly, soularr-web responds.
        moduleVm = import ./nix/tests/module-vm.nix {
          inherit pkgs system;
          soularrModule = ./nix/module.nix;
          soularrSrc = ./.;
        };
      });
    };
}
