# Back-compat shim — delegates to nix/shell.nix so there's one definition.
# Prefer `nix develop` (uses the flake-pinned nixpkgs); `nix-shell` still works
# but uses whatever <nixpkgs> resolves to in your channel.
import ./nix/shell.nix { pkgs = import <nixpkgs> {}; }
