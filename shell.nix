# Back-compat shim — delegates to nix/shell.nix so there's one definition.
# No explicit pkgs: nix/shell.nix's default resolves the flake-locked
# nixpkgs (via a flake.lock-reading fetchTarball), so plain `nix-shell`
# and `nix develop` consume the same pinned rev — never the host channel.
import ./nix/shell.nix {}
