# Plain `nix-shell` resolves the flake-locked nixpkgs, NOT the host's
# <nixpkgs> channel. One pinned rev everywhere (dev shell, flake checks,
# module) is the tier-2 packaging invariant: the real-beets contract test
# only guards drift if it runs against the beets production will run.
# `nix develop` gets the same pin via flake.nix; this shim covers the
# `nix-shell --run "..."` path every rule and script uses. The fetched
# tarball is GC-rooted from the shellHook (.nixpkgs-src) so
# nix-collect-garbage doesn't force a re-download.
{ pkgs ? import (
    let
      lock = builtins.fromJSON (builtins.readFile ../flake.lock);
      node = lock.nodes.${lock.nodes.${lock.root}.inputs.nixpkgs}.locked;
    in
    builtins.fetchTarball {
      url = "https://github.com/${node.owner}/${node.repo}/archive/${node.rev}.tar.gz";
      sha256 = node.narHash;
    }
  ) {} }:

let
  cratedigger = import ./package.nix { inherit pkgs; };

  # Dev env: production deps + dev-only tooling. ``ps.beets`` was
  # previously listed again here because the production env excluded
  # it; now that ``lib.beets_distance`` makes beets a first-class
  # library dependency it's already in ``pythonPackages``, so the
  # dev shell inherits it without duplication.
  #
  # Dev-only additions:
  #   - vulture: static dead-code finder (scripts/find_dead_code.sh)
  testPythonEnv = pkgs.python3.withPackages (ps:
    cratedigger.pythonPackages ps
    ++ [ ps.vulture ]
  );
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql          # initdb, pg_ctl for ephemeral test DB
    testPythonEnv
    pkgs.sox                 # spectral analysis tests
    pkgs.ffmpeg              # ffprobe for bitrate measurement in quality tests
    pkgs.yt-dlp              # YouTube-rescue ingest worker invokes this binary;
                             # dev-shell tests resolve it via `shutil.which("yt-dlp")`
  ];

  shellHook = ''
    # Echo to stderr so the banner doesn't pollute stdout when callers do
    # ``nix-shell --run "cmd" > out`` (e.g. regenerating the vulture whitelist).
    echo "cratedigger dev shell — run: python3 -m unittest discover -s tests -t . -v" >&2

    # Pin the IDE's pyright to THIS interpreter so in-editor diagnostics match
    # ``nix-shell --run pyright`` (pyrightconfig.json points venvPath/venv here).
    # Without it, the editor's pyright falls back to the system python3 — which
    # lacks psycopg2/msgspec/pydantic/beets/... — and floods the file with
    # spurious reportMissingImports. Refreshed every shell entry (the store path
    # changes on ``nix flake update``); registered as an indirect GC root so
    # ``nix-collect-garbage`` won't sever the symlink.
    _cd_pyenv="$(${testPythonEnv}/bin/python3 -c 'import sys,os;print(os.path.dirname(os.path.dirname(sys.executable)))')"
    nix-store --realise "$_cd_pyenv" --indirect --add-root "$PWD/.pyright-venv" >/dev/null 2>&1 \
      || ln -sfn "$_cd_pyenv" "$PWD/.pyright-venv"
    unset _cd_pyenv

    # GC-root the pinned nixpkgs source tree (fetched by the flake.lock
    # shim above) so nix-collect-garbage doesn't force a re-download on
    # the next shell entry. Same pattern as .pyright-venv.
    nix-store --realise ${pkgs.path} --indirect --add-root "$PWD/.nixpkgs-src" >/dev/null 2>&1 \
      || ln -sfn ${pkgs.path} "$PWD/.nixpkgs-src"
  '';
}
