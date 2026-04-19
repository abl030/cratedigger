{ pkgs ? import <nixpkgs> {} }:

let
  cratedigger = import ./package.nix { inherit pkgs; };

  # Dev-only python env: production deps + beets so
  # harness/beets_harness.py is importable for tests. Production
  # pythonEnv deliberately excludes beets — see nix/package.nix.
  testPythonEnv = pkgs.python3.withPackages (ps:
    cratedigger.pythonPackages ps ++ [ps.beets]
  );
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql          # initdb, pg_ctl for ephemeral test DB
    testPythonEnv
    pkgs.sox                 # spectral analysis tests
    pkgs.ffmpeg              # ffprobe for bitrate measurement in quality tests
  ];

  shellHook = ''
    echo "soularr dev shell — run: python3 -m unittest discover tests -v"
  '';
}
