{ pkgs ? import <nixpkgs> {} }:

let
  cratedigger = import ./package.nix { inherit pkgs; };

  # Dev env: production deps. ``ps.beets`` was previously listed
  # again here because the production env excluded it; now that
  # ``lib.beets_distance`` makes beets a first-class library
  # dependency it's already in ``pythonPackages``, so the dev shell
  # inherits it without duplication.
  testPythonEnv = pkgs.python3.withPackages (ps:
    cratedigger.pythonPackages ps
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
    echo "cratedigger dev shell — run: python3 -m unittest discover tests -v"
  '';
}
