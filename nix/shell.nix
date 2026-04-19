{ pkgs ? import <nixpkgs> {} }:

let
  cratedigger = import ./package.nix { inherit pkgs; };
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql          # initdb, pg_ctl for ephemeral test DB
    cratedigger.pythonEnv
    pkgs.sox                 # spectral analysis tests
    pkgs.ffmpeg              # ffprobe for bitrate measurement in quality tests
  ];

  shellHook = ''
    echo "soularr dev shell — run: python3 -m unittest discover tests -v"
  '';
}
