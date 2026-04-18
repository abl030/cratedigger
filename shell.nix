{ pkgs ? import <nixpkgs> {} }:

let
  python = pkgs.python3;

  slskd-api = python.pkgs.buildPythonPackage rec {
    pname = "slskd-api";
    version = "0.2.3";
    format = "wheel";
    src = python.pkgs.fetchPypi {
      pname = "slskd_api";
      inherit version;
      format = "wheel";
      dist = "py3";
      python = "py3";
      hash = "sha256-X80Ct2oxzuMj8vTV9zfN0FABhekDAoejvaSs0A1pv8E=";
    };
    propagatedBuildInputs = [ python.pkgs.requests ];
  };
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql          # initdb, pg_ctl for ephemeral test DB
    (python.withPackages (ps: [
      ps.psycopg2
      ps.music-tag
      ps.beets
      ps.msgspec
      slskd-api
    ]))
    pkgs.sox                 # spectral analysis tests
    pkgs.ffmpeg              # ffprobe for bitrate measurement in quality tests
  ];

  shellHook = ''
    echo "soularr dev shell — run: python3 -m unittest discover tests -v"
  '';
}
