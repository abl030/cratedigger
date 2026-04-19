{ pkgs }:

let
  slskd-api = pkgs.callPackage ./slskd-api.nix { };
in {
  inherit slskd-api;

  pythonEnv = pkgs.python3.withPackages (ps: [
    ps.psycopg2
    ps.music-tag
    ps.beets
    ps.msgspec
    ps.redis     # web UI cache (graceful no-op if redis server is down, but the module must be importable)
    slskd-api
  ]);
}
