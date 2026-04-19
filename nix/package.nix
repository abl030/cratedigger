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
    slskd-api
  ]);
}
