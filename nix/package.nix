{ pkgs, beetsPackage ? import ./beets.nix { inherit pkgs; } }:

let
  # Production python deps. beets is cratedigger-owned (tier-2 plan U3):
  # ``beetsPackage`` (nix/beets.nix, optionally mirror-patched by the
  # module) is BOTH the library ``lib/beets_distance.py`` imports from
  # cratedigger-web AND the ``bin/beet`` behind cratedigger-beet — one
  # store path for every beets consumer (the harness joins in U5; until
  # then it still resolves the consumer's ``beet``).
  pythonPackages = ps: [
    ps.psycopg2
    ps.music-tag
    ps.msgspec
    ps.pydantic  # HTTP request-body validation in web/routes/* (issue #343); msgspec stays for internal wire boundaries
    ps.redis     # web UI cache (graceful no-op if redis server is down, but the module must be importable)
    ps.zstandard # peer cache compresses msgpack directory payloads before writing Redis bytes
    beetsPackage # the one beets: autotag.distance library + bin/beet (nix/beets.nix)
    ps.ytmusicapi # YouTube Music album resolver — anonymous `YTMusic()` for search + get_album
  ];
in {
  inherit pythonPackages;

  pythonEnv = pkgs.python3.withPackages pythonPackages;
}
