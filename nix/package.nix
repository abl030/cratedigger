{ pkgs, beetsPackage }:

let
  # Production Python deps consume an explicit deployment-selected Beets
  # package. Keeping it in this environment makes the application, harness,
  # and checker resolve one admitted Python runtime without making this
  # package factory the owner of Beets configuration or storage.
  pythonPackages = ps: [
    ps.psycopg2
    ps.defusedxml # Plex XML responses are untrusted network input
    ps.music-tag
    ps.msgspec
    ps.pydantic  # HTTP request-body validation in web/routes/* (issue #343); msgspec stays for internal wire boundaries
    ps.redis     # web UI cache (graceful no-op if redis server is down, but the module must be importable)
    ps.zstandard # peer cache compresses msgpack directory payloads before writing Redis bytes
    ps.numpy     # lib/aac_lattice.py (MDCT frame-lattice detector, issue #829) AND
                 # lib/composite_audio_gap.py (composite-audio silence-gap detector,
                 # issue #1237) — the latter is pulled in transitively at
                 # web/server.py startup via lib/library_completeness_snapshot.py ->
                 # lib/library_completeness.py; scipy is deliberately NOT here,
                 # erfinv is computed from math.erf
    beetsPackage # admitted Beets: autotag.distance library + bin/beet
    ps.ytmusicapi # YouTube Music album resolver — anonymous `YTMusic()` for search + get_album
  ];
in {
  inherit pythonPackages;

  pythonEnv = pkgs.python3.withPackages pythonPackages;
}
