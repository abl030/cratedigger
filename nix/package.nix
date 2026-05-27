{ pkgs }:

let
  slskd-api = pkgs.callPackage ./slskd-api.nix { };
  coverageSubprocess = pkgs.callPackage ./coverage-subprocess.nix { };

  # Production python deps. Includes beets only as a Python library —
  # ``lib/beets_distance.py`` calls ``beets.autotag.distance`` directly
  # from cratedigger-web to compute Replace-picker distance scores. The
  # consumer's ``beet`` CLI is still the source of truth for import:
  # ``harness/run_beets_harness.sh`` invokes it explicitly with its own
  # PATH + plugin config, and the systemd unit doesn't put the Nix
  # ``beet`` binary on the cratedigger user's shell PATH. So adding the
  # library here does NOT shadow the consumer's binary at import time.
  # If we ever spawn ``beet`` from the cratedigger process itself, that
  # invariant breaks and this needs revisiting.
  pythonPackages = ps: [
    ps.psycopg2
    ps.music-tag
    ps.msgspec
    ps.pydantic  # HTTP request-body validation in web/routes/* (issue #343); msgspec stays for internal wire boundaries
    ps.redis     # web UI cache (graceful no-op if redis server is down, but the module must be importable)
    ps.zstandard # peer cache compresses msgpack directory payloads before writing Redis bytes
    ps.beets     # beets.autotag.distance for /api/beets-distance — library import only
    ps.ytmusicapi # YouTube Music album resolver — anonymous `YTMusic()` for search + get_album
    slskd-api
  ];

  # Coverage instrumentation layer. Adding coverageSubprocess pulls in
  # coverage.py + a .pth file that attaches to subprocesses when
  # COVERAGE_PROCESS_START is set. The .pth is a no-op without the env var,
  # so this is safe to layer onto the prod env unconditionally when the
  # operator opts into services.cratedigger.coverage.enable. See
  # nix/coverage-subprocess.nix.
  pythonPackagesWithCoverage = ps: pythonPackages ps ++ [ coverageSubprocess ];
in {
  inherit slskd-api pythonPackages pythonPackagesWithCoverage coverageSubprocess;

  pythonEnv = pkgs.python3.withPackages pythonPackages;
  pythonEnvWithCoverage = pkgs.python3.withPackages pythonPackagesWithCoverage;
}
