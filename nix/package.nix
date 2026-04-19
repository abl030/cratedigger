{ pkgs }:

let
  slskd-api = pkgs.callPackage ./slskd-api.nix { };

  # Production python deps. Deliberately does NOT include beets — soularr
  # invokes beets out-of-process via harness/run_beets_harness.sh, which
  # uses whatever `beet` the deployment environment provides (so the
  # consumer's plugins + MB host config + library DB all just work).
  # Putting `beet` in our pythonEnv would shadow the consumer's binary
  # via PATH and cause silent validation failures.
  pythonPackages = ps: [
    ps.psycopg2
    ps.music-tag
    ps.msgspec
    ps.redis     # web UI cache (graceful no-op if redis server is down, but the module must be importable)
    slskd-api
  ];
in {
  inherit slskd-api pythonPackages;

  pythonEnv = pkgs.python3.withPackages pythonPackages;
}
