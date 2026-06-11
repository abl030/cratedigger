{ pkgs ? import <nixpkgs> {} }:

let
  cratedigger = import ./package.nix { inherit pkgs; };

  # Dev env: production deps + dev-only tooling. ``ps.beets`` was
  # previously listed again here because the production env excluded
  # it; now that ``lib.beets_distance`` makes beets a first-class
  # library dependency it's already in ``pythonPackages``, so the
  # dev shell inherits it without duplication.
  #
  # Dev-only additions:
  #   - vulture: static dead-code finder (scripts/find_dead_code.sh)
  #   - coverage: runtime coverage for both the test suite and
  #     production-instrumented systemd units (scripts/coverage_report.sh
  #     + scripts/coverage_diff.py)
  testPythonEnv = pkgs.python3.withPackages (ps:
    cratedigger.pythonPackages ps
    ++ [ ps.vulture ps.coverage ]
  );
in
pkgs.mkShell {
  packages = [
    pkgs.postgresql          # initdb, pg_ctl for ephemeral test DB
    testPythonEnv
    pkgs.sox                 # spectral analysis tests
    pkgs.ffmpeg              # ffprobe for bitrate measurement in quality tests
    pkgs.yt-dlp              # YouTube-rescue ingest worker invokes this binary;
                             # dev-shell tests resolve it via `shutil.which("yt-dlp")`
  ];

  shellHook = ''
    # Echo to stderr so the banner doesn't pollute stdout when callers do
    # ``nix-shell --run "cmd" > out`` (e.g. regenerating the vulture whitelist).
    echo "cratedigger dev shell — run: python3 -m unittest discover -s tests -t . -v" >&2
  '';
}
