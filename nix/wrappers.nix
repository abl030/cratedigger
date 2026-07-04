# Standalone CLI wrapper bundle for `nix run` / `nix profile install`
# (tier-2 plan U8, R8 / KTD4).
#
# Deliberately NOT a buildPythonApplication: the repo is flat (no
# __init__.py in lib/scripts/harness, sys.path bootstraps in entry points,
# vulture/unittest/pyright all assume repo-root layout), so the package
# output is the same writeShellScriptBin shape the NixOS module uses —
# pinned interpreter + PYTHONPATH=<repo root> only (the lib/beets.py
# shadow hazard forbids subdirectories).
#
# Only the operator-facing CLI surfaces are bundled. The daemons
# (cratedigger loop, importer, web, workers) are the NixOS module's job —
# they need rendered config and systemd ordering, not `nix run`.
{ pkgs, src ? ../., version ? "0-unstable-dirty" }:

let
  cratedigger = pkgs.callPackage ./package.nix { };
  pythonEnv = cratedigger.pythonEnv;

  runtimePath = pkgs.lib.makeBinPath [
    pkgs.coreutils
    pkgs.ffmpeg
    pkgs.sox
    pkgs.flac
    pkgs.mp3val
  ];

  mkCliTool = name: script: pkgs.writeShellScriptBin name ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pythonEnv}/bin/python ${src}/${script} "$@"
  '';
in
pkgs.symlinkJoin {
  name = "cratedigger-${version}";
  pname = "cratedigger";
  inherit version;
  paths = [
    # DSN via --dsn / PIPELINE_DB_DSN, same contract as everywhere else.
    # pipeline-cli is a package (scripts/pipeline_cli/, issue #495); exec
    # the __main__.py entry shim, same script-path invocation style.
    (mkCliTool "pipeline-cli" "scripts/pipeline_cli/__main__.py")
    (mkCliTool "pipeline-migrate" "scripts/migrate_db.py")
  ];
  meta = {
    description = "Cratedigger operator CLI (pipeline-cli, pipeline-migrate)";
    mainProgram = "pipeline-cli";
  };
}
