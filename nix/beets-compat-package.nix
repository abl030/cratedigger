{ pkgs, src, version, buildBackend }:

# Checks-only historical/tip Beets package.  Production consumes the
# deployment-selected runtime package through nix/module.nix; this builder is
# deliberately not available to packages, shells, or the exported module.
let
  python = pkgs.python3Packages;
  backend = if buildBackend == "hatchling" then python.hatchling
    else if buildBackend == "poetry-core" then python.poetry-core
    else throw "unsupported Beets compatibility build backend: ${buildBackend}";
  poetryDynamicVersioning = python.poetry-dynamic-versioning;
in
python.beets.overridePythonAttrs (old: {
  inherit src version;
  pyproject = true;
  # v2.5-v2.12 load poetry_dynamic_versioning from their PEP 517 backend;
  # earlier Poetry and later Hatch sources simply ignore the extra build input.
  build-system = [ backend poetryDynamicVersioning ];
  # Match production's complete built-in plugin dependency closure. The
  # contracts load Cratedigger's active profile, so a hand-pruned core list
  # would make a green historical result less representative than deploy.
  dependencies = old.dependencies;
  # v2.2.0's own wheel metadata still says 2.1.0. The compatibility matrix
  # admits immutable upstream tags, so verify the installed source contract
  # instead of rewriting historical package metadata to please Nix.
  dontCheckPythonMetadata = true;
  doCheck = false;
})
