# Checks-only Python package set with the audio-authority libraries at
# upstream tip.
#
# Returns a `python3` whose package set has Beets, mutagen and mediafile built
# from the tip flake inputs. Everything else resolves normally, so a package
# that does not depend on the three keeps its pinned store path and is not
# rebuilt.
#
# Why one SET rather than three separate packages: mutagen is a dependency of
# both mediafile and music-tag, and mediafile is a dependency of Beets. Three
# independent `src` swaps would put two mutagens in one environment — the
# `withPackages` collision hook would either fail the build or silently
# resolve one of them, and a canary that tests an impossible combination
# proves nothing. `packageOverrides` rebuilds the dependents so the whole
# environment agrees on one tip mutagen.
#
# Production never sees this. Deployment consumes the pinned runtime package
# through nix/module.nix, and flake.nix's own topology check asserts the tip
# inputs never reach a runtime closure.
{ pkgs, beetsSrc, mutagenSrc, mediafileSrc }:

let
  # Upstream tip carries no release version, and both packages' nixpkgs
  # expressions pin patches against a released tree. Clearing `patches` is
  # required, not cosmetic: the pinned mutagen patch deletes a test file that
  # tip no longer ships, so the build fails at patchPhase without it.
  fromTip = prev: src: prev.overridePythonAttrs (_: {
    inherit src;
    version = "0-unstable-tip";
    patches = [];
    # Upstream's own suite is not our contract, and running it at tip turns
    # any upstream test flake into a red canary that says nothing about
    # Cratedigger. Our full suite is the assertion.
    doCheck = false;
    dontCheckPythonMetadata = true;
  });

  python = pkgs.python3.override {
    self = python;
    packageOverrides = _final: prev: {
      mutagen = fromTip prev.mutagen mutagenSrc;
      mediafile = fromTip prev.mediafile mediafileSrc;
      # Beets goes through the shared checks-only builder, which owns the
      # PEP 517 backend and metadata handling a non-release tree needs.
      # `prev` is deliberate: it resolves Beets' own base expression from
      # the un-overridden set while its dependencies still come from the
      # overridden one.
      beets = import ./beets-compat-package.nix {
        python = prev;
        src = beetsSrc;
        version = "2.13.1";
        buildBackend = "hatchling";
      };
    };
  };
in
python
