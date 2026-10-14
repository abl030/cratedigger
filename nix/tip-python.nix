# Checks-only Python package set with the audio-authority libraries at
# upstream tip.
#
# Returns a `python3` whose package set has Beets, mutagen and mediafile built
# from the tip flake inputs. Everything else resolves normally, so a package
# that does not depend on the three keeps its pinned store path and is not
# rebuilt. The one exception is a released dependency tip Beets requires at
# a version nixpkgs does not ship yet; those go through `releaseAhead`, which
# fails evaluation once nixpkgs catches up.
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

  # A released dependency that tip Beets needs before nixpkgs ships it. The
  # pin expires on its own: once nixpkgs reaches `version`, evaluating the
  # tip set fails with the deletion instruction, so the canary goes red
  # pre-diagnosed instead of the override outliving its reason unnoticed.
  # Only the tip canary evaluates this set; the stable nixpkgs gate builds
  # `beetsStableCandidate`, which never reaches it, so the expiry cannot
  # block a nixpkgs lock update. A pinned release keeps its own tests.
  releaseAhead = prev: { version, src }:
    if pkgs.lib.versionAtLeast prev.version version then
      throw ("nix/tip-python.nix: nixpkgs now ships ${prev.pname} "
        + "${prev.version}, which satisfies the ${version} pin. Delete the "
        + "${prev.pname} releaseAhead override.")
    else prev.overridePythonAttrs (_: { inherit version src; });

  python = pkgs.python3.override {
    self = python;
    packageOverrides = _final: prev: {
      mutagen = fromTip prev.mutagen mutagenSrc;
      mediafile = fromTip prev.mediafile mediafileSrc;
      # Beets tip requires python3-discogs-client >= 2.10: its discogs
      # plugin imports MalformedResponseError, new in 2.10 (#1418).
      discogs-client = releaseAhead prev.discogs-client {
        version = "2.10";
        src = pkgs.fetchFromGitHub {
          owner = "joalla";
          repo = "discogs_client";
          tag = "v2.10";
          hash = "sha256-1Dx0KSYP15+izEfNSGfEw8lpjqSQPuhQkGntRA4srmo=";
        };
      };
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
