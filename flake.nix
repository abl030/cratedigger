{
  description = "Cratedigger — quality-obsessed music acquisition pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    beets-tip = {
      url = "github:beetbox/beets/master";
      flake = false;
    };
    # The audio-authority libraries the suite actually asserts against:
    # mutagen reads every tag and stream fact, mediafile is Beets' tag layer.
    # Checks-only, exactly like beets-tip. Note the branch names differ —
    # mutagen releases from `main`, the beetbox repositories from `master`.
    mutagen-tip = {
      url = "github:quodlibet/mutagen/main";
      flake = false;
    };
    mediafile-tip = {
      url = "github:beetbox/mediafile/master";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, beets-tip, mutagen-tip, mediafile-tip }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
      });

      linuxSystems = [ "x86_64-linux" "aarch64-linux" ];
      forLinux = f: nixpkgs.lib.genAttrs linuxSystems (system: f {
        inherit system;
        pkgs = import nixpkgs { inherit system; };
      });
      # Runtime source only — the store path every unit's PYTHONPATH and
      # the CLI wrappers embed. Docs, tests, examples, tooling and repo
      # metadata are dev-only: keeping them out of the fileset means a
      # docs/tests-only commit produces the identical (content-addressed)
      # src path, so the package and moduleVm stay cache hits on push.
      runtimeSrc = nixpkgs.lib.fileset.toSource {
        root = ./.;
        fileset = nixpkgs.lib.fileset.unions [
          ./cratedigger.py
          ./album_source.py
          ./lib
          ./web
          ./harness
          ./scripts
          ./migrations
        ];
      };
      # Content-addressed version (first 8 chars of runtimeSrc's store
      # hash): unique per runtime content, deliberately NOT per commit —
      # a rev-coupled version would invalidate the package + moduleVm on
      # every commit regardless of what changed. Deployment identity comes
      # from the exact Git revision and signed nixosconfig pin.
      version = "0-unstable-"
        + builtins.substring 0 8 (baseNameOf (toString runtimeSrc));
    in {
      devShells = forAllSystems ({ pkgs, ... }: {
        default = import ./nix/shell.nix { inherit pkgs; };
        # The same dev shell, with Beets/mutagen/mediafile at upstream tip.
        # Deliberately built by handing overridden `pkgs` to the ONE shell
        # definition rather than by forking it: the canary must run the
        # environment developers and the daily gate run, differing only in
        # the three packages under test. `nix/beets.nix` and
        # `nix/package.nix` both resolve through `pkgs.python3*`, so they
        # pick up the tip set without knowing it exists.
        tip = import ./nix/shell.nix {
          pkgs = pkgs.extend (_final: prev: let
            tipPython = import ./nix/tip-python.nix {
              pkgs = prev;
              beetsSrc = beets-tip;
              mutagenSrc = mutagen-tip;
              mediafileSrc = mediafile-tip;
            };
          in {
            python3 = tipPython;
            python3Packages = tipPython.pkgs;
          });
        };
      });

      packages = forAllSystems ({ pkgs, ... }: let
        beetsPackage = import ./nix/beets.nix { inherit pkgs; };
      in rec {
        default = import ./nix/wrappers.nix {
          inherit pkgs version beetsPackage;
          src = runtimeSrc;
        };
        cratedigger = default;
      });

      apps = forAllSystems ({ system, ... }: {
        pipeline-cli = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/pipeline-cli";
        };
      });

      # The module is exported as a wrapper that pins its package set to
      # cratedigger's OWN flake.lock (tier-2 plan U2 / KTD1): the runtime
      # closure — python env, beets — is the one the test suite verified,
      # independent of the consumer's nixpkgs. mkDefault so a consumer
      # setting services.cratedigger.packageSet (the escape hatch) wins.
      # Cost: a second nixpkgs evaluation on the consumer host — the
      # standard trade for closure fidelity.
      nixosModules.default = { config, lib, pkgs, ... }: {
        imports = [ ./nix/module.nix ];
        # Same filtered source the moduleVm check boots — consumers run
        # exactly what was verified, and docs/tests-only bumps of the
        # cratedigger-src input leave the rendered units unchanged.
        services.cratedigger.src = lib.mkDefault runtimeSrc;
        services.cratedigger.packageSet = lib.mkDefault (import nixpkgs {
          system = pkgs.stdenv.hostPlatform.system;
        });
      };

      checks = forLinux ({ pkgs, system }: let
        manifest = builtins.fromJSON (builtins.readFile ./nix/beets-compat-releases.json);
        compatSource = entry: pkgs.fetchFromGitHub {
          owner = "beetbox";
          repo = "beets";
          rev = entry.rev;
          hash = entry.narHash;
        };
        compatPackage = entry: import ./nix/beets-compat-package.nix {
          python = pkgs.python3Packages;
          version = entry.version;
          buildBackend = entry.buildBackend;
          src = compatSource entry;
        };
        contract = name: beets: let
          cratedigger = import ./nix/package.nix { inherit pkgs; beetsPackage = beets; };
          python = pkgs.python3.withPackages (ps: cratedigger.pythonPackages ps);
          activePlugins = if name == "beets-release-2.1.0" then
            "mbsync, discogs, fetchart, embedart, lyrics, lastgenre, scrub, info, missing, duplicates, edit, fromfilename, ftintitle, the, inline, permissions"
          else
            "musicbrainz, mbsync, discogs, fetchart, embedart, lyrics, lastgenre, scrub, info, missing, duplicates, edit, fromfilename, ftintitle, the, inline, permissions";
          # The historical matrix keeps a hand-picked list because a 19-leg
          # sweep of old releases cannot run the modern suite. The tip leg
          # no longer uses this function at all: it runs the whole
          # deterministic suite in the `tip` devShell, so
          # `tests.test_beets_distance.TestBeetsDistanceIntegrationSlice` —
          # named here explicitly for the tip leg after #1088 review
          # finding 2, because a wrong `_beets_match_distance` adaptation
          # would otherwise fail soft into "distance_failed" with the canary
          # green — is now reached as an ordinary suite member, along with
          # every other test a curated list could omit.
          testTargets = [
            "tests.test_harness_beets2_contract.TestHarnessBeets2Contract.test_help_stays_on_normal_stdout_and_protocol_is_private"
            "tests.test_harness_beets2_contract.TestHarnessBeets2Contract.test_real_beets_import_library_and_duplicate_action"
            "tests.test_harness_beets2_contract.TestHarnessBeets2Contract.test_real_incremental_import_uses_external_statefile_only"
            "tests.test_harness_beets2_contract.TestHarnessBeets2Contract.test_real_harness_pretend_keeps_source_manifest_unchanged"
          ];
          authority = pkgs.runCommand "cratedigger-${name}-matrix-authority" { } ''
            mkdir -p "$out/beets" "$out/secrets"
            cat > "$out/runtime.ini" <<EOF
[Beets]
config_dir = $out/beets
library = /build/cratedigger-matrix/library.db
directory = /build/cratedigger-matrix/library
state_file = $out/state.pickle
python = ${python}/bin/python
secret_include = $out/secrets/discogs.yaml
[MusicBrainz]
api_base = https://musicbrainz.org
EOF
            cat > "$out/beets/config.yaml" <<'EOF'
library: /build/cratedigger-matrix/library.db
directory: /build/cratedigger-matrix/library
statefile: __AUTHORITY__/state.pickle
include: [__AUTHORITY__/secrets/discogs.yaml]
plugins: [${activePlugins}]
import:
  autotag: true
  move: true
  write: true
  incremental: true
  incremental_skip_later: true
  duplicate_keys:
    album: [mb_albumid, discogs_albumid]
paths:
  default: $albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title
  comp: Compilations/$album%aunique{albumartist album,path_disambig}/$track $title
  singleton: Non-Album/$artist/$title
album_fields:
  path_disambig: albumdisambig or releasegroupdisambig or catalognum or label or str(year)
permissions:
  file: "0664"
  dir: "02775"
fetchart:
  auto: false
embedart:
  auto: false
lyrics:
  auto: false
lastgenre:
  auto: false
scrub:
  auto: false
musicbrainz:
  enabled: true
  host: musicbrainz.org
  https: true
EOF
            sed -i "s|__AUTHORITY__|$out|g" "$out/beets/config.yaml"
            cp -R "$out/beets" "$out/importer-beets"
            sed -i 's|statefile: .*|statefile: /build/cratedigger-matrix/state.pickle|' \
              "$out/importer-beets/config.yaml"
            sed -i 's|  move: true|  move: false|; s|  write: true|  write: false|' \
              "$out/importer-beets/config.yaml"
            : > "$out/state.pickle"
            printf '%s\n' 'discogs:' '  user_token: matrix-token' > "$out/secrets/discogs.yaml"
            cat > "$out/importer-runtime.ini" <<EOF
[Beets]
config_dir = $out/importer-beets
library = /build/cratedigger-matrix/library.db
directory = /build/cratedigger-matrix/library
state_file = /build/cratedigger-matrix/state.pickle
python = ${python}/bin/python
secret_include = $out/secrets/discogs.yaml
[MusicBrainz]
api_base = https://musicbrainz.org
EOF
          '';
        in pkgs.runCommand "cratedigger-${name}-contract" {
          nativeBuildInputs = [ python pkgs.ffmpeg ];
        } ''
          set -euo pipefail
          export HOME="$TMPDIR/home"
          mkdir -p "$HOME"
          unset BEETSDIR TEST_DB_DSN CRATEDIGGER_RUNTIME_CONFIG
          export PYTHONPATH=${self}
          export CRATEDIGGER_BEETS_PYTHON=${python}/bin/python
          mkdir -p /build/cratedigger-matrix/library /build/cratedigger-matrix/runtime
          : > /build/cratedigger-matrix/state.pickle
          ${python}/bin/python -c \
            'from beets.library import Library; lib = Library("/build/cratedigger-matrix/library.db", "/build/cratedigger-matrix/library"); lib._close()'
          ${python}/bin/python ${self}/scripts/check_beets_config.py \
            --config ${authority}/runtime.ini \
            --runtime-dir /build/cratedigger-matrix/runtime \
            --role web > "$TMPDIR/admission.json"
          grep -F '"ok":true' "$TMPDIR/admission.json"
          CRATEDIGGER_BEETS_MATRIX_RUNTIME_CONFIG=${authority}/importer-runtime.ini \
          ${python}/bin/python -m unittest \
            ${nixpkgs.lib.concatStringsSep " \\\n            " testTargets} \
            > "$TMPDIR/contract.stdout" 2> "$TMPDIR/contract.stderr" || {
              cat "$TMPDIR/contract.stdout" >&2
              cat "$TMPDIR/contract.stderr" >&2
              exit 1
            }
          test -s /build/cratedigger-matrix/state.pickle
          test ! -e /build/cratedigger-matrix/library/.beetsstate
          test ! -e ${self}/.beetsstate
          touch "$out"
        '';
        tipPackage = import ./nix/beets-compat-package.nix {
          python = pkgs.python3Packages;
          version = "2.13.1";
          buildBackend = "hatchling";
          src = beets-tip;
        };
        releaseChecks = nixpkgs.lib.listToAttrs (map (entry: {
          name = "beets-release-${builtins.replaceStrings [ "." ] [ "_" ] entry.version}-contract";
          value = contract "beets-release-${entry.version}" (compatPackage entry);
        }) manifest);
      in (rec {
        # Boots a NixOS VM with the upstream module enabled against an
        # ephemeral postgres + a stubbed slskd. Verifies: migrator runs,
        # the immutable runtime config is wired correctly, and the web responds.
        # Consumes the wrapped export — the same thing consumers import.
        # `nix flake check` must build the CLI bundle (U8): a stranger's
        # `nix run .#pipeline-cli` is only as green as this check.
        packageDefault = self.packages.${system}.default;
        # No beetsTip* checks: scripts/daily_beets_tip_update.sh now runs the
        # whole deterministic suite in the `tip` devShell, which subsumes all
        # three (the build is a prerequisite of entering the shell, the
        # harness contract tests are suite members, and the suite's own
        # pyright phase resolves against the tip interpreter through the
        # shellHook's .pyright-venv pin). `tipPackage` itself stays: the
        # topology check and beetsStableCandidate both assert against its
        # derivation path.

        # Execute the installed configuration checker, rather than merely
        # inspecting its wrapper source. A caller-controlled PYTHONPATH
        # containing a shadow ``beets`` package must not affect the pinned
        # interpreter's imports. The intentionally missing config is a fixed
        # rejected fixture: it proves the command crosses its normal JSON
        # error boundary after importing the real admitted Beets package.
        checkBeetsConfigPackageBoundary = let
          hostilePythonPath = pkgs.runCommand "cratedigger-hostile-pythonpath" { } ''
            mkdir -p "$out/beets"
            cat > "$out/beets/__init__.py" <<'PY'
            raise RuntimeError("hostile inherited PYTHONPATH imported beets")
            PY
          '';
        in pkgs.runCommand "cratedigger-check-beets-config-package-boundary" { } ''
          set -euo pipefail
          mkdir runtime
          if PYTHONPATH="${hostilePythonPath}" \
            ${self.packages.${system}.default}/bin/cratedigger-check-beets-config \
              --config "$PWD/missing.ini" \
              --runtime-dir "$PWD/runtime" \
              --role importer > stdout.json 2> stderr.log; then
            echo "checker unexpectedly accepted missing config" >&2
            exit 1
          fi
          grep -Fx '{"ok":false,"report":null,"error":"config_load_error"}' stdout.json
          ! grep -Fq "hostile inherited PYTHONPATH" stderr.log
          touch "$out"
        '';

        moduleVm = import ./nix/tests/module-vm.nix {
          inherit pkgs system;
          cratediggerModule = self.nixosModules.default;
          cratediggerSrc = runtimeSrc;
        };

        # Boots the flake-pinned Jellyfin and proves the real targeted
        # post-import notifier populates tagged album/track metadata without
        # broadening scope or wiping an existing curated item.
        jellyfinMetadataVm = import ./nix/tests/jellyfin-metadata-vm.nix {
          inherit pkgs;
          cratediggerSrc = runtimeSrc;
        };

        # Eval-level guard for the src threading: the exported wrapper must
        # default services.cratedigger.src to the filtered runtimeSrc (the
        # same source the moduleVm boots). A regression to the raw module
        # default (../. — the whole tree) would silently re-couple consumer
        # closures to docs/tests churn. Pure evaluation, like packageSetPin.
        runtimeSrcPin = let
          srcVal = (nixpkgs.lib.nixosSystem {
            modules = [ self.nixosModules.default {
              nixpkgs.pkgs = import nixpkgs { inherit system; };
            } ];
          }).config.services.cratedigger.src;
        in
          assert toString srcVal == toString runtimeSrc;
          pkgs.runCommand "cratedigger-runtime-src-pin-ok" { } "touch $out";

        # Eval-level guard for the packageSet threading. The "consumer" is
        # simulated with a marked nixpkgs instantiation installed as the
        # system's ambient pkgs (nixpkgs.pkgs) — so a regression of the
        # wrapper to `packageSet = lib.mkDefault pkgs` (consumer's set)
        # makes the default inherit the marker and fails the first assert.
        # Pure evaluation — only the option value is forced, so no required
        # options are needed.
        packageSetPin = let
          markedPkgs = import nixpkgs {
            inherit system;
            overlays = [ (final: prev: { cratediggerEscapeHatchMarker = "consumer-pkgs"; }) ];
          };
          evalWith = extraModule: (nixpkgs.lib.nixosSystem {
            modules = [ self.nixosModules.default { nixpkgs.pkgs = markedPkgs; } extraModule ];
          }).config.services.cratedigger.packageSet;
          pinned = evalWith { };
          overridden = evalWith { services.cratedigger.packageSet = markedPkgs; };
          expected = import nixpkgs { inherit system; };
        in
          assert !(pinned ? cratediggerEscapeHatchMarker);
          assert pinned.path == expected.path;
          assert (overridden.cratediggerEscapeHatchMarker or "") == "consumer-pkgs";
          pkgs.runCommand "cratedigger-packageset-pin-ok" { } "touch $out";

        # U7/R11: required options fail with actionable messages naming the
        # option — verified on MESSAGE CONTENT (tryEval would lose it).
        # Also pins both DB postures: bare enable trips the dsn-or-
        # createLocally assertion; a fully-set external-DSN config (the
        # doc2 shape) and a createLocally config both eval with zero
        # failing assertions.
        moduleAssertions = let
          beetsPackage = import ./nix/beets.nix { inherit pkgs; };
          runtimeCapability = {
            package = beetsPackage;
            configDir = "/etc/beets";
            expectedLibrary = "/srv/beets/beets-library.db";
            expectedDirectory = "/srv/music";
            expectedStateFile = "/var/lib/beets/state.pickle";
            expectedSecretInclude = "/run/secrets/beets.yaml";
          };
          evalAssertions = extra: (nixpkgs.lib.nixosSystem {
            modules = [ self.nixosModules.default {
              nixpkgs.pkgs = import nixpkgs { inherit system; };
              services.cratedigger.enable = true;
            } extra ];
          }).config.assertions;
          # Scope to our own assertions — a minimal nixosSystem also fails
          # NixOS's fileSystems/bootloader assertions, which aren't ours.
          failingMsgs = extra: builtins.filter
            (m: nixpkgs.lib.hasPrefix "services.cratedigger" m)
            (map (a: a.message)
              (builtins.filter (a: !a.assertion) (evalAssertions extra)));
          bare = failingMsgs { };
          hasMsg = needle: builtins.any (m: nixpkgs.lib.hasInfix needle m) bare;
          doc2Shape = failingMsgs {
            services.cratedigger = {
              slskd.apiKeyFile = "/run/secrets/slskd-key";
              slskd.downloadDir = "/mnt/music/slskd";
              pipelineDb.dsn = "postgresql://cratedigger@10.20.0.11:5432/cratedigger";
              beets.runtime = runtimeCapability;
              beets.validation = {
                stagingDir = "/mnt/music/incoming";
                trackingFile = "/mnt/music/incoming/tracking.jsonl";
              };
            };
          };
          strangerShape = failingMsgs {
            services.cratedigger = {
              slskd.apiKeyFile = "/etc/slskd-key";
              slskd.downloadDir = "/srv/slskd";
              pipelineDb.createLocally = true;
              beets.runtime = runtimeCapability;
              beets.validation = {
                stagingDir = "/srv/incoming";
                trackingFile = "/srv/incoming/tracking.jsonl";
              };
            };
          };
        in
          assert hasMsg "slskd.apiKeyFile";
          assert hasMsg "slskd.downloadDir";
          assert hasMsg "pipelineDb.createLocally = true";
          assert hasMsg "beets.runtime.package";
          assert hasMsg "beets.runtime.configDir";
          assert hasMsg "beets.runtime.expectedLibrary";
          assert hasMsg "beets.runtime.expectedDirectory";
          assert hasMsg "beets.runtime.expectedStateFile";
          assert hasMsg "beets.runtime.expectedSecretInclude";
          assert doc2Shape == [ ];
          assert strangerShape == [ ];
          pkgs.runCommand "cratedigger-module-assertions-ok" { } "touch $out";

        # nix/beets.nix mirror knobs: with the knobs set, the built plugin
        # files carry the mirror URLs; with them unset, stock upstream URLs.
        # `--replace-fail` inside beets.nix is the primary drift alarm (the
        # patched build fails if a future beets drops the target strings);
        # this check additionally pins the unpatched default to stock
        # behaviour so the knobs can never become always-on.
        beetsMirrorPatches = let
          patched = import ./nix/beets.nix {
            inherit pkgs;
            discogsMirrorUrl = "https://discogs-mirror.example.test";
            lrclibUrl = "http://lrclib.example.test/api";
          };
          unpatched = import ./nix/beets.nix { inherit pkgs; };
          # Compose the patched variant into a withPackages env the same way
          # pythonEnv does in production — the standalone build alone would
          # leave the real deploy shape (patched beets inside the env) as
          # the first-ever composition.
          patchedEnv = pkgs.python3.withPackages (ps: [ patched ]);
        in pkgs.runCommand "cratedigger-beets-mirror-patches-ok" { } ''
          set -euo pipefail
          p_lyrics=$(echo ${patched}/lib/python*/site-packages/beetsplug/lyrics.py)
          p_discogs=$(echo ${patched}/lib/python*/site-packages/beetsplug/discogs/__init__.py)
          u_lyrics=$(echo ${unpatched}/lib/python*/site-packages/beetsplug/lyrics.py)
          u_discogs=$(echo ${unpatched}/lib/python*/site-packages/beetsplug/discogs/__init__.py)
          grep -q 'BASE_URL = "http://lrclib.example.test/api"' "$p_lyrics"
          grep -q '_base_url = "https://discogs-mirror.example.test"' "$p_discogs"
          # Positive stock-URL/stock-line assertions: if the knob defaults
          # ever became non-null (always-on mirrors), these lines change and
          # the grep fails — a negated grep would not survive set -e anyway.
          grep -q 'BASE_URL = "https://lrclib.net/api"' "$u_lyrics"
          grep -q 'self.discogs_client = Client(USER_AGENT, user_token=user_token)$' "$u_discogs"
          test -x ${patchedEnv}/bin/beet
          touch $out
        '';

        # Compatibility sources are checks-only inputs. Query the realised
        # closures rather than trusting this flake's separation comments:
        # default package, shell, app package, and the exported-module VM
        # must never retain tip or historical Beets source/package inputs.
        beetsCompatibilityTopology = let
          # These are comparison values, never dependencies. `toString` on a
          # derivation/path carries its Nix string context; interpolating that
          # context into the builder would itself make the topology check (and
          # therefore the stable candidate) depend on every canary package.
          comparisonPath = value:
            builtins.unsafeDiscardStringContext (toString value);
          forbidden = [ (comparisonPath beets-tip) (comparisonPath tipPackage) ]
            ++ map (entry: comparisonPath (compatSource entry)) manifest
            ++ map (entry: comparisonPath (compatPackage entry)) manifest;
          forbiddenWords = builtins.concatStringsSep " " forbidden;
          runtimeClosure = pkgs.closureInfo {
            rootPaths = [
              packageDefault
              self.devShells.${system}.default
              self.apps.${system}.pipeline-cli.program
              moduleVm
            ];
          };
          unchecked = pkgs.runCommand "cratedigger-beets-compatibility-topology" {
          } ''
            set -euo pipefail
            for forbidden in ${forbiddenWords}; do
              if grep -Fxq "$forbidden" ${runtimeClosure}/store-paths; then
                echo "checks-only Beets input leaked into runtime closure: $forbidden" >&2
                exit 1
              fi
            done
            touch "$out"
          '';
          # `drvAttrs.buildCommand` carries the exact direct derivation
          # context Nix will serialise into the .drv. Assert against the
          # context-free tip drv path, so the proof cannot add the forbidden
          # edge it is checking for.
          tipDrv = comparisonPath tipPackage.drvPath;
          uncheckedInputs = builtins.getContext unchecked.drvAttrs.buildCommand;
        in assert !(builtins.hasAttr tipDrv uncheckedInputs);
          unchecked;

        # The daily Nixpkgs candidate is green only when every ordinary
        # flake check and every reviewed Beets release contract is green.
        # Tip is intentionally separate: a moving upstream canary must
        # alert without blocking a stable lock update.
        beetsStableCandidate = let
          unchecked = pkgs.runCommand "cratedigger-stable-candidate" {
            nativeBuildInputs = [
              packageDefault
              checkBeetsConfigPackageBoundary
              moduleVm
              jellyfinMetadataVm
              runtimeSrcPin
              packageSetPin
              moduleAssertions
              beetsMirrorPatches
              beetsCompatibilityTopology
            ] ++ builtins.attrValues releaseChecks;
          } "touch $out";
          tipDrv = builtins.unsafeDiscardStringContext tipPackage.drvPath;
          # mkDerivation serialises this list separately from buildCommand;
          # inspect its direct context as well, otherwise a future addition
          # to nativeBuildInputs could evade the topology-style assertion.
          nativeInputContext = builtins.getContext (builtins.concatStringsSep " "
            (map toString unchecked.nativeBuildInputs));
        in assert !(builtins.hasAttr tipDrv nativeInputContext);
          unchecked;
      } // releaseChecks));
    };
}
