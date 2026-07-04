{
  description = "Cratedigger — quality-obsessed music acquisition pipeline";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { self, nixpkgs }:
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
      version = "0-unstable-"
        + (builtins.substring 0 8 (self.lastModifiedDate or "19700101"))
        + (if self ? shortRev then "-${self.shortRev}" else "-dirty");
    in {
      devShells = forAllSystems ({ pkgs, ... }: {
        default = import ./nix/shell.nix { inherit pkgs; };
      });

      # Tag-based versions are not readable in pure flake eval, so the
      # version above is date+rev (sortable, unique per commit); release
      # tags (vYYYY.MM.DD, U9) record verified states in git itself.
      packages = forAllSystems ({ pkgs, ... }: rec {
        default = import ./nix/wrappers.nix {
          inherit pkgs version;
          src = ./.;
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
        services.cratedigger.packageSet = lib.mkDefault (import nixpkgs {
          system = pkgs.stdenv.hostPlatform.system;
        });
      };

      checks = forLinux ({ pkgs, system }: {
        # Boots a NixOS VM with the upstream module enabled against an
        # ephemeral postgres + a stubbed slskd. Verifies: migrator runs,
        # config.ini is rendered correctly, cratedigger-web responds.
        # Consumes the wrapped export — the same thing consumers import.
        # `nix flake check` must build the CLI bundle (U8): a stranger's
        # `nix run .#pipeline-cli` is only as green as this check.
        packageDefault = self.packages.${system}.default;

        moduleVm = import ./nix/tests/module-vm.nix {
          inherit pkgs system;
          cratediggerModule = self.nixosModules.default;
          cratediggerSrc = ./.;
        };

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
          assert doc2Shape == [ ];
          assert strangerShape == [ ];
          pkgs.runCommand "cratedigger-module-assertions-ok" { } "touch $out";

        # KTD6 derivation, asserted on rendered VALUES (not source text):
        # the beets musicbrainz block flips host/https/ratelimit for a
        # mirror origin vs the public default. Eval-only.
        apiBaseDerivation = let
          evalMb = apiBase: (nixpkgs.lib.nixosSystem {
            modules = [ self.nixosModules.default {
              nixpkgs.pkgs = import nixpkgs { inherit system; };
              services.cratedigger.enable = true;
              services.cratedigger.musicbrainz.apiBase = apiBase;
            } ];
          }).config.services.cratedigger.beets.config.musicbrainz;
          mirror = evalMb "http://192.168.1.35:5200";
          public = evalMb "https://musicbrainz.org";
        in
          assert mirror.host == "192.168.1.35:5200";
          assert mirror.https == false;
          assert mirror.ratelimit == 100;
          assert public.host == "musicbrainz.org";
          assert public.https == true;
          assert public.ratelimit == 1;
          pkgs.runCommand "cratedigger-api-base-derivation-ok" { } "touch $out";

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
      });
    };
}
