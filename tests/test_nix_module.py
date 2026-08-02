"""Contract tests for nix/module.nix.

The Nix wrappers in ``nix/module.nix`` decide what environment
subprocesses (``beet``, ``import_one.py``, etc.) inherit. Historically,
leaks here have caused pipeline-wide failures that were hard to trace:

* 2026-04-21 ``cratedigger-web`` force-import path crashed on every
  post-import ``beet remove`` with ``ModuleNotFoundError: No module
  named 'msgspec'``. Root cause: the wrapper exported
  ``PYTHONPATH=${src}:${src}/lib:${src}/web:...`` which put
  ``lib/beets.py`` at sys.path top level as a bare ``beets`` module,
  shadowing the real beets PyPI package. The ``beet`` subprocess did
  ``from beets.ui import main`` → loaded our ``lib/beets.py`` → hit
  ``import msgspec`` (line 11) → ``ModuleNotFoundError`` because the
  beet-wrapped Python doesn't carry msgspec. The accumulated effect
  was three split-brain rows for one MBID (Unter Null "Sick Fuck"
  request 1748).

These grep-based contracts are cheap to write and catch the whole
class of "an export in module.nix leaked into a subprocess and broke
something five layers away". They run inside the Python suite because
most invariants only need a source-level check. The state-directory authority
boundary is the exception: it has a known-bad ``nix eval`` pin because the
failure depends on Nix option assertion evaluation.
"""

from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_NIX = REPO_ROOT / "nix" / "module.nix"
FLAKE_NIX = REPO_ROOT / "flake.nix"
WRAPPERS_NIX = REPO_ROOT / "nix" / "wrappers.nix"
PACKAGE_NIX = REPO_ROOT / "nix" / "package.nix"
BEETS_NIX = REPO_ROOT / "nix" / "beets.nix"
SHELL_NIX = REPO_ROOT / "nix" / "shell.nix"
MODULE_VM_NIX = REPO_ROOT / "nix" / "tests" / "module-vm.nix"


class TestPythonPathCarriesOnlyRepoRoot(unittest.TestCase):
    """No wrapper in ``nix/module.nix`` may export PYTHONPATH that includes
    ``${src}/lib`` or ``${src}/web``.

    All internal imports use the qualified form ``from lib.X import Y`` /
    ``from web.X import Y``, so the repo root on PYTHONPATH is sufficient.
    Adding the sub-directories promotes our internal modules (``lib/beets.py``,
    ``web/discogs.py``, ``web/classify.py``) to top-level names, where they
    shadow the real ``beets``, ``discogs_client`` and anything else a
    subprocess might import. The beet subprocess has historically been
    the first victim because its wrapper does ``from beets.ui import main``.
    """

    # Matches any ``export PYTHONPATH=...${src}/<subdir>...``
    # The test looks for the forbidden sub-paths specifically rather than
    # trying to parse the full expression — that keeps the pattern simple
    # and catches any future ``${src}/foo`` that would cause the same class
    # of shadowing.
    FORBIDDEN = re.compile(r'PYTHONPATH=.*\$\{src\}/(lib|web)')

    def test_no_wrapper_leaks_subdir(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        hits: list[tuple[int, str]] = []
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip comments — comments are explanation, not code.
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if self.FORBIDDEN.search(line):
                hits.append((lineno, line.strip()))
        self.assertEqual(
            hits, [],
            f"{MODULE_NIX} exports PYTHONPATH with ${{src}}/lib or "
            f"${{src}}/web — these shadow PyPI packages (beets, "
            f"discogs_client, ...) in any subprocess that inherits "
            f"PYTHONPATH. Use ${{src}} only; internal imports are "
            f"qualified (from lib.X import Y). Offending lines:\n"
            + "\n".join(f"  {n}: {s}" for n, s in hits)
        )


class TestPipelineCliWrapperContract(unittest.TestCase):
    """API-backed CLI commands use the module-owned Unix listener."""

    def _wrapper(self) -> str:
        text = MODULE_NIX.read_text(encoding="utf-8")
        wrapper_start = text.index('writeShellScriptBin "pipeline-cli"')
        wrapper_end = text.index('writeShellScriptBin "pipeline-migrate"')
        return text[wrapper_start:wrapper_end]

    def test_wrapper_selects_non_overridable_unix_socket(self) -> None:
        wrapper = self._wrapper()
        self.assertIn('main(api_socket="${webSocketPath}")', wrapper)
        self.assertNotIn("--api-base", wrapper)
        self.assertNotIn("127.0.0.1", wrapper)

    def test_wrapper_uses_safe_path_with_trusted_source_first(self) -> None:
        wrapper = self._wrapper()
        trusted_path = (
            'export PYTHONPATH="${src}\'\'${PYTHONPATH:+:$PYTHONPATH}"'
        )
        safe_exec = "exec ${pythonEnv}/bin/python -P -c"
        self.assertIn(trusted_path, wrapper)
        self.assertIn(safe_exec, wrapper)
        self.assertLess(wrapper.index(trusted_path), wrapper.index(safe_exec))


class TestDefaultHeadlessComposition(unittest.TestCase):
    """The exported module keeps the direct CLI usable without the web."""

    def test_exported_module_installs_cli_without_web_units_or_sockets(
        self,
    ) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            lib = f.inputs.nixpkgs.lib;
            system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                    };
                  };
                })
              ];
            };
          in builtins.toJSON {
            webEnabled = system.config.services.cratedigger.web.enable;
            systemPackages =
              map lib.getName system.config.environment.systemPackages;
            hasWebService =
              builtins.hasAttr
                "cratedigger-web"
                system.config.systemd.services;
            cratediggerSockets =
              builtins.filter
                (name: lib.hasPrefix "cratedigger" name)
                (builtins.attrNames system.config.systemd.sockets);
          }
        '''
        result = subprocess.run(
            ["nix", "eval", "--raw", "--impure", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        composition = json.loads(result.stdout)
        self.assertFalse(composition["webEnabled"])
        self.assertIn("pipeline-cli", composition["systemPackages"])
        self.assertFalse(composition["hasWebService"])
        self.assertEqual(composition["cratediggerSockets"], [])


class TestWebAuthenticationModuleContract(unittest.TestCase):
    """The enabled web surface has one fail-closed module-owned perimeter."""

    def test_basic_and_insecure_mode_matrix_is_evaluated(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            lib = f.inputs.nixpkgs.lib;
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            evaluate = extra:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    f.nixosModules.default
                    ({ ... }: {
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        web.enable = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                      };
                    })
                    extra
                  ];
                };
              in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix "services.cratedigger.web" assertion.message)
                  system.config.assertions);
          in {
            missing = evaluate {
              services.cratedigger.web.hostName = "music.example.test";
            };
            basic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
            };
            insecure = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
            };
            conflict = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                  enableInsecure = true;
                };
              };
            };
            storeBasic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/nix/store/fake-cratedigger.htpasswd";
                };
              };
            };
            badHost = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test;\nreturn 200";
                enableInsecure = true;
              };
            };
            uppercaseHost = evaluate {
              services.cratedigger.web = {
                hostName = "Music.example.test";
                enableInsecure = true;
              };
            };
            ipHost = evaluate {
              services.cratedigger.web = {
                hostName = "127.0.0.1";
                enableInsecure = true;
              };
            };
            injectedBasic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile =
                    "/run/secrets/file; satisfy any; allow all; #";
                };
              };
            };
            disabled = evaluate {
              services.cratedigger.web.enable = lib.mkForce false;
            };
            disabledBasic = evaluate {
              services.cratedigger.web = {
                enable = lib.mkForce false;
                basicAuthFile = "/run/secrets/cratedigger.htpasswd";
              };
            };
            disabledInsecure = evaluate {
              services.cratedigger.web = {
                enable = lib.mkForce false;
                enableInsecure = true;
              };
            };
            serviceGroupOverlap = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "cratedigger";
                };
              };
            };
            nginxGroupOverlap = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
                accessGroup = "nginx";
              };
            };
            rootAccessGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "root";
                };
              };
            };
            wheelAccessGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "wheel";
                };
              };
            };
            explicitOperatorGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "music-operators";
                };
              };
              users.users.operator = {
                isNormalUser = true;
                extraGroups = [ "music-operators" ];
              };
            };
            secretGroupOverlap = evaluate {
              services.cratedigger = {
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "cratedigger-ops";
                };
              };
            };
            nginxAccountSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.users.nginx.extraGroups = [ "cratedigger-ops" ];
            };
            nginxReverseSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.cratedigger-ops.members = [ "nginx" ];
            };
            nginxAliasedReverseSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.hiddenSecret = {
                name = "cratedigger-ops";
                members = [ "nginx" ];
              };
            };
            nginxServiceMediaGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.SupplementaryGroups = [
                "users"
              ];
            };
            nginxServiceNumericMediaGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                };
              };
              users.groups.cratedigger.gid = 4242;
              systemd.services.nginx.serviceConfig.SupplementaryGroups = [
                "4242"
              ];
            };
            nginxPrimaryServiceGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                };
              };
              services.nginx.group = "cratedigger";
            };
            nginxServiceRootUserOverride = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.User =
                lib.mkForce "root";
            };
            nginxServiceRootGroupOverride = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.Group =
                lib.mkForce "root";
            };
            nginxMissingAccessGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.users.nginx.extraGroups = lib.mkForce [];
              systemd.services.nginx.serviceConfig.SupplementaryGroups =
                lib.mkForce [];
            };
            nginxNumericAccessGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.cratedigger-web.gid = 4243;
              users.users.nginx.extraGroups = lib.mkForce [];
              systemd.services.nginx.serviceConfig.SupplementaryGroups =
                lib.mkForce [ "4243" ];
            };
            webServiceCredentialGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.SupplementaryGroups = [
                "nginx"
              ];
            };
            webServiceRootOverride = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.User =
                lib.mkForce "root";
            };
            webServiceNginxGroupOverride = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.Group =
                lib.mkForce "nginx";
            };
            nginxReverseUnrelatedGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.smokeping.members = [ "nginx" ];
            };
            nginxReloadDisabled = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              services.nginx.enableReload = lib.mkForce false;
            };
            nginxRestartDisabled = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.restartIfChanged = lib.mkForce false;
            };
          }
        '''
        result = subprocess.run(
            ["nix", "eval", "--impure", "--json", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        worlds = json.loads(result.stdout)
        self.assertTrue(
            any("exactly one" in message for message in worlds["missing"])
        )
        self.assertEqual(worlds["basic"], [])
        self.assertEqual(worlds["insecure"], [])
        self.assertTrue(
            any("mutually exclusive" in message for message in worlds["conflict"])
        )
        self.assertTrue(
            any("outside /nix/store" in message for message in worlds["storeBasic"])
        )
        self.assertTrue(
            any("canonical DNS hostname" in message for message in worlds["badHost"])
        )
        self.assertTrue(
            any("lowercase canonical" in message for message in worlds["uppercaseHost"])
        )
        self.assertTrue(
            any("not an IP literal" in message for message in worlds["ipHost"])
        )
        self.assertTrue(
            any("nginx-token-safe" in message for message in worlds["injectedBasic"])
        )
        self.assertEqual(worlds["disabled"], [])
        self.assertTrue(
            any("inactive-mode residue" in message for message in worlds["disabledBasic"])
        )
        self.assertTrue(
            any(
                "inactive-mode residue" in message
                for message in worlds["disabledInsecure"]
            )
        )
        for world in (
            "serviceGroupOverlap",
            "nginxGroupOverlap",
            "secretGroupOverlap",
        ):
            self.assertTrue(
                any("must be dedicated" in message for message in worlds[world]),
                (world, worlds[world]),
            )
        for world in ("rootAccessGroup", "wheelAccessGroup"):
            self.assertTrue(
                any(
                    "forbidden authority group" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertEqual(worlds["explicitOperatorGroup"], [])
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxAccountSecretGroup"]
            ),
            worlds["nginxAccountSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxReverseSecretGroup"]
            ),
            worlds["nginxReverseSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxAliasedReverseSecretGroup"]
            ),
            worlds["nginxAliasedReverseSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxServiceMediaGroup"]
            ),
            worlds["nginxServiceMediaGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxServiceNumericMediaGroup"]
            ),
            worlds["nginxServiceNumericMediaGroup"],
        )
        self.assertTrue(
            any(
                "primary group" in message
                for message in worlds["nginxPrimaryServiceGroup"]
            ),
            worlds["nginxPrimaryServiceGroup"],
        )
        for world in (
            "nginxServiceRootUserOverride",
            "nginxServiceRootGroupOverride",
        ):
            self.assertTrue(
                any(
                    "final nginx.service User and Group" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertTrue(
            any(
                "membership in web.accessGroup" in message
                for message in worlds["nginxMissingAccessGroup"]
            ),
            worlds["nginxMissingAccessGroup"],
        )
        self.assertEqual(worlds["nginxNumericAccessGroup"], [])
        self.assertTrue(
            any(
                "cratedigger-web.service SupplementaryGroups" in message
                for message in worlds["webServiceCredentialGroup"]
            ),
            worlds["webServiceCredentialGroup"],
        )
        for world in (
            "webServiceRootOverride",
            "webServiceNginxGroupOverride",
        ):
            self.assertTrue(
                any(
                    "final cratedigger-web.service User and Group" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertEqual(worlds["nginxReverseUnrelatedGroup"], [])
        self.assertTrue(
            any(
                "requires services.nginx.enableReload" in message
                for message in worlds["nginxReloadDisabled"]
            ),
            worlds["nginxReloadDisabled"],
        )
        self.assertTrue(
            any(
                "requires systemd.services.nginx.restartIfChanged"
                in message
                for message in worlds["nginxRestartDisabled"]
            ),
            worlds["nginxRestartDisabled"],
        )

    def test_injected_basic_path_cannot_render_toplevel(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            system = f.inputs.nixpkgs.lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    user = "cratedigger";
                    group = "cratedigger";
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                    };
                    web = {
                      enable = true;
                      hostName = "music.example.test";
                      basicAuthFile =
                        "/run/secrets/file; satisfy any; allow all; #";
                    };
                  };
                })
              ];
            };
          in system.config.system.build.toplevel.drvPath
        '''
        result = subprocess.run(
            ["nix", "eval", "--impure", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nginx-token-safe segments", result.stderr)

    def test_merged_basic_gateway_values_are_exact(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            lib = f.inputs.nixpkgs.lib;
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            render = enableIPv6: basicAuthFile:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    f.nixosModules.default
                    ({ ... }: {
                      networking.enableIPv6 = enableIPv6;
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        user = "cratedigger";
                        group = "cratedigger";
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                        web = ({
                          enable = true;
                          hostName = "music.example.test";
                          enableInsecure = basicAuthFile == null;
                        } // lib.optionalAttrs (basicAuthFile != null) {
                          inherit basicAuthFile;
                        });
                      };
                      services.nginx.virtualHosts.cratedigger-auth-gateway
                        .locations."/merged-probe" = {
                          proxyPass =
                            "http://unix:/run/cratedigger-web/web.sock:";
                          recommendedProxySettings = false;
                        };
                    })
                  ];
                };
                gateway =
                  system.config.services.nginx.virtualHosts.cratedigger-auth-gateway;
                reject =
                  system.config.services.nginx.virtualHosts.cratedigger-auth-reject;
                socket = system.config.systemd.sockets.cratedigger-web;
                webService =
                  system.config.systemd.services.cratedigger-web;
                nginxService = system.config.systemd.services.nginx;
              in {
                failures = map (assertion: assertion.message)
                  (builtins.filter
                    (assertion:
                      !assertion.assertion
                      && lib.hasPrefix
                        "services.cratedigger.web"
                        assertion.message)
                    system.config.assertions);
                listen = map (item: {
                  inherit (item) addr port;
                }) gateway.listen;
                hostName = gateway.serverName;
                gatewayExtra = gateway.extraConfig;
                gatewayPolicy =
                  system.config.environment.etc
                    ."cratedigger/web-gateway-policy".text;
                basicAuthFile = gateway.basicAuthFile;
                rootBasicAuthFile = gateway.locations."/".basicAuthFile;
                mergedBasicAuthFile =
                  gateway.locations."/merged-probe".basicAuthFile;
                proxyPass = gateway.locations."/".proxyPass;
                healthProxy = gateway.locations."= /healthz".proxyPass;
                healthExtra = gateway.locations."= /healthz".extraConfig;
                rejectDefault = reject.default;
                rejectConfig = reject.locations."/".extraConfig;
                socketListen = socket.listenStreams;
                socketGroup = socket.socketConfig.SocketGroup;
                socketMode = socket.socketConfig.SocketMode;
                webAfter = webService.after;
                webRequires = webService.requires;
                webGroups = webService.serviceConfig.SupplementaryGroups;
                webUser = webService.serviceConfig.User;
                webGroup = webService.serviceConfig.Group;
                webStartPre = webService.serviceConfig.ExecStartPre;
                nginxEnableReload =
                  system.config.services.nginx.enableReload;
                nginxRestartIfChanged = nginxService.restartIfChanged;
                nginxAfter = nginxService.after;
                nginxWants = nginxService.wants;
                nginxRequires = nginxService.requires;
                nginxUnit =
                  system.config.systemd.units."nginx.service".text;
                nginxGroups = nginxService.serviceConfig.SupplementaryGroups;
                nginxUser = nginxService.serviceConfig.User;
                nginxGroup = nginxService.serviceConfig.Group;
                nginxUserGroups =
                  system.config.users.users.${system.config.services.nginx.user}.extraGroups;
                applicationUserGroups =
                  system.config.users.users.cratedigger.extraGroups;
                startPre = nginxService.serviceConfig.ExecStartPre;
                reload = nginxService.serviceConfig.ExecReload;
              };
          in {
            dualStack =
              render true "/run/secrets/cratedigger.htpasswd";
            ipv4Only =
              render false "/run/secrets/cratedigger.htpasswd";
            insecureRecovery = render false null;
            alternateBasic =
              render false "/run/secrets/cratedigger-alternate.htpasswd";
          }
        '''
        result = subprocess.run(
            ["nix", "eval", "--impure", "--json", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        worlds = json.loads(result.stdout)
        dual = worlds["dualStack"]
        ipv4 = worlds["ipv4Only"]
        insecure = worlds["insecureRecovery"]
        alternate = worlds["alternateBasic"]
        self.assertEqual(dual["failures"], [])
        self.assertEqual(ipv4["failures"], [])
        self.assertEqual(insecure["failures"], [])
        self.assertEqual(alternate["failures"], [])
        self.assertEqual(
            dual["listen"],
            [
                {"addr": "127.0.0.1", "port": 8086},
                {"addr": "[::1]", "port": 8086},
            ],
        )
        self.assertEqual(
            ipv4["listen"], [{"addr": "127.0.0.1", "port": 8086}]
        )
        self.assertEqual(dual["hostName"], "music.example.test")
        self.assertIn("gateway_mode=basic", dual["gatewayPolicy"])
        self.assertTrue(dual["gatewayPolicy"].startswith("format=1\n"))
        self.assertIn(
            "gateway_credential_path=/run/secrets/cratedigger.htpasswd",
            dual["gatewayPolicy"],
        )
        self.assertIn("gateway_mode=insecure", insecure["gatewayPolicy"])
        self.assertIn(
            "gateway_credential_path=-", insecure["gatewayPolicy"]
        )
        self.assertIn(
            "gateway_marker_path=/run/cratedigger-web/gateway-policy-",
            insecure["gatewayPolicy"],
        )
        marker_pattern = re.compile(
            r"if \(!-f "
            r"(/run/cratedigger-web/gateway-policy-[0-9a-f]{64})\)"
        )
        basic_marker = marker_pattern.search(dual["gatewayExtra"])
        ipv4_marker = marker_pattern.search(ipv4["gatewayExtra"])
        insecure_marker = marker_pattern.search(insecure["gatewayExtra"])
        alternate_marker = marker_pattern.search(alternate["gatewayExtra"])
        self.assertIsNotNone(basic_marker)
        self.assertIsNotNone(ipv4_marker)
        self.assertIsNotNone(insecure_marker)
        self.assertIsNotNone(alternate_marker)
        assert basic_marker is not None
        assert ipv4_marker is not None
        assert insecure_marker is not None
        assert alternate_marker is not None
        self.assertEqual(basic_marker.group(1), ipv4_marker.group(1))
        self.assertNotEqual(basic_marker.group(1), insecure_marker.group(1))
        self.assertNotEqual(basic_marker.group(1), alternate_marker.group(1))
        self.assertNotEqual(insecure_marker.group(1), alternate_marker.group(1))
        self.assertEqual(
            dual["basicAuthFile"], "/run/secrets/cratedigger.htpasswd"
        )
        self.assertEqual(dual["rootBasicAuthFile"], None)
        self.assertEqual(dual["mergedBasicAuthFile"], None)
        self.assertEqual(
            dual["proxyPass"], "http://unix:/run/cratedigger-web/web.sock:"
        )
        self.assertNotIn("proxy_read_timeout", dual["gatewayExtra"])
        self.assertIn(
            "proxy_pass_request_headers off;", dual["gatewayExtra"]
        )
        self.assertIn(
            "proxy_set_header X-Cratedigger-Request-Channel browser;",
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header Content-Security-Policy "frame-ancestors \'none\'" always;',
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header X-Frame-Options "DENY" always;',
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header Cross-Origin-Resource-Policy "same-origin" always;',
            dual["gatewayExtra"],
        )
        self.assertEqual(
            dual["healthProxy"],
            "http://unix:/run/cratedigger-web/web.sock:/healthz",
        )
        self.assertIn('if ($request_uri != "/healthz")', dual["healthExtra"])
        self.assertIn("limit_except GET", dual["healthExtra"])
        self.assertIn("auth_basic off;", dual["healthExtra"])
        self.assertIn("proxy_http_version 1.0;", dual["healthExtra"])
        self.assertIn("proxy_pass_request_body off;", dual["healthExtra"])
        self.assertIn('proxy_set_header Connection close;', dual["healthExtra"])
        self.assertIn('proxy_set_header Content-Length "";', dual["healthExtra"])
        self.assertIn(
            'proxy_set_header Transfer-Encoding "";', dual["healthExtra"]
        )
        self.assertTrue(dual["rejectDefault"])
        self.assertEqual(dual["rejectConfig"], "return 444;")
        self.assertEqual(
            dual["socketListen"], ["/run/cratedigger-web/web.sock"]
        )
        self.assertEqual(dual["socketGroup"], "cratedigger-web")
        self.assertEqual(dual["socketMode"], "0660")
        for dependency in (
            "cratedigger-db-migrate.service",
            "cratedigger-web.socket",
        ):
            self.assertIn(dependency, dual["webAfter"])
            self.assertIn(dependency, dual["webRequires"])
        self.assertEqual(dual["webGroups"], ["cratedigger-web"])
        self.assertEqual(dual["webUser"], "cratedigger")
        self.assertEqual(dual["webGroup"], "cratedigger")
        self.assertTrue(dual["webStartPre"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-basic-auth-validate", dual["webStartPre"][0]
        )
        self.assertIn(
            "cratedigger-web-basic-auth-app-isolation",
            dual["webStartPre"][1],
        )
        self.assertFalse(dual["webStartPre"][1].startswith("+"))
        self.assertEqual(len(dual["webStartPre"]), 2)
        self.assertEqual(insecure["webStartPre"], [])
        self.assertTrue(dual["nginxEnableReload"])
        self.assertTrue(insecure["nginxEnableReload"])
        self.assertTrue(alternate["nginxEnableReload"])
        self.assertTrue(dual["nginxRestartIfChanged"])
        self.assertTrue(insecure["nginxRestartIfChanged"])
        self.assertTrue(alternate["nginxRestartIfChanged"])
        self.assertIn("cratedigger-web.socket", dual["nginxAfter"])
        self.assertIn("cratedigger-web.socket", dual["nginxWants"])
        self.assertNotIn("cratedigger-web.socket", dual["nginxRequires"])
        self.assertEqual(dual["nginxUnit"], ipv4["nginxUnit"])
        self.assertEqual(dual["nginxUnit"], insecure["nginxUnit"])
        self.assertEqual(dual["nginxUnit"], alternate["nginxUnit"])
        self.assertEqual(dual["nginxGroups"], ["cratedigger-web"])
        self.assertEqual(dual["nginxUser"], "nginx")
        self.assertEqual(dual["nginxGroup"], "nginx")
        self.assertIn("cratedigger-web", dual["nginxUserGroups"])
        self.assertIn("cratedigger-web", dual["applicationUserGroups"])
        self.assertTrue(dual["startPre"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-gateway-clear-start", dual["startPre"][0]
        )
        self.assertFalse(dual["startPre"][1].startswith("+"))
        self.assertIn(
            "cratedigger-web-nginx-effective-identity", dual["startPre"][1]
        )
        self.assertTrue(dual["startPre"][2].startswith("+"))
        self.assertIn("cratedigger-web-gateway-start", dual["startPre"][2])
        self.assertIn("nginx-pre-start", dual["startPre"][3])
        self.assertTrue(dual["reload"][0].startswith("+"))
        self.assertIn("cratedigger-web-gateway-prepare-reload", dual["reload"][0])
        self.assertIn("nginx", dual["reload"][1])
        self.assertIn("kill", dual["reload"][2])
        self.assertTrue(dual["reload"][3].startswith("+"))
        self.assertIn("cratedigger-web-gateway-finish-reload", dual["reload"][3])
        self.assertEqual(dual["startPre"], insecure["startPre"])
        self.assertEqual(dual["startPre"], alternate["startPre"])
        self.assertEqual(dual["reload"], insecure["reload"])
        self.assertEqual(dual["reload"], alternate["reload"])
        self.assertEqual(insecure["basicAuthFile"], None)
        self.assertIn(
            "cratedigger-web-gateway-clear-start", insecure["startPre"][0]
        )
        self.assertIn(
            "cratedigger-web-gateway-prepare-reload", insecure["reload"][0]
        )
        self.assertIn(
            "cratedigger-web-gateway-finish-reload", insecure["reload"][3]
        )

    def test_socket_activation_and_access_group_are_explicit(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn(
            'webRuntimeDirectory = "/run/cratedigger-web";',
            text,
        )
        self.assertIn(
            'webSocketPath = "${webRuntimeDirectory}/web.sock";',
            text,
        )
        self.assertIn("systemd.sockets.cratedigger-web", text)
        self.assertIn("listenStreams = [webSocketPath];", text)
        self.assertIn('SocketMode = "0660";', text)
        self.assertIn("SocketGroup = cfg.web.accessGroup;", text)
        self.assertIn(
            '"d /run/cratedigger-web 0750 root ${cfg.web.accessGroup} -"', text
        )
        self.assertIn('"cratedigger-web.socket"', text)
        self.assertIn(
            "${config.services.nginx.user}.extraGroups = "
            "[cfg.web.accessGroup];",
            text,
        )

    def test_gateway_is_exact_host_loopback_and_default_reject(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertNotIn("cfg.web.port", text)
        self.assertIn("services.nginx.virtualHosts", text)
        self.assertIn('addr = "127.0.0.1";', text)
        self.assertIn('addr = "[::1]";', text)
        self.assertIn("port = cfg.web.gatewayPort;", text)
        self.assertIn("serverName = webHostName;", text)
        self.assertIn("default = true;", text)
        self.assertIn("return 444;", text)
        self.assertIn('locations."= /healthz"', text)
        self.assertIn(':/healthz";', text)
        self.assertIn('if (\'\'$request_uri != "/healthz")', text)
        self.assertIn("limit_except GET", text)

    def test_gateway_reconstructs_only_reviewed_backend_headers(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("proxy_pass_request_headers off;", text)
        self.assertIn("proxy_set_header Host ${webHostName};", text)
        self.assertIn(
            "proxy_set_header X-Cratedigger-Request-Channel browser;", text
        )
        self.assertIn("proxy_set_header Content-Length ''$content_length;", text)
        self.assertIn("proxy_set_header Content-Type ''$content_type;", text)
        self.assertIn("proxy_set_header Accept ''$http_accept;", text)
        self.assertIn("proxy_set_header Range ''$http_range;", text)
        self.assertIn("proxy_set_header Origin ''$http_origin;", text)
        self.assertIn("proxy_set_header Referer ''$http_referer;", text)
        self.assertNotIn("proxy_set_header Authorization", text)
        self.assertNotIn("proxy_set_header Cookie", text)
        self.assertIn("Content-Security-Policy", text)
        self.assertIn("frame-ancestors 'none'", text)
        self.assertIn("X-Frame-Options", text)
        self.assertIn("Cross-Origin-Resource-Policy", text)

    def test_web_wrapper_uses_exact_canonical_https_origin(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_end = text.index(
            'writeShellScriptBin "cratedigger-youtube-ingest"', web_start
        )
        wrapper = text[web_start:web_end]
        self.assertIn(
            '--canonical-origin "https://${webHostName}"',
            wrapper,
        )

    def test_web_wrapper_passes_insecure_flag_only_for_explicit_mode(
        self,
    ) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_end = text.index(
            'writeShellScriptBin "cratedigger-youtube-ingest"', web_start
        )
        wrapper = text[web_start:web_end]

        self.assertIn(
            '${optionalString cfg.web.enableInsecure "--insecure-mode"}',
            wrapper,
        )
        self.assertEqual(wrapper.count("--insecure-mode"), 1)

    def test_basic_secret_is_runtime_only_and_checked_before_nginx_start(
        self,
    ) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("basicAuthFile = mkOption", text)
        self.assertNotIn("basicAuth = ", text)
        self.assertIn(
            'writeShellScript "cratedigger-web-basic-auth-validate"', text
        )
        self.assertIn(
            '"cratedigger-web-basic-auth-app-isolation"', text
        )
        self.assertIn(
            "the web application can read its gateway credential", text
        )
        self.assertIn("systemd.services.nginx = mkIf cfg.web.enable", text)
        self.assertIn("ExecStartPre = lib.mkBefore", text)
        self.assertIn("ExecReload = lib.mkBefore", text)
        self.assertIn("ExecReload = lib.mkAfter", text)
        marker_helpers = text[
            text.index("webGatewayClearMarkers =") :
            text.index('writeShellScript "cratedigger-web-gateway-start"')
        ]
        start_script = text[
            text.index('writeShellScript "cratedigger-web-gateway-start"') :
            text.index(
                'writeShellScript "cratedigger-web-gateway-prepare-reload"'
            )
        ]
        reload_prepare = text[
            text.index(
                'writeShellScript "cratedigger-web-gateway-prepare-reload"'
            ) :
            text.index(
                'writeShellScript "cratedigger-web-gateway-finish-reload"'
            )
        ]
        reload_finish = text[
            text.index(
                'writeShellScript "cratedigger-web-gateway-finish-reload"'
            ) :
            text.index("webNginxUserExtraGroups")
        ]
        self.assertLess(
            start_script.index("${webBasicAuthValidationScript}"),
            start_script.index("${webGatewayPublishMarker}"),
        )
        self.assertLess(
            reload_prepare.index("${webGatewayClearMarkers}"),
            reload_prepare.index("${webBasicAuthValidationScript}"),
        )
        self.assertNotIn("${webGatewayPublishMarker}", reload_prepare)
        self.assertIn("${webGatewayPublishMarker}", reload_finish)
        self.assertNotIn("webGatewayPendingMarker", text)
        self.assertNotIn("webGatewayStageMarker", text)
        self.assertIn("${pkgs.findutils}/bin/find", marker_helpers)

        self.assertIn(
            "${lib.escapeShellArg webRuntimeDirectory}", marker_helpers
        )
        self.assertIn("-maxdepth 1", marker_helpers)
        self.assertIn(
            '-name ${lib.escapeShellArg "gateway-policy-*"}',
            marker_helpers,
        )
        self.assertIn("-delete", marker_helpers)
        self.assertIn("-m 0440", marker_helpers)
        self.assertIn("-o root", marker_helpers)
        self.assertIn(
            "-g ${lib.escapeShellArg cfg.web.accessGroup}", marker_helpers
        )
        self.assertIn('"$gateway_marker_path"', marker_helpers)
        self.assertIn(
            "gateway_marker_path=${webGatewayActiveMarker}",
            text,
        )
        self.assertNotIn(
            ". ${lib.escapeShellArg webGatewayPolicyFile}", marker_helpers
        )
        self.assertIn("mapfile -t policy_lines", marker_helpers)
        self.assertIn("policy descriptor must contain exactly four lines", text)
        self.assertIn("gateway_policy_sha256", text)
        self.assertIn("webGatewayWriteReloadReceipt", text)
        self.assertIn("webGatewayReadReloadReceipt", text)
        self.assertIn("policy_sha256=", text)
        self.assertIn("gateway_credential_sha256=", text)
        self.assertIn("credential changed after reload validation", text)
        self.assertIn(
            "policy descriptor differs from the validated receipt",
            text,
        )
        self.assertIn(
            'configured_path="\'\'${1:-}"',
            text,
        )
        self.assertIn("realpath -e", text)
        self.assertIn("runuser -u", text)
        self.assertIn("${pkgs.acl}/bin/getfacl", text)
        self.assertIn("expected_target_acl", text)
        self.assertIn("only the base 0440 ACL", text)
        self.assertIn("must not be group/other writable", text)
        self.assertIn("must not have extended/default ACLs", text)
        self.assertIn("check_ancestors", text)
        self.assertIn("resolved credential target is inside /nix/store", text)

    def test_nginx_effective_identity_is_checked_before_gateway_readiness(
        self,
    ) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        identity_start = text.index(
            '"cratedigger-web-nginx-effective-identity"'
        )
        identity_script = text[
            identity_start : text.index("webGatewayClearMarkers =")
        ]
        self.assertIn("${pkgs.coreutils}/bin/id -u", identity_script)
        self.assertIn("${pkgs.coreutils}/bin/id -g", identity_script)
        self.assertIn("${pkgs.coreutils}/bin/id -G", identity_script)
        self.assertIn("webForbiddenAuthorityGroups", identity_script)
        self.assertIn("effective nginx UID must not be 0", identity_script)
        self.assertIn(
            "effective nginx group set contains forbidden",
            identity_script,
        )
        self.assertIn(
            "effective nginx group set lacks required accessGroup",
            identity_script,
        )

    def test_vm_tls_private_key_is_generated_outside_tracked_source(
        self,
    ) -> None:
        text = MODULE_VM_NIX.read_text(encoding="utf-8")
        self.assertNotRegex(
            text,
            r"-----BEGIN (?:EC |RSA |)PRIVATE KEY-----",
        )
        self.assertIn(
            'pkgs.runCommand "cratedigger-module-vm-tls"',
            text,
        )
        self.assertIn(
            "security.pki.certificateFiles = [publicTlsCertificate];",
            text,
        )


class TestImporterServiceContract(unittest.TestCase):
    def test_importer_wrapper_and_service_are_defined(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('writeShellScriptBin "cratedigger-importer"', text)
        self.assertIn("${src}/scripts/importer.py", text)
        self.assertIn("systemd.services.cratedigger-importer", text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${importerPkg}/bin/cratedigger-importer"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${pipelineDsn}"', text)
        self.assertIn("WorkingDirectory = cfg.stateDir", text)

    def test_importer_service_restarts_on_switch(self) -> None:
        """Deploy should restart the importer worker.

        Launch-fence recovery handles mid-job kills at startup; leaving a worker
        dead after switch-to-configuration is worse than restarting it.
        """
        text = MODULE_NIX.read_text(encoding="utf-8")
        # Find the importer service block and assert restartIfChanged=true
        # appears within it (not just somewhere in the file).
        importer_block_start = text.index("systemd.services.cratedigger-importer")
        importer_block_end = text.index(
            "systemd.services.cratedigger-import-preview-worker"
        )
        importer_block = text[importer_block_start:importer_block_end]
        self.assertIn("restartIfChanged = true", importer_block)

    def test_preview_worker_service_restarts_on_switch(self) -> None:
        """Same rationale as the importer worker.

        requeue_stale_import_preview_jobs handles mid-measurement kills at
        startup; deploy should not leave the preview worker dead.
        """
        text = MODULE_NIX.read_text(encoding="utf-8")
        preview_block_start = text.index(
            "systemd.services.cratedigger-import-preview-worker"
        )
        # The next service definition or end of the systemd.services block
        # bounds the preview-worker block. Use a sentinel that's safe.
        preview_block = text[preview_block_start:preview_block_start + 4000]
        self.assertIn("restartIfChanged = true", preview_block)

    def test_services_consume_one_immutable_runtime_config(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('configTemplate = pkgs.writeText "cratedigger-config.ini"', text)
        self.assertNotIn("renderConfigScript", text)
        self.assertNotIn("systemd.services.cratedigger-config-render", text)

    def test_preview_worker_wrapper_service_and_worker_count_are_defined(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('writeShellScriptBin "cratedigger-import-preview-worker"', text)
        self.assertIn("${src}/scripts/import_preview_worker.py", text)
        self.assertIn("systemd.services.cratedigger-import-preview-worker", text)
        # Preview is mandatory: service gated only on importer.enable.
        self.assertIn("mkIf cfg.importer.enable", text)
        self.assertIn("previewWorkers", text)
        self.assertIn("default = 2", text)
        self.assertIn("cfg.importer.previewWorkers >= 1", text)
        self.assertIn("services.cratedigger.importer.previewWorkers must be at least 1", text)
        self.assertIn('--workers ${toString cfg.importer.previewWorkers}', text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${pipelineDsn}"', text)


class TestSearchSchedulerConfigContract(unittest.TestCase):
    def test_page_size_preserves_capacity_for_both_cohorts(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn(
            "cfg.searchSettings.numberOfAlbumsToGrab >= 2",
            text,
        )
        self.assertIn(
            "services.cratedigger.searchSettings.numberOfAlbumsToGrab "
            "must be at least 2",
            text,
        )


class TestPinnedPackageSetContract(unittest.TestCase):
    """The runtime closure builds from cratedigger's own flake.lock, not the
    consumer's nixpkgs (tier-2 plan U2, R1 / KTD1).

    ``nix/module.nix`` must build its python env from ``cfg.packageSet``
    (defaulting to the ambient ``pkgs`` so the file stays importable
    standalone), and ``flake.nix`` must export ``nixosModules.default`` as a
    wrapper that pins ``packageSet`` to the flake's own locked nixpkgs. A
    consumer setting ``packageSet`` explicitly is the deliberate escape
    hatch — it forfeits the tested-closure guarantee.
    """

    def test_module_builds_package_from_packageSet(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("packageSet = mkOption", text)
        self.assertIn("cratedigger = cfg.packageSet.callPackage ./package.nix", text)
        self.assertNotIn("pkgs.callPackage ./package.nix", text)

    def test_flake_export_pins_packageSet_to_own_lock(self) -> None:
        text = FLAKE_NIX.read_text(encoding="utf-8")
        self.assertIn("nixosModules.default", text)
        self.assertIn("imports = [ ./nix/module.nix ];", text)
        self.assertIn(
            "services.cratedigger.packageSet = lib.mkDefault", text,
            "flake.nix must pin packageSet via mkDefault so a consumer's "
            "explicit packageSet (the escape hatch) still wins",
        )
        self.assertIn("pkgs.stdenv.hostPlatform.system", text)

    def test_moduleVm_consumes_the_wrapped_export(self) -> None:
        """The VM gate must exercise what consumers actually import."""
        text = FLAKE_NIX.read_text(encoding="utf-8")
        self.assertIn("cratediggerModule = self.nixosModules.default;", text)


class TestExternalBeetsRuntimeCapability(unittest.TestCase):
    """The public module consumes one externally owned Beets capability."""

    RUNTIME_FIELDS = (
        "package",
        "configDir",
        "expectedLibrary",
        "expectedDirectory",
        "expectedStateFile",
        "expectedSecretInclude",
    )

    @staticmethod
    def _nix_eval_json(expression: str) -> dict[str, object]:
        result = subprocess.run(
            ["nix", "eval", "--impure", "--json", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise TypeError(value)
        return value

    @staticmethod
    def _string_list(value: object) -> list[str]:
        if not isinstance(value, list):
            raise TypeError(value)
        strings = [item for item in value if isinstance(item, str)]
        if len(strings) != len(value):
            raise AssertionError(value)
        return strings

    def test_capability_assertions_cover_happy_missing_invalid_and_disabled(self) -> None:
        worlds = self._nix_eval_json(r'''
          let
            f = builtins.getFlake (toString ./.);
            lib = f.inputs.nixpkgs.lib;
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            runtime = {
              package = beetsPackage;
              configDir = "/etc/beets";
              expectedLibrary = "/srv/beets/beets-library.db";
              expectedDirectory = "/srv/music";
              expectedStateFile = "/var/lib/beets/state.pickle";
              expectedSecretInclude = "/run/secrets/beets.yaml";
              readinessUnits = [];
            };
            failures = candidate:
              let system = lib.nixosSystem {
                system = builtins.currentSystem;
                modules = [
                  f.nixosModules.default
                  ({ ... }: {
                    services.cratedigger = {
                      enable = true;
                      src = ./.;
                      packageSet = modulePkgs;
                      slskd.apiKeyFile = "/run/secrets/slskd-key";
                      slskd.downloadDir = "/srv/slskd";
                      pipelineDb.createLocally = true;
                      beets.runtime = candidate;
                      beets.validation = {
                        stagingDir = "/srv/incoming";
                        trackingFile = "/srv/incoming/tracking.jsonl";
                      };
                    };
                  })
                ];
              }; in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix
                      "services.cratedigger.beets.runtime"
                      assertion.message)
                  system.config.assertions);
            disabled = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [ f.nixosModules.default ];
            };
            identityFailures = user: group:
              let system = lib.nixosSystem {
                system = builtins.currentSystem;
                modules = [
                  f.nixosModules.default
                  ({ ... }: {
                    services.cratedigger = {
                      enable = true;
                      src = ./.;
                      packageSet = modulePkgs;
                      inherit user group;
                      slskd.apiKeyFile = "/run/secrets/slskd-key";
                      slskd.downloadDir = "/srv/slskd";
                      pipelineDb.createLocally = true;
                      beets.runtime = runtime;
                      beets.validation = {
                        stagingDir = "/srv/incoming";
                        trackingFile = "/srv/incoming/tracking.jsonl";
                      };
                    };
                  })
                ];
              }; in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix "services.cratedigger"
                      assertion.message)
                  system.config.assertions);
            defaultIdentity = let system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    packageSet = modulePkgs;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = runtime;
                    beets.validation = {
                      stagingDir = "/srv/incoming";
                      trackingFile = "/srv/incoming/tracking.jsonl";
                    };
                  };
                })
              ];
            }; in {
              user = system.config.services.cratedigger.user;
              group = system.config.services.cratedigger.group;
              serviceUser = system.config.systemd.services.cratedigger.serviceConfig.User;
              serviceGroup = system.config.systemd.services.cratedigger.serviceConfig.Group;
            };
          in {
            valid = failures runtime;
            missing = builtins.listToAttrs (map (field: {
              name = field;
              value = failures (builtins.removeAttrs runtime [ field ]);
            }) [
              "package" "configDir" "expectedLibrary" "expectedDirectory"
              "expectedStateFile" "expectedSecretInclude"
            ]);
            incompatiblePackage = failures (runtime // {
              package = beetsPackage // { pythonModule = null; };
            });
            invalidPaths = {
              configDir = failures (runtime // { configDir = "etc/beets"; });
              expectedLibrary = failures (runtime // {
                expectedLibrary = "/srv/beets/../beets-library.db";
              });
              expectedDirectory = failures (runtime // {
                expectedDirectory = "/srv//music";
              });
              expectedStateFile = failures (runtime // {
                expectedStateFile = "var/lib/beets/state.pickle";
              });
              expectedSecretInclude = failures (runtime // {
                expectedSecretInclude = "/run/secrets/./beets.yaml";
              });
            };
            rootPaths = {
              configDir = failures (runtime // { configDir = "/"; });
              expectedDirectory = failures (runtime // {
                expectedDirectory = "/";
              });
              expectedLibrary = failures (runtime // {
                expectedLibrary = "/beets-library.db";
              });
            };
            rootIdentity = identityFailures "root" "root";
            numericIdentity = identityFailures "0" "0";
            inherit defaultIdentity;
            disabled = {
              assertions = builtins.filter
                (assertion:
                  !assertion.assertion
                  && lib.hasPrefix
                    "services.cratedigger"
                    assertion.message)
                disabled.config.assertions;
              services = builtins.filter
                (name: lib.hasPrefix "cratedigger" name)
                (builtins.attrNames disabled.config.systemd.services);
            };
          }
        ''')
        self.assertEqual(worlds["valid"], [])
        missing = worlds["missing"]
        assert isinstance(missing, dict)
        for field in self.RUNTIME_FIELDS:
            messages = self._string_list(missing[field])
            self.assertTrue(messages, field)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message and "required" in message
                    for message in messages
                ),
                (field, messages),
            )
        incompatible_messages = self._string_list(worlds["incompatiblePackage"])
        self.assertTrue(
            any(
                "package.pythonModule must match services.cratedigger.packageSet.python3"
                in message
                for message in incompatible_messages
            ),
            incompatible_messages,
        )
        invalid_paths = worlds["invalidPaths"]
        assert isinstance(invalid_paths, dict)
        for field, value in invalid_paths.items():
            messages = self._string_list(value)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message
                    and "absolute normalized path" in message
                    for message in messages
                ),
                (field, messages),
            )
        root_paths = worlds["rootPaths"]
        assert isinstance(root_paths, dict)
        for field, value in root_paths.items():
            messages = self._string_list(value)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message
                    and "must not be /" in message
                    for message in messages
                ),
                (field, messages),
            )
        root_identity = self._string_list(worlds["rootIdentity"])
        self.assertTrue(
            any("guarded application identity" in message for message in root_identity),
            root_identity,
        )
        numeric_identity = self._string_list(worlds["numericIdentity"])
        self.assertTrue(
            any("guarded application identity" in message for message in numeric_identity),
            numeric_identity,
        )
        self.assertEqual(
            worlds["defaultIdentity"],
            {
                "user": "cratedigger",
                "group": "cratedigger",
                "serviceUser": "cratedigger",
                "serviceGroup": "cratedigger",
            },
        )
        self.assertEqual(worlds["disabled"], {"assertions": [], "services": []})

    def test_readiness_and_role_state_capabilities_evaluate(self) -> None:
        units = self._nix_eval_json(r'''
          let
            f = builtins.getFlake (toString ./.);
            lib = f.inputs.nixpkgs.lib;
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    packageSet = modulePkgs;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    web = {
                      enable = true;
                      hostName = "music.example.test";
                      enableInsecure = true;
                    };
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                      readinessUnits = [
                        "beets-config-ready.service"
                        "beets-secret-ready.service"
                      ];
                    };
                    beets.validation = {
                      stagingDir = "/srv/incoming";
                      trackingFile = "/srv/incoming/tracking.jsonl";
                    };
                  };
                })
              ];
            };
            unit = name: let value = system.config.systemd.services.${name}; in {
              after = value.after;
              wants = value.wants;
              requires = value.requires;
              bindReadOnlyPaths = value.serviceConfig.BindReadOnlyPaths or [];
              bindPaths = value.serviceConfig.BindPaths or [];
              readWritePaths = value.serviceConfig.ReadWritePaths or [];
            };
          in {
            main = unit "cratedigger";
            importer = unit "cratedigger-importer";
            preview = unit "cratedigger-import-preview-worker";
            web = unit "cratedigger-web";
          }
        ''')
        readiness = {
            "beets-config-ready.service",
            "beets-secret-ready.service",
        }
        typed_units: dict[str, dict[str, list[str]]] = {}
        for role, value in units.items():
            if not isinstance(value, dict):
                raise TypeError(value)
            unit = {
                field: self._string_list(value.get(field))
                for field in (
                    "after",
                    "wants",
                    "requires",
                    "bindReadOnlyPaths",
                    "bindPaths",
                    "readWritePaths",
                )
            }
            typed_units[role] = unit
            self.assertLessEqual(readiness, set(unit["after"]), role)
            if role == "main":
                self.assertLessEqual(readiness, set(unit["wants"]), role)
                self.assertTrue(
                    readiness.isdisjoint(unit["requires"]),
                    (role, unit["requires"]),
                )
            else:
                self.assertLessEqual(readiness, set(unit["requires"]), role)
            self.assertNotIn("/etc/beets", unit["readWritePaths"], role)
        state = "/var/lib/beets/state.pickle"
        for role in ("main", "preview", "web"):
            self.assertIn(f"-{state}", typed_units[role]["bindReadOnlyPaths"], role)
            self.assertNotIn(state, typed_units[role]["bindPaths"], role)
        self.assertIn(f"-{state}", typed_units["importer"]["bindPaths"])
        self.assertIn(f"-{state}", typed_units["importer"]["readWritePaths"])
        for role in ("importer", "web"):
            self.assertIn("-/srv/music", typed_units[role]["readWritePaths"], role)
            self.assertIn("-/srv/beets", typed_units[role]["readWritePaths"], role)
        for role in ("main", "preview"):
            self.assertNotIn("/srv/music", typed_units[role]["readWritePaths"], role)
            self.assertNotIn("/srv/beets", typed_units[role]["readWritePaths"], role)
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('missingOkExternalPath = path: map (value: "-${value}")', text)
        self.assertIn(
            'BindPaths = missingOkExternalPath cfg.beets.runtime.expectedStateFile;',
            text,
        )

    def test_closure_config_environment_and_removal_ratchets(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        package = PACKAGE_NIX.read_text(encoding="utf-8")
        shell = SHELL_NIX.read_text(encoding="utf-8")
        self.assertIn("beetsPackage = cfg.beets.runtime.package;", text)
        self.assertIn(
            "cratedigger = cfg.packageSet.callPackage ./package.nix { inherit beetsPackage; };",
            text,
        )
        self.assertIn('configTemplate = pkgs.writeText "cratedigger-config.ini"', text)
        for line in (
            "directory = ${cfg.beets.runtime.expectedDirectory}",
            "library = ${cfg.beets.runtime.expectedLibrary}",
            "config_dir = ${cfg.beets.runtime.configDir}",
            "state_file = ${cfg.beets.runtime.expectedStateFile}",
            "python = ${pythonEnv}/bin/python",
            "secret_include = ${cfg.beets.runtime.expectedSecretInclude}",
        ):
            self.assertIn(line, text)
        self.assertEqual(
            text.count('export BEETSDIR="${cfg.beets.runtime.configDir}"'),
            1,
        )
        self.assertEqual(
            text.count('export CRATEDIGGER_RUNTIME_CONFIG="${configTemplate}"'),
            1,
        )
        self.assertEqual(text.count("${beetsRuntimeEnvironment}"), 9)
        for wrapper in (
            "cratedigger",
            "cratedigger-importer",
            "cratedigger-import-preview-worker",
            "cratedigger-web",
            "cratedigger-check-beets-config",
        ):
            start = text.index(f'writeShellScriptBin "{wrapper}"')
            block = text[start:start + 1800]
            self.assertIn("${beetsRuntimeEnvironment}", block)
            self.assertIn('--config "${configTemplate}"', block)
            self.assertIn('--runtime-dir "${cfg.stateDir}"', block)
        self.assertIn("{ pkgs, beetsPackage }:", package)
        self.assertNotIn("beetsPackage ?", package)
        self.assertIn("beetsPackage = import ./beets.nix", shell)
        self.assertIn("inherit pkgs beetsPackage", shell)
        for obsolete in (
            "cfg.beets.package",
            "cfg.beets.config",
            "beetsSettings",
            "beetsConfigTemplate",
            "cratediggerBeet",
            'writeShellScriptBin "cratedigger-beet"',
            "discogsTokenFile",
            "discogsOperatorGroup",
            "defaultBeetsDbDir",
            "systemd.services.cratedigger-config-render",
            "services.cratedigger.beets.config.musicbrainz",
        ):
            self.assertNotIn(obsolete, text)
        self.assertNotIn('mktemp "$config_dir/.config.ini.XXXXXX"', text)
        self.assertNotIn('mktemp "$beets_dir/.config.yaml.XXXXXX"', text)
        self.assertIsNone(
            re.search(
                r"ExecStartPre\s*=\s*[^;]*checkBeetsConfigPkg",
                text,
                re.DOTALL,
            ),
            "the local checker must remain operator-invoked, never a systemd prestart",
        )


class TestJellyfinNotifierConfigContract(unittest.TestCase):
    def test_library_id_is_nullable_and_only_rendered_when_configured(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        jellyfin_options = text[text.index("jellyfin = {"):]
        self.assertIn("libraryId = mkOption {", jellyfin_options)
        self.assertIn("type = types.nullOr types.nonEmptyStr;", jellyfin_options)
        self.assertIn("default = null;", jellyfin_options)
        self.assertIn(
            '${optionalString (cfg.notifiers.jellyfin.libraryId != null) "library_id = ${cfg.notifiers.jellyfin.libraryId}"}',
            text,
        )


class TestCreateLocallyContract(unittest.TestCase):
    """pipelineDb.createLocally (tier-2 plan U7, R10/KTD5): local postgres
    with peer auth by construction — role + database named after cfg.user,
    socket DSN default, migrate unit ordered after NixOS setup completes."""

    def test_provisioning_block(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("services.postgresql = mkIf cfg.pipelineDb.createLocally", text)
        self.assertIn("ensureDatabases = [ cfg.user ];", text)
        self.assertIn("name = cfg.user;", text)
        self.assertIn("ensureDBOwnership = true;", text)
        self.assertIn('lib.mkDefault "postgresql:///${cfg.user}?host=/run/postgresql"', text)

    def test_migrate_ordered_after_local_postgres_setup(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn(
            'after = optional cfg.pipelineDb.createLocally "postgresql-setup.service";',
            text,
        )
        self.assertIn(
            'requires = optional cfg.pipelineDb.createLocally "postgresql-setup.service";',
            text,
        )

    def test_dsn_guard_gives_actionable_error(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("pipelineDsn =", text)
        self.assertIn("pipelineDb.createLocally = true", text)
        # No unit interpolates the raw nullable option.
        self.assertNotIn("${cfg.pipelineDb.dsn}", text)


class TestApiBaseThreading(unittest.TestCase):
    """One app API value; external Beets configuration is independent.

    Discogs is
    mirror-required with no public default (R13)."""

    def test_config_ini_renders_api_bases(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("[MusicBrainz]", text)
        self.assertIn("api_base = ${cfg.musicbrainz.apiBase}", text)
        self.assertIn("[Discogs]", text)

    def test_mb_default_is_public_and_discogs_has_none(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('default = "https://musicbrainz.org";', text)
        # discogs.apiBase: nullOr with null default — mirror-required.
        idx = text.index("discogs = {")
        self.assertIn("default = null;", text[idx:idx + 800])

    def test_web_wrapper_does_not_pass_api_base_flags(self) -> None:
        """Issue #497: config.ini is the ONE production source for the MB/
        Discogs API bases (read at startup via
        configure_api_bases_from_runtime_config()). The module must not also
        pass --mb-api/--discogs-api on the actual ExecStart invocation —
        that was a second path carrying the same two values, which is
        exactly the double-plumbing this consolidation removes. The flags
        themselves stay on web/server.py for a manual dev-only override,
        and a comment nearby is allowed to
        mention them by name — only the invocation argv is asserted here."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        exec_start = text.index("exec ${pyRunner} ${src}/web/server.py", web_start)
        exec_end = text.index("'';", exec_start)
        exec_block = text[exec_start:exec_end]
        self.assertNotIn("--mb-api", exec_block)
        self.assertNotIn("--discogs-api", exec_block)

    def test_api_base_does_not_derive_external_beets_config(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertNotIn("services.cratedigger.beets.config", text)
        self.assertNotIn("mbHost = lib.removePrefix", text)


class TestOwnedRedisContract(unittest.TestCase):
    def test_cratedigger_owns_local_redis_server_by_default(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("redis = {", text)
        self.assertIn('default = true;', text)
        self.assertIn("services.redis.servers.cratedigger", text)
        self.assertIn("enable = cfg.redis.enable", text)
        self.assertIn("bind = cfg.redis.host", text)
        self.assertIn("port = cfg.redis.port", text)
        self.assertIn('default = "3gb";', text)
        self.assertIn('maxmemory = cfg.redis.maxmemory', text)
        self.assertIn('"maxmemory-policy" = "allkeys-lru"', text)

    def test_peer_cache_config_is_rendered(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("[Peer Cache]", text)
        self.assertIn("redis_host = ${cfg.redis.host}", text)
        self.assertIn("redis_port = ${toString cfg.redis.port}", text)
        self.assertIn("ttl_seconds = ${toString cfg.peerCache.ttlSeconds}", text)
        self.assertIn("speed_ttl_seconds = ${toString cfg.peerCache.speedTtlSeconds}", text)
        self.assertIn("redis_connect_timeout_ms = ${toString cfg.peerCache.redisConnectTimeoutMs}", text)
        self.assertIn("redis_operation_timeout_ms = ${toString cfg.peerCache.redisOperationTimeoutMs}", text)

    def test_pipeline_and_web_are_ordered_after_owned_redis(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('redisServiceUnits = optional cfg.redis.enable "redis-cratedigger.service";', text)
        self.assertGreaterEqual(
            text.count("++ redisServiceUnits ++ beetsReadinessUnits"),
            2,
        )
        self.assertGreaterEqual(text.count("wants = redisServiceUnits;"), 1)

    def test_pipeline_wrapper_passes_redis_host_and_port(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('--redis-host "${cfg.redis.host}"', text)
        self.assertIn("--redis-port ${toString cfg.redis.port}", text)


class TestStandaloneCheckerPackageIdentity(unittest.TestCase):
    def test_wrapper_requires_and_threads_the_admitted_beets_package(self) -> None:
        wrappers = WRAPPERS_NIX.read_text(encoding="utf-8")
        flake = FLAKE_NIX.read_text(encoding="utf-8")
        self.assertIn("{ pkgs, beetsPackage,", wrappers)
        self.assertNotIn("beetsPackage ?", wrappers)
        self.assertIn("./package.nix { inherit beetsPackage; }", wrappers)
        self.assertIn("beetsPackage = import ./nix/beets.nix", flake)
        self.assertIn("inherit pkgs version beetsPackage;", flake)

    def test_wrapper_drops_inherited_pythonpath_and_flake_executes_checker(self) -> None:
        wrappers = WRAPPERS_NIX.read_text(encoding="utf-8")
        flake = FLAKE_NIX.read_text(encoding="utf-8")
        self.assertIn('export PYTHONPATH="${src}"', wrappers)
        self.assertNotIn('PYTHONPATH="${src}\'\'${PYTHONPATH', wrappers)
        self.assertIn("checkBeetsConfigPackageBoundary", flake)
        self.assertIn("cratedigger-check-beets-config-package-boundary", flake)
        self.assertIn("hostile inherited PYTHONPATH imported beets", flake)
        self.assertIn("/bin/cratedigger-check-beets-config", flake)


if __name__ == "__main__":
    unittest.main()
