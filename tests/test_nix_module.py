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

    def test_wrapper_selects_non_overridable_unix_socket(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        wrapper_start = text.index('writeShellScriptBin "pipeline-cli"')
        wrapper_end = text.index('writeShellScriptBin "pipeline-migrate"')
        wrapper = text[wrapper_start:wrapper_end]
        self.assertIn('main(api_socket="${webSocketPath}")', wrapper)
        self.assertNotIn("--api-base", wrapper)
        self.assertNotIn("127.0.0.1", wrapper)


class TestWebAuthenticationModuleContract(unittest.TestCase):
    """The enabled web surface has one fail-closed module-owned perimeter."""

    def test_basic_and_insecure_mode_matrix_is_evaluated(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            lib = f.inputs.nixpkgs.lib;
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
                beets.package = {
                  discogsTokenFile = "/run/secrets/discogs-token";
                  discogsOperatorGroup = "cratedigger-web";
                };
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
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
            nginxReverseUnrelatedGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.smokeping.members = [ "nginx" ];
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
                "primary group" in message
                for message in worlds["nginxPrimaryServiceGroup"]
            ),
            worlds["nginxPrimaryServiceGroup"],
        )
        self.assertEqual(worlds["nginxReverseUnrelatedGroup"], [])

    def test_injected_basic_path_cannot_render_toplevel(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
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
            render = enableIPv6:
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
                        web = {
                          enable = true;
                          hostName = "music.example.test";
                          basicAuthFile =
                            "/run/secrets/cratedigger.htpasswd";
                        };
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
                basicAuthFile = gateway.locations."/".basicAuthFile;
                proxyPass = gateway.locations."/".proxyPass;
                proxyExtra = gateway.locations."/".extraConfig;
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
                nginxGroups = nginxService.serviceConfig.SupplementaryGroups;
                nginxUserGroups =
                  system.config.users.users.${system.config.services.nginx.user}.extraGroups;
                applicationUserGroups =
                  system.config.users.users.cratedigger.extraGroups;
                startPre = nginxService.serviceConfig.ExecStartPre;
                reload = nginxService.serviceConfig.ExecReload;
              };
          in {
            dualStack = render true;
            ipv4Only = render false;
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
        self.assertEqual(dual["failures"], [])
        self.assertEqual(ipv4["failures"], [])
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
        self.assertEqual(
            dual["basicAuthFile"], "/run/secrets/cratedigger.htpasswd"
        )
        self.assertEqual(
            dual["proxyPass"], "http://unix:/run/cratedigger-web/web.sock:"
        )
        self.assertEqual(
            dual["healthProxy"],
            "http://unix:/run/cratedigger-web/web.sock:/healthz",
        )
        self.assertIn('if ($request_uri != "/healthz")', dual["healthExtra"])
        self.assertIn("limit_except GET", dual["healthExtra"])
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
        self.assertEqual(dual["nginxGroups"], ["cratedigger-web"])
        self.assertIn("cratedigger-web", dual["nginxUserGroups"])
        self.assertIn("cratedigger-web", dual["applicationUserGroups"])
        self.assertTrue(dual["startPre"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-basic-auth-validate", dual["startPre"][0]
        )
        self.assertIn("nginx-pre-start", dual["startPre"][1])
        self.assertTrue(dual["reload"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-basic-auth-validate", dual["reload"][0]
        )
        self.assertIn("nginx", dual["reload"][1])

    def test_socket_activation_and_access_group_are_explicit(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('webSocketPath = "/run/cratedigger-web/web.sock";', text)
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
        self.assertIn("systemd.services.nginx = mkIf cfg.web.enable", text)
        self.assertIn("ExecStartPre = lib.mkBefore", text)
        self.assertIn("ExecReload = lib.mkBefore", text)
        self.assertIn("realpath -e", text)
        self.assertIn("runuser -u", text)
        self.assertIn("${pkgs.acl}/bin/getfacl", text)
        self.assertIn("expected_target_acl", text)
        self.assertIn("only the base 0440 ACL", text)
        self.assertIn("must not be group/other writable", text)
        self.assertIn("must not have extended/default ACLs", text)
        self.assertIn("check_ancestors", text)
        self.assertIn("resolved credential target is inside /nix/store", text)


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

    def test_prestart_renders_config_atomically_for_parallel_services(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('mktemp "$config_dir/.config.ini.XXXXXX"', text)
        self.assertIn('mv -f "$tmp" "$config_dir/config.ini"', text)

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


class TestOwnedBeetsContract(unittest.TestCase):
    """Cratedigger owns the beet runtime (tier-2 plan U3, R4 / KTD3).

    One pinned beets derivation (nix/beets.nix, from cfg.packageSet, mirror
    patches as opt-in knobs) serves pythonEnv, the dev shell, the harness,
    and the cratedigger-beet wrapper. The wrapper pins BEETSDIR at the
    module's beets config dir so every consumer reads the same rendered
    config.
    """

    def test_module_threads_beets_env_from_packageSet(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("beetsEnv = import ./beets.nix {", text)
        self.assertIn("pkgs = cfg.packageSet;", text)
        self.assertIn("discogsMirrorUrl = cfg.beets.package.discogsMirrorUrl;", text)
        self.assertIn("lrclibUrl = cfg.beets.package.lrclibUrl;", text)
        self.assertIn(
            "cratedigger = cfg.packageSet.callPackage ./package.nix { beetsPackage = beetsEnv; };",
            text,
        )

    def test_cratedigger_beet_wrapper_pins_beetsdir(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('pkgs.writeShellScriptBin "cratedigger-beet"', text)
        self.assertIn('beetsConfigDir = "${cfg.stateDir}/beets";', text)
        self.assertIn('export BEETSDIR="${beetsConfigDir}"', text)
        self.assertIn("exec ${pythonEnv}/bin/beet", text)
        # On systemPackages as the canonical manual-ops binary.
        self.assertIn("cratediggerBeet pkgs.postgresql", text)

    def test_mirror_knobs_default_off(self) -> None:
        """Strangers get stock plugin behaviour — knobs are opt-in."""
        beets_nix = (REPO_ROOT / "nix" / "beets.nix").read_text(encoding="utf-8")
        self.assertIn("discogsMirrorUrl ? null", beets_nix)
        self.assertIn("lrclibUrl ? null", beets_nix)
        self.assertIn("--replace-fail", beets_nix)

    def test_beets_option_tree_is_consolidated(self) -> None:
        """Issue #497: ONE beets option tree —
        beets.{package,config,directory,validation} — not four separate
        beets/beetsConfig/beetsValidation/beetsDirectory groups. No aliases,
        no compat shims (scope.md): the old flat option names must be
        entirely gone. (``beetsConfigDir``/``beetsConfigTemplate`` are
        unrelated internal let-bindings for the rendered-config path/file —
        not part of the option surface — so they're excluded here.)"""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("beets = {", text)
        self.assertIn("package = {", text)
        self.assertIn("config = {", text)
        self.assertIn("validation = {", text)
        self.assertNotIn("cfg.beetsConfig", text)
        self.assertNotIn("beetsConfig = {", text)
        self.assertNotIn("services.cratedigger.beetsConfig", text)
        self.assertNotIn("beetsValidation", text)
        self.assertNotIn("beetsDirectory", text)


class TestRenderedBeetsConfigContract(unittest.TestCase):
    """The module owns beets config.yaml (tier-2 plan U4, R5).

    Rendered into ``${stateDir}/beets/config.yaml`` by the preStart script
    (atomic mv, same as config.ini). The data-loss invariant
    ``import.duplicate_keys.album: [mb_albumid, discogs_albumid]`` is a
    hard-coded literal — no option may expose it (Palo Santo guard moved to
    first line of defense). The plugin list is fixed, not operator-blankable
    (the zero-candidates guard: ``musicbrainz`` must be present).
    """

    PRODUCTION_PLUGINS = (
        "musicbrainz discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    )

    def test_duplicate_keys_is_a_literal_under_import(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('duplicate_keys = {', text)
        self.assertIn('album = ["mb_albumid" "discogs_albumid"];', text)
        self.assertIn('item = ["artist" "title"];', text)
        # No option surface for it — the literal lives in the render
        # attrset, not in an mkOption default someone can override.
        self.assertNotIn("duplicateKeys", text)

    def test_plugin_list_is_fixed_and_contains_musicbrainz(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn(f'plugins = "{self.PRODUCTION_PLUGINS}";', text)

    def test_cratedigger_sidecar_is_exact_beets_clutter(self) -> None:
        """Beet remove may prune our sidecar, never arbitrary leftovers."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        match = re.search(r"clutter = \[(.*?)\];", text, re.DOTALL)
        self.assertIsNotNone(match)
        assert match is not None
        clutter = match.group(1)
        self.assertIn('"cratedigger.json"', clutter)
        self.assertNotIn('"cratedigger.*"', clutter)

    def test_permissions_plugin_configured_with_media_server_friendly_modes(
        self,
    ) -> None:
        """Issue #570 defect 1: beets' native ``fetchart`` writes album art
        via ``mkstemp`` (forces 0600) then renames it into place — nothing
        else chmods it, so art is unreadable by media servers.
        ``fix_library_modes`` (lib/permissions.py) deliberately touches
        directories only, never files, so the ``permissions`` plugin (its
        ``art_set -> fix_art`` listener) is what covers both initial import
        AND manual ``beet fetchart`` re-fetches.

        ``dir`` is ``02775`` (setgid + group-writable), not a plain
        ``0775`` — setgid so child dirs beets creates underneath inherit
        the library group, group-writable so gid-consumers (Jellyfin) can
        write alongside the media. This mirrors ``lib.permissions.
        LIBRARY_DIR_MODE`` (``0o2775``); a bare ``0775`` here would leave
        beets itself stripping the setgid bit on every import."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("permissions", self.PRODUCTION_PLUGINS.split())
        self.assertIn(
            'permissions = {\n      file = "0664";\n      dir = "02775";\n    };',
            text,
        )

    def test_config_yaml_rendered_atomically_into_beetsdir(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('mktemp "$beets_dir/.config.yaml.XXXXXX"', text)
        self.assertIn('mv -f "$tmp_yaml" "$beets_dir/config.yaml"', text)

    def test_discogs_token_file_pattern(self) -> None:
        """Real token access is explicit for service and operator principals."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("discogsTokenFile", text)
        # The default remains service-only. An explicit operator group uses
        # group-read without exposing the token to unrelated users.
        self.assertIn("discogsOperatorGroup", text)
        self.assertIn('chmod 0400 "$tmp_secrets"', text)
        self.assertIn('chmod 0440 "$tmp_secrets"', text)
        self.assertIn('chgrp', text)
        self.assertIn('mv -f "$tmp_secrets" "$beets_dir/secrets.yaml"', text)
        self.assertIn('rm -f "$beets_dir/secrets.yaml"', text)
        self.assertIn("extraGroups = optional", text)
        # Fail-loud on unreadable/empty token: a bare assignment trips
        # set -e on cat failure, and an empty token is rejected (an empty
        # user_token re-enables the discogs interactive OAuth at load).
        self.assertIn('discogs_token="$(', text)
        self.assertIn('if [ -z "$discogs_token" ]; then', text)
        # Tokenless default: non-empty placeholder suppresses the discogs
        # plugin's interactive OAuth at load (R7).
        self.assertIn("cratedigger-placeholder-token", text)

    def test_beets_runtime_keys_rendered_into_config_ini(self) -> None:
        """[Beets] carries the one DB/root pair plus the pinned runtime."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("directory = ${cfg.beets.config.directory}", text)
        self.assertNotIn("services.cratedigger.beets.directory", text)
        self.assertIn("library = ${cfg.beets.config.library}", text)
        self.assertIn("config_dir = ${beetsConfigDir}", text)
        self.assertIn("python = ${pythonEnv}/bin/python", text)

    def test_default_library_has_a_module_owned_dedicated_parent(self) -> None:
        """#847: fresh hardened installs need no Music-root DB write grant."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('defaultBeetsDbDir = "${canonicalStateDir}-beets-db";', text)
        self.assertIn('default = "${defaultBeetsDbDir}/beets-library.db";', text)
        self.assertIn('"d ${defaultBeetsDbDir} 2775 ${cfg.user} ${cfg.group} -"', text)

    def test_state_dir_requires_a_canonical_sibling_boundary(self) -> None:
        """#847: a trailing slash would place the sibling DB inside stateDir."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("canonicalStateDirIsValid =", text)
        self.assertIn('!lib.hasSuffix "/" canonicalStateDir', text)
        self.assertIn("assertion = canonicalStateDirIsValid;", text)

    def test_trailing_state_dir_fails_nix_evaluation(self) -> None:
        """#847: `/srv/cratedigger/` must not turn the DB sibling into a child."""
        expression = '''
          let
            f = builtins.getFlake (toString ./.);
            system = f.inputs.nixpkgs.lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    stateDir = "/srv/cratedigger/";
                    slskd.apiKeyFile = "/tmp/cratedigger-test-key";
                    slskd.downloadDir = "/srv/cratedigger-downloads";
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
        self.assertIn(
            "services.cratedigger.stateDir must be an absolute normalized non-root path without a trailing slash",
            result.stderr,
        )

    def test_web_wrapper_exports_beetsdir(self) -> None:
        """cratedigger-web imports beets in-process (beets_distance) —
        BEETSDIR must point it at the module-rendered config."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_block = text[web_start:web_start + 1200]
        self.assertIn('export BEETSDIR="${beetsConfigDir}"', web_block)
        self.assertNotIn("--beets-db", web_block)

    def test_musicbrainz_defaults_are_public(self) -> None:
        """Stranger default = public MB (functional-but-slow, R13/U4 leg)."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('default = "musicbrainz.org";', text)
        # ratelimit 1 for public MB; the mirror override arrives via U6.
        self.assertIn("ratelimit", text)


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
    socket DSN default, migrate unit ordered after postgresql.service."""

    def test_provisioning_block(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("services.postgresql = mkIf cfg.pipelineDb.createLocally", text)
        self.assertIn("ensureDatabases = [ cfg.user ];", text)
        self.assertIn("name = cfg.user;", text)
        self.assertIn("ensureDBOwnership = true;", text)
        self.assertIn('lib.mkDefault "postgresql:///${cfg.user}?host=/run/postgresql"', text)

    def test_migrate_ordered_after_local_postgres(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('after = optional cfg.pipelineDb.createLocally "postgresql.service";', text)
        self.assertIn('requires = optional cfg.pipelineDb.createLocally "postgresql.service";', text)

    def test_dsn_guard_gives_actionable_error(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("pipelineDsn =", text)
        self.assertIn("pipelineDb.createLocally = true", text)
        # No unit interpolates the raw nullable option.
        self.assertNotIn("${cfg.pipelineDb.dsn}", text)


class TestApiBaseThreading(unittest.TestCase):
    """One MB value, three consumers (tier-2 plan U6 / KTD6); Discogs is
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

    def test_beets_musicbrainz_derives_from_the_one_value(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("services.cratedigger.beets.config.musicbrainz = let", text)
        self.assertIn('mbHost = lib.removePrefix "https://" (lib.removePrefix "http://" cfg.musicbrainz.apiBase);', text)
        self.assertIn("ratelimit = lib.mkDefault (if mbPublic then 1 else 100);", text)


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
        self.assertIn('after = ["cratedigger-db-migrate.service"] ++ redisServiceUnits;', text)
        self.assertIn('wants = redisServiceUnits;', text)
        self.assertIn('after = ["cratedigger-db-migrate.service"] ++ redisServiceUnits;', text)
        self.assertIn('wants = redisServiceUnits;', text)

    def test_pipeline_wrapper_passes_redis_host_and_port(self) -> None:
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('--redis-host "${cfg.redis.host}"', text)
        self.assertIn("--redis-port ${toString cfg.redis.port}", text)


if __name__ == "__main__":
    unittest.main()
