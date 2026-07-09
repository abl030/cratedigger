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
we don't want to depend on ``nix eval`` at test time — a text grep
against the source file is enough for the invariants we care about.
"""

from __future__ import annotations

import re
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

        requeue_running_import_jobs handles mid-job kills at startup; leaving a
        worker dead after switch-to-configuration is worse than restarting it.
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
        """Real token via issue #117 *File include; placeholder otherwise."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("discogsTokenFile", text)
        # secrets.yaml materialized 0400 from the *File — the token itself
        # never lands in the world-readable config.yaml.
        self.assertIn('chmod 0400 "$tmp_secrets"', text)
        self.assertIn('mv -f "$tmp_secrets" "$beets_dir/secrets.yaml"', text)
        # Fail-loud on unreadable/empty token: a bare assignment trips
        # set -e on cat failure, and an empty token is rejected (an empty
        # user_token re-enables the discogs interactive OAuth at load).
        self.assertIn('discogs_token="$(', text)
        self.assertIn('if [ -z "$discogs_token" ]; then', text)
        # Tokenless default: non-empty placeholder suppresses the discogs
        # plugin's interactive OAuth at load (R7).
        self.assertIn("cratedigger-placeholder-token", text)

    def test_beets_runtime_keys_rendered_into_config_ini(self) -> None:
        """[Beets] config_dir / beet_binary / python — the U5 seam values
        every beets subprocess resolves (BEETSDIR + pinned interpreter)."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn("config_dir = ${beetsConfigDir}", text)
        self.assertIn("beet_binary = ${pythonEnv}/bin/beet", text)
        self.assertIn("python = ${pythonEnv}/bin/python", text)

    def test_web_wrapper_exports_beetsdir(self) -> None:
        """cratedigger-web imports beets in-process (beets_distance) —
        BEETSDIR must point it at the module-rendered config."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_block = text[web_start:web_start + 1200]
        self.assertIn('export BEETSDIR="${beetsConfigDir}"', web_block)

    def test_musicbrainz_defaults_are_public(self) -> None:
        """Stranger default = public MB (functional-but-slow, R13/U4 leg)."""
        text = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn('default = "musicbrainz.org";', text)
        # ratelimit 1 for public MB; the mirror override arrives via U6.
        self.assertIn("ratelimit", text)


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
