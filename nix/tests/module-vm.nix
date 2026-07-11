# NixOS VM test for the upstream cratedigger module — the STRANGER-BOOT
# gate (tier-2 plan U10, R12): a competent NixOS stranger's first boot,
# every `nix flake check`.
#
# Posture: pipelineDb.createLocally = true (module-provisioned postgres,
# peer auth, no hand-rolled DB block), beets.validation ON, VM-local beets
# paths, NO mirror knobs (public-MB defaults), no secrets beyond the
# stubbed slskd key.
#
# Verifies: migrate green behind module-owned postgres ordering; rendered
# config.ini (api keys as *File paths, [Beets] runtime keys, api_base
# defaults, socket DSN with no credentials) AND rendered beets config.yaml
# (duplicate_keys nesting, fixed plugin list, public-MB, placeholder
# token); cratedigger-beet loads the full plugin set; web serves;
# youtube-ingest + unfindable units structurally sound.
#
# Does NOT exercise: slskd interaction, real downloads, real imports —
# those need heavyweight fixtures that belong in the python suite.
{ pkgs, system, cratediggerModule, cratediggerSrc }:

let
  # Parses the module-rendered beets config and asserts the invariants that
  # have bitten in production: duplicate_keys nesting (Palo Santo guard),
  # the fixed plugin list with musicbrainz present (zero-candidates guard),
  # public-MB stranger defaults, and the tokenless placeholder (no OAuth).
  pyWithYaml = pkgs.python3.withPackages (ps: [ ps.pyyaml ]);
  checkRenderedBeetsConfig = pkgs.writeText "check-rendered-beets-config.py" ''
    import yaml

    with open("/var/lib/cratedigger/beets/config.yaml") as f:
        cfg = yaml.safe_load(f)

    dk = cfg["import"]["duplicate_keys"]
    assert dk["album"] == ["mb_albumid", "discogs_albumid"], dk
    assert dk["item"] == ["artist", "title"], dk

    plugins = cfg["plugins"].split()
    expected = (
        "musicbrainz discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    ).split()
    assert plugins == expected, plugins

    mb = cfg["musicbrainz"]
    assert mb["host"] == "musicbrainz.org", mb
    assert mb["https"] is True, mb
    assert mb["ratelimit"] == 1, mb

    # Tokenless stranger posture: placeholder token, no secrets include.
    assert cfg["discogs"]["user_token"] == "cratedigger-placeholder-token"
    assert "include" not in cfg, cfg.get("include")

    # Path-affecting keys present and production-shaped.
    assert cfg["asciify_paths"] is True
    assert "short_mbid" in cfg["paths"]["default"], cfg["paths"]

    print("BEETS_CONFIG_OK")
  '';
in
pkgs.testers.nixosTest {
  name = "cratedigger-module-vm";

  nodes.machine = { config, lib, pkgs, ... }: {
    imports = [ cratediggerModule ];

    # Fake slskd API key — never actually called because healthCheck is off.
    environment.etc."cratedigger/slskd-api-key" = {
      text = "test-api-key-do-not-use\n";
      mode = "0400";
    };

    # Stub beets library DB so cratedigger-web can open it read-only.
    environment.etc."cratedigger/beets.db" = {
      text = "";
      mode = "0644";
    };

    services.cratedigger = {
      enable = true;
      src = cratediggerSrc;
      slskd = {
        apiKeyFile = "/etc/cratedigger/slskd-api-key";
        downloadDir = "/var/lib/cratedigger-downloads";
      };
      # Stranger posture (U7/R10): the module provisions PostgreSQL —
      # role + database named after cfg.user (root here), unix-socket
      # peer auth, DSN defaulted to the socket. No hand-rolled postgres
      # block, no manual unit ordering, no password material anywhere.
      pipelineDb.createLocally = true;
      # Stranger posture (U10/R12): beets validation ON — the full
      # rendered-config surface (config.ini beets keys + config.yaml) is
      # what a real first boot produces.
      beets.validation = {
        enable = true;
        stagingDir = "/var/lib/cratedigger-staging";
        trackingFile = "/var/lib/cratedigger-staging/tracking.jsonl";
      };
      # Stranger-set beets paths (VM-local). The library's PARENT dir must
      # exist or `beet` prompts "Create it (Y/n)?" interactively — which
      # blocks forever under the test driver's backdoor shell. stateDir
      # exists by tmpfiles, so the library parent is guaranteed.
      beets.config = {
        directory = "/var/lib/cratedigger-music";
        library = "/var/lib/cratedigger/beets-library.db";
      };
      web = {
        enable = true;
        beetsDb = "/etc/cratedigger/beets.db";
      };
      # Enable the YouTube-rescue ingest worker so its unit is rendered.
      # We only assert structural properties (dependencies, PATH, lock
      # contention) — the worker process itself starts but stays idle
      # because no download_log source='youtube' outcome='youtube_running'
      # rows exist in the test DB.
      youtubeIngest.enable = true;
      # Host-specific VPN-NIC bind address (KTD9). The VM's --once run
      # never invokes yt-dlp (empty queue), so this is exercised only at
      # the wrapper-render seam: we assert the flag lands in the ExecStart.
      youtubeIngest.sourceAddress = "10.0.2.15";
      # Exercise the configured branch of Jellyfin's targeted refresh option.
      # The Python config/notifier tests separately pin the null -> full-library
      # fallback.
      notifiers.jellyfin.libraryId = "music-library-item-id";
      timer.enable = false;
      healthCheck.enable = false;
    };

    # NO manual postgres ordering: the module owns
    # cratedigger-db-migrate's after/requires on postgresql.service when
    # createLocally is set, and every app unit requires the migrate unit —
    # transitively serialising first boot behind PostgreSQL.

    # Speed up the VM
    virtualisation.memorySize = 2048;
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("postgresql.service")
    machine.wait_for_unit("redis-cratedigger.service")
    machine.wait_for_unit("cratedigger-db-migrate.service")

    # The migrator is a oneshot with RemainAfterExit=true — confirm it landed
    # in active (exited), not failed.
    state = machine.succeed("systemctl is-active cratedigger-db-migrate.service").strip()
    assert state == "active", f"migrator unit not active: {state}"

    # Migrations recorded
    out = machine.succeed("sudo -u postgres psql root -At -c 'SELECT version FROM schema_migrations ORDER BY version'")
    versions = [v.strip() for v in out.strip().split() if v.strip()]
    assert "1" in versions, f"baseline migration missing, got {versions}"
    assert "2" in versions, f"002 migration missing, got {versions}"

    # config.ini rendered with api_key_file pointing at the out-of-band secret,
    # not the plaintext key itself (issue #117). The cratedigger ExecStart will
    # fail because there's no real slskd, but ExecStartPre (preStartScript)
    # runs first and writes the config — that's all we need to assert here.
    machine.succeed("systemctl start cratedigger.service || true")
    machine.succeed("test -f /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'api_key_file = /etc/cratedigger/slskd-api-key' /var/lib/cratedigger/config.ini")
    # The secret itself must NEVER appear in config.ini — that's the whole fix.
    machine.fail("grep -q 'test-api-key-do-not-use' /var/lib/cratedigger/config.ini")
    # config.ini is now world-readable since it contains no secrets.
    mode = machine.succeed("stat -c %a /var/lib/cratedigger/config.ini").strip()
    assert mode == "644", f"config.ini should be 0644, got {mode}"
    machine.succeed("grep -q 'enabled = True' /var/lib/cratedigger/config.ini")  # beets validation ON (stranger posture)
    machine.succeed("grep -q '\\[Quality Ranks\\]' /var/lib/cratedigger/config.ini")
    # U5 (tier-2): the module renders the beets runtime keys so every
    # beets subprocess resolves the pinned interpreter + rendered config.
    machine.succeed("grep -q 'config_dir = /var/lib/cratedigger/beets' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'beet_binary = /nix/store/' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'python = /nix/store/' /var/lib/cratedigger/config.ini")
    # U6 (tier-2): one MB value, rendered for the python consumers too.
    machine.succeed("grep -q 'api_base = https://musicbrainz.org' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '\\[Peer Cache\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'redis_host = 127.0.0.1' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'ttl_seconds = 604800' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^library_id = music-library-item-id$' /var/lib/cratedigger/config.ini")
    machine.succeed("${pkgs.redis}/bin/redis-cli -p 6379 CONFIG GET maxmemory-policy | grep -q allkeys-lru")
    machine.succeed("systemctl show -p After cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p After cratedigger-web.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger-web.service | grep -q redis-cratedigger.service")

    # Deploy-kill-migrate fix: cratedigger.service is timer-driven and
    # restartIfChanged=false, so it must NOT Requires= the migrate unit --
    # that unit's ExecStart store path changes on every deploy, and a
    # Requires= edge would propagate its every-switch restart as a SIGTERM
    # to a mid-flight cycle. It still Wants=+After= the migrate unit (so it
    # normally starts behind a first-boot migration) and gates on schema
    # currency itself at startup instead (lib/migrator.py
    # assert_schema_current, exercised by the Python suite).
    machine.succeed("systemctl show -p Wants cratedigger.service | grep -q cratedigger-db-migrate.service")
    machine.fail("systemctl show -p Requires cratedigger.service | grep -q cratedigger-db-migrate.service")

    # Counterpart pins: the long-running workers restart on switch anyway
    # (restartIfChanged=true), so they MUST keep the hard Requires= gate --
    # for them it's harmless AND it's their only "failed migration blocks
    # start" guarantee (they have no assert_schema_current startup gate).
    # A future edit flipping one of these to Wants= would silently lose
    # that guarantee. (youtube-ingest's identical pin lives in its U7
    # block further down.)
    machine.succeed("systemctl show -p Requires cratedigger-web.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-importer.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-import-preview-worker.service | grep -q cratedigger-db-migrate.service")

    # Peer auth by construction (KTD5): the socket DSN carries no
    # password, and none exists in the rendered config or unit files.
    machine.succeed("grep -q 'dsn = postgresql:///root?host=/run/postgresql' /var/lib/cratedigger/config.ini")
    # (password_file *keys* are fine — they are the #117 *File pattern;
    # what must not exist is an actual credential value.)
    machine.fail("grep -Eqi 'password *= *[^ ]|pgpassword' /var/lib/cratedigger/config.ini")
    machine.succeed(
        "systemctl show cratedigger-db-migrate -p Environment"
        " | grep -q 'PIPELINE_DB_DSN=postgresql:///root?host=/run/postgresql'"
    )

    # Module-owned first-boot ordering (U7/U10): migrate is serialised
    # behind PostgreSQL; every app unit requires migrate — the stranger's
    # first boot cannot race the database.
    machine.succeed("systemctl show -p After cratedigger-db-migrate.service | grep -q postgresql.service")
    machine.succeed("systemctl show -p Requires cratedigger-db-migrate.service | grep -q postgresql.service")

    # pipeline-cli on PATH and connects (over the peer-auth socket)
    machine.succeed("pipeline-cli list wanted")

    # Web UI listens
    machine.wait_for_unit("cratedigger-web.service")
    machine.wait_for_open_port(8085)
    machine.succeed("curl -sf http://127.0.0.1:8085/ > /dev/null")

    # U13: cratedigger-unfindable.service + .timer exist and are
    # ordered correctly. Structural assertions only — we do NOT fire
    # the unit because slskd is not available in the VM. This guards
    # the module against future deployments that forget to render the
    # detection unit, or render it without the migrate dependency.
    machine.succeed("systemctl cat cratedigger-unfindable.service > /dev/null")
    machine.succeed("systemctl cat cratedigger-unfindable.timer > /dev/null")
    # After= must include the db-migrate unit so the detection job normally
    # runs behind a first-boot migration. Same deploy-kill-migrate fix as
    # cratedigger.service above: Wants=, NOT Requires= (restartIfChanged
    # here is false too, so a switch-time migrate restart must not
    # SIGTERM a mid-flight run) -- the fail-loud assert_schema_current
    # startup gate re-provides the "never runs against an un-migrated
    # schema" guarantee.
    machine.succeed("systemctl show -p After cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Wants cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    machine.fail("systemctl show -p Requires cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    # Timer is enabled (wantedBy timers.target) — the daily fire is
    # not opt-in. ``systemctl is-enabled`` returns "enabled" for units
    # wired into timers.target.
    enabled = machine.succeed("systemctl is-enabled cratedigger-unfindable.timer").strip()
    assert enabled == "enabled", f"unfindable timer not enabled: {enabled}"

    # U7: cratedigger-youtube-ingest.service. The worker is long-lived
    # (Type=simple); we verify it comes up active, idle (no pending
    # jobs in the test DB), and that the structural contracts hold:
    #
    #   - migrate-dependency ordering (Requires + After)
    #   - the wrapper exports `yt-dlp` onto the worker's PATH (worker-
    #     specific, NOT on the shared runtime path)
    #   - the per-process temp dir is created by systemd-tmpfiles
    #   - second-instance start exits 0 fast (advisory-lock contention)
    machine.wait_for_unit("cratedigger-youtube-ingest.service")
    state = machine.succeed("systemctl is-active cratedigger-youtube-ingest.service").strip()
    assert state == "active", f"youtube-ingest unit not active: {state}"

    machine.succeed("systemctl show -p After cratedigger-youtube-ingest.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-youtube-ingest.service | grep -q cratedigger-db-migrate.service")

    # The wrapper exports yt-dlp's bin onto PATH for the worker process.
    # The wrapper binary itself is on systemPackages PATH; grep its body
    # for the yt-dlp path-prepend so we know the worker process's PATH
    # will resolve the binary.
    machine.succeed("grep -q 'yt-dlp.*bin' $(command -v cratedigger-youtube-ingest)")

    # The configured sourceAddress renders into the worker's ExecStart so
    # yt-dlp binds its client socket to the VPN-routed NIC (egress hardening).
    machine.succeed("grep -q -- '--source-address \"10.0.2.15\"' $(command -v cratedigger-youtube-ingest)")

    # The drainer's per-process temp dir was created by systemd-tmpfiles
    # with the same ownership as the cratedigger user.
    machine.succeed("test -d /var/lib/cratedigger/youtube-ingest-temp")

    # Advisory-lock contention: starting a second instance manually
    # must exit 0 (clean — duplicate-start is expected, not a crash)
    # and not respawn. The systemd unit holds the lock; this invocation
    # fails to acquire and returns 0 immediately.
    machine.succeed("cratedigger-youtube-ingest --once")

    # U3+U4 (tier-2): cratedigger owns the beet runtime AND its config.
    # The module rendered config.yaml into BEETSDIR during ExecStartPre
    # (the `systemctl start cratedigger.service` above); cratedigger-beet
    # resolves it and loads the FULL production plugin set — including a
    # tokenless discogs (placeholder token = no interactive OAuth, no
    # network at load).
    machine.succeed("command -v cratedigger-beet")
    machine.succeed("test -f /var/lib/cratedigger/beets/config.yaml")
    mode = machine.succeed("stat -c %a /var/lib/cratedigger/beets/config.yaml").strip()
    assert mode == "644", f"config.yaml should be 0644, got {mode}"
    # No token file configured -> no secrets.yaml materialized.
    machine.fail("test -e /var/lib/cratedigger/beets/secrets.yaml")

    # Semantic assertions on the rendered YAML (duplicate_keys nesting,
    # plugin list, public-MB defaults, placeholder token).
    machine.succeed("${pyWithYaml}/bin/python3 ${checkRenderedBeetsConfig}")

    version_out = machine.succeed("cratedigger-beet version")
    plugins_line = next(
        line for line in version_out.splitlines() if line.startswith("plugins:")
    )
    loaded = {p.strip() for p in plugins_line.split(":", 1)[1].split(",")}
    for plugin in (
        "musicbrainz discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    ).split():
        assert plugin in loaded, f"plugin {plugin} not loaded: {version_out}"
    # Tokenless/stranger posture: config loads without crash or prompt.
    machine.succeed("cratedigger-beet config > /dev/null")
  '';
}
