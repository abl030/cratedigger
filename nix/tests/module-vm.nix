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

    # Path-affecting keys present and production-shaped. path_disambig is
    # the never-empty aunique disambiguator (Passenger collision fix,
    # 2026-07-18) — it must appear in the template AND be defined as an
    # inline album field, or same-key sibling pressings collide into one
    # folder again.
    assert cfg["asciify_paths"] is True
    assert "path_disambig" in cfg["paths"]["default"], cfg["paths"]
    assert "path_disambig" in cfg["album_fields"], cfg.get("album_fields")

    print("BEETS_CONFIG_OK")
  '';
in
pkgs.testers.nixosTest {
  name = "cratedigger-module-vm";

  nodes.machine = { config, lib, pkgs, ... }: let
    configHoldGate = pkgs.writeShellScript "cratedigger-test-config-hold" ''
      test ! -e /run/cratedigger-test-config-hold
    '';
    deployHoldTool = pkgs.writeShellScriptBin "cratedigger-deploy-hold" ''
      exec ${pkgs.python3}/bin/python3 \
        ${cratediggerSrc}/scripts/cratedigger_deploy_hold.py "$@"
    '';
    metadataGateTool = pkgs.writeShellScriptBin "cratedigger-metadata-gate" ''
      set -euo pipefail
      hold_dir=/run/cratedigger-metadata-gate/holds
      case "''${1:-}" in
        hold)
          test "''${2:-}" = manual
          install -d -m 0755 "$hold_dir"
          printf 'manual\n' > "$hold_dir/manual"
          ;;
        release)
          test "''${2:-}" = manual
          rm -f "$hold_dir/manual"
          ;;
        resume-if-clear)
          test ! -e "$hold_dir/manual"
          ;;
        *) exit 64 ;;
      esac
    '';
    deployHoldBlocker = pkgs.writeShellScript "cratedigger-deploy-hold-blocker" ''
      set -euo pipefail
      while test -e /run/cratedigger-deploy-hold-blocker; do
        sleep 0.1
      done
    '';
  in {
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
        hostUrl = "http://192.0.2.21:5030";
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
      # Render the real NixOS-managed timer while keeping it far from firing.
      # The deploy-hold VM regression below needs the actual /etc unit path.
      timer = {
        enable = true;
        onBootSec = "1d";
        onUnitInactiveSec = "1d";
      };
      healthCheck.enable = false;
    };

    environment.systemPackages = [deployHoldTool metadataGateTool];

    # Simulate a downstream metadata gate holding every application unit.
    # Only the independent renderer may materialise runtime configuration on
    # first boot; the test removes this hold before exercising the apps.
    systemd.tmpfiles.rules = ["f /run/cratedigger-test-config-hold 0644 root root - held"];
    systemd.services = lib.mkMerge [
      (lib.genAttrs [
        "cratedigger"
        "cratedigger-unfindable"
        "cratedigger-importer"
        "cratedigger-import-preview-worker"
        "cratedigger-youtube-ingest"
        "cratedigger-web"
      ] (_: {
        serviceConfig.ExecCondition = [configHoldGate];
      }))
      {
        # The blocker has no dependency edge from the application units. Its
        # ordering matters only while the VM test has explicitly queued both
        # jobs, which gives us a deterministic real systemd `start/waiting`.
        cratedigger.after = ["cratedigger-deploy-hold-blocker.service"];
        cratedigger-unfindable.after = ["cratedigger-deploy-hold-blocker.service"];
        cratedigger-metadata-gate-watchdog = {
          after = ["cratedigger-deploy-hold-blocker.service"];
          serviceConfig = {
            Type = "oneshot";
            ExecStart = "${pkgs.coreutils}/bin/true";
          };
        };
        cratedigger-deploy-hold-blocker.serviceConfig = {
          Type = "oneshot";
          ExecStart = deployHoldBlocker;
        };
      }
    ];

    systemd.timers.cratedigger-metadata-gate-watchdog = {
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = "1d";
        OnUnitInactiveSec = "1d";
      };
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

    # A deploy must materialise the new runtime config independently of every
    # application unit. Downstream consumers can intentionally gate those
    # units with ExecCondition; systemd evaluates that before ExecStartPre, so
    # an app-owned renderer leaves stale mutable config throughout an outage.
    machine.wait_for_unit("cratedigger-config-render.service")
    state = machine.succeed("systemctl is-active cratedigger-config-render.service").strip()
    assert state == "active", f"config renderer unit not active: {state}"
    machine.succeed("test -f /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^host_url = http://192.0.2.21:5030$' /var/lib/cratedigger/config.ini")

    # Re-rendering on a config-only deploy must neither remove nor recreate the
    # main pipeline's active singleton lock. Pin both the fresh config and lock
    # preservation across an explicit renderer restart.
    machine.succeed("printf 'active-cycle\\n' > /var/lib/cratedigger/.cratedigger.lock")
    machine.succeed("sed -i 's#http://192.0.2.21:5030#http://stale.invalid#' /var/lib/cratedigger/config.ini")
    machine.succeed("before=$(stat -c '%d:%i' /var/lib/cratedigger/.cratedigger.lock); systemctl restart cratedigger-config-render.service; after=$(stat -c '%d:%i' /var/lib/cratedigger/.cratedigger.lock); test \"$before\" = \"$after\"")
    machine.succeed("grep -q '^host_url = http://192.0.2.21:5030$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -qx 'active-cycle' /var/lib/cratedigger/.cratedigger.lock")
    # Long-running workers may restart when their unit changes, but their
    # fallback is render-only. Only the timer-owned main service may clear the
    # pipeline lock.
    machine.succeed("systemctl cat cratedigger-importer.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-import-preview-worker.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-unfindable.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-youtube-ingest.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-web.service | grep -q cratedigger-render-config")
    machine.fail("systemctl cat cratedigger-importer.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-import-preview-worker.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-unfindable.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-youtube-ingest.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-web.service | grep -q cratedigger-pipeline-prestart")
    machine.succeed("systemctl cat cratedigger.service | grep -q cratedigger-pipeline-prestart")

    # Migrations recorded
    out = machine.succeed("sudo -u postgres psql root -At -c 'SELECT version FROM schema_migrations ORDER BY version'")
    versions = [v.strip() for v in out.strip().split() if v.strip()]
    assert "1" in versions, f"baseline migration missing, got {versions}"
    assert "2" in versions, f"002 migration missing, got {versions}"

    # #750: NixOS materialises generated units in /etc/systemd/system, which
    # outranks the ordinary runtime-mask location. Reproduce the real failure:
    # /run/systemd/system/<timer> -> /dev/null exists, yet the unit remains
    # loaded from /etc and an already-queued service start survives.
    machine.succeed("test -L /etc/systemd/system/cratedigger.timer")
    machine.succeed("touch /run/cratedigger-deploy-hold-blocker")
    machine.succeed("systemctl start --no-block cratedigger-deploy-hold-blocker.service")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=MainPID --value | grep -Ev '^(0)?$'")
    machine.succeed("systemctl start --no-block cratedigger.service")
    queued_job = machine.wait_until_succeeds("systemctl show cratedigger.service --property=Job --value | grep -E '^[0-9]+$'").strip()
    queued_state = machine.succeed(f"systemctl show {queued_job} --property=State --value").strip()
    assert queued_state == "waiting", f"expected queued start job, got {queued_state}"
    machine.succeed("systemctl mask --runtime cratedigger.timer")
    machine.succeed("test \"$(readlink /run/systemd/system/cratedigger.timer)\" = /dev/null")
    machine.succeed("systemctl daemon-reload")
    load_state = machine.succeed("systemctl show cratedigger.timer --property=LoadState --value").strip()
    assert load_state == "loaded", f"ordinary runtime mask unexpectedly won: {load_state}"
    machine.succeed(f"test \"$(systemctl show cratedigger.service --property=Job --value)\" = {queued_job}")
    machine.succeed(f"systemctl cancel {queued_job}")
    machine.succeed("systemctl unmask --runtime cratedigger.timer")
    machine.succeed("systemctl daemon-reload")
    machine.succeed("rm /run/cratedigger-deploy-hold-blocker")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=ActiveState --value | grep -qx inactive")

    # Exercise the reviewed helper against real systemd. Queue two exact
    # services behind the blocker and leave the watchdog in a job-free terminal
    # failure; acquire must cancel only waiting starts, reset only the terminal
    # failure, mask only the three timers through system.control, and reach
    # stable inactivity before it returns.
    machine.succeed("install -d /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d")
    machine.succeed("printf '[Service]\\nExecStart=\\nExecStart=/run/current-system/sw/bin/false\\n' > /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d/fail.conf")
    machine.succeed("systemctl daemon-reload")
    machine.fail("systemctl start cratedigger-metadata-gate-watchdog.service")
    machine.succeed("systemctl show cratedigger-metadata-gate-watchdog.service --property=ActiveState --value | grep -qx failed")
    machine.succeed("touch /run/cratedigger-deploy-hold-blocker")
    machine.succeed("systemctl start --no-block cratedigger-deploy-hold-blocker.service")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=MainPID --value | grep -Ev '^(0)?$'")
    for service in (
        "cratedigger.service",
        "cratedigger-unfindable.service",
    ):
        machine.succeed(f"systemctl start --no-block {service}")
        job = machine.wait_until_succeeds(f"systemctl show {service} --property=Job --value | grep -E '^[0-9]+$'").strip()
        state = machine.succeed(f"systemctl show {job} --property=State --value").strip()
        assert state == "waiting", f"{service} job was not waiting: {state}"

    acquire_status, acquire_output = machine.execute("timeout 60 cratedigger-deploy-hold acquire")
    if acquire_status != 0:
        print(acquire_output)
        print(machine.succeed("systemctl list-jobs --no-legend || true"))
        for service in (
            "cratedigger.service",
            "cratedigger-unfindable.service",
            "cratedigger-metadata-gate-watchdog.service",
        ):
            print(machine.succeed(f"systemctl show {service} --property=Job --property=LoadState --property=ActiveState --property=SubState"))
    assert acquire_status == 0, f"deploy hold acquire failed: {acquire_status}"
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(f"test \"$(readlink /run/systemd/system.control/{timer})\" = /dev/null")
        state = machine.succeed(f"systemctl show {timer} --property=LoadState --value").strip()
        assert state == "masked", f"{timer} not authoritatively masked: {state}"
    for service in (
        "cratedigger.service",
        "cratedigger-unfindable.service",
        "cratedigger-metadata-gate-watchdog.service",
    ):
        machine.succeed(f"test -z \"$(systemctl show {service} --property=Job --value)\"")
        state = machine.succeed(f"systemctl show {service} --property=ActiveState --value").strip()
        assert state == "inactive", f"{service} not inactive after hold: {state}"

    # Qualify idempotent post-switch verification and the staged release. The
    # config ExecCondition keeps the controlled VM cycle cheap; PR1 owns real
    # invocation capture/verification in the deploy workflow.
    machine.succeed("rm -r /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d")
    machine.succeed("systemctl daemon-reload")
    machine.succeed("cratedigger-deploy-hold verify-held")
    machine.succeed("rm /run/cratedigger-deploy-hold-blocker")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=ActiveState --value | grep -qx inactive")
    machine.succeed("cratedigger-deploy-hold prepare-controlled")
    machine.succeed("cratedigger-deploy-hold open-main-timer")
    machine.succeed("cratedigger-deploy-hold finish-release aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.succeed("cratedigger-deploy-hold complete aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.fail("test -e /run/cratedigger-deploy-hold")
    machine.fail("test -e /run/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.fail(f"test -e /run/systemd/system.control/{timer}")
        state = machine.succeed(f"systemctl show {timer} --property=LoadState --value").strip()
        assert state == "loaded", f"{timer} not restored after release: {state}"

    # Starting the main service remains safe: its idempotent pre-start render is
    # retained as a fallback and clears the test's deliberately stale lock. It
    # will fail because there is no real slskd.
    machine.succeed("rm /run/cratedigger-test-config-hold")
    machine.succeed("systemctl start cratedigger.service || true")
    machine.fail("test -f /var/lib/cratedigger/.cratedigger.lock")
    machine.succeed("systemctl start cratedigger-importer.service cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service cratedigger-web.service")
    # config.ini points at the out-of-band secret, never its plaintext value.
    machine.succeed("grep -q 'api_key_file = /etc/cratedigger/slskd-api-key' /var/lib/cratedigger/config.ini")
    # The secret itself must NEVER appear in config.ini — that's the whole fix.
    machine.fail("grep -q 'test-api-key-do-not-use' /var/lib/cratedigger/config.ini")
    # config.ini is now world-readable since it contains no secrets.
    mode = machine.succeed("stat -c %a /var/lib/cratedigger/config.ini").strip()
    assert mode == "644", f"config.ini should be 0644, got {mode}"
    machine.succeed("grep -q 'enabled = True' /var/lib/cratedigger/config.ini")  # beets validation ON (stranger posture)
    machine.succeed("grep -q '\\[Quality Ranks\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.transparent = 192$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.excellent = 160$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.good = 112$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.acceptable = 96$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.transparent = 320$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.excellent = 256$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.good = 192$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.acceptable = 128$' /var/lib/cratedigger/config.ini")
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
