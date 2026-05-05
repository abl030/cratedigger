# NixOS VM test for the upstream cratedigger module.
#
# Boots a single VM that runs:
#   - PostgreSQL with a `cratedigger` user/db (the module's expected backend)
#   - The cratedigger module enabled with web ON, no slskd, fake API key file
#
# Verifies:
#   - cratedigger-db-migrate.service runs to active (exited)
#   - schema_migrations table is populated
#   - /var/lib/cratedigger/config.ini exists with the API key substituted
#   - pipeline-cli is on PATH and connects to the DB
#   - cratedigger-web responds on its port
#
# Does NOT exercise: slskd interaction, real downloads, beets — those need
# heavyweight fixtures that belong in the python test suite.
{ pkgs, system, cratediggerModule, cratediggerSrc }:

pkgs.testers.nixosTest {
  name = "cratedigger-module-vm";

  nodes.machine = { config, lib, pkgs, ... }: {
    imports = [ cratediggerModule ];

    services.postgresql = {
      enable = true;
      ensureDatabases = [ "cratedigger" ];
      ensureUsers = [
        {
          name = "cratedigger";
          ensureDBOwnership = true;
        }
      ];
      authentication = lib.mkOverride 10 ''
        local all all              trust
        host  all all 127.0.0.1/32 trust
        host  all all ::1/128      trust
      '';
    };

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
      pipelineDb.dsn = "postgresql://cratedigger@localhost/cratedigger";
      beetsValidation = {
        enable = false;
        stagingDir = "/var/lib/cratedigger-staging";
        trackingFile = "/var/lib/cratedigger-staging/tracking.jsonl";
      };
      web = {
        enable = true;
        beetsDb = "/etc/cratedigger/beets.db";
      };
      timer.enable = false;
      healthCheck.enable = false;
    };

    # Order our migrate unit after postgres comes up.
    systemd.services.cratedigger-db-migrate = {
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];
    };
    systemd.services.cratedigger.after = [ "postgresql.service" ];
    systemd.services.cratedigger.wants = [ "postgresql.service" ];
    systemd.services.cratedigger-web.after = [ "postgresql.service" ];
    systemd.services.cratedigger-web.wants = [ "postgresql.service" ];

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
    out = machine.succeed("sudo -u postgres psql cratedigger -At -c 'SELECT version FROM schema_migrations ORDER BY version'")
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
    machine.succeed("grep -q 'enabled = False' /var/lib/cratedigger/config.ini")  # beets disabled
    machine.succeed("grep -q '\\[Quality Ranks\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '\\[Peer Cache\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'redis_host = 127.0.0.1' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'ttl_seconds = 604800' /var/lib/cratedigger/config.ini")
    machine.succeed("${pkgs.redis}/bin/redis-cli -p 6379 CONFIG GET maxmemory-policy | grep -q allkeys-lru")
    machine.succeed("systemctl show -p After cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p After cratedigger-web.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger-web.service | grep -q redis-cratedigger.service")

    # pipeline-cli on PATH and connects
    machine.succeed("pipeline-cli list wanted")

    # Web UI listens
    machine.wait_for_unit("cratedigger-web.service")
    machine.wait_for_open_port(8085)
    machine.succeed("curl -sf http://127.0.0.1:8085/ > /dev/null")
  '';
}
