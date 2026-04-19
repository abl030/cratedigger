# NixOS VM test for the upstream soularr module.
#
# Boots a single VM that runs:
#   - PostgreSQL with a `soularr` user/db (the module's expected backend)
#   - The soularr module enabled with web ON, no slskd, fake API key file
#
# Verifies:
#   - soularr-db-migrate.service runs to active (exited)
#   - schema_migrations table is populated
#   - /var/lib/soularr/config.ini exists with the API key substituted
#   - pipeline-cli is on PATH and connects to the DB
#   - soularr-web responds on its port
#
# Does NOT exercise: slskd interaction, real downloads, beets — those need
# heavyweight fixtures that belong in the python test suite.
{ pkgs, system, soularrModule, soularrSrc }:

pkgs.testers.nixosTest {
  name = "soularr-module-vm";

  nodes.machine = { config, lib, pkgs, ... }: {
    imports = [ soularrModule ];

    services.postgresql = {
      enable = true;
      ensureDatabases = [ "soularr" ];
      ensureUsers = [
        {
          name = "soularr";
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
    environment.etc."soularr/slskd-api-key" = {
      text = "test-api-key-do-not-use\n";
      mode = "0400";
    };

    # Stub beets library DB so soularr-web can open it read-only.
    environment.etc."soularr/beets.db" = {
      text = "";
      mode = "0644";
    };

    services.soularr = {
      enable = true;
      src = soularrSrc;
      slskd = {
        apiKeyFile = "/etc/soularr/slskd-api-key";
        downloadDir = "/var/lib/soularr-downloads";
      };
      pipelineDb.dsn = "postgresql://soularr@localhost/soularr";
      beetsValidation = {
        enable = false;
        stagingDir = "/var/lib/soularr-staging";
        trackingFile = "/var/lib/soularr-staging/tracking.jsonl";
      };
      web = {
        enable = true;
        beetsDb = "/etc/soularr/beets.db";
        redis.host = "127.0.0.1";
      };
      timer.enable = false;
      healthCheck.enable = false;
    };

    # The module doesn't enable redis itself — provide one for the web UI.
    services.redis.servers."" = {
      enable = true;
      port = 6379;
      bind = "127.0.0.1";
    };

    # Order our migrate unit after postgres comes up.
    systemd.services.soularr-db-migrate = {
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];
    };
    systemd.services.soularr-web.after = [ "postgresql.service" "redis.service" ];
    systemd.services.soularr-web.wants = [ "postgresql.service" "redis.service" ];

    # Speed up the VM
    virtualisation.memorySize = 2048;
  };

  testScript = ''
    machine.start()
    machine.wait_for_unit("postgresql.service")
    machine.wait_for_unit("soularr-db-migrate.service")

    # The migrator is a oneshot with RemainAfterExit=true — confirm it landed
    # in active (exited), not failed.
    state = machine.succeed("systemctl is-active soularr-db-migrate.service").strip()
    assert state == "active", f"migrator unit not active: {state}"

    # Migrations recorded
    out = machine.succeed("sudo -u postgres psql soularr -At -c 'SELECT version FROM schema_migrations ORDER BY version'")
    versions = [v.strip() for v in out.strip().split() if v.strip()]
    assert "1" in versions, f"baseline migration missing, got {versions}"
    assert "2" in versions, f"002 migration missing, got {versions}"

    # config.ini rendered with API key substituted. The soularr ExecStart will
    # fail because there's no real slskd, but ExecStartPre (preStartScript)
    # runs first and writes the config — that's all we need to assert here.
    machine.succeed("systemctl start soularr.service || true")
    machine.succeed("test -f /var/lib/soularr/config.ini")
    machine.succeed("grep -q 'api_key = test-api-key-do-not-use' /var/lib/soularr/config.ini")
    machine.succeed("grep -q 'enabled = False' /var/lib/soularr/config.ini")  # beets disabled
    machine.succeed("grep -q '\\[Quality Ranks\\]' /var/lib/soularr/config.ini")

    # pipeline-cli on PATH and connects
    machine.succeed("pipeline-cli list wanted")

    # Web UI listens
    machine.wait_for_unit("soularr-web.service")
    machine.wait_for_open_port(8085)
    machine.succeed("curl -sf http://127.0.0.1:8085/ > /dev/null")
  '';
}
