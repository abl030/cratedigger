# SAMPLE — the Discogs mirror: a Rust JSON API over PostgreSQL, loaded
# from the monthly CC0 XML dumps (data.discogs.com). ~19M releases;
# plan ~100GB of disk for postgres + dumps.
#
# This is a distilled version of the operator's production deployment
# (which uses an nspawn postgres container + sops). It builds the
# `discogs-api` project from source; add the input to your flake:
#
#   inputs.discogs-api-src = {
#     url = "github:abl030/discogs-api";
#     flake = false;
#   };
#
# Wire cratedigger at it with:
#   services.cratedigger.discogs.apiBase = "http://localhost:8086";
#   services.cratedigger.beets.discogsMirrorUrl = "http://localhost:8086";
#
# Two units:
#   discogs-import.service/.timer — monthly oneshot: download the latest
#     dumps, parse, COPY into postgres (idempotent: drops + recreates)
#   discogs-api.service           — long-running axum HTTP server
{ config, lib, pkgs, inputs, ... }:

let
  discogsPkg = pkgs.rustPlatform.buildRustPackage {
    pname = "discogs-api";
    version = "0.1.0";
    src = inputs.discogs-api-src;
    cargoLock.lockFile = "${inputs.discogs-api-src}/Cargo.lock";
    nativeBuildInputs = [ pkgs.pkg-config ];
    buildInputs = [ pkgs.openssl ];
  };

  # Local postgres over the unix socket, role + db named "discogs",
  # peer auth — no password material (same pattern cratedigger's
  # pipelineDb.createLocally uses).
  dsn = "postgresql:///discogs?host=/run/postgresql";
  dumpDir = "/var/lib/discogs-mirror/dumps";
  apiPort = 8086;
in
{
  services.postgresql = {
    enable = true;
    ensureDatabases = [ "discogs" ];
    ensureUsers = [
      {
        name = "discogs";
        ensureDBOwnership = true;
      }
    ];
  };

  users.users.discogs = {
    isSystemUser = true;
    group = "discogs";
  };
  users.groups.discogs = { };

  systemd.tmpfiles.rules = [
    "d /var/lib/discogs-mirror 0755 discogs discogs -"
    "d ${dumpDir} 0755 discogs discogs -"
  ];

  systemd.services.discogs-import = {
    description = "Discogs dump importer (monthly)";
    after = [ "postgresql.service" "network-online.target" ];
    requires = [ "postgresql.service" ];
    wants = [ "network-online.target" ];
    serviceConfig = {
      Type = "oneshot";
      User = "discogs";
      Group = "discogs";
      # The import takes hours on first run (streaming-parses multi-GB
      # XML); it is idempotent, so on-failure retry is safe.
      TimeoutStartSec = "12h";
      Restart = "on-failure";
      RestartSec = "30min";
      ExecStart = "${discogsPkg}/bin/discogs-import --dsn ${dsn} --dump-dir ${dumpDir}";
    };
  };

  systemd.timers.discogs-import = {
    description = "Monthly Discogs dump import";
    wantedBy = [ "timers.target" ];
    timerConfig = {
      # Dumps publish at the start of each month; the 2nd avoids racing
      # the upload.
      OnCalendar = "*-*-02 04:00:00";
      Persistent = true;
    };
  };

  systemd.services.discogs-api = {
    description = "Discogs mirror JSON API";
    after = [ "postgresql.service" ];
    requires = [ "postgresql.service" ];
    wantedBy = [ "multi-user.target" ];
    serviceConfig = {
      Type = "simple";
      User = "discogs";
      Group = "discogs";
      ExecStart = "${discogsPkg}/bin/discogs-api --dsn ${dsn} --port ${toString apiPort}";
      Restart = "on-failure";
      RestartSec = 5;
      NoNewPrivileges = true;
      ProtectSystem = "strict";
    };
  };
}
