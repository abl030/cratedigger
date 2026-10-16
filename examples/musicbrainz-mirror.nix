# SAMPLE — a local MusicBrainz mirror on NixOS.
#
# The supported way to run an MB mirror is upstream's compose stack:
#   https://github.com/metabrainz/musicbrainz-docker
# (postgres + WS/2 web service + search indexes + live replication).
# There is no nixpkgs-native MB mirror; this sample shows the shape of
# wrapping the upstream stack in a podman-compose systemd unit, which is
# how the operator's production mirror runs. Expect to spend real time
# on the initial data import (~100GB+, hours to days) — read upstream's
# README first. The long-term plan in this project is `mb-api`, a Rust
# reimplementation of the WS/2 subset cratedigger uses; until that
# exists, upstream's stack is the mirror.
#
# What you need:
#   1. A checkout of musicbrainz-docker (pin it — a flake input with
#      flake = false works well).
#   2. A (free) MetaBrainz replication token for hourly replication:
#      https://metabrainz.org/supporters/account-type
#   3. Disk: ~100GB+ for the database and search indexes.
#
# Wire cratedigger at it with:
#   services.cratedigger.musicbrainz.apiBase = "http://localhost:5200";
# (one value — web browse, pipeline-cli, and the rendered beets
# musicbrainz block all derive from it; ratelimit flips to 100
# automatically for a non-public host.)
{ config, lib, pkgs, inputs, ... }:

let
  # Pin upstream: inputs.musicbrainz-docker = { url = "github:metabrainz/musicbrainz-docker"; flake = false; };
  composeDir = "/var/lib/musicbrainz-docker";
  # Upstream compose exposes the web service on 5000 by default; the
  # operator maps it to 5200. Set via compose overrides (see upstream's
  # `admin/configure` and compose override docs).
  port = 5200;
in
{
  virtualisation.podman = {
    enable = true;
    defaultNetwork.settings.dns_enabled = true;
  };

  # One-time setup (manual, from upstream's README — the initial import
  # is an interactive, hours-long operation and does not belong in a
  # systemd unit):
  #   cp -r ${toString inputs.musicbrainz-docker or "<musicbrainz-docker checkout>"} ${composeDir}
  #   cd ${composeDir}
  #   admin/configure add publishing-db-port   # and any other overrides
  #   echo "MB_REPLICATION_TOKEN=<token>" >> .env
  #   podman-compose build
  #   podman-compose run --rm musicbrainz createdb.sh -fetch   # the big one
  #   podman-compose run --rm musicbrainz bash -c 'carton exec -- ./admin/BuildMaterializedTables --database=MAINTENANCE all'
  #   podman-compose run --rm indexer python -m sir reindex    # search indexes
  #
  # The materialized tables are REQUIRED for cratedigger, although upstream
  # calls them optional. Without artist_release, /ws/2/release?artist=
  # browses run a fallback query whose sort has no unique tiebreaker:
  # releases tied on date/country/barcode/title can appear on two pages
  # while a sibling pressing appears on none. Cratedigger's artist page then
  # fails closed ("browse did not conserve N unique identities"); see
  # docs/musicbrainz-mirror.md. The unit below re-runs the build on every
  # start: upstream skips any table that already has rows, so it is a no-op
  # once built, and replication keeps the tables current.

  # Day-2: the stack as a unit. Replication runs inside the stack (cron
  # in the musicbrainz container) once the token is configured.
  systemd.services.musicbrainz-mirror = {
    description = "MusicBrainz mirror (upstream musicbrainz-docker via podman-compose)";
    after = [ "network-online.target" "podman.service" ];
    wants = [ "network-online.target" ];
    wantedBy = [ "multi-user.target" ];
    path = [ pkgs.podman pkgs.podman-compose ];
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
      WorkingDirectory = composeDir;
      ExecStart = "${pkgs.podman-compose}/bin/podman-compose up -d";
      ExecStop = "${pkgs.podman-compose}/bin/podman-compose down";
      TimeoutStartSec = "10min";
    };
  };

  # Build any empty materialized tables (see the setup note above). Only
  # the first run does real work (tens of minutes, during which artist
  # browses wait on the table lock); later starts skip every table.
  systemd.services.musicbrainz-materialized-tables = {
    description = "Build MusicBrainz materialized tables when empty";
    after = [ "musicbrainz-mirror.service" ];
    requires = [ "musicbrainz-mirror.service" ];
    wantedBy = [ "multi-user.target" ];
    path = [ pkgs.podman pkgs.podman-compose ];
    serviceConfig = {
      Type = "simple";
      WorkingDirectory = composeDir;
    };
    script = ''
      podman-compose run --rm musicbrainz \
        bash -c 'carton exec -- ./admin/BuildMaterializedTables --database=MAINTENANCE all'
    '';
  };

  networking.firewall.allowedTCPPorts = [ port ];
}
