# SAMPLE — minimal cratedigger consumer config.
#
# Add the flake input and import this module (adapt paths to taste):
#
#   inputs.cratedigger.url = "github:abl030/cratedigger";
#   ...
#   nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
#     modules = [
#       cratedigger.nixosModules.default
#       ./examples/cratedigger.nix      # <- this file, adapted
#     ];
#   };
#
# The module builds its runtime (python env + beets) from CRATEDIGGER'S
# OWN flake.lock — the exact closure its test suite ran against — not
# your nixpkgs. That costs one extra nixpkgs evaluation and is the whole
# point; `services.cratedigger.packageSet` is the escape hatch if you
# refuse the trade.
{ config, pkgs, ... }:

{
  # ---------------------------------------------------------------------
  # slskd — the Soulseek client cratedigger drives. Bring your own
  # credentials; the API key must also land in the file cratedigger reads
  # (slskd.apiKeyFile below). See `services.slskd` options in nixpkgs.
  # ---------------------------------------------------------------------
  services.slskd = {
    enable = true;
    domain = null;
    settings = {
      shares.directories = [ "/srv/music/library" ];
      directories.downloads = "/srv/music/slskd-downloads";
    };
    # slskd reads SLSKD_SLSK_USERNAME / SLSKD_SLSK_PASSWORD / SLSKD_API_KEY
    # from this env file. Any secrets backend works; a root-owned file is
    # the floor.
    environmentFile = "/var/lib/secrets/slskd.env";
  };

  services.cratedigger = {
    enable = true;

    # --- The two things you must always provide -----------------------
    slskd = {
      # Raw API key, one line, readable by the cratedigger user (root by
      # default). Same value slskd itself was given above.
      apiKeyFile = "/var/lib/secrets/slskd-api-key";
      downloadDir = "/srv/music/slskd-downloads";
    };

    # --- Database: provisioned locally, peer auth, zero passwords -----
    pipelineDb.createLocally = true;

    # --- Beets: cratedigger owns the package, config, and binary ------
    # `cratedigger-beet` lands on your PATH for manual ops (run it with
    # sudo — the service runs as root by default; see the "Running as a
    # non-root user" block near the end of this file for the group-`users`
    # / media-server-friendly alternative).
    beets.config = {
      directory = "/srv/music/library";          # where tagged albums live
      library = "/srv/music/beets-library.db";   # parent dir must exist
    };
    beets.validation = {
      stagingDir = "/srv/music/incoming";        # validated albums stage here
      trackingFile = "/srv/music/beets-validated.jsonl";
    };

    # --- Web UI (album browser + request manager) ---------------------
    web = {
      enable = true;
      port = 8085;
      beetsDb = "/srv/music/beets-library.db";
    };

    # --- Mirrors: all optional ----------------------------------------
    # Without any of this, MusicBrainz browse/matching uses public
    # musicbrainz.org (works, rate-limited ~1 req/s) and Discogs browse
    # is off with a clear 503. See docs/mirrors.md and the sibling
    # examples for standing the mirrors up.
    #
    # musicbrainz.apiBase = "http://mb-mirror.lan:5200";
    # discogs.apiBase = "http://discogs-mirror.lan:8086";
    # beets.package = {
    #   discogsMirrorUrl = "http://discogs-mirror.lan:8086";
    #   lrclibUrl = "http://lrclib.lan:3300/api";
    #   # Discogs user token (https://www.discogs.com/settings/developers),
    #   # raw, one line. Without it, public-Discogs lookups during import
    #   # fail per-use (everything still loads cleanly).
    #   discogsTokenFile = "/var/lib/secrets/discogs-token";
    # };
  };

  # ---------------------------------------------------------------------
  # Running as a non-root user (recommended for media-server integration)
  # ---------------------------------------------------------------------
  # By default (above), cratedigger runs as root — zero-config, since slskd
  # downloads and the beets library commonly live outside any unprivileged
  # user's reach. If Jellyfin/Plex/etc. also need to read album art AND
  # write NFO/artwork into the library, run cratedigger non-root instead
  # (issue #570). Uncomment and adapt:
  #
  # users.users.cratedigger = {
  #   isSystemUser = true;
  #   group = "users";                     # shared consumer group (Jellyfin's gid)
  #   extraGroups = [
  #     "slskd"           # slskd's download-dir group — reap/move in-flight files
  #     "cratedigger-ops" # whatever group owns /run/cratedigger-secrets/*
  #   ];
  # };
  #
  # services.cratedigger.user = "cratedigger";
  # services.cratedigger.group = "users";
  #
  # Give the library roots a setgid layout so new album/artist dirs inherit
  # the `users` group automatically — plain 0775 strips the setgid bit and
  # silently breaks this:
  #
  # systemd.tmpfiles.rules = [
  #   "d /srv/music/library 2775 cratedigger users -"
  #   "d /srv/music/incoming 2775 cratedigger users -"
  # ];
  #
  # For a library tree that already exists, fix it once as an operator
  # action (not committed config):
  #   chgrp -R users /srv/music/library
  #   find /srv/music/library -type d -exec chmod 2775 {} +
  #   find /srv/music/library -type f -exec chmod 0664 {} +
  #
  # The discogsTokenFile above must stay readable by the cratedigger user's
  # secrets group. A root-owned 0400 token under the state dir can't be
  # fixed by systemd-tmpfiles once the state dir itself is owned by a
  # non-root user (tmpfiles refuses with "unsafe path transition") — do a
  # one-time `chown root:cratedigger-ops` + `chmod 0440` on the token, or
  # manage it via sops-nix with `owner = "cratedigger"` instead.
  #
  # Payoff: gid-`users` media servers (Jellyfin) can both read fetched
  # album art and write NFO/artwork alongside the media — see
  # docs/nixos-module.md § "Running non-root + filesystem permissions".

  # The staging/library parents must exist; the module manages only its
  # own state dir. (Swap the mode/owner below to `2775 cratedigger users`
  # if you enable the non-root block above.)
  systemd.tmpfiles.rules = [
    "d /srv/music 0775 root root -"
    "d /srv/music/incoming 0775 root root -"
  ];
}
