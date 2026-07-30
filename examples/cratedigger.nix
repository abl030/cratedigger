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
  # The default example deliberately exercises the non-root service shape.
  # `users` is the shared media-reader group; slskd owns its download tree.
  users.users.cratedigger = {
    isSystemUser = true;
    group = "users";
    extraGroups = [ "slskd" "cratedigger-ops" ];
  };

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
    user = "cratedigger";
    group = "users";

    # --- The two things you must always provide -----------------------
    slskd = {
      # Raw API key, one line, readable by the cratedigger user. Same value
      # slskd itself was given above.
      apiKeyFile = "/var/lib/secrets/slskd-api-key";
      downloadDir = "/srv/music/slskd-downloads";
    };
    # Private high-capacity processing belongs beneath a root-owned parent,
    # never in the group-writable slskd tree. Keep the service identities
    # distinct; Cratedigger needs only the slskd download-directory group.
    processingDir = "/srv/cratedigger-processing";

    # --- Database: provisioned locally, peer auth, zero passwords -----
    # Keep this safe default unless you deliberately operate an external
    # PostgreSQL server. PostgreSQL data must remain on a supported local
    # filesystem, never virtiofs/NFS/FUSE or the shared music filesystem.
    #
    # If an external PostgreSQL runs in nspawn with its data directory bind
    # mounted from the host, the host directory must retain the container
    # PostgreSQL user's mapped numeric UID/GID across every NixOS switch.
    # A systemd.tmpfiles.rules `d` entry is reapplied: declaring that bind
    # root as root:root can leave PostgreSQL apparently healthy on open file
    # handles, then panic when the next checkpoint opens pg_control.
    pipelineDb.createLocally = true;

    # --- Beets: cratedigger owns the package, config, and binary ------
    # `cratedigger-beet` lands on your PATH for manual ops. Run it as the
    # configured cratedigger service identity so it uses the same ownership
    # and rendered configuration as automated imports.
    beets.config = {
      directory = "/srv/music/library";          # where tagged albums live
      # Keep DB, journals, import log, and harness audit outside the curated
      # music root. Explicit override parents remain operator-provisioned.
      library = "/var/lib/cratedigger-beets-db/beets-library.db";
    };
    beets.validation = {
      stagingDir = "/srv/music/incoming";        # validated albums stage here
      trackingFile = "/srv/music/beets-validated.jsonl";
    };

    # --- Web UI (album browser + request manager) ---------------------
    web = {
      enable = true;
      hostName = "music.example.net";
      gatewayPort = 8086;
      accessGroup = "cratedigger-web";
      # Provision this BEFORE the first switch (for example with sops-nix) as
      # a non-empty bcrypt htpasswd file owned root:nginx with mode 0440.
      # Missing/invalid material blocks the first nginx start. A later invalid
      # reload leaves the shared nginx master and unrelated vhosts running,
      # while Cratedigger itself returns 503 until a valid reload. Configure
      # the sops secret with reloadUnits = [ "nginx.service" ] and
      # restartUnits = [ ]. Never inline the plaintext or verifier, use a Nix
      # path/store derivation, or substitute enableInsecure in production.
      basicAuthFile = "/run/secrets/cratedigger.htpasswd";
    };
    # The only other current mode is deliberate insecure operation: omit
    # basicAuthFile and set enableInsecure = true. The two modes are exclusive.
    #
    # Local API-backed pipeline-cli mutations use the permissioned Unix socket,
    # not Basic credentials. Grant that complete API authority explicitly:
    # users.users.your-operator.extraGroups = [ "cratedigger-web" ];
    # Other pipeline-cli families still need their own PostgreSQL, filesystem,
    # Beets, and secret-file authority.

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

  # A separate public HTTPS vhost must forward to the LOOPBACK gateway above,
  # never to a Python port. For example, adapt your TLS/ACME policy and use:
  #
  # services.nginx.virtualHosts."music.example.net" = {
  #   enableACME = true;
  #   forceSSL = true;
  #   locations."/" = {
  #     proxyPass = "http://127.0.0.1:8086";
  #     recommendedProxySettings = false;
  #     extraConfig = ''
  #       proxy_set_header Host music.example.net;
  #     '';
  #   };
  # };
  #
  # The module-owned gateway then applies whole-site Basic (except exact
  # anonymous GET/HEAD /healthz without a query), strips credentials/identity
  # headers, and forwards through /run/cratedigger-web/web.sock.

  # ---------------------------------------------------------------------
  # Non-root media-server filesystem posture
  # ---------------------------------------------------------------------
  # Give the library roots a setgid layout so new album/artist dirs inherit
  # the `users` group automatically — plain 0775 strips the setgid bit and
  # silently breaks this:
  #
  # The enabled tmpfiles rules below create this shape on a fresh host.
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
  # own state and private processing roots.
  systemd.tmpfiles.rules = [
    "d /srv/music 0755 root root -"
    "d /srv/music/library 2775 cratedigger users -"
    "d /srv/music/incoming 2775 cratedigger users -"
  ];
}
