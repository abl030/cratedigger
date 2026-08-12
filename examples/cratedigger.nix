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
# Cratedigger consumes Beets; the deployment instantiates and owns the
# package, immutable config, catalog, library, host-local state, token-only
# include, readiness unit, and plain operator `beet` below.
# Deployments upgrading from the removed beets.package/beets.config interface
# must supply this external runtime capability in the same change; there are no
# compatibility aliases.
{ config, pkgs, ... }:

let
  cratediggerPkgs = config.services.cratedigger.packageSet;
  beetsLibrary = "/var/lib/beets-library/beets-library.db";
  beetsDirectory = "/srv/music/library";
  beetsStateFile = "/var/lib/beets/state.pickle";
  beetsSecretInclude = "/run/secrets/beets-discogs.yaml";

  # Optional mirror patches are deployment package choices, not Cratedigger
  # module options. Leave either value null for stock upstream behavior.
  beetsPackage = import ../nix/beets.nix {
    pkgs = cratediggerPkgs;
    discogsMirrorUrl = null;
    lrclibUrl = null;
  };
  beetsPython = cratediggerPkgs.python3.withPackages (_: [ beetsPackage ]);
  beetsYaml = (cratediggerPkgs.formats.yaml { }).generate "config.yaml" {
    library = beetsLibrary;
    directory = beetsDirectory;
    statefile = beetsStateFile;
    include = [ beetsSecretInclude ];
    asciify_paths = true;
    plugins = "musicbrainz mbsync discogs fetchart embedart lyrics lastgenre scrub info missing duplicates edit fromfilename ftintitle the inline permissions";
    import = {
      copy = false;
      autotag = true;
      write = true;
      move = true;
      timid = false;
      incremental = true;
      incremental_skip_later = true;
      log = "/var/lib/beets-library/beets-import.log";
      languages = [ "en" ];
      duplicate_keys = {
        album = [ "mb_albumid" "discogs_albumid" ];
        item = [ "artist" "title" ];
      };
    };
    paths = {
      default = "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title";
      singleton = "Non-Album/$artist/$title";
      comp = "Compilations/$album%aunique{albumartist album,path_disambig}/$track $title";
    };
    album_fields.path_disambig =
      "albumdisambig or releasegroupdisambig or catalognum or label or str(year)";
    musicbrainz = {
      host = "musicbrainz.org";
      https = true;
      ratelimit = 1;
    };
    permissions = {
      file = "0664";
      dir = "02775";
    };
    convert = {
      auto = false;
      auto_keep = false;
    };
    fetchart = {
      auto = true;
      minwidth = 300;
      maxwidth = 500;
    };
  };
  beetsConfigDir = cratediggerPkgs.runCommand
    "cratedigger-example-external-beets-config" { } ''
      mkdir -p "$out"
      ln -s ${beetsYaml} "$out/config.yaml"
    '';
  # This is deliberately a deployment-owned command, rather than a global
  # BEETSDIR export: every invocation resolves both the Beets package and its
  # immutable configuration through this generation's wrapper.
  beet = cratediggerPkgs.writeShellScriptBin "beet" ''
    export BEETSDIR=${beetsConfigDir}
    exec ${beetsPython}/bin/beet "$@"
  '';

  # Deployment-owned initialization/readiness. In an existing-library
  # deployment, replace the initialization branch with a read-only assertion
  # that the already-backed-up catalog exists; never copy or move it here.
  beetsRuntimeReady = pkgs.writeShellScript "beets-runtime-ready" ''
    set -euo pipefail
    install -d -o cratedigger -g users -m 2775 ${beetsDirectory}
    install -d -o cratedigger -g cratedigger-ops -m 2770 /var/lib/beets-library
    install -d -o root -g cratedigger-ops -m 0750 /var/lib/beets
    test -r ${beetsSecretInclude}
    if test ! -e ${beetsStateFile}; then
      ${beetsPython}/bin/python - <<'PY'
    import pickle
    from pathlib import Path
    Path("${beetsStateFile}").write_bytes(pickle.dumps({}))
    PY
      chown root:cratedigger-ops ${beetsStateFile}
      chmod 0660 ${beetsStateFile}
    fi
    if test ! -e ${beetsLibrary}; then
      BEETSDIR=${beetsConfigDir} ${beetsPython}/bin/python - <<'PY'
    from beets.library import Library
    library = Library("${beetsLibrary}", "${beetsDirectory}")
    library._close()
    PY
      chown cratedigger:cratedigger-ops ${beetsLibrary}
      chmod 0660 ${beetsLibrary}
    fi
  '';
in
{
  users.groups.cratedigger-ops = { };
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
      # Must already exist and be usable by the cratedigger identity before
      # the first switch: every application unit proves it at startup and
      # refuses to start otherwise (docs/nixos-module.md "Startup write-
      # probe"). No Cratedigger tmpfiles rule creates it -- it is either
      # created by services.slskd itself (as above) or provisioned by you.
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

    # --- Beets: consume the external deployment-owned capability ------
    beets.runtime.package = beetsPackage;
    beets.runtime.configDir = toString beetsConfigDir;
    beets.runtime.expectedLibrary = beetsLibrary;
    beets.runtime.expectedDirectory = beetsDirectory;
    beets.runtime.expectedStateFile = beetsStateFile;
    beets.runtime.expectedSecretInclude = beetsSecretInclude;
    beets.runtime.readinessUnits = [ "beets-runtime-ready.service" ];
    beets.validation = {
      # Validated albums stage here. Also gated by the startup write-probe
      # (must already exist before the first switch) whenever validation
      # is enabled -- see the downloadDir comment above; the tmpfiles rule
      # below is what actually provisions it in this example.
      stagingDir = "/srv/music/incoming";
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
      # If you already run an identity provider, drop this option entirely and
      # set externalAuth = true instead; see examples/external-auth-nginx.nix.
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
    # For Beets mirrors, set the arguments to nix/beets.nix above and update
    # this immutable config's matching network policy. Provision
    # beetsSecretInclude via sops/agenix as exactly:
    #   discogs:
    #     user_token: <non-empty scalar>
    # No other key is admitted, and token/config rotation must restart the
    # guarded Cratedigger application units.
  };

  # Deployment-owned plain operator authority. The `beet` wrapper points at
  # the same immutable config Cratedigger admits; do not run mutations
  # concurrently with the importer, raw imports, or `beet remove -d`.
  environment.systemPackages = [ beet ];

  systemd.services.beets-runtime-ready = {
    description = "Provision deployment-owned Beets runtime authority";
    wantedBy = [ "multi-user.target" ];
    before = [
      "cratedigger.service"
      "cratedigger-importer.service"
      "cratedigger-import-preview-worker.service"
      "cratedigger-web.service"
    ];
    serviceConfig = {
      Type = "oneshot";
      RemainAfterExit = true;
      ExecStart = beetsRuntimeReady;
    };
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
  # The token-only include must stay runtime-managed outside the Nix store and
  # readable by the dedicated cratedigger/operator group (for example
  # root:cratedigger-ops 0440). Add trusted plain-beet operators to that group.
  #
  # Payoff: gid-`users` media servers (Jellyfin) can both read fetched
  # album art and write NFO/artwork alongside the media — see
  # docs/nixos-module.md § "Running non-root + filesystem permissions".

  # The deployment owns Beets storage; Cratedigger manages only its own state
  # and private processing roots. These declarative entries complement the
  # readiness service and do not relocate an existing catalog.
  systemd.tmpfiles.rules = [
    "d /srv/music 0755 root root -"
    "d /srv/music/library 2775 cratedigger users -"
    "d /srv/music/incoming 2775 cratedigger users -"
    "d /var/lib/beets-library 2770 cratedigger cratedigger-ops -"
    "d /var/lib/beets 0750 root cratedigger-ops -"
  ];
}
