# Upstream NixOS module for Soularr / Cratedigger.
#
# Generic, paths-as-options, no sops/homelab/nspawn assumptions. Downstream
# wrappers (e.g. ~/nixosconfig) layer their secrets backend, DB host, and
# reverse-proxy on top via standard NixOS option merging.
#
# Identity defaults to root because slskd downloads land outside the soularr
# user's home and beets needs broad filesystem access. Override with
# `services.soularr.user` / `services.soularr.group` if you're hardened.
{
  config,
  lib,
  pkgs,
  ...
}: let
  inherit (lib) mkOption mkEnableOption mkIf optionalString types concatStringsSep;

  cfg = config.services.soularr;
  src = cfg.src;

  # Same python env the dev shell uses — single source of truth.
  cratedigger = pkgs.callPackage ./package.nix {};
  pythonEnv = cratedigger.pythonEnv;

  runtimePath = lib.makeBinPath [
    pkgs.bash
    pkgs.coreutils
    pkgs.gnugrep
    pkgs.gnused
    pkgs.curl
    pkgs.jq
    pkgs.ffmpeg
    pkgs.mp3val
    pkgs.flac
    pkgs.sox
  ];

  # CLI wrappers — the only place PYTHONPATH is set.
  soularrPkg = pkgs.writeShellScriptBin "soularr" ''
    export PATH="${runtimePath}:$PATH"
    exec ${pythonEnv}/bin/python ${src}/soularr.py "$@"
  '';

  pipelineCli = pkgs.writeShellScriptBin "pipeline-cli" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:${src}/lib:''${PYTHONPATH:-}"
    exec ${pythonEnv}/bin/python ${src}/scripts/pipeline_cli.py \
      --dsn "${cfg.pipelineDb.dsn}" "$@"
  '';

  pipelineMigrate = pkgs.writeShellScriptBin "pipeline-migrate" ''
    export PYTHONPATH="${src}:${src}/lib:''${PYTHONPATH:-}"
    exec ${pythonEnv}/bin/python ${src}/scripts/migrate_db.py \
      --dsn "${cfg.pipelineDb.dsn}" \
      --migrations-dir "${src}/migrations" "$@"
  '';

  webPkg = pkgs.writeShellScriptBin "soularr-web" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:${src}/lib:${src}/web:''${PYTHONPATH:-}"
    exec ${pythonEnv}/bin/python ${src}/web/server.py \
      --port ${toString cfg.web.port} \
      --dsn "${cfg.pipelineDb.dsn}" \
      --beets-db "${cfg.web.beetsDb}" \
      --redis-host "${cfg.web.redis.host}" \
      --redis-port ${toString cfg.web.redis.port} "$@"
  '';

  # [Quality Ranks] section — declarative mirror of QualityRankConfig.defaults().
  # Pinned by TestQualityRankConfigDefaults in tests/test_quality_decisions.py.
  qualityRanksSection = let
    qr = cfg.qualityRanks;
    bandSection = codecKey: bands: ''
      ${codecKey}.transparent = ${toString bands.transparent}
      ${codecKey}.excellent = ${toString bands.excellent}
      ${codecKey}.good = ${toString bands.good}
      ${codecKey}.acceptable = ${toString bands.acceptable}
    '';
  in
    lib.strings.removeSuffix "\n" ''
      [Quality Ranks]
      bitrate_metric = ${qr.bitrateMetric}
      gate_min_rank = ${qr.gateMinRank}
      within_rank_tolerance_kbps = ${toString qr.withinRankToleranceKbps}

      ${bandSection "opus" qr.bands.opus}
      ${bandSection "mp3_vbr" qr.bands.mp3Vbr}
      ${bandSection "mp3_cbr" qr.bands.mp3Cbr}
      ${bandSection "aac" qr.bands.aac}
    '';

  configTemplate = pkgs.writeText "soularr-config.ini" ''
    [Slskd]
    api_key = SLSKD_API_KEY_PLACEHOLDER
    host_url = ${cfg.slskd.hostUrl}
    url_base = ${cfg.slskd.urlBase}
    download_dir = ${cfg.slskd.downloadDir}
    delete_searches = ${if cfg.slskd.deleteSearches then "True" else "False"}
    stalled_timeout = ${toString cfg.slskd.stalledTimeout}
    remote_queue_timeout = ${toString cfg.slskd.remoteQueueTimeout}

    [Release Settings]
    use_most_common_tracknum = ${if cfg.releaseSettings.useMostCommonTracknum then "True" else "False"}
    allow_multi_disc = ${if cfg.releaseSettings.allowMultiDisc then "True" else "False"}
    accepted_countries = ${concatStringsSep "," cfg.releaseSettings.acceptedCountries}
    skip_region_check = ${if cfg.releaseSettings.skipRegionCheck then "True" else "False"}
    accepted_formats = ${concatStringsSep "," cfg.releaseSettings.acceptedFormats}

    [Search Settings]
    search_timeout = ${toString cfg.searchSettings.searchTimeout}
    maximum_peer_queue = ${toString cfg.searchSettings.maximumPeerQueue}
    minimum_peer_upload_speed = ${toString cfg.searchSettings.minimumPeerUploadSpeed}
    minimum_filename_match_ratio = ${toString cfg.searchSettings.minimumFilenameMatchRatio}
    allowed_filetypes = ${concatStringsSep "," cfg.searchSettings.allowedFiletypes}
    ignored_users = ${concatStringsSep "," cfg.searchSettings.ignoredUsers}
    search_for_tracks = ${if cfg.searchSettings.searchForTracks then "True" else "False"}
    album_prepend_artist = ${if cfg.searchSettings.albumPrependArtist then "True" else "False"}
    track_prepend_artist = ${if cfg.searchSettings.trackPrependArtist then "True" else "False"}
    search_type = ${cfg.searchSettings.searchType}
    parallel_searches = ${toString cfg.searchSettings.parallelSearches}
    number_of_albums_to_grab = ${toString cfg.searchSettings.numberOfAlbumsToGrab}
    title_blacklist = ${concatStringsSep "," cfg.searchSettings.titleBlacklist}
    search_blacklist = ${concatStringsSep "," cfg.searchSettings.searchBlacklist}

    [Download Settings]
    download_filtering = ${if cfg.downloadSettings.downloadFiltering then "True" else "False"}
    use_extension_whitelist = ${if cfg.downloadSettings.useExtensionWhitelist then "True" else "False"}
    extensions_whitelist = ${concatStringsSep "," cfg.downloadSettings.extensionsWhitelist}

    [Beets Validation]
    enabled = ${if cfg.beetsValidation.enable then "True" else "False"}
    harness_path = ${cfg.beetsValidation.harnessPath}
    distance_threshold = ${toString cfg.beetsValidation.distanceThreshold}
    staging_dir = ${cfg.beetsValidation.stagingDir}
    tracking_file = ${cfg.beetsValidation.trackingFile}
    verified_lossless_target = ${cfg.beetsValidation.verifiedLosslessTarget}

    ${qualityRanksSection}
    [Pipeline DB]
    enabled = ${if cfg.pipelineDb.enable then "True" else "False"}
    dsn = ${cfg.pipelineDb.dsn}

    [Meelo]
    url = ${cfg.notifiers.meelo.url}
    username = MEELO_USERNAME_PLACEHOLDER
    password = MEELO_PASSWORD_PLACEHOLDER

    [Plex]
    url = ${cfg.notifiers.plex.url}
    token = PLEX_TOKEN_PLACEHOLDER
    library_section_id = ${toString cfg.notifiers.plex.librarySectionId}
    path_map = ${cfg.notifiers.plex.pathMap}

    [Jellyfin]
    url = ${cfg.notifiers.jellyfin.url}
    token = JELLYFIN_TOKEN_PLACEHOLDER

    [Logging]
    level = ${cfg.logging.level}
    format = ${cfg.logging.format}
    datefmt = ${cfg.logging.datefmt}
  '';

  # Reads each enabled secret file, substitutes placeholders. Generic — doesn't
  # care whether the files were placed by sops, agenix, or `echo > file`.
  preStartScript = pkgs.writeShellScript "soularr-prestart" ''
    set -euo pipefail
    config_dir="${cfg.stateDir}"
    mkdir -p "$config_dir"

    read_file() {
      local f="$1"
      if [[ ! -r "$f" ]]; then
        echo "soularr: required secret file $f not readable" >&2
        exit 1
      fi
      ${pkgs.coreutils}/bin/cat "$f"
    }

    slskd_key=$(read_file "${cfg.slskd.apiKeyFile}")
    ${optionalString cfg.notifiers.meelo.enable ''
      meelo_user=$(read_file "${toString cfg.notifiers.meelo.usernameFile}")
      meelo_pass=$(read_file "${toString cfg.notifiers.meelo.passwordFile}")
    ''}
    ${optionalString (!cfg.notifiers.meelo.enable) ''
      meelo_user=""
      meelo_pass=""
    ''}
    ${optionalString cfg.notifiers.plex.enable ''
      plex_token=$(read_file "${toString cfg.notifiers.plex.tokenFile}")
    ''}
    ${optionalString (!cfg.notifiers.plex.enable) ''
      plex_token=""
    ''}
    ${optionalString cfg.notifiers.jellyfin.enable ''
      jellyfin_token=$(read_file "${toString cfg.notifiers.jellyfin.tokenFile}")
    ''}
    ${optionalString (!cfg.notifiers.jellyfin.enable) ''
      jellyfin_token=""
    ''}

    ${pkgs.gnused}/bin/sed \
      -e "s|SLSKD_API_KEY_PLACEHOLDER|$slskd_key|" \
      -e "s|MEELO_USERNAME_PLACEHOLDER|$meelo_user|" \
      -e "s|MEELO_PASSWORD_PLACEHOLDER|$meelo_pass|" \
      -e "s|PLEX_TOKEN_PLACEHOLDER|$plex_token|" \
      -e "s|JELLYFIN_TOKEN_PLACEHOLDER|$jellyfin_token|" \
      ${configTemplate} > "$config_dir/config.ini"

    chmod 600 "$config_dir/config.ini"
    rm -f "$config_dir/.soularr.lock"
  '';

  # Optional health check for a stuck slskd reconnect loop. Generic — the
  # restart command is configurable so non-systemd slskd setups still work.
  slskdHealthCheck = pkgs.writeShellScript "soularr-slskd-healthcheck" ''
    set -euo pipefail
    api_key=$(${pkgs.coreutils}/bin/cat "${cfg.slskd.apiKeyFile}")
    status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
    connected=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isConnected // false')
    logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
    if [ "$connected" = "true" ] && [ "$logged_in" = "true" ]; then
      exit 0
    fi
    echo "soularr: slskd not connected (connected=$connected, loggedIn=$logged_in)" >&2
    ${optionalString (cfg.healthCheck.onFailureCommand != "") ''
      echo "soularr: running onFailureCommand to recover slskd..." >&2
      ${cfg.healthCheck.onFailureCommand}
      for i in $(${pkgs.coreutils}/bin/seq 1 12); do
        ${pkgs.coreutils}/bin/sleep 5
        status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
        logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
        if [ "$logged_in" = "true" ]; then
          echo "soularr: slskd reconnected after recovery" >&2
          exit 0
        fi
      done
    ''}
    echo "soularr: slskd unhealthy, skipping run" >&2
    exit 1
  '';

  # `mkCodecBands` is the same factory the legacy module used.
  mkCodecBands = codec: defaults: {
    transparent = mkOption {
      type = types.int;
      default = defaults.transparent;
      description = "${codec} TRANSPARENT rank floor (kbps).";
    };
    excellent = mkOption {
      type = types.int;
      default = defaults.excellent;
      description = "${codec} EXCELLENT rank floor (kbps).";
    };
    good = mkOption {
      type = types.int;
      default = defaults.good;
      description = "${codec} GOOD rank floor (kbps).";
    };
    acceptable = mkOption {
      type = types.int;
      default = defaults.acceptable;
      description = "${codec} ACCEPTABLE rank floor (kbps).";
    };
  };
in {
  options.services.soularr = {
    enable = mkEnableOption "Soularr — Soulseek download pipeline";

    src = mkOption {
      type = types.path;
      default = ../.;
      defaultText = lib.literalExpression "../.";
      description = "Path to the soularr source tree. Defaults to this flake's repo root.";
    };

    user = mkOption {
      type = types.str;
      default = "root";
      description = ''
        UNIX user to run soularr as. Defaults to root because slskd downloads
        and the beets library typically live outside any service-user home and
        soularr needs broad read/write access. Override only if you've set up
        the surrounding permissions (slskd group membership, beets DB
        ownership, /Incoming write access, etc.) for an unprivileged user.
      '';
    };

    group = mkOption {
      type = types.str;
      default = "root";
      description = "UNIX group to run soularr as. See `user` for context.";
    };

    stateDir = mkOption {
      type = types.str;
      default = "/var/lib/soularr";
      description = "Runtime state directory (config.ini, lock file).";
    };

    timer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run soularr periodically via systemd timer.";
      };
      onBootSec = mkOption {
        type = types.str;
        default = "5min";
        description = "Delay after boot before first timer fire.";
      };
      onUnitActiveSec = mkOption {
        type = types.str;
        default = "5min";
        description = "Interval between cycles.";
      };
    };

    slskd = {
      apiKeyFile = mkOption {
        type = types.path;
        description = ''
          Path to a file containing the slskd API key (raw, no envvar prefix).
          Must be readable by services.soularr.user. Use sops/agenix or any
          out-of-band mechanism — the module just reads the file at runtime.
        '';
      };
      hostUrl = mkOption {
        type = types.str;
        default = "http://localhost:5030";
        description = "slskd HTTP base URL.";
      };
      urlBase = mkOption {
        type = types.str;
        default = "/";
        description = "slskd URL prefix when behind a reverse proxy.";
      };
      downloadDir = mkOption {
        type = types.str;
        description = "Directory slskd downloads land in.";
      };
      deleteSearches = mkOption {
        type = types.bool;
        default = true;
      };
      stalledTimeout = mkOption {
        type = types.int;
        default = 600;
        description = "Seconds before a stalled download is abandoned.";
      };
      remoteQueueTimeout = mkOption {
        type = types.int;
        default = 3600;
        description = "Seconds before a remote-queued download is abandoned.";
      };
    };

    pipelineDb = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Use the pipeline DB as album source (currently the only supported mode).";
      };
      dsn = mkOption {
        type = types.str;
        example = "postgresql://soularr@localhost/soularr";
        description = "PostgreSQL connection string for the pipeline DB.";
      };
    };

    beetsValidation = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Validate every download against MusicBrainz via beets before import.";
      };
      harnessPath = mkOption {
        type = types.str;
        default = "${cfg.src}/harness/run_beets_harness.sh";
        defaultText = lib.literalExpression "\${cfg.src}/harness/run_beets_harness.sh";
        description = "Path to the beets harness wrapper script.";
      };
      distanceThreshold = mkOption {
        type = types.float;
        default = 0.15;
        description = "Maximum beets match distance to accept (0.0 = perfect, 1.0 = no match).";
      };
      stagingDir = mkOption {
        type = types.str;
        description = "Directory to stage validated albums for beets import.";
      };
      trackingFile = mkOption {
        type = types.str;
        description = "JSONL file tracking beets validation results.";
      };
      verifiedLosslessTarget = mkOption {
        type = types.str;
        default = "";
        description = "Target format after verified lossless (e.g. 'opus 128', 'mp3 v2'). Empty = keep V0.";
      };
    };

    web = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Run the web UI (album request manager).";
      };
      port = mkOption {
        type = types.port;
        default = 8085;
      };
      beetsDb = mkOption {
        type = types.str;
        description = "Path to the beets library SQLite database (read-only).";
      };
      redis = {
        host = mkOption {
          type = types.str;
          default = "127.0.0.1";
          description = "Redis host for caching. The module does NOT enable a redis server — provide one via services.redis.servers.* in your own config.";
        };
        port = mkOption {
          type = types.port;
          default = 6379;
        };
      };
    };

    notifiers = {
      meelo = {
        enable = mkEnableOption "Meelo post-import scanner notifier";
        url = mkOption {
          type = types.str;
          default = "";
          example = "https://meelo.example.com";
        };
        usernameFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
        passwordFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
      };
      plex = {
        enable = mkEnableOption "Plex post-import scanner notifier";
        url = mkOption {
          type = types.str;
          default = "";
          example = "https://plex.example.com";
        };
        tokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
        librarySectionId = mkOption {
          type = types.int;
          default = 0;
          description = "Plex library section ID (numeric).";
        };
        pathMap = mkOption {
          type = types.str;
          default = "";
          example = "/mnt/virtio/Music/Beets:/prom_music";
          description = "host:container path remap for partial-section refreshes.";
        };
      };
      jellyfin = {
        enable = mkEnableOption "Jellyfin post-import scanner notifier";
        url = mkOption {
          type = types.str;
          default = "";
        };
        tokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
      };
    };

    healthCheck = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Verify slskd is connected before running each cycle.";
      };
      onFailureCommand = mkOption {
        type = types.str;
        default = "";
        example = "systemctl restart slskd.service";
        description = ''
          Shell command to run when the health check fails. Empty = log and skip
          the run. The command is invoked as root (or services.soularr.user) and
          retries are attempted for up to a minute after it returns.
        '';
      };
    };

    releaseSettings = {
      useMostCommonTracknum = mkOption { type = types.bool; default = true; };
      allowMultiDisc = mkOption { type = types.bool; default = true; };
      acceptedCountries = mkOption {
        type = types.listOf types.str;
        default = ["Europe" "Japan" "United Kingdom" "United States" "[Worldwide]" "Australia" "Canada"];
      };
      skipRegionCheck = mkOption { type = types.bool; default = false; };
      acceptedFormats = mkOption {
        type = types.listOf types.str;
        default = ["CD" "Digital Media" "Vinyl"];
      };
    };

    searchSettings = {
      searchTimeout = mkOption { type = types.int; default = 30000; description = "Milliseconds."; };
      maximumPeerQueue = mkOption { type = types.int; default = 50; };
      minimumPeerUploadSpeed = mkOption { type = types.int; default = 0; };
      minimumFilenameMatchRatio = mkOption { type = types.float; default = 0.6; };
      allowedFiletypes = mkOption {
        type = types.listOf types.str;
        default = ["mp3 v0" "mp3 320" "flac 24/192" "flac 24/96" "flac 24/48" "flac 16/44.1" "flac" "alac" "aac" "opus" "ogg" "mp3" "wav"];
        description = ''
          Priority-ordered filetype filter. The rank model in lib/quality.py is
          the authoritative quality decision (post-download); this filter is
          only for search-time peer/codec preference.
        '';
      };
      ignoredUsers = mkOption {
        type = types.listOf types.str;
        default = [];
      };
      searchForTracks = mkOption { type = types.bool; default = true; };
      albumPrependArtist = mkOption { type = types.bool; default = true; };
      trackPrependArtist = mkOption { type = types.bool; default = true; };
      searchType = mkOption {
        type = types.enum ["incrementing_page" "all_at_once"];
        default = "incrementing_page";
      };
      parallelSearches = mkOption { type = types.int; default = 8; };
      numberOfAlbumsToGrab = mkOption { type = types.int; default = 16; };
      titleBlacklist = mkOption {
        type = types.listOf types.str;
        default = [];
      };
      searchBlacklist = mkOption {
        type = types.listOf types.str;
        default = [];
      };
    };

    downloadSettings = {
      downloadFiltering = mkOption { type = types.bool; default = true; };
      useExtensionWhitelist = mkOption { type = types.bool; default = false; };
      extensionsWhitelist = mkOption {
        type = types.listOf types.str;
        default = ["lrc" "nfo" "txt"];
      };
    };

    qualityRanks = {
      gateMinRank = mkOption {
        type = types.enum ["unknown" "poor" "acceptable" "good" "excellent" "transparent" "lossless"];
        default = "excellent";
      };
      bitrateMetric = mkOption {
        type = types.enum ["min" "avg" "median"];
        default = "avg";
      };
      withinRankToleranceKbps = mkOption {
        type = types.int;
        default = 5;
      };
      bands = {
        opus = mkCodecBands "Opus" {
          transparent = 112;
          excellent = 88;
          good = 64;
          acceptable = 48;
        };
        mp3Vbr = mkCodecBands "MP3 VBR" {
          transparent = 245;
          excellent = 210;
          good = 170;
          acceptable = 130;
        };
        mp3Cbr = mkCodecBands "MP3 CBR" {
          transparent = 320;
          excellent = 256;
          good = 192;
          acceptable = 128;
        };
        aac = mkCodecBands "AAC" {
          transparent = 192;
          excellent = 144;
          good = 112;
          acceptable = 80;
        };
      };
    };

    logging = {
      level = mkOption { type = types.str; default = "INFO"; };
      format = mkOption {
        type = types.str;
        default = "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s";
      };
      datefmt = mkOption {
        type = types.str;
        default = "%Y-%m-%dT%H:%M:%S%z";
      };
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = !cfg.notifiers.meelo.enable || (cfg.notifiers.meelo.usernameFile != null && cfg.notifiers.meelo.passwordFile != null && cfg.notifiers.meelo.url != "");
        message = "services.soularr.notifiers.meelo: enable requires url, usernameFile, and passwordFile";
      }
      {
        assertion = !cfg.notifiers.plex.enable || (cfg.notifiers.plex.tokenFile != null && cfg.notifiers.plex.url != "");
        message = "services.soularr.notifiers.plex: enable requires url and tokenFile";
      }
      {
        assertion = !cfg.notifiers.jellyfin.enable || (cfg.notifiers.jellyfin.tokenFile != null && cfg.notifiers.jellyfin.url != "");
        message = "services.soularr.notifiers.jellyfin: enable requires url and tokenFile";
      }
    ];

    environment.systemPackages = [pipelineCli pipelineMigrate pkgs.postgresql];

    users.users = mkIf (cfg.user != "root") {
      ${cfg.user} = {
        isSystemUser = true;
        group = cfg.group;
        description = "Soularr service user";
      };
    };
    users.groups = mkIf (cfg.group != "root") {
      ${cfg.group} = {};
    };

    systemd.tmpfiles.rules = [
      "d ${cfg.stateDir} 0750 ${cfg.user} ${cfg.group} -"
    ];

    # Schema migrator. RemainAfterExit=true so soularr / soularr-web can
    # require us without re-running on every cycle. Idempotent — fast no-op
    # when schema is already current.
    systemd.services.soularr-db-migrate = {
      description = "Apply Soularr pipeline DB schema migrations";
      wantedBy = ["multi-user.target"];
      restartIfChanged = true;
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.user;
        Group = cfg.group;
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
        ExecStart = "${pipelineMigrate}/bin/pipeline-migrate";
      };
    };

    systemd.services.soularr = {
      description = "Soularr — Soulseek download pipeline";
      after = ["soularr-db-migrate.service"];
      requires = ["soularr-db-migrate.service"];
      restartIfChanged = false;
      # Deliberately exclude pythonEnv: it ships a `beet` binary (because
      # `pkgs.beets` is in pythonEnv for the dev shell + tests), and putting
      # it on PATH shadows whatever `beet` the consumer has provisioned for
      # the harness wrapper to find. The python interpreter is invoked via
      # absolute path inside soularrPkg / pipelineCli, so it doesn't need
      # to be on PATH. The harness wrapper at harness/run_beets_harness.sh
      # uses `command -v beet` to find a beets installation that already
      # has the consumer's plugins/config (e.g. home-manager).
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = lib.optional cfg.healthCheck.enable slskdHealthCheck ++ [preStartScript];
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
        ExecStart = "${soularrPkg}/bin/soularr";
        WorkingDirectory = cfg.stateDir;
      };
    };

    systemd.timers.soularr = mkIf cfg.timer.enable {
      description = "Soularr periodic run timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = cfg.timer.onBootSec;
        OnUnitActiveSec = cfg.timer.onUnitActiveSec;
        Persistent = true;
      };
    };

    systemd.services.soularr-web = mkIf cfg.web.enable {
      description = "Soularr web UI";
      after = ["soularr-db-migrate.service"];
      requires = ["soularr-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${webPkg}/bin/soularr-web";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
      };
    };
  };
}
