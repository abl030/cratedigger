# Upstream NixOS module for Cratedigger.
#
# Generic, paths-as-options, no sops/homelab/nspawn assumptions. Downstream
# wrappers (e.g. ~/nixosconfig) layer their secrets backend, DB host, and
# reverse-proxy on top via standard NixOS option merging.
#
# Identity defaults to root because slskd downloads land outside the cratedigger
# user's home and beets needs broad filesystem access. Override with
# `services.cratedigger.user` / `services.cratedigger.group` if you're hardened.
{
  config,
  lib,
  pkgs,
  ...
}: let
  inherit (lib) mkOption mkEnableOption mkIf optionalString types concatStringsSep;

  cfg = config.services.cratedigger;
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
  importPreviewEnableEnv = if cfg.importer.preview.enable then "1" else "0";

  # CLI wrappers — the only place PYTHONPATH is set.
  cratediggerPkg = pkgs.writeShellScriptBin "cratedigger" ''
    export PATH="${runtimePath}:$PATH"
    export CRATEDIGGER_IMPORT_PREVIEW_ENABLE="${importPreviewEnableEnv}"
    exec ${pythonEnv}/bin/python ${src}/cratedigger.py "$@"
  '';

  # PYTHONPATH carries ONLY the repo root. Adding ${src}/lib or ${src}/web
  # puts our modules at the top level of sys.path, where lib/beets.py
  # shadows the beets PyPI package for any subprocess (including `beet`)
  # that inherits PYTHONPATH. That shadow load executes our `import
  # msgspec` before the subprocess can reach its own site-packages, and
  # crashes it with ModuleNotFoundError. All internal imports use
  # `from lib.X import Y` / `from web.X import Y` against the repo root
  # already, so the flat entries are both unnecessary and harmful.
  pipelineCli = pkgs.writeShellScriptBin "pipeline-cli" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:''${PYTHONPATH:-}"
    export CRATEDIGGER_IMPORT_PREVIEW_ENABLE="${importPreviewEnableEnv}"
    exec ${pythonEnv}/bin/python ${src}/scripts/pipeline_cli.py \
      --dsn "${cfg.pipelineDb.dsn}" "$@"
  '';

  pipelineMigrate = pkgs.writeShellScriptBin "pipeline-migrate" ''
    export PYTHONPATH="${src}:''${PYTHONPATH:-}"
    exec ${pythonEnv}/bin/python ${src}/scripts/migrate_db.py \
      --dsn "${cfg.pipelineDb.dsn}" \
      --migrations-dir "${src}/migrations" "$@"
  '';

  importerPkg = pkgs.writeShellScriptBin "cratedigger-importer" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:''${PYTHONPATH:-}"
    export CRATEDIGGER_IMPORT_PREVIEW_ENABLE="${importPreviewEnableEnv}"
    exec ${pythonEnv}/bin/python ${src}/scripts/importer.py \
      --dsn "${cfg.pipelineDb.dsn}" "$@"
  '';

  previewWorkerPkg = pkgs.writeShellScriptBin "cratedigger-import-preview-worker" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:''${PYTHONPATH:-}"
    export CRATEDIGGER_IMPORT_PREVIEW_ENABLE="${importPreviewEnableEnv}"
    exec ${pythonEnv}/bin/python ${src}/scripts/import_preview_worker.py \
      --dsn "${cfg.pipelineDb.dsn}" \
      --workers ${toString cfg.importer.previewWorkers} "$@"
  '';

  webPkg = pkgs.writeShellScriptBin "cratedigger-web" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}:''${PYTHONPATH:-}"
    export CRATEDIGGER_IMPORT_PREVIEW_ENABLE="${importPreviewEnableEnv}"
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

  # Issue #117: secrets live at the *File paths referenced here. The cratedigger
  # Python code reads them on demand via CratediggerConfig.resolved_*() accessors,
  # so nothing sensitive is ever embedded in config.ini and the file can be
  # world-readable (see absence of chmod/chgrp in preStartScript).
  configTemplate = pkgs.writeText "cratedigger-config.ini" ''
    [Slskd]
    api_key_file = ${cfg.slskd.apiKeyFile}
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
    search_response_limit = ${toString cfg.searchSettings.searchResponseLimit}
    search_file_limit = ${toString cfg.searchSettings.searchFileLimit}
    browse_top_k = ${toString cfg.searchSettings.browseTopK}
    browse_wave_deadline_s = ${toString cfg.searchSettings.browseWaveDeadlineS}
    browse_global_max_workers = ${toString cfg.searchSettings.browseGlobalMaxWorkers}
    browse_cycle_budget_s = ${toString cfg.searchSettings.browseCycleBudgetS}

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
    username_file = ${toString cfg.notifiers.meelo.usernameFile}
    password_file = ${toString cfg.notifiers.meelo.passwordFile}

    [Plex]
    url = ${cfg.notifiers.plex.url}
    token_file = ${toString cfg.notifiers.plex.tokenFile}
    library_section_id = ${toString cfg.notifiers.plex.librarySectionId}
    path_map = ${cfg.notifiers.plex.pathMap}

    [Jellyfin]
    url = ${cfg.notifiers.jellyfin.url}
    token_file = ${toString cfg.notifiers.jellyfin.tokenFile}

    [Logging]
    level = ${cfg.logging.level}
    format = ${cfg.logging.format}
    datefmt = ${cfg.logging.datefmt}
  '';

  # Install the rendered template into stateDir. Since config.ini no longer
  # embeds any plaintext secrets (issue #117 — they're *File paths now), there's
  # no chmod dance, no sed substitution, and no group-ownership hack. The
  # secrets themselves still need to be readable by cfg.user at whatever paths
  # slskd.apiKeyFile / notifiers.*.{username,password,token}File point to.
  preStartScript = pkgs.writeShellScript "cratedigger-prestart" ''
    set -euo pipefail
    config_dir="${cfg.stateDir}"
    mkdir -p "$config_dir"
    tmp="$(${pkgs.coreutils}/bin/mktemp "$config_dir/.config.ini.XXXXXX")"
    trap '${pkgs.coreutils}/bin/rm -f "$tmp"' EXIT
    ${pkgs.coreutils}/bin/cp ${configTemplate} "$tmp"
    ${pkgs.coreutils}/bin/chmod 0644 "$tmp"
    ${pkgs.coreutils}/bin/mv -f "$tmp" "$config_dir/config.ini"
    trap - EXIT
    rm -f "$config_dir/.cratedigger.lock"
  '';

  # Optional health check for a stuck slskd reconnect loop. Generic — the
  # restart command is configurable so non-systemd slskd setups still work.
  slskdHealthCheck = pkgs.writeShellScript "cratedigger-slskd-healthcheck" ''
    set -euo pipefail
    api_key=$(${pkgs.coreutils}/bin/cat "${cfg.slskd.apiKeyFile}")
    status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
    connected=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isConnected // false')
    logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
    if [ "$connected" = "true" ] && [ "$logged_in" = "true" ]; then
      exit 0
    fi
    echo "cratedigger: slskd not connected (connected=$connected, loggedIn=$logged_in)" >&2
    ${optionalString (cfg.healthCheck.onFailureCommand != "") ''
      echo "cratedigger: running onFailureCommand to recover slskd..." >&2
      ${cfg.healthCheck.onFailureCommand}
      for i in $(${pkgs.coreutils}/bin/seq 1 12); do
        ${pkgs.coreutils}/bin/sleep 5
        status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
        logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
        if [ "$logged_in" = "true" ]; then
          echo "cratedigger: slskd reconnected after recovery" >&2
          exit 0
        fi
      done
    ''}
    echo "cratedigger: slskd unhealthy, skipping run" >&2
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
  options.services.cratedigger = {
    enable = mkEnableOption "Cratedigger — Soulseek download pipeline";

    src = mkOption {
      type = types.path;
      default = ../.;
      defaultText = lib.literalExpression "../.";
      description = "Path to the cratedigger source tree. Defaults to this flake's repo root.";
    };

    user = mkOption {
      type = types.str;
      default = "root";
      description = ''
        UNIX user to run cratedigger as. Defaults to root because slskd downloads
        and the beets library typically live outside any service-user home and
        cratedigger needs broad read/write access. Override only if you've set up
        the surrounding permissions (slskd group membership, beets DB
        ownership, /Incoming write access, etc.) for an unprivileged user.
      '';
    };

    group = mkOption {
      type = types.str;
      default = "root";
      description = "UNIX group to run cratedigger as. See `user` for context.";
    };

    stateDir = mkOption {
      type = types.str;
      default = "/var/lib/cratedigger";
      description = "Runtime state directory (config.ini, lock file).";
    };

    timer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run cratedigger periodically via systemd timer.";
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

    importer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run the long-lived importer worker that drains the shared import queue.";
      };
      preview.enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Enable the async preview gate before the serial importer. When false,
          newly enqueued import jobs are marked importable immediately for
          backward-compatible draining without preview workers.
        '';
      };
      previewWorkers = mkOption {
        type = types.int;
        default = 2;
        description = "Number of async import preview workers to run before the serial importer lane.";
      };
    };

    slskd = {
      apiKeyFile = mkOption {
        type = types.path;
        description = ''
          Path to a file containing the slskd API key (raw, no envvar prefix).
          Must be readable by services.cratedigger.user. Use sops/agenix or any
          out-of-band mechanism — the module just reads the file at runtime.

          Since issue #117 this path is written directly into config.ini and
          read on demand by the Python pipeline. No plaintext copy lives in
          config.ini, and the rendered file is world-readable. If non-root
          tooling (e.g. pipeline-cli force-import) also needs to reach slskd,
          that operator user must be able to read this file too — typically
          done by mode 0440 + an operator group, not by loosening config.ini.
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
        example = "postgresql://cratedigger@localhost/cratedigger";
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

    # Notifier credential *File options follow the same contract as
    # slskd.apiKeyFile (issue #117): paths written into config.ini, read on
    # demand by CratediggerConfig.resolved_*(). They must be readable by
    # services.cratedigger.user. If the operator also triggers imports via
    # pipeline-cli from a non-root shell, the same files must be readable
    # by that user too, otherwise notifier scans silently no-op after
    # CLI-triggered imports (the import itself still succeeds).
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
          the run. The command is invoked as root (or services.cratedigger.user) and
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
      searchResponseLimit = mkOption {
        type = types.int;
        default = 1000;
        description = ''
          Caps how many peer responses slskd collects per search. Maps to
          slskd's `responseLimit` ceiling — raising this lets the matcher
          consider more peers per query at the cost of a longer search window.
        '';
      };
      searchFileLimit = mkOption {
        type = types.int;
        default = 50000;
        description = ''
          Caps how many total files slskd collects across all peer responses
          per search. Maps to slskd's `fileLimit` ceiling. The slskd-api
          default (10000) terminates popular multi-disc searches in a few
          seconds — possibly before the right peer responds. 50000 gives the
          matcher more peer diversity for albums where each peer holds 50+
          files (compilations, OSTs, multi-disc reissues).
        '';
      };
      browseTopK = mkOption {
        type = types.int;
        default = 20;
        description = ''
          First wave size for parallel peer browse fan-out. After ranking
          eligible peers by upload speed, the top K are browsed concurrently
          and the cache is matched against them. If no match is found, the
          tail is browsed in further chunks of K. Tune downward if first-match
          rank is consistently low; tune upward only if browse budget allows.
          See issue #198.
        '';
      };
      browseWaveDeadlineS = mkOption {
        type = types.float;
        default = 20.0;
        description = ''
          Wall-clock deadline per fan-out wave, in seconds. Peers that have
          not returned a directory listing by the deadline are abandoned for
          the rest of this cycle (added to a per-cycle broken-user set).
          Soulseek's per-peer TCP timeout is ~30–60s and is not configurable
          per-call, so this client-side deadline is the only effective tail-
          latency defense.
        '';
      };
      browseGlobalMaxWorkers = mkOption {
        type = types.int;
        default = 32;
        description = ''
          Global cap on the ThreadPoolExecutor used by browse fan-out. Limits
          simultaneous in-flight `users.directory()` calls across all users
          and all dirs in a wave. Higher than browseTopK so a single user
          contributing many candidate dirs still gets meaningful parallelism.
          Watch slskd's own logs for serialisation if raised.
        '';
      };
      browseCycleBudgetS = mkOption {
        type = types.float;
        default = 240.0;
        description = ''
          Per-cycle cumulative browse wall-time budget, in seconds. When
          exceeded, remaining `wanted` records short-circuit (no further
          browse waves) and stay queued for the next cycle. Defends against
          multi-album-no-match cycles compounding into 50+ minute runs.
        '';
      };
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
        message = "services.cratedigger.notifiers.meelo: enable requires url, usernameFile, and passwordFile";
      }
      {
        assertion = !cfg.notifiers.plex.enable || (cfg.notifiers.plex.tokenFile != null && cfg.notifiers.plex.url != "");
        message = "services.cratedigger.notifiers.plex: enable requires url and tokenFile";
      }
      {
        assertion = !cfg.notifiers.jellyfin.enable || (cfg.notifiers.jellyfin.tokenFile != null && cfg.notifiers.jellyfin.url != "");
        message = "services.cratedigger.notifiers.jellyfin: enable requires url and tokenFile";
      }
      {
        assertion = cfg.importer.previewWorkers >= 1;
        message = "services.cratedigger.importer.previewWorkers must be at least 1";
      }
    ];

    environment.systemPackages = [pipelineCli pipelineMigrate importerPkg previewWorkerPkg pkgs.postgresql];

    users.users = mkIf (cfg.user != "root") {
      ${cfg.user} = {
        isSystemUser = true;
        group = cfg.group;
        description = "Cratedigger service user";
      };
    };
    users.groups = mkIf (cfg.group != "root") {
      ${cfg.group} = {};
    };

    # Since config.ini no longer embeds plaintext secrets (issue #117), the
    # state directory and the rendered config can both be world-readable. The
    # secrets themselves live at operator-chosen paths (see slskd.apiKeyFile
    # / notifiers.*.{username,password,token}File) and retain their own
    # restrictive modes from whatever provisioned them (sops-nix, agenix, etc).
    systemd.tmpfiles.rules = [
      "d ${cfg.stateDir} 0755 ${cfg.user} ${cfg.group} -"
    ];

    # Schema migrator. RemainAfterExit=true so cratedigger / cratedigger-web can
    # require us without re-running on every cycle. Idempotent — fast no-op
    # when schema is already current.
    systemd.services.cratedigger-db-migrate = {
      description = "Apply Cratedigger pipeline DB schema migrations";
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

    systemd.services.cratedigger = {
      description = "Cratedigger — Soulseek download pipeline";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      restartIfChanged = false;
      # Deliberately exclude pythonEnv: it ships a `beet` binary (because
      # `pkgs.beets` is in pythonEnv for the dev shell + tests), and putting
      # it on PATH shadows whatever `beet` the consumer has provisioned for
      # the harness wrapper to find. The python interpreter is invoked via
      # absolute path inside cratediggerPkg / pipelineCli, so it doesn't need
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
        ExecStart = "${cratediggerPkg}/bin/cratedigger";
        WorkingDirectory = cfg.stateDir;
      };
    };

    systemd.timers.cratedigger = mkIf cfg.timer.enable {
      description = "Cratedigger periodic run timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = cfg.timer.onBootSec;
        OnUnitActiveSec = cfg.timer.onUnitActiveSec;
        Persistent = true;
      };
    };

    systemd.services.cratedigger-importer = mkIf cfg.importer.enable {
      description = "Cratedigger importer queue worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = [preStartScript];
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
        ExecStart = "${importerPkg}/bin/cratedigger-importer";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.cratedigger-import-preview-worker = mkIf (cfg.importer.enable && cfg.importer.preview.enable) {
      description = "Cratedigger async import preview worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = [preStartScript];
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
        ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.cratedigger-web = mkIf cfg.web.enable {
      description = "Cratedigger web UI";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStart = "${webPkg}/bin/cratedigger-web";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = "PIPELINE_DB_DSN=${cfg.pipelineDb.dsn}";
      };
    };
  };
}
