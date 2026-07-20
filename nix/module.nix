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
  inherit (lib) mkOption mkEnableOption mkIf optional optionalString types concatStringsSep;

  cfg = config.services.cratedigger;
  src = cfg.src;

  # Every unit/wrapper interpolates the DSN; guard it so a missing value
  # yields the actionable message even if string coercion is forced before
  # the module assertions run. createLocally mkDefaults this option to the
  # local unix socket (peer auth as cfg.user — no password material, KTD5).
  pipelineDsn =
    if cfg.pipelineDb.dsn != null
    then cfg.pipelineDb.dsn
    else throw "services.cratedigger.pipelineDb.dsn is not set: either set it to your PostgreSQL connection string, or set services.cratedigger.pipelineDb.createLocally = true to provision a local database.";

  # The one beets (tier-2 plan U3, R4): pinned package + full built-in
  # plugin closure, with the mirror patches applied only when the operator
  # sets the knobs. This same derivation is the python library in pythonEnv
  # (lib/beets_distance.py) and the bin/beet behind cratedigger-beet; the
  # harness interpreter joins in U5.
  beetsEnv = import ./beets.nix {
    pkgs = cfg.packageSet;
    discogsMirrorUrl = cfg.beets.package.discogsMirrorUrl;
    lrclibUrl = cfg.beets.package.lrclibUrl;
  };

  # Config dir every beets consumer resolves via BEETSDIR (beets' native
  # config-dir override). preStart renders config.yaml (and, when a token
  # file is configured, secrets.yaml) into it.
  beetsConfigDir = "${cfg.stateDir}/beets";

  # The module owns beets config.yaml (tier-2 plan U4, R5). The attrset
  # mirrors the production config exactly (source of truth: the operator's
  # live HM-rendered ~/.config/beets/config.yaml, 2026-07-03); the U12
  # cutover gate diffs the rendered file against it, so change values here
  # only in lockstep with production intent.
  #
  # HARD-CODED, not options:
  #   - import.duplicate_keys.album = [mb_albumid discogs_albumid] — the
  #     Palo Santo data-loss invariant. Beets reads it strictly from
  #     config["import"]["duplicate_keys"]; a top-level duplicate_keys is
  #     silently ignored and re-enables cross-MBID sibling destruction.
  #     NEVER expose this as an option.
  #   - plugins — fixed production list; musicbrainz must be present or
  #     beets returns 0 candidates for everything.
  #   - paths / asciify_paths — path-affecting keys; drift here plus the
  #     importer's post-import `beet move` reproduces the 2026-05-18
  #     asciify mass-split (1,178 albums).
  #   - clutter includes the exact derived sidecar name `cratedigger.json` so
  #     the canonical `beet remove -d` bad-rip/Replace cleanup can prune a
  #     sidecar-only album directory. Files outside the configured clutter
  #     patterns still block Beets pruning.
  beetsSettings = let
    bc = cfg.beets.config;
  in {
    directory = bc.directory;
    library = bc.library;
    asciify_paths = true;
    clutter = [
      "Thumbs.DB" "Thumbs.db" ".DS_Store" "*.jpg" "*.png" "AlbumArt*"
      "Folder.*" "desktop.ini" "cratedigger.json"
    ];
    import = {
      copy = false;
      write = true;
      move = true;
      timid = false;
      incremental = true;
      incremental_skip_later = true;
      log = "${dirOf bc.library}/beets-import.log";
      languages = ["en"];
      duplicate_keys = {
        album = ["mb_albumid" "discogs_albumid"];
        item = ["artist" "title"];
      };
    };
    # %aunique disambiguates same-key (albumartist+album) sibling pressings
    # into distinct folders. It picks the first disambiguator field whose
    # values are all-distinct across the set, then renders each album's OWN
    # value — an album whose value is EMPTY renders NO bracket and lands on
    # the plain path, straight inside the sibling's sticky folder (the
    # Passenger collision, 2026-07-18: old pressing label='ATO Records',
    # new pressing label='' → label "won", new bracket empty). The fix is a
    # single computed disambiguator that is never empty by construction
    # (falling through pretty fields to $year); when siblings tie on it,
    # beets' built-in album-id fallback still yields a non-empty bracket.
    # Contract: tests/test_harness_beets2_contract.py
    # (TestAuniqueCollisionContract) sweeps this exact shipped config
    # against real beets.
    paths = {
      default = "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title";
      singleton = "Non-Album/$artist/$title";
      comp = "Compilations/$album%aunique{albumartist album,path_disambig}/$track $title";
    };
    album_fields.path_disambig = "albumdisambig or releasegroupdisambig or catalognum or label or str(year)";
    musicbrainz = {
      host = bc.musicbrainz.host;
      https = bc.musicbrainz.https;
      ratelimit = bc.musicbrainz.ratelimit;
    };
    match = {
      ignore_video_tracks = false;
      strong_rec_thresh = 0.10;
      medium_rec_thresh = 0.25;
      preferred = {
        countries = ["AU" "US" "GB|UK"];
        media = ["Digital Media|File" "CD"];
        original_year = true;
      };
    };
    plugins = "musicbrainz discogs fetchart embedart lyrics lastgenre scrub info missing duplicates edit fromfilename ftintitle the inline permissions";
    chroma.auto = false;
    permissions = {
      file = "0664";
      dir = "02775";
    };
    fetchart = {
      auto = true;
      minwidth = bc.fetchart.minwidth;
      maxwidth = bc.fetchart.maxwidth;
      quality = 75;
      high_resolution = false;
      sources = ["coverart" "itunes" "amazon" "albumart" "cover_art_url" "filesystem"];
    };
    embedart.auto = true;
    scrub.auto = true;
    lyrics = {
      auto = true;
      synced = true;
      sources = ["lrclib"];
    };
    lastgenre = {
      auto = true;
      count = 3;
      source = "album";
      canonical = true;
      separator = ", ";
      force = false;
    };
    the = {
      a = true;
      the = true;
    };
  } // (
    if cfg.beets.package.discogsTokenFile != null then {
      # Real token: issue #117 *File pattern — preStart materializes
      # secrets.yaml (0400) next to config.yaml; the world-readable
      # config.yaml carries only the include.
      include = ["secrets.yaml"];
    } else {
      # Tokenless stranger default (R7): any non-empty user_token makes
      # the discogs plugin skip its interactive OAuth flow at load, so
      # every plugin loads cleanly offline. Public-Discogs lookups with
      # this placeholder are documented token-required (they 401 per-use;
      # beets logs and falls back to MB candidates).
      discogs.user_token = "cratedigger-placeholder-token";
    }
  );

  beetsConfigTemplate = (pkgs.formats.yaml {}).generate "cratedigger-beets-config.yaml" beetsSettings;

  # Same python env the dev shell uses — single source of truth. Built from
  # cfg.packageSet (the flake export pins it to cratedigger's own flake.lock,
  # tier-2 plan U2/R1) so the runtime closure matches what the test suite ran
  # against, not whatever nixpkgs the consumer happens to be on.
  cratedigger = cfg.packageSet.callPackage ./package.nix { beetsPackage = beetsEnv; };
  pythonEnv = cratedigger.pythonEnv;

  # Canonical manual-ops beet for the library cratedigger manages. Pins
  # BEETSDIR so operator invocations and pipeline subprocesses read the
  # SAME module-rendered config — never a per-user ~/.config/beets.
  cratediggerBeet = pkgs.writeShellScriptBin "cratedigger-beet" ''
    export BEETSDIR="${beetsConfigDir}"
    exec ${pythonEnv}/bin/beet "$@"
  '';

  pyRunner = "${pythonEnv}/bin/python";

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
  redisServiceUnits = optional cfg.redis.enable "redis-cratedigger.service";

  # CLI wrappers — the only place PYTHONPATH is set.
  cratediggerPkg = pkgs.writeShellScriptBin "cratedigger" ''
    export PATH="${runtimePath}:$PATH"
    exec ${pyRunner} ${src}/cratedigger.py \
      --redis-host "${cfg.redis.host}" \
      --redis-port ${toString cfg.redis.port} "$@"
  '';

  # PYTHONPATH carries ONLY the repo root. Adding ${src}/lib or ${src}/web
  # puts our modules at the top level of sys.path, where lib/beets.py
  # shadows the beets PyPI package for any subprocess (including `beet`)
  # that inherits PYTHONPATH. That shadow load executes our `import
  # msgspec` before the subprocess can reach its own site-packages, and
  # crashes it with ModuleNotFoundError. All internal imports use
  # `from lib.X import Y` / `from web.X import Y` against the repo root
  # already, so the flat entries are both unnecessary and harmful.
  # pipeline-cli is a package (scripts/pipeline_cli/, issue #495) — exec
  # the __main__.py entry shim, which bootstraps sys.path the same way
  # the old flat file did (one extra ".." for the extra directory level)
  # before importing anything package-local.
  pipelineCli = pkgs.writeShellScriptBin "pipeline-cli" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pythonEnv}/bin/python ${src}/scripts/pipeline_cli/__main__.py \
      --dsn "${pipelineDsn}" "$@"
  '';

  pipelineMigrate = pkgs.writeShellScriptBin "pipeline-migrate" ''
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pythonEnv}/bin/python ${src}/scripts/migrate_db.py \
      --dsn "${pipelineDsn}" \
      --migrations-dir "${src}/migrations" "$@"
  '';

  importerPkg = pkgs.writeShellScriptBin "cratedigger-importer" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/importer.py \
      --dsn "${pipelineDsn}" "$@"
  '';

  previewWorkerPkg = pkgs.writeShellScriptBin "cratedigger-import-preview-worker" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/import_preview_worker.py \
      --dsn "${pipelineDsn}" \
      --workers ${toString cfg.importer.previewWorkers} "$@"
  '';

  webPkg = pkgs.writeShellScriptBin "cratedigger-web" ''
    export PATH="${runtimePath}:$PATH"
    # cratedigger-web imports beets IN-PROCESS (lib/beets_distance.py);
    # BEETSDIR points that import at the module-rendered config so
    # Replace-picker distances use the same match config the importer
    # sees (previously it silently read the invoking user's defaults).
    export BEETSDIR="${beetsConfigDir}"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    # MB/Discogs API bases are NOT passed here (issue #497): config.ini's
    # [MusicBrainz]/[Discogs] api_base is the ONE production source, read at
    # startup via configure_api_bases_from_runtime_config(). The
    # --mb-api/--discogs-api flags still exist on web/server.py for
    # dev-only overrides for a manual `cratedigger-web` invocation — the module
    # deliberately stops passing them so there is no second path to keep
    # in sync with config.ini.
    exec ${pyRunner} ${src}/web/server.py \
      --port ${toString cfg.web.port} \
      --dsn "${pipelineDsn}" \
      --beets-db "${cfg.web.beetsDb}" \
      --redis-host "${cfg.web.redis.host}" \
      --redis-port ${toString cfg.web.redis.port} \
      "$@"
  '';

  # YouTube-rescue ingest drainer — see scripts/youtube_ingest_worker.py.
  # Worker-specific PATH: pkgs.yt-dlp is prepended so the worker's
  # `shutil.which("yt-dlp")` resolves. It is deliberately NOT added to
  # `runtimePath` (which is shared across the rest of the cratedigger
  # units) because no other service needs yt-dlp and we want a single
  # boundary owning the binary lookup. The worker runs `yt-dlp` via
  # subprocess and inherits this PATH from the wrapper.
  youtubeIngestWorkerPkg = pkgs.writeShellScriptBin "cratedigger-youtube-ingest" ''
    export PATH="${pkgs.yt-dlp}/bin:${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/youtube_ingest_worker.py \
      --dsn "${pipelineDsn}" \
      --temp-dir "${cfg.youtubeIngest.tempDir}" \
      --staging-dir "${toString cfg.beets.validation.stagingDir}" \
      --poll-interval ${toString cfg.youtubeIngest.pollIntervalSeconds} \
      ${optionalString (cfg.youtubeIngest.sourceAddress != "") ''--source-address "${cfg.youtubeIngest.sourceAddress}" ''}"$@"
  '';

  # Unfindable detection oneshot — see lib/unfindable_detection_service.py.
  # Runs in its own process so the R20 cadence-never-changes invariant
  # is structurally enforceable at the systemd level: this binary has
  # no way to reach the regular 5-min plan loop's cursor mutators.
  unfindableDetectionPkg = pkgs.writeShellScriptBin "cratedigger-unfindable" ''
    export PATH="${runtimePath}:$PATH"
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/run_unfindable_detection.py \
      --dsn "${pipelineDsn}" "$@"
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
      within_rank_tolerance_kbps = ${toString qr.withinRankToleranceKbps}

      ${bandSection "opus" qr.bands.opus}
      ${bandSection "mp3_vbr" qr.bands.mp3Vbr}
      ${bandSection "mp3_cbr" qr.bands.mp3Cbr}
      ${bandSection "aac" qr.bands.aac}
      ${bandSection "vorbis" qr.bands.vorbis}
      ${bandSection "wma" qr.bands.wma}
    '';

  # Issue #117: secrets live at the *File paths referenced here. The cratedigger
  # Python code reads them on demand via CratediggerConfig.resolved_*() accessors,
  # so nothing sensitive is ever embedded in config.ini and the file can be
  # world-readable (see absence of chmod/chgrp in renderConfigScript).
  configTemplate = pkgs.writeText "cratedigger-config.ini" ''
    [Slskd]
    api_key_file = ${toString cfg.slskd.apiKeyFile}
    host_url = ${cfg.slskd.hostUrl}
    url_base = ${cfg.slskd.urlBase}
    download_dir = ${toString cfg.slskd.downloadDir}
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
    browse_global_max_workers = ${toString cfg.searchSettings.browseGlobalMaxWorkers}
    search_max_inflight = ${toString cfg.searchSettings.searchMaxInflight}

    [Download Settings]
    download_filtering = ${if cfg.downloadSettings.downloadFiltering then "True" else "False"}
    use_extension_whitelist = ${if cfg.downloadSettings.useExtensionWhitelist then "True" else "False"}
    extensions_whitelist = ${concatStringsSep "," cfg.downloadSettings.extensionsWhitelist}

    [Beets]
    directory = ${cfg.beets.directory}
    config_dir = ${beetsConfigDir}
    beet_binary = ${pythonEnv}/bin/beet
    python = ${pythonEnv}/bin/python

    [Beets Validation]
    enabled = ${if cfg.beets.validation.enable then "True" else "False"}
    harness_path = ${cfg.beets.validation.harnessPath}
    distance_threshold = ${toString cfg.beets.validation.distanceThreshold}
    staging_dir = ${toString cfg.beets.validation.stagingDir}
    tracking_file = ${toString cfg.beets.validation.trackingFile}
    verified_lossless_target = ${cfg.beets.validation.verifiedLosslessTarget}

    [MusicBrainz]
    api_base = ${cfg.musicbrainz.apiBase}

    [Discogs]
    api_base = ${if cfg.discogs.apiBase != null then cfg.discogs.apiBase else ""}

    ${qualityRanksSection}
    [Pipeline DB]
    enabled = ${if cfg.pipelineDb.enable then "True" else "False"}
    dsn = ${pipelineDsn}

    [Peer Cache]
    redis_host = ${cfg.redis.host}
    redis_port = ${toString cfg.redis.port}
    ttl_seconds = ${toString cfg.peerCache.ttlSeconds}
    speed_ttl_seconds = ${toString cfg.peerCache.speedTtlSeconds}
    redis_connect_timeout_ms = ${toString cfg.peerCache.redisConnectTimeoutMs}
    redis_operation_timeout_ms = ${toString cfg.peerCache.redisOperationTimeoutMs}

    [Plex]
    url = ${cfg.notifiers.plex.url}
    token_file = ${toString cfg.notifiers.plex.tokenFile}
    library_section_id = ${toString cfg.notifiers.plex.librarySectionId}
    path_map = ${cfg.notifiers.plex.pathMap}

    [Jellyfin]
    url = ${cfg.notifiers.jellyfin.url}
    token_file = ${toString cfg.notifiers.jellyfin.tokenFile}
    ${optionalString (cfg.notifiers.jellyfin.libraryId != null) "library_id = ${cfg.notifiers.jellyfin.libraryId}"}
    path_map = ${cfg.notifiers.jellyfin.pathMap}

    [Logging]
    level = ${cfg.logging.level}
    format = ${cfg.logging.format}
    datefmt = ${cfg.logging.datefmt}
  '';

  # Install the rendered template into stateDir. Since config.ini no longer
  # embeds any plaintext secrets (issue #117 — they're *File paths now), there's
  # no chmod dance, no sed substitution, and no group-ownership hack. The
  # secrets themselves still need to be readable by cfg.user at whatever paths
  # slskd.apiKeyFile / notifiers.*.tokenFile point to.
  renderConfigScript = pkgs.writeShellScript "cratedigger-render-config" ''
    set -euo pipefail
    config_dir="${cfg.stateDir}"
    mkdir -p "$config_dir"
    tmp="$(${pkgs.coreutils}/bin/mktemp "$config_dir/.config.ini.XXXXXX")"
    trap '${pkgs.coreutils}/bin/rm -f "$tmp"' EXIT
    ${pkgs.coreutils}/bin/cp ${configTemplate} "$tmp"
    ${pkgs.coreutils}/bin/chmod 0644 "$tmp"
    ${pkgs.coreutils}/bin/mv -f "$tmp" "$config_dir/config.ini"
    trap - EXIT
    # Beets config (tier-2 plan U4): atomic render into BEETSDIR, same
    # temp-file-and-rename dance as config.ini because the importer,
    # preview worker and timer-driven oneshot can start concurrently.
    beets_dir="${beetsConfigDir}"
    mkdir -p "$beets_dir"
    tmp_yaml="$(${pkgs.coreutils}/bin/mktemp "$beets_dir/.config.yaml.XXXXXX")"
    trap '${pkgs.coreutils}/bin/rm -f "$tmp_yaml"' EXIT
    ${pkgs.coreutils}/bin/cp ${beetsConfigTemplate} "$tmp_yaml"
    ${pkgs.coreutils}/bin/chmod 0644 "$tmp_yaml"
    ${pkgs.coreutils}/bin/mv -f "$tmp_yaml" "$beets_dir/config.yaml"
    trap - EXIT
    ${optionalString (cfg.beets.package.discogsTokenFile != null) ''
      # Discogs token: *File pattern (issue #117) — the token lands only
      # in a 0400 secrets.yaml owned by the service user, never in the
      # world-readable config.yaml. Bare assignment so a failed cat
      # (unreadable secret) fails the unit under set -e instead of being
      # swallowed inside a printf argument.
      discogs_token="$(${pkgs.coreutils}/bin/cat "${cfg.beets.package.discogsTokenFile}")"
      if [ -z "$discogs_token" ]; then
        # An empty user_token re-enables the discogs plugin's interactive
        # OAuth flow at load — the exact hazard the placeholder/token
        # design exists to kill. Fail loud instead of deploying green
        # with a broken beets.
        echo "cratedigger: beets.package.discogsTokenFile (${cfg.beets.package.discogsTokenFile}) is empty — refusing to render an empty discogs user_token" >&2
        exit 1
      fi
      # YAML single-quoted scalar; embedded single quotes doubled.
      discogs_token="''${discogs_token//\'/\'\'}"
      tmp_secrets="$(${pkgs.coreutils}/bin/mktemp "$beets_dir/.secrets.yaml.XXXXXX")"
      trap '${pkgs.coreutils}/bin/rm -f "$tmp_secrets"' EXIT
      {
        echo 'discogs:'
        echo "  user_token: '$discogs_token'"
      } > "$tmp_secrets"
      ${pkgs.coreutils}/bin/chmod 0400 "$tmp_secrets"
      ${pkgs.coreutils}/bin/mv -f "$tmp_secrets" "$beets_dir/secrets.yaml"
      trap - EXIT
    ''}
  '';

  # Only the main pipeline owns this singleton lock. Its start retains an
  # idempotent render fallback, then clears a stale lock left by a crashed
  # predecessor. Workers and deployment-time rendering must never remove it:
  # either can start while a timer-owned pipeline cycle is active.
  pipelinePreStartScript = pkgs.writeShellScript "cratedigger-pipeline-prestart" ''
    set -euo pipefail
    ${renderConfigScript}
    rm -f "${cfg.stateDir}/.cratedigger.lock"
  '';

  # Optional health check for a stuck slskd reconnect loop. Generic — the
  # restart command is configurable so non-systemd slskd setups still work.
  slskdHealthCheck = pkgs.writeShellScript "cratedigger-slskd-healthcheck" ''
    set -euo pipefail
    api_key=$(${pkgs.coreutils}/bin/cat "${toString cfg.slskd.apiKeyFile}")
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

    packageSet = mkOption {
      type = types.pkgs;
      default = pkgs;
      defaultText = lib.literalExpression "pkgs";
      description = ''
        Package set used to build cratedigger's runtime closure (the python
        env, and from it the pinned beets). When this module is imported via
        the flake's `nixosModules.default`, this is pinned to the nixpkgs
        from cratedigger's own flake.lock — the rev the test suite and the
        real-beets contract test ran against. Setting it explicitly is the
        deliberate escape hatch for consumers who refuse the second nixpkgs
        evaluation; doing so forfeits the tested-closure guarantee (your
        beets/python may drift from what cratedigger's suite verified).
      '';
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
      onUnitInactiveSec = mkOption {
        type = types.str;
        default = "1s";
        description = "Delay after each completed cycle before starting the next one.";
      };
    };

    importer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run the long-lived importer worker that drains the shared import queue.";
      };
      previewWorkers = mkOption {
        type = types.int;
        default = 2;
        description = "Number of async import preview workers to run before the serial importer lane.";
      };
    };

    # YouTube-rescue ingest worker. Drains album_requests rows the operator
    # has marked for YouTube fallback (`pipeline-cli youtube-rescue <id>` or
    # POST /api/pipeline/<id>/youtube-rescue), invoking `yt-dlp` to stage
    # audio into the configured beets-validation staging directory's
    # auto-import child for the existing importer worker to pick up. The
    # unit is defined here but `enable` defaults to `false` so
    # the in-flake module ships dormant — the downstream NixOS wrapper at
    # ~/nixosconfig/modules/nixos/services/cratedigger.nix is the right
    # layer to flip this on and layer on network-namespace hardening
    # (`serviceConfig.NetworkNamespacePath`, `BindReadOnlyPaths`, etc.).
    youtubeIngest = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Run the long-lived YouTube-rescue ingest worker
          (cratedigger-youtube-ingest.service). Requires `yt-dlp` on the
          worker's PATH — the unit's wrapper prepends `${pkgs.yt-dlp}/bin`
          for this unit only, never on the shared runtime PATH.
        '';
      };
      tempDir = mkOption {
        type = types.str;
        default = "${cfg.stateDir}/youtube-ingest-temp";
        defaultText = lib.literalExpression ''"''${cfg.stateDir}/youtube-ingest-temp"'';
        description = ''
          Per-process scratch directory yt-dlp downloads into before files
          are moved to the configured auto-import staging directory. Created by systemd-tmpfiles
          with the same ownership as the cratedigger user.
        '';
      };
      pollIntervalSeconds = mkOption {
        type = types.int;
        default = 5;
        description = ''
          Seconds the drainer sleeps between idle queue polls. Matches the
          importer worker's poll cadence; tune downward only if the operator
          wants tighter latency on rescue jobs.
        '';
      };
      sourceAddress = mkOption {
        type = types.str;
        default = "";
        example = "192.168.1.36";
        description = ''
          Local IP to bind yt-dlp's client socket to (passed through as
          ``yt-dlp --source-address``). Leave empty for default-route
          egress. Set this to the host's VPN-routed NIC IP so YouTube
          egress is policy-routed through the upstream VPN, the same way
          slskd's traffic is routed: the host's source-IP routing rule
          (``ip rule from <addr> lookup <table>``) sends sockets bound to
          this address out the VPN interface. The worker's DB/control
          traffic is unaffected because only yt-dlp binds to this address.
          This is host-specific, so it lives in the downstream wrapper, not
          the in-flake module's defaults (KTD9).
        '';
      };
    };

    slskd = {
      apiKeyFile = mkOption {
        type = types.nullOr types.path;
        default = null;
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
        type = types.nullOr types.str;
        default = null;
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
      createLocally = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Provision PostgreSQL on this host (services.postgresql +
          ensureDatabases/ensureUsers). The ensure-role and database are
          named after services.cratedigger.user, so unix-socket PEER
          authentication works by construction — no password material
          anywhere (KTD5). The DSN defaults to the local socket and
          cratedigger-db-migrate is ordered after postgresql.service so
          first boot cannot race the database. The operator's external-DB
          setup (createLocally = false + an explicit dsn) is unchanged.
        '';
      };
      dsn = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "postgresql://cratedigger@localhost/cratedigger";
        description = ''
          PostgreSQL connection string for the pipeline DB. Required
          unless pipelineDb.createLocally = true (which defaults it to
          the local unix socket).
        '';
      };
    };

    redis = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the local Redis server owned by cratedigger for peer-cache and web metadata caching.";
      };
      host = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Redis bind/client host used by the app-owned cratedigger Redis server.";
      };
      port = mkOption {
        type = types.port;
        default = 6379;
      };
      maxmemory = mkOption {
        type = types.str;
        default = "3gb";
        description = "Redis maxmemory setting for the app-owned cratedigger server.";
      };
    };

    peerCache = {
      ttlSeconds = mkOption {
        type = types.int;
        default = 7 * 24 * 60 * 60;
        description = "TTL in seconds for Redis peer_dir, peer_dir_neg, and peer_dir_count entries.";
      };
      speedTtlSeconds = mkOption {
        type = types.int;
        default = 24 * 60 * 60;
        description = "TTL in seconds for Redis peer_speed entries.";
      };
      redisConnectTimeoutMs = mkOption {
        type = types.int;
        default = 200;
        description = "Redis connect timeout for the pipeline peer cache, in milliseconds.";
      };
      redisOperationTimeoutMs = mkOption {
        type = types.int;
        default = 100;
        description = "Redis command timeout for the pipeline peer cache, in milliseconds.";
      };
    };

    # ONE beets option tree (issue #497): package build-time knobs
    # (beets.package.*), the operator-tunable subset of the rendered
    # config.yaml (beets.config.*), the config.ini [Beets] directory
    # (beets.directory), and the pipeline validation gate (beets.validation.*)
    # all live under services.cratedigger.beets.*. Everything under
    # beets.config NOT listed here (path templates, duplicate_keys, plugin
    # list, match weights, ...) is rendered as a fixed production-parity
    # literal — see beetsSettings above for why.
    beets = {
      package = {
        discogsMirrorUrl = mkOption {
          type = types.nullOr types.str;
          default = null;
          example = "https://discogs.ablz.au";
          description = ''
            When set, the beets discogs plugin's client is patched
            (substituteInPlace at build time) to use this base URL instead of
            public api.discogs.com. Null = stock plugin behaviour (public
            Discogs, token required for lookups — see the discogs token
            handling in the rendered beets config).
          '';
        };
        lrclibUrl = mkOption {
          type = types.nullOr types.str;
          default = null;
          example = "http://192.168.1.35:3300/api";
          description = ''
            When set, the beets lyrics plugin's LRCLIB base URL is patched
            (substituteInPlace at build time) to this value instead of public
            lrclib.net. Null = stock plugin behaviour.
          '';
        };
        discogsTokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = ''
            Path to a file containing a Discogs user token (raw, one line).
            Same contract as slskd.apiKeyFile (issue #117): must be readable
            by services.cratedigger.user. When set, preStart materializes it
            into ''${stateDir}/beets/secrets.yaml (mode 0400) and the rendered
            config.yaml includes it. When null, a non-empty placeholder token
            is rendered instead so the discogs plugin loads without its
            interactive OAuth flow — public-Discogs lookups then fail
            per-use until a real token is provided (documented
            token-required).
          '';
        };
      };

      config = {
        directory = mkOption {
          type = types.str;
          default = "/mnt/virtio/Music/Beets";
          description = ''
            Beets library root (config.yaml `directory:`). Production-matching
            default (tier-2 plan R5); strangers point this at their music
            root.
          '';
        };
        library = mkOption {
          type = types.str;
          default = "/mnt/virtio/Music/beets-library.db";
          description = ''
            Beets library SQLite DB (config.yaml `library:`). The import log
            renders next to it as beets-import.log. The parent directory must
            exist at runtime: `beet` prompts interactively ("Create it
            (Y/n)?") when it's missing, which blocks any non-interactive
            invocation.
          '';
        };
        fetchart = {
          maxwidth = mkOption {
            type = types.int;
            default = 500;
            description = ''
              fetchart maxwidth. Load-bearing: embedded art is duplicated in
              every track, so width drives library size (500px ≈ 71KB vs
              1138KB unresized across ~83K tracks = ~85GB saved).
            '';
          };
          minwidth = mkOption {
            type = types.int;
            default = 300;
            description = ''
              fetchart minwidth. Reject unusably small embedded artwork;
              300px is the collection's established quality floor.
            '';
          };
        };
        musicbrainz = {
          host = mkOption {
            type = types.str;
            default = "musicbrainz.org";
            description = ''
              MusicBrainz host for beets (config.yaml `musicbrainz.host`).
              Default is public MB — functional but rate-limited (~1 req/s).
              Point at a local mirror (host:port) for production-speed
              matching; https and ratelimit below must be set coherently.
            '';
          };
          https = mkOption {
            type = types.bool;
            default = true;
            description = "Whether beets talks TLS to musicbrainz.host (true for public MB, typically false for a LAN mirror).";
          };
          ratelimit = mkOption {
            type = types.int;
            default = 1;
            description = "beets musicbrainz ratelimit (req/s). 1 for public MB; 100 against a local mirror.";
          };
        };
      };

      # config.ini [Beets] directory. Follows beets.config.directory by
      # default (mkDefault'd in the config block below).
      directory = mkOption {
        type = types.str;
        default = "";
        example = "/mnt/virtio/Music/Beets";
        description = ''
          Absolute path to the beets library root (matches `directory:` in
          ~/.config/beets/config.yaml). Beets stores file paths in its SQLite
          DB as relative to this root, so consumers that perform host-side
          filesystem ops (cleanup_disambiguation_orphans) or send absolute
          paths to external services (trigger_plex_scan on bare-metal Plex)
          need to absolutize against this root.

          Optional but recommended. Leave empty if you provide an equivalent
          absolute prefix via notifiers.plex.pathMap (Docker remap form
          `/host/beets:/container/path`).
        '';
      };

      validation = {
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
          type = types.nullOr types.str;
          default = null;
          description = "Directory to stage validated albums for beets import. Required when beets.validation.enable.";
        };
        trackingFile = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "JSONL file tracking beets validation results. Required when beets.validation.enable.";
        };
        verifiedLosslessTarget = mkOption {
          type = types.str;
          default = "";
          description = "Target format after verified lossless (e.g. 'opus 128', 'mp3 v2'). Empty = keep V0.";
        };
      };
    };

    musicbrainz = {
      apiBase = mkOption {
        type = types.str;
        default = "https://musicbrainz.org";
        example = "http://192.168.1.35:5200";
        description = ''
          MusicBrainz API origin (scheme://host[:port], no path) — ONE value
          threaded to all three consumers (tier-2 plan U6/KTD6): web/mb.py
          (via config.ini [MusicBrainz] api_base, read at cratedigger-web
          startup by configure_api_bases_from_runtime_config()), pipeline-cli
          release lookups, and the rendered beets
          musicbrainz.{host,https,ratelimit}. Public MB default is functional
          but rate-limited (~1 req/s); point at a local mirror for
          production-speed matching.
        '';
      };
    };

    discogs = {
      apiBase = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "https://discogs.ablz.au";
        description = ''
          Discogs mirror origin. Mirror-REQUIRED (R13): web/discogs.py
          speaks the Rust mirror's endpoint shape, which public
          api.discogs.com does not serve — there is no public fallback.
          Null = Discogs browse off (clear 503 mirror-required message);
          MusicBrainz browse is unaffected. The beets discogs plugin's own
          public-Discogs path (used by imports) is separate — see
          beets.package.discogsMirrorUrl for its mirror knob.
        '';
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
          description = "Redis host for web metadata caching. Defaults to the app-owned cratedigger Redis host.";
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
        libraryId = mkOption {
          type = types.nullOr types.nonEmptyStr;
          default = null;
          example = "music-library-item-id";
          description = ''
            Jellyfin music library item ID used only as the fallback target
            when observing an album deletion. Post-import notifications use
            the final album path and never refresh this collection item.
          '';
        };
        pathMap = mkOption {
          type = types.str;
          default = "";
          example = "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets";
          description = ''
            local:remote path remap from the beets library path on this host
            to the path Jellyfin sees. Used by both the album-path media
            update and the "Recently Added" DateCreated pin.
          '';
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
          the run. The command is invoked as root (the health-check ExecStartPre is
          "+"-prefixed, so it runs as root even when services.cratedigger.user is
          non-root — e.g. so `systemctl restart slskd.service` works) and
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
        default = ["flac 24/192" "flac 24/96" "flac 24/48" "flac 16/44.1" "flac" "alac" "mp3 v0" "mp3 320" "aac" "opus" "ogg" "mp3" "wav"];
        description = ''
          Priority-ordered filetype filter. The rank model in lib/quality/ranks.py is
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
      searchMaxInflight = mkOption {
        type = types.int;
        default = 4;
        description = ''
          Pipeline depth for the parallel search executor — number of
          in-flight search-collection futures at once. Submission stays
          sequential (slskd's `SearchRequestLimiter` is on POST only, with
          a built-in 429-retry loop), but the collect-side workload runs
          in this many threads. Raised from the legacy hard-coded 2 once
          browse fan-out (issue #198) stops being the dominant cost.
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
        vorbis = mkCodecBands "Vorbis" {
          transparent = 192;
          excellent = 160;
          good = 112;
          acceptable = 96;
        };
        wma = mkCodecBands "WMA" {
          transparent = 320;
          excellent = 256;
          good = 192;
          acceptable = 128;
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
        assertion = cfg.slskd.apiKeyFile != null;
        message = "services.cratedigger.slskd.apiKeyFile is not set: point it at a file containing your slskd API key (readable by services.cratedigger.user).";
      }
      {
        assertion = cfg.slskd.downloadDir != null;
        message = "services.cratedigger.slskd.downloadDir is not set: point it at the directory slskd downloads land in (slskd's directories.downloads).";
      }
      {
        assertion = cfg.pipelineDb.createLocally || cfg.pipelineDb.dsn != null;
        message = "services.cratedigger.pipelineDb: set either pipelineDb.dsn (external PostgreSQL) or pipelineDb.createLocally = true (provision a local database with peer auth).";
      }
      {
        assertion = !cfg.beets.validation.enable || (cfg.beets.validation.stagingDir != null && cfg.beets.validation.trackingFile != null);
        message = "services.cratedigger.beets.validation: enable requires stagingDir (where validated albums stage for import) and trackingFile (JSONL validation log).";
      }
      {
        # The rescue worker stages into the same beets Incoming root; an
        # unset stagingDir would silently render --staging-dir "" and
        # strand rescues under the state dir.
        assertion = !cfg.youtubeIngest.enable || cfg.beets.validation.stagingDir != null;
        message = "services.cratedigger.youtubeIngest: enable requires beets.validation.stagingDir (rescues stage under its auto-import/ child).";
      }
      {
        assertion = lib.hasPrefix "http://" cfg.musicbrainz.apiBase || lib.hasPrefix "https://" cfg.musicbrainz.apiBase;
        message = "services.cratedigger.musicbrainz.apiBase must be an origin URL (scheme://host[:port], no path), e.g. https://musicbrainz.org or http://192.168.1.35:5200.";
      }
      {
        assertion = cfg.discogs.apiBase == null || lib.hasPrefix "http://" cfg.discogs.apiBase || lib.hasPrefix "https://" cfg.discogs.apiBase;
        message = "services.cratedigger.discogs.apiBase must be an origin URL (scheme://host[:port]) when set, e.g. https://discogs.ablz.au.";
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

    environment.systemPackages = [pipelineCli pipelineMigrate importerPkg previewWorkerPkg youtubeIngestWorkerPkg cratediggerBeet pkgs.postgresql];

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
    # / notifiers.*.tokenFile) and retain their own
    # restrictive modes from whatever provisioned them (sops-nix, agenix, etc).
    systemd.tmpfiles.rules =
      [
        "d ${cfg.stateDir} 0755 ${cfg.user} ${cfg.group} -"
        # BEETSDIR for every beets consumer (cratedigger-beet, harness).
        # U4 renders config.yaml into it; the dir must exist regardless so
        # the wrapper works on a fresh boot.
        "d ${beetsConfigDir} 0755 ${cfg.user} ${cfg.group} -"
      ]
      ++ optional cfg.youtubeIngest.enable
        "d ${cfg.youtubeIngest.tempDir} 0755 ${cfg.user} ${cfg.group} -";

    # Local PostgreSQL (stranger ergonomics, KTD5): role + database named
    # after cfg.user so unix-socket peer auth works with zero credentials
    # (ensureDBOwnership requires database name == role name). The DSN
    # defaults to the socket; nothing for the *File secret pattern to
    # carry, no pg_hba loosening.
    services.postgresql = mkIf cfg.pipelineDb.createLocally {
      enable = true;
      ensureDatabases = [ cfg.user ];
      ensureUsers = [
        {
          name = cfg.user;
          ensureDBOwnership = true;
        }
      ];
    };
    services.cratedigger.pipelineDb.dsn = mkIf cfg.pipelineDb.createLocally (
      lib.mkDefault "postgresql:///${cfg.user}?host=/run/postgresql"
    );

    services.cratedigger.web.redis.host = lib.mkDefault cfg.redis.host;
    services.cratedigger.web.redis.port = lib.mkDefault cfg.redis.port;
    # One concept, one value: the config.ini [Beets] directory follows the
    # rendered beets config.yaml `directory:` unless explicitly overridden.
    services.cratedigger.beets.directory = lib.mkDefault cfg.beets.config.directory;

    # One MB value, three consumers (U6/KTD6): the rendered beets
    # musicbrainz block derives from musicbrainz.apiBase — mirror =>
    # host:port / plain http / ratelimit 100 (the harness --upstream
    # block's inverse); public => musicbrainz.org / https / ratelimit 1.
    # mkDefault so an operator can still pin the beets block explicitly.
    services.cratedigger.beets.config.musicbrainz = let
      mbHost = lib.removePrefix "https://" (lib.removePrefix "http://" cfg.musicbrainz.apiBase);
      mbPublic = mbHost == "musicbrainz.org";
    in {
      host = lib.mkDefault mbHost;
      https = lib.mkDefault (lib.hasPrefix "https://" cfg.musicbrainz.apiBase);
      ratelimit = lib.mkDefault (if mbPublic then 1 else 100);
    };

    services.redis.servers.cratedigger = {
      enable = cfg.redis.enable;
      bind = cfg.redis.host;
      port = cfg.redis.port;
      settings = {
        maxmemory = cfg.redis.maxmemory;
        "maxmemory-policy" = "allkeys-lru";
      };
    };

    # Schema migrator. RemainAfterExit=true so cratedigger-web (and the other
    # long-running/Requires= units below) can require us without re-running
    # on every cycle. Idempotent — fast no-op when schema is already current.
    # The two timer-driven, restartIfChanged=false units (cratedigger,
    # cratedigger-unfindable) deliberately do NOT require us: this unit's
    # ExecStart store path changes on every code deploy, so it restarts on
    # every switch, and systemd Requires= propagates that restart as a
    # SIGTERM to anything requiring it — killing a mid-flight cycle even
    # though the app code didn't change. Those two use Wants=+After= instead
    # and gate on schema currency themselves at startup
    # (lib.migrator.assert_schema_current) so a failed/behind migration still
    # blocks them from running. Long-running units that already
    # restartIfChanged=true on deploy (importer, preview worker, web,
    # youtube-ingest) keep Requires= — the propagated restart is harmless for
    # them.
    systemd.services.cratedigger-db-migrate = {
      description = "Apply Cratedigger pipeline DB schema migrations";
      wantedBy = ["multi-user.target"];
      # With a locally-provisioned DB, first boot must not race PostgreSQL
      # (this ordering used to live only in the VM test's hand-rolled
      # node — now the module owns it, U7/R10).
      after = optional cfg.pipelineDb.createLocally "postgresql.service";
      requires = optional cfg.pipelineDb.createLocally "postgresql.service";
      restartIfChanged = true;
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.user;
        Group = cfg.group;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${pipelineMigrate}/bin/pipeline-migrate";
      };
    };

    # Materialise declarative runtime configuration independently of every
    # application unit. A downstream ExecCondition is evaluated before an
    # application's ExecStartPre, so app-owned rendering alone leaves stale
    # mutable config throughout an intentional dependency hold.
    systemd.services.cratedigger-config-render = {
      description = "Render Cratedigger runtime configuration";
      wantedBy = ["multi-user.target"];
      restartIfChanged = true;
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.user;
        Group = cfg.group;
        ExecStart = renderConfigScript;
      };
    };

    systemd.services.cratedigger = {
      description = "Cratedigger — Soulseek download pipeline";
      after = ["cratedigger-db-migrate.service"] ++ redisServiceUnits;
      wants = ["cratedigger-db-migrate.service"] ++ redisServiceUnits;
      restartIfChanged = false;
      # Deliberately exclude pythonEnv from PATH: the python interpreter is
      # invoked via absolute path inside the wrappers, and every beets
      # consumer resolves the pinned interpreter/binary from the rendered
      # [Beets] config keys (config_dir / beet_binary / python) rather than
      # PATH lookup — keeping PATH lean avoids ever re-introducing an
      # ambient-beet dependency (tier-2 plan R6).
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        # slskdHealthCheck is prefixed with "+" so it always runs as root,
        # regardless of cfg.user: its onFailureCommand (e.g. `systemctl
        # restart slskd.service`) needs root, and once cfg.user is
        # non-root a bare ExecStartPre would run as that user and be
        # unable to restart slskd. pipelinePreStartScript must NOT get "+" — it
        # renders config as cfg.user so ownership on the rendered files
        # matches the service that reads them.
        ExecStartPre = lib.optional cfg.healthCheck.enable "+${slskdHealthCheck}" ++ [pipelinePreStartScript];
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${cratediggerPkg}/bin/cratedigger";
        WorkingDirectory = cfg.stateDir;
        # Defense-in-depth (issue #212 R13): if anything escapes the
        # in-band 90s per-search progress watchdog (clock-injection bug,
        # TCP socket hang inside the watchdog itself, etc.), systemd
        # SIGTERMs the process at 60 min. Healthy cycles run well under
        # 60 min so this never fires; the systemd timer simply schedules
        # the next cycle. Cycle-boundary checkpointing already tolerates
        # a forced kill — the importer service owns beets writes
        # independently and is unaffected.
        #
        # `RuntimeMaxSec` does NOT apply to Type=oneshot — systemd warns
        # `RuntimeMaxSec= has no effect in combination with Type=oneshot.
        # Ignoring.` and the defense-in-depth silently disappears. For a
        # oneshot, the entire service runtime IS the start phase, so
        # `TimeoutStartSec` is the right knob. It SIGTERMs (then SIGKILLs
        # after `TimeoutStopSec`) the ExecStart process at the cap.
        TimeoutStartSec = "1h";
      };
    };

    systemd.timers.cratedigger = mkIf cfg.timer.enable {
      description = "Cratedigger periodic run timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = cfg.timer.onBootSec;
        OnUnitInactiveSec = cfg.timer.onUnitInactiveSec;
        Persistent = true;
      };
    };

    # Unfindable detection oneshot + daily timer. Lives in its own
    # systemd unit, NOT inline in the main cratedigger.service loop,
    # because R20 ("the system never stops searching") forbids the
    # regular search cadence from being throttled by detection state.
    # The structural separation makes that invariant enforceable: this
    # process shares no code path with the regular plan loop and
    # cannot accidentally mutate plan cursors.
    systemd.services.cratedigger-unfindable = {
      description = "Cratedigger unfindable detection oneshot";
      after = ["cratedigger-db-migrate.service" "network.target"];
      wants = ["cratedigger-db-migrate.service"];
      restartIfChanged = false;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        # Same health-check shape as cratedigger.service (including the "+"
        # root-escalation prefix — see the comment there), followed by a
        # render-only fallback. The detection job never owns the main pipeline
        # lock, so it must not clear that lock while a cycle is active. It
        # gates on slskd reachability when the operator has health-check enabled
        # and hits slskd just as much as the main loop does, so a slskd
        # outage should fail the unit fast rather than write garbage
        # probe-failed rows for every cohort member.
        ExecStartPre = lib.optional cfg.healthCheck.enable "+${slskdHealthCheck}" ++ [renderConfigScript];
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${unfindableDetectionPkg}/bin/cratedigger-unfindable";
        WorkingDirectory = cfg.stateDir;
        # Generous cap: a 100-row batch over a slow slskd is roughly
        # 100 × ~30s = 50 min worst case. 2h gives headroom while
        # still surfacing genuinely stuck runs.
        TimeoutStartSec = "2h";
      };
    };

    systemd.timers.cratedigger-unfindable = {
      description = "Cratedigger unfindable detection daily timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        # Jitter the daily fire so the detection batch does not
        # collide with other midnight tasks on doc2 (logrotate,
        # postgres autovacuum, etc.). Single-operator install — there
        # is no fleet of NixOS deployments to spread across, the
        # randomisation is purely a local cron-collision avoidance.
        RandomizedDelaySec = "30min";
      };
    };

    systemd.services.cratedigger-importer = mkIf cfg.importer.enable {
      description = "Cratedigger importer queue worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      # Restart on deploy. The previous "skip restart to avoid killing
      # in-flight work" rationale failed in practice on 2026-05-16:
      # switch-to-configuration SIGTERM'd both workers anyway (units
      # changed transitively) and never brought them back, leaving the
      # pipeline silently dead for ~96 minutes. ``Restart=on-failure``
      # doesn't help — SIGTERM is a clean exit. The import-job launch fence
      # safely requeues only pre-launch work and stops ambiguous Beets work
      # for the operator; that's the right place to handle a mid-job kill,
      # not by leaving the worker dead.
      restartIfChanged = true;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = [renderConfigScript];
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${importerPkg}/bin/cratedigger-importer";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.cratedigger-import-preview-worker = mkIf cfg.importer.enable {
      description = "Cratedigger async import preview worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      # Restart on deploy. Same reasoning as cratedigger-importer: deploy
      # SIGTERM'd this unit on 2026-05-16 and never brought it back.
      # ``requeue_stale_import_preview_jobs`` handles mid-job kills at startup;
      # leaving the worker dead instead is strictly worse.
      restartIfChanged = true;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = [renderConfigScript];
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    # YouTube-rescue ingest drainer. Long-lived Type=simple worker that
    # polls `download_log` rows where source='youtube' and
    # outcome='youtube_running' that the operator explicitly opted in via
    # `pipeline-cli youtube-rescue <id>` or
    # POST /api/pipeline/<id>/youtube-rescue, invokes yt-dlp, stages audio
    # under the configured auto-import staging directory, and enqueues a
    # `youtube_import` row in `import_jobs` for the existing
    # cratedigger-importer worker to drain.
    #
    # Advisory-lock contention exits the process with code 0 (not 1), so
    # `Restart=on-failure` won't fire on duplicate-start. A genuine crash
    # (DB unreachable at boot, etc.) exits 1 and systemd will respawn after
    # `RestartSec=5`. There is NO `RuntimeMaxSec` — this is a long-running
    # daemon and the per-job yt-dlp timeout lives inside the worker
    # (DEFAULT_YTDLP_TIMEOUT_SEC = 600s). See
    # `docs/solutions/runtimemaxsec-vs-type-oneshot-systemd-incompatibility.md`.
    systemd.services.cratedigger-youtube-ingest = mkIf cfg.youtubeIngest.enable {
      description = "Cratedigger YouTube-rescue ingest worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      # Deliberate `restartIfChanged = true`: deploy MUST pick up worker
      # code changes. Accepted-but-unclaimed `youtube_running` rows survive
      # restart and remain drainable; rows claimed by a previous worker are
      # swept to terminal `youtube_failed` on startup. Mirrors the importer /
      # preview-worker restart posture (2026-05-16 lesson).
      restartIfChanged = true;
      # Worker-specific PATH is set inside the wrapper (yt-dlp is
      # prepended there). The unit's `path` mirrors the importer's so
      # subprocess invocations have the standard toolchain available.
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        ExecStartPre = [renderConfigScript];
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${youtubeIngestWorkerPkg}/bin/cratedigger-youtube-ingest";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.cratedigger-web = mkIf cfg.web.enable {
      description = "Cratedigger web UI";
      after = ["cratedigger-db-migrate.service"] ++ redisServiceUnits;
      wants = redisServiceUnits;
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        ExecStartPre = [renderConfigScript];
        ExecStart = "${webPkg}/bin/cratedigger-web";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
      };
    };
  };
}
