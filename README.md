# Cratedigger

A quality-obsessed music acquisition pipeline. Searches Soulseek via [slskd](https://github.com/slskd/slskd), validates downloads against [MusicBrainz](https://musicbrainz.org/) or [Discogs](https://www.discogs.com/) via [beets](https://beets.io/), and curates a library toward verified lossless sources — automatically.

Cratedigger doesn't just download albums. It siphons the best available quality out of Soulseek over time: downloading, verifying via spectral analysis, converting, comparing against what's already on disk, and re-queuing for upgrades when better sources appear.

> This project was originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr), a clever script that connected Lidarr with Soulseek. Cratedigger has since diverged into its own thing — PostgreSQL pipeline DB, beets validation, spectral quality verification, async downloads, a web UI — but the original idea of bridging Soulseek into a music library workflow came from mrusse's work. Thank you.
>
> If you'd like to support the original author: [buy mrusse a coffee](https://ko-fi.com/mrusse).

## How it works

```
Web UI / CLI                 slskd (Soulseek)           beets
      |                            |                       |
      |  add album                 |                       |
      v                            |                       |
Pipeline DB (PostgreSQL)           |                       |
      |                            |                       |
      |  Phase 1: poll_active_downloads()                  |
      |    check status of previous downloads              |
      |    complete/timeout/retry                          |
      |                            |                       |
      |  Phase 2: get_wanted()     |                       |
      |    search + enqueue ------>|                       |
      |    set status=downloading  |  download (async)     |
      |    return immediately      |<-----------           |
      |                            |                       |
      |  (next 5-min cycle)        |                       |
      |    poll sees completion    |                       |
      |    validate against MBID --|---------------------->|
      |                            |                       |
      |  source=request            |                       |
      |    spectral analysis       |                       |
      |    FLAC->V0 conversion     |                       |
      |    quality gate            |  auto-import -------->| -> /Beets
      |                            |                       |
      |  source=redownload         |                       |
      |    stage to /Incoming      |  (manual review)      |
      |                            |                       |
      |  ImportResult + ValidationResult JSON               |
      |<---------------------------------------------------+
```

## Features

- **PostgreSQL pipeline DB** as the sole source of truth for album requests, download state, and quality history
- **Web UI** (`music.ablz.au`) for browsing local MusicBrainz and Discogs mirrors with a source toggle, and adding albums
- **Beets validation** -- every download validated against the target release ID (MusicBrainz UUID or Discogs numeric) before import
- **Auto-import** with FLAC->V0 conversion (or configurable target: Opus, MP3 V2, AAC, FLAC on disk), spectral analysis, and quality gating
- **Async downloads** -- non-blocking: enqueue downloads, persist state to DB, poll on next run. Downloads span multiple 5-minute cycles.
- **Parallel Soulseek searches** -- `ThreadPoolExecutor` fires all searches concurrently
- **Spectral quality verification** -- sox-based transcode detection catches fake FLACs and upsampled MP3s
- **Quality upgrade system** -- automatically re-queues albums when better sources become available. CBR -> lossless -> verified V0.
- **User cooldowns** -- global, temporary cooldowns for Soulseek users who consistently timeout or fail (5 consecutive failures = 3-day cooldown)
- **Force-import** -- manually import rejected downloads via CLI or web API
- **Full audit trail** -- every decision stored as queryable JSONB in PostgreSQL
- **Typed decision pipeline** -- pure functions in `quality.py`, typed dataclasses throughout, pyright enforced
- **Discogs as first-class citizen** -- browse, add, and import Discogs releases through the same pipeline as MusicBrainz. Beets auto-routes numeric IDs to the Discogs plugin; local Discogs mirror eliminates external API dependencies.
- **Comprehensive test suite** -- 1500+ tests (`nix-shell --run "bash scripts/run_tests.sh"`) with a 4-category taxonomy (pure / seam / orchestration / integration slice), shared `FakePipelineDB`/`FakeSlskdAPI` fakes, builders for typed data, and a route contract audit guard that fails at test time if a new web endpoint is added without frontend contract coverage

## Request Retry Backoff

Cratedigger distinguishes between:

- `status = 'wanted'` -- the request is still active in the pipeline
- `eligible for search now` -- `status = 'wanted'` **and** `next_retry_after <= now()`

`get_wanted()` only returns rows that are both wanted and due. A request can
therefore sit in the wanted bucket while being intentionally skipped for a few
hours.

Retry-worthy failures call `record_attempt()` and set a shared per-request
`next_retry_after` timestamp. There are three counters:

- `search_attempts` -- search miss / enqueue failure
- `download_attempts` -- download timeout or local move/processing failure
- `validation_attempts` -- auto-import rejection that requeues the album

The counters are separate, but the retry clock is shared. The most recent
recorded failure overwrites `next_retry_after` for the request.

Today this backoff is hardcoded in Python (`BACKOFF_BASE_MINUTES = 30`,
`BACKOFF_MAX_MINUTES = 360`) and is **not yet tunable via the NixOS module**.
The schedule is exponential and capped at 6 hours:

| Attempt count | Delay before next search |
|---|---|
| 1 | 30 minutes |
| 2 | 60 minutes |
| 3 | 120 minutes |
| 4 | 240 minutes |
| 5 | 360 minutes |
| 6 | 360 minutes |
| 7+ | 360 minutes (6 hour cap) |

That means a repeatedly failing request is retried at roughly:
`t0`, `t0+30m`, `t0+1h30m`, `t0+3h30m`, `t0+7h30m`, then every 6 hours after
the cap kicks in.

Some transitions go back to `wanted` without backoff. `reset_to_wanted()`
clears all three counters and nulls `next_retry_after`, so upgrade/requeue
paths from already-imported albums become eligible again immediately.

This is separate from **user cooldowns**. Backoff is per album request; user
cooldowns are global per Soulseek user.

## Install

Cratedigger ships as a Nix flake. The repo provides:

- `nixosModules.default` — the NixOS module (systemd units, configTemplate, options surface)
- `packages.<system>.slskd-api` — the `slskd-api` PyPI build (not in nixpkgs)
- `devShells.<system>.default` — dev environment for running tests
- `checks.<system>.moduleVm` — NixOS VM test booting the module against an ephemeral postgres

Add it to your flake:

```nix
{
  inputs.cratedigger.url = "github:abl030/cratedigger";
  outputs = { self, nixpkgs, cratedigger, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        cratedigger.nixosModules.default
        ({ config, ... }: {
          services.cratedigger = {
            enable = true;
            slskd = {
              apiKeyFile = "/run/secrets/slskd-api-key";   # raw key, one line
              downloadDir = "/var/lib/slskd/downloads";
            };
            pipelineDb.dsn = "postgresql://cratedigger@localhost/cratedigger";
            beetsValidation = {
              enable = true;
              stagingDir = "/srv/music/Incoming";
              trackingFile = "/srv/music/beets-validated.jsonl";
            };
            web = {
              enable = true;
              beetsDb = "/srv/music/beets-library.db";
            };
          };
        })
      ];
    };
  };
}
```

You provide: PostgreSQL (the module just takes a DSN), slskd, redis if you want web caching, and a secrets backend that materializes `*File` paths the module reads. The module defaults to running as root because slskd downloads land outside any service-user home and beets needs broad filesystem access; override `services.cratedigger.user` once you've set up the surrounding permissions.

Try it without committing:

```bash
nix develop github:abl030/cratedigger        # dev shell with deps
nix run github:abl030/cratedigger#slskd-api  # build the slskd-api package
nix flake check github:abl030/cratedigger    # boot the VM check
```

For homelab consumers (sops-nix, nspawn DB containers, reverse proxies), see `~/nixosconfig/modules/nixos/services/cratedigger.nix` for a worked example wrapping this module.

## Quality decision pipeline

All quality decisions are pure functions in `lib/quality.py` with full unit test coverage:

1. **`spectral_import_decision()`** -- Pre-import: should we import this CBR download?
2. **`import_quality_decision()`** -- Is this an upgrade or downgrade? Genuine FLAC->V0 always wins.
3. **`transcode_detection()`** -- Post-FLAC-conversion: was the FLAC a transcode?
4. **`quality_gate_decision()`** -- Post-import: accept, or re-queue for better quality?
5. **`determine_verified_lossless()`** -- Single source of truth for verified lossless status.
6. **`should_cooldown()`** -- Global user cooldown: skip users with 5+ consecutive failures for 3 days.

## Tuning the quality rank model

Every threshold, enum, and per-codec band in the rank model is tunable via Nix options on the deployment side. The runtime parses them from `[Quality Ranks]` in `/var/lib/cratedigger/config.ini`, which is regenerated on every `nixos-rebuild switch` from the Nix module. Full rationale and per-band justification lives in [`docs/quality-ranks.md`](docs/quality-ranks.md); this section is the tuning reference.

### Where to tune

All options live under `services.cratedigger.qualityRanks.*` and are declared by the upstream NixOS module at [`nix/module.nix`](nix/module.nix) in this repo. Set them anywhere in your NixOS config that imports `cratedigger.nixosModules.default` — typically a host config or a homelab wrapper. The `[Quality Ranks]` section of `config.ini` is regenerated from these options on every `nixos-rebuild switch`; Cratedigger picks up the new values on its next 5-min timer fire.

**Source of truth**: `QualityRankConfig.defaults()` in `lib/quality.py`, pinned by `TestQualityRankConfigDefaults` in `tests/test_quality_decisions.py`. The Nix options mirror those defaults for declarative visibility -- you should be able to open `cratedigger.nix` and read your current policy without grepping Python. Drift between Python and Nix is caught at cratedigger test time: bump a default in either repo, the pin test fails and reminds you to update the other.

### Nix-exposed options

**Policy scalars:**

| Option | Type | Default | Meaning |
|---|---|---|---|
| `gateMinRank` | enum (`unknown`, `poor`, `acceptable`, `good`, `excellent`, `transparent`, `lossless`) | `"excellent"` | Minimum rank an imported album must reach before the quality gate accepts it. Below this → re-queue for upgrade. Raise to tighten (reject more albums); lower to accept lower-quality sources. |
| `bitrateMetric` | enum (`min`, `avg`, `median`) | `"avg"` | Which per-album bitrate statistic feeds rank classification. `avg` is robust to VBR per-track variance. `median` is outlier-resistant -- prefer when albums commonly have quiet intros/hidden tracks/skits that skew `avg`. `min` is legacy and penalizes legitimately-encoded lo-fi VBR. See `docs/quality-ranks.md` "When to prefer median". |
| `withinRankToleranceKbps` | int | `5` | Same-rank equivalence window in kbps. Two bare-codec measurements in the same rank tier within this tolerance are "equivalent"; outside it, one is "better"/"worse". |

**Per-codec band tables** (`bands.<codec>.{transparent,excellent,good,acceptable}`, all in kbps, used when the format hint is a bare codec string like `"MP3"` rather than an explicit label like `"mp3 v0"`):

| Codec | transparent | excellent | good | acceptable | Notes |
|---|---|---|---|---|---|
| `bands.opus`   | 112 | 88  | 64  | 48  | Unconstrained Opus VBR averages 120-135 kbps typical / 95-150 kbps per track. 112 leaves headroom for sparse material; 88 matches Opus 96 hydrogenaudio quality. |
| `bands.mp3Vbr` | 245 | 210 | 170 | 130 | `excellent=210` preserves the legacy `QUALITY_MIN_BITRATE_KBPS=210` gate threshold. V0 typically averages 220-260; V2 ~190 → `good=170`. **`excellent` also feeds `transcode_detection()` as the spectral-fallback threshold** (#66) so lowering it implicitly lowers what counts as "credible V0" when spectral is unavailable. |
| `bands.mp3Cbr` | 320 | 256 | 192 | 128 | Unverifiable CBR is only `transparent` at 320 because we can't prove a CBR file came from lossless source. Below that → requeue for a FLAC source to re-verify. |
| `bands.aac`    | 192 | 144 | 112 | 80  | Hydrogenaudio consensus places the "no meaningful quality gain above here" ceiling for music at 192 kbps. |

Leaving every option at its default produces exactly `QualityRankConfig.defaults()` -- the defaults above are the shipping values.

### Collection fields (NOT exposed via Nix -- edit `lib/quality.py` directly)

Three fields are part of the rank model but are NOT surfaced as Nix options because they're rarely-if-ever retuned outside of development. They live on `QualityRankConfig` in `lib/quality.py`, are parseable from `[Quality Ranks]` as CSV (see #65), and default to sensible values. If you want to tune them, the cleanest path is editing the dataclass defaults and updating `TestQualityRankConfigDefaults` to pin the new values. Extending `nix/module.nix` to render them is a trivial follow-up if you find yourself retuning them often.

- **`mp3_vbr_levels`** -- 10-tuple mapping LAME V-levels to ranks (V0..V9). The V-level is an **explicit label contract** -- when a download advertises `"mp3 v0"`, the rank model reads V0 from this tuple and bypasses `bands.mp3Vbr` entirely. This is why a 207 kbps lo-fi V0 still classifies as TRANSPARENT: the V0 label beats the 210 threshold.

  **Default ladder**: `V0=TRANSPARENT, V1-V2=EXCELLENT, V3-V4=GOOD, V5-V9=ACCEPTABLE`

  **When to retune**: tighten if you don't trust LAME's claim that V2 is transparent (move V1/V2 to EXCELLENT → GOOD). Loosen if you encode at V4 locally and want your own rips to pass the gate (move V4 up to EXCELLENT).

- **`lossless_codecs`** -- set of codec identity strings that **short-circuit to LOSSLESS** regardless of measured bitrate. Checked against the first whitespace-separated token of the format hint during rank classification. If the format hint starts with any of these, the rank model skips bitrate-based classification entirely and returns LOSSLESS.

  **Default**: `{"flac", "lossless", "alac", "wav"}`

  **When to retune**: add `"ape"`, `"dsf"`, or `"wavpack"` if your library carries them. Remove nothing -- removing entries is a footgun that would reclassify genuine lossless files as UNKNOWN.

- **`mixed_format_precedence`** -- ordered tuple used by `_reduce_album_format()` when an album on disk has tracks in multiple codecs (rare -- usually a manually-merged album). Walked in order; the first codec that appears on the album becomes the album's canonical codec for rank classification. Order matters.

  **Default**: `("mp3", "aac", "opus", "flac")` -- worst codec wins, so a mixed FLAC+MP3 album classifies as MP3 (conservative).

  **When to retune**: reverse to `("flac", "opus", "aac", "mp3")` if you'd rather have mixed-format albums classified by the *best* codec on disk (less conservative -- you'll accept more as "good enough"). The default is the conservative choice for a curated library.

### How to tune and deploy

The exact deploy flow depends on where you set the options. For a host config that imports `cratedigger.nixosModules.default` directly:

```bash
$EDITOR hosts/<your-host>/configuration.nix   # tweak services.cratedigger.qualityRanks.*
git commit -am "cratedigger: retune <what>" && git push
sudo nixos-rebuild switch --flake .
```

For the abl030 homelab (this project's reference deployment):

```bash
# On doc1 — has git push credentials for nixosconfig
cd ~/nixosconfig
$EDITOR hosts/doc2/configuration.nix          # tweak services.cratedigger.qualityRanks.*
git commit -am "cratedigger: retune <what>" && git push
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

### How to verify the new config is live

1. **Read the generated file** -- `ssh doc2 'sudo cat /var/lib/cratedigger/config.ini | grep -A 30 "\[Quality Ranks\]"'`. The section should show the exact values from your Nix edit.

2. **Check the runtime picks them up** -- `ssh doc2 'pipeline-cli quality <any_request_id>'`. The output prints the active `gate_min_rank`, `bitrate_metric`, and thresholds the simulator is using. Mismatch means Cratedigger hasn't restarted since the rebuild (it's a 5-min timer) -- wait a cycle or `sudo systemctl start cratedigger --no-block`.

3. **Visual confirmation** -- open the [Decisions tab at music.ablz.au](https://music.ablz.au). The top of the tab renders three pills (**Gate min rank** / **Bitrate metric** / **Within-rank tolerance**) pulled from the same `_runtime_rank_config()` snapshot. If your tuning is live, the pills show the new values (#68). The transcode stage rule threshold also reflects `bands.mp3Vbr.excellent` live, since `get_decision_tree()` threads `cfg` through (#75).

### The search filter is deliberately permissive

The `[Search Settings] allowed_filetypes` list in `config.ini` (exposed as `services.cratedigger.searchSettings.allowedFiletypes` on the upstream module) is a **priority-ordered preference list**, not a quality gate. It decides which codecs Cratedigger prefers when multiple options exist for the same album and drives peer selection within each tier; it does NOT decide whether a file is "good enough". That call belongs to the rank model.

**Current production list** (top-to-bottom priority):

```
mp3 v0,mp3 320,flac 24/192,flac 24/96,flac 24/48,flac 16/44.1,flac,alac,aac,opus,ogg,mp3,wav
```

**How it actually works** (in `cratedigger.py:_build_search_cache` → `lib/enqueue.py:_try_filetype`):

1. slskd returns every peer that has a file matching the raw text query "artist album". No filetype restriction on the wire — the codec preference is applied locally.
2. `_build_search_cache()` walks each file and buckets it into the **first tier it matches**. Files that match zero tiers are dropped.
3. `_try_filetype()` walks tiers in list order. For each tier, it sorts peers by upload speed (descending) and enqueues the first peer with a complete album directory.
4. If no tier has a complete album, the fallback catch-all unions all tiers the peer already matched — it does NOT rescue files that matched no tier at all.

**Why the high-quality tiers lead**: `mp3 v0` and `mp3 320` sit at the top because the curation philosophy in `CLAUDE.md` says "MP3 V0 is efficient TRANSPARENT MP3; take it if available". FLAC sample-rate tiers follow so hi-res FLAC beats CD-quality FLAC. ALAC comes before the lossy codecs because it's lossless. AAC/Opus/OGG get specific tiers before the bare-codec fallbacks so preferred lossy codecs have priority.

**Why the bare-codec tiers at the end**: `aac`, `opus`, `ogg`, `mp3`, `wav` with no quality qualifier match any bitrate/bitdepth for that codec. They exist so that **any audio file the rank model understands reaches the rank model**. The rank model then handles the real decision via:

- `quality_rank()` → codec-specific band classification from `cfg.{opus,aac,mp3_vbr,mp3_cbr}.{transparent,excellent,good,acceptable}`
- `import_quality_decision()` → accepts imports unconditionally when there's no existing album, rejects downgrades against existing albums
- `quality_gate_decision()` → accepts or `requeue_upgrade` based on rank vs `cfg.gate_min_rank`
- Spectral verification via `spectral_import_decision()` for MP3/CBR
- Transcode detection via `transcode_detection()` for FLAC→V0 conversions

**The design intent**: low-quality placeholder imports for new requests that have nothing else, followed by automatic upgrade cycles as better sources appear on Soulseek. An obscure album that only exists as MP3 V4 gets imported at V4, then upgrades to FLAC if/when a lossless seeder shows up. No silent "no match found" blind spots.

**When to tune this list**:

- **Reorder the preferred tiers** if you want a different curation philosophy. E.g. moving `flac 16/44.1` above `mp3 v0` means "always prefer FLAC even though it's 5x bigger". Current order prefers space-efficient transparent MP3.
- **Remove a bare-codec fallback tier** if you want the search filter to enforce a codec floor. E.g. removing `opus` means Opus 128 is invisible again, matching the pre-2026-04-11 behavior.
- **Add format/bitrate-specific tiers** if you want to promote or demote specific subsets. E.g. inserting `opus 128+` between `alac` and the bare-codec fallbacks would prefer Opus 128 before falling through to AAC/OGG.

**Exposed as `services.cratedigger.searchSettings.allowedFiletypes`** (a `listOf str`). Set it on the host config that imports the upstream module to override the default list above.

### Where the docs live

- [`docs/quality-ranks.md`](docs/quality-ranks.md) -- the full rank model rationale: rank ladder, codec resolution order, band table justification, bitrate metric tradeoffs, when to prefer median.
- [`docs/quality-verification.md`](docs/quality-verification.md) -- spectral cliff detection methodology and tuning history.
- `lib/quality.py:QualityRankConfig` -- the dataclass, with docstrings next to each default.
- `tests/test_quality_decisions.py:TestQualityRankConfigDefaults` -- pin tests that fail loudly on any drift.
- `lib/quality.py:file_identity()` / `filetype_matches()` / `parse_filetype_config()` -- the search-side filetype spec model that backs `allowed_filetypes`.

## Audit trail

Every download stores two JSONB blobs in `download_log` for complete auditability:

**`import_result`** -- from `import_one.py` via `ImportResult` dataclass:
- Decision (import/downgrade/transcode_upgrade/transcode_downgrade/error)
- Per-track spectral analysis (grade, HF deficit, cliff detection per track)
- Conversion details (FLAC->V0 or configurable target format, post-conversion bitrate)
- Quality comparison (new vs existing bitrate, verified_lossless flag)
- Postflight verification (beets ID, track count, imported path)

**`validation_result`** -- from `beets_validate()` via `ValidationResult` dataclass:
- Full beets candidate list with distance breakdown per component (album, artist, tracks, media, source, year...)
- Track mapping: which local file matched which MusicBrainz track
- Local file list (path, title, bitrate, format) vs MB track list (title, length, track_id)
- Beets recommendation confidence level
- Soulseek username, download folder, failed_path (used by force-import), denylisted users, corrupt files

```sql
-- Why was this rejected?
SELECT validation_result->'candidates'->0->'distance_breakdown',
       import_result->>'decision',
       import_result->'spectral'->>'grade'
FROM download_log WHERE id = <id>;

-- Which tracks matched?
SELECT m->'item'->>'title' as local, m->'track'->>'title' as mb
FROM download_log, jsonb_array_elements(validation_result->'candidates'->0->'mapping') AS m
WHERE id = <id>;
```

All types are fully typed dataclasses with pyright enforcement and JSON round-trip serialization:
`ImportResult`, `ValidationResult`, `CandidateSummary`, `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `DownloadInfo`, `SpectralContext`, `AlbumInfo`, `ActiveDownloadState`, `ActiveDownloadFileState`, `CooldownConfig`.

## Pipeline CLI diagnostics

`pipeline-cli` already has the pipeline DB connection configured, so use it for ad-hoc debugging instead of hand-rolling `psql` or Python one-offs.

```bash
# Inline SQL (runs in a read-only DB session)
pipeline-cli query "SELECT id, status, artist_name, album_title FROM album_requests WHERE status = 'wanted' LIMIT 5"

# Multi-line SQL without shell quoting
pipeline-cli query - <<'SQL'
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC
LIMIT 10
SQL

# JSON output for scripting
pipeline-cli query --json "SELECT id, outcome, import_result FROM download_log ORDER BY id DESC LIMIT 3"
```

## MusicBrainz mirror

All MusicBrainz lookups hit a local mirror at `http://192.168.1.35:5200` (doc2), not the public API. This avoids rate limits and provides sub-second response times. The mirror runs [musicbrainz-docker](https://github.com/metabrainz/musicbrainz-docker) and replicates nightly.

```bash
# Search releases
curl -s "http://192.168.1.35:5200/ws/2/release?query=artist:radiohead+AND+release:ok+computer&fmt=json"

# Get release with tracks
curl -s "http://192.168.1.35:5200/ws/2/release/<MBID>?inc=recordings+media&fmt=json"
```

## Verified lossless target format

After verifying a FLAC download is genuine (via spectral analysis + V0 conversion), the pipeline can convert to a configurable target format instead of keeping V0. Set via the upstream module:

```nix
services.cratedigger.beetsValidation.verifiedLosslessTarget = "opus 128";
```

This renders into `[Beets Validation] verified_lossless_target = opus 128` in the runtime `config.ini`.

Supported formats:

| Config value | ffmpeg codec | Output | Notes |
|---|---|---|---|
| `opus 128` | libopus VBR 128kbps | `.opus` | ~half the bitrate of V0 at equivalent quality |
| `opus 96` | libopus VBR 96kbps | `.opus` | Good for space-constrained libraries |
| `mp3 v0` | LAME VBR quality 0 | `.mp3` | Same as default (no target needed) |
| `mp3 v2` | LAME VBR quality 2 | `.mp3` | ~190kbps, smaller than V0 |
| `mp3 192` | LAME CBR 192kbps | `.mp3` | Fixed bitrate |
| `aac 128` | AAC VBR 128kbps | `.m4a` | Apple ecosystem |
| *(empty)* | *(none)* | `.mp3` | Keep V0 — the default |

The V0 verification step always runs first regardless of target format. Genuineness is judged by `transcode_detection()`: spectral cliff analysis is authoritative when available (suspect grade → transcode, genuine/marginal → not), and the post-conversion V0 bitrate is a fallback only when spectral is unavailable. The fallback threshold defaults to `cfg.mp3_vbr.excellent` (210 kbps) and tracks retuning of `[Quality Ranks]` automatically. Only after verification does the target conversion run from the original FLAC. If the FLAC is a transcode, the target conversion is skipped and V0 is kept.

## Notifiers (Meelo / Plex / Jellyfin)

After a successful auto-import, Cratedigger fires a best-effort library-refresh POST to each configured downstream media server so newly-imported albums appear without a manual rescan. Each notifier is independent and disabled by default. Failures are logged and non-fatal: a broken Plex will never block a successful beets import.

Configure via the upstream module:

```nix
services.cratedigger.notifiers = {
  meelo = {
    enable = true;
    url = "https://meelo.example.com";
    usernameFile = "/run/secrets/meelo-username";
    passwordFile = "/run/secrets/meelo-password";
  };
  plex = {
    enable = true;
    url = "https://plex.example.com";
    tokenFile = "/run/secrets/plex-token";
    librarySectionId = 3;
    pathMap = "/srv/music/Beets:/data/music";   # optional, scopes refresh to one path
  };
  jellyfin = {
    enable = true;
    url = "https://jellyfin.example.com";
    tokenFile = "/run/secrets/jellyfin-token";
  };
};
```

The module renders the `[Meelo]` / `[Plex]` / `[Jellyfin]` sections of `config.ini` from these options at boot, sed-substituting credentials read from each `*File` path. The typed fields + `from_config` wiring live in [`lib/config.py`](lib/config.py); the HTTP calls themselves are `trigger_meelo_scan()` / `trigger_plex_scan()` / `trigger_jellyfin_scan()` in [`lib/util.py`](lib/util.py), dispatched from `lib/import_dispatch.py` under the `trigger_notifiers` flag on `DispatchAction`. Jellyfin's `library_id` is optional — leave it unset to trigger a full refresh.

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"    # full suite, saves to /tmp/cratedigger-test-output.txt
nix-shell --run "python3 -m unittest tests.<module> -v"  # single module
```

The test layer follows a 4-category taxonomy documented in `.claude/rules/code-quality.md`:

- **Pure function tests** — direct input → output, exhaustive subTest tables for decision matrices
- **Seam tests** — interface boundaries (subprocess argv, route contract fields, SQL shape)
- **Orchestration tests** — assert domain state via `FakePipelineDB`/`FakeSlskdAPI`, not mock call shapes
- **Integration slices** — real code paths in `tests/test_integration_slices.py`, minimal patching, required for every high-risk orchestration boundary

Shared infrastructure lives in `tests/fakes.py` (stateful fakes) and `tests/helpers.py` (typed data builders + the `patch_dispatch_externals()` context manager). New web routes must be classified in `TestRouteContractAudit.CLASSIFIED_ROUTES` — the suite fails at test time if a route is added without contract coverage.

## Deployment

Deployed via NixOS. The upstream module at [`nix/module.nix`](nix/module.nix) builds a Python environment with dependencies and registers four systemd units:

- `cratedigger-db-migrate.service` — oneshot, runs the schema migrator (`scripts/migrate_db.py`) on every `nixos-rebuild switch`. `restartIfChanged = true` + `RemainAfterExit = true` so it always re-evaluates when the unit derivation changes but doesn't re-run between cycles.
- `cratedigger.service` — oneshot pipeline run, triggered by `cratedigger.timer`. Runs healthcheck → prestart (renders `config.ini`) → `cratedigger.py`. `restartIfChanged = false` so deploys don't restart it mid-cycle; the next timer fire picks up the new code.
- `cratedigger.timer` — fires every 5 minutes by default (`services.cratedigger.timer.onUnitActiveSec`).
- `cratedigger-web.service` — long-running web UI bound to `services.cratedigger.web.port` (default 8085).

`cratedigger.service` and `cratedigger-web.service` both `requires` the migrate unit, so the app cannot start against an un-migrated DB.

To deploy a new revision:

```bash
# 1. Push your cratedigger changes
cd /path/to/cratedigger && git push

# 2. Update the flake input on the consumer side
cd /path/to/your-nixos-config
nix flake update cratedigger              # or whatever you named the input
git commit -am "cratedigger: bump" && git push

# 3. Rebuild
sudo nixos-rebuild switch --flake .       # or via --flake github:user/repo#host
```

`restartIfChanged = false` on `cratedigger.service` means the deploy completes without restarting an in-flight cycle; the next 5-min timer fire runs the new code.

### Schema migrations

Schema lives in `migrations/NNN_name.sql`, applied by a tiny custom migrator (`lib/migrator.py`) that tracks applied versions in a `schema_migrations` table. The deploy systemd unit `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs the migrator on every `nixos-rebuild switch` BEFORE the app services start. `cratedigger.service` and `cratedigger-web.service` both `requires` the migrate unit, so a failed migration blocks the app from coming up against an inconsistent schema.

To add a schema change:

1. Drop a new file in `migrations/` named `NNN_describe_change.sql` (next number).
2. Plain SQL — each file runs in its own transaction, exactly once per DB. No `IF NOT EXISTS` guards needed.
3. Test it: `nix-shell --run "python3 -m unittest tests.test_migrator -v"`
4. Commit, push, deploy. The migrator picks it up automatically.

`PipelineDB` itself never runs DDL — it expects the schema to already be current. The migration unit is the only path.

## Credits

This project grew out of [mrusse/soularr](https://github.com/mrusse/soularr) by [Michael Russell](https://github.com/mrusse) -- the original idea of bridging Soulseek into a music library workflow. If you appreciate that idea, [buy mrusse a coffee](https://ko-fi.com/mrusse).

**Libraries**: [slskd-api](https://github.com/bigoulours/slskd-python-api), [music-tag](https://github.com/KristoforMaynworWormo/music-tag), [psycopg2](https://www.psycopg.org/), [beets](https://beets.io/)

## License

[MIT](LICENSE)
