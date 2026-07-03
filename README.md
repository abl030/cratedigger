# Cratedigger

A quality-obsessed music acquisition pipeline. Searches Soulseek via [slskd](https://github.com/slskd/slskd), validates every download against a specific [MusicBrainz](https://musicbrainz.org/) or [Discogs](https://www.discogs.com/) release via [beets](https://beets.io/), and curates a library toward verified lossless sources — automatically.

Cratedigger doesn't just download albums. It siphons the best available quality out of Soulseek over time: downloading, verifying via spectral analysis, converting, comparing against what's already on disk, and re-queuing for upgrades when better sources appear.

It is an **archival tool first**: requests point at exact pressings (a specific release ID, never a "close enough" sibling), the system never stops searching for what it hasn't found, and nothing irreversible ever happens without the operator. Much of the long tail it hunts is genuinely vanishing — the peer who had it logs off, and that's that.

> This project was originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr). Cratedigger has since diverged into its own thing — PostgreSQL pipeline DB, beets validation, spectral quality verification, async downloads, a web UI — but the original idea of bridging Soulseek into a music library workflow came from mrusse's work. If you appreciate that idea, [buy mrusse a coffee](https://ko-fi.com/mrusse).

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
      |    quality gate            |  auto-import -------->| -> library
      |                            |                       |
      |  source=redownload         |                       |
      |    stage to /Incoming      |  (manual review)      |
```

## Features

- **Strict pressing identity** — every request targets one release ID; validation rejects anything that isn't that exact pressing
- **PostgreSQL pipeline DB** as the sole source of truth for requests, download state, and quality history (full JSONB audit trail)
- **Web UI** for browsing MusicBrainz and Discogs and adding albums to the pipeline
- **Spectral quality verification** — sox-based transcode detection catches fake FLACs and upsampled MP3s
- **Quality upgrade system** — automatically re-queues albums when better sources appear (CBR → lossless → verified target format)
- **Async, parallel operation** — searches fan out concurrently; downloads span 5-minute cycles without blocking
- **Persisted search plans** with escalation (wildcarded queries → exact → per-track) and long-tail "unfindable" triage
- **Owned beets runtime** — the module ships a pinned beets with the full plugin closure and renders its config; your library's path layout is protected by config invariants tested in a VM on every `nix flake check`
- **Operator surface twice over** — every action exists as both a `pipeline-cli` subcommand and a web API endpoint
- **User cooldowns, force-import, wrong-match triage, YouTube rescue** for the long tail

## Running it (NixOS)

Cratedigger is a Nix flake with a NixOS module. It deliberately builds its runtime (python env + beets) from **its own flake.lock** — the exact closure its test suite ran against — not your system's nixpkgs.

You need: **NixOS**, **slskd** (`services.slskd` is in nixpkgs), and disk for music. PostgreSQL is provisioned for you.

```nix
{
  inputs.cratedigger.url = "github:abl030/cratedigger";

  outputs = { self, nixpkgs, cratedigger, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      modules = [
        cratedigger.nixosModules.default
        {
          services.cratedigger = {
            enable = true;
            slskd = {
              apiKeyFile = "/var/lib/secrets/slskd-api-key";  # raw key, one line
              downloadDir = "/srv/music/slskd-downloads";
            };
            pipelineDb.createLocally = true;   # local postgres, peer auth, no passwords
            beetsConfig = {
              directory = "/srv/music/library";
              library = "/srv/music/beets-library.db";
            };
            beetsValidation = {
              stagingDir = "/srv/music/incoming";
              trackingFile = "/srv/music/beets-validated.jsonl";
            };
            web = {
              enable = true;                    # UI on :8085
              beetsDb = "/srv/music/beets-library.db";
            };
          };
        }
      ];
    };
  };
}
```

A complete, commented version of this (including slskd itself) is [`examples/cratedigger.nix`](examples/cratedigger.nix). Misconfigurations fail at eval time with messages that name the option to set. The module runs as root by default (Soulseek downloads and the library live outside any service user's home) — override `services.cratedigger.user` if you've arranged permissions.

`cratedigger-beet` lands on your PATH as the canonical beets binary for the managed library (run it with sudo). The operator CLI is also available without installing anything:

```bash
nix run github:abl030/cratedigger#pipeline-cli -- --help
```

### Mirrors (optional, recommended for speed)

Out of the box, MusicBrainz matching uses **public musicbrainz.org** (works, rate-limited ~1 req/s) and **Discogs browse is off** (the web UI is MB-only; you get a clear 503 explaining why). Local mirrors remove both limits:

| Mirror | Without it | Option | Sample |
|---|---|---|---|
| MusicBrainz | Functional but ~1 req/s | `musicbrainz.apiBase` | [`examples/musicbrainz-mirror.nix`](examples/musicbrainz-mirror.nix) |
| Discogs | Discogs browse off; MB browse unaffected | `discogs.apiBase` + `beets.discogsMirrorUrl` | [`examples/discogs-mirror.nix`](examples/discogs-mirror.nix) |
| LRCLIB (lyrics) | Public lrclib.net | `beets.lrclibUrl` | — |

The full account (sizes, replication tokens, degraded-mode math) is in [`docs/mirrors.md`](docs/mirrors.md).

### Verifying before you trust it

```bash
nix flake check github:abl030/cratedigger
```

boots a NixOS VM in the stranger posture — local postgres, rendered beets config, public-MB defaults — and asserts the invariants that have historically eaten libraries (beets `duplicate_keys` nesting, the plugin list, path templates). The same check gates every push to this repo via a pre-push hook; known-good states are tagged `vYYYY.MM.DD`.

## Quality pipeline in one paragraph

Every download is validated against its exact target release (beets match distance ≤ 0.15), spectrally analysed (sox), converted (FLAC→V0 by default, or a configured `verifiedLosslessTarget` like `opus 128`), and compared against what's already on disk before beets imports it. All decisions are pure functions in `lib/quality.py` with a CLI simulator (`pipeline-cli quality <id>`); every outcome lands as queryable JSONB in the pipeline DB. Details: [docs/quality-ranks.md](docs/quality-ranks.md), [docs/quality-verification.md](docs/quality-verification.md).

| Config value for `verifiedLosslessTarget` | Output | Notes |
|---|---|---|
| `opus 128` / `opus 96` | `.opus` | ~half V0's bitrate at equivalent quality |
| `mp3 v0` / `mp3 v2` / `mp3 192` | `.mp3` | LAME VBR/CBR |
| `aac 128` | `.m4a` | Apple ecosystem |
| *(empty)* | `.mp3` | keep V0 — the default |

## Request retry backoff

A request can be `wanted` while intentionally skipped for a few hours: retry-worthy failures (search miss, download timeout, rejected import) set a shared exponential `next_retry_after` (30 min base, 6 h cap), and `get_wanted()` only returns rows that are due. Search, download, and validation attempts are counted separately; the retry clock is shared. The backoff is currently hardcoded (`BACKOFF_BASE_MINUTES = 30`, `BACKOFF_MAX_MINUTES = 360`), not module-tunable.

## Going deeper

| Topic | Where |
|---|---|
| Every module option | [docs/nixos-module.md](docs/nixos-module.md) |
| Mirrors: setup + degraded modes | [docs/mirrors.md](docs/mirrors.md) |
| Beets ownership, harness, config invariants | [docs/beets-primer.md](docs/beets-primer.md) |
| Quality model + tuning the rank bands | [docs/quality-ranks.md](docs/quality-ranks.md), [docs/quality-verification.md](docs/quality-verification.md) |
| Search plans, escalation, unfindable triage | [docs/persisted-search-plans-rollout.md](docs/persisted-search-plans-rollout.md), [docs/search-plan-iter2-deploy.md](docs/search-plan-iter2-deploy.md) |
| Pipeline DB schema + audit blobs | [docs/pipeline-db-schema.md](docs/pipeline-db-schema.md) |
| Debugging a decision (`pipeline-cli show/quality/debug-download`) | [docs/debugging-cli.md](docs/debugging-cli.md) |
| Web UI internals + dev server | [docs/webui-primer.md](docs/webui-primer.md), [docs/web-dev-server.md](docs/web-dev-server.md) |
| Post-import notifiers (Meelo / Plex / Jellyfin) | [docs/meelo-primer.md](docs/meelo-primer.md), [docs/plex-primer.md](docs/plex-primer.md) |

## Development

```bash
nix-shell --run "bash scripts/run_tests.sh"   # full suite (~2 min): JS, dead-code sweep, 4600+ python tests
nix-shell --run "pyright"                     # 0 errors, enforced
```

The dev shell resolves the same pinned nixpkgs as the module and production — one beets everywhere; `tests/test_harness_beets2_contract.py` runs the real beets so version drift fails the suite instead of production.

## Credits

This project grew out of [mrusse/soularr](https://github.com/mrusse/soularr) by [Michael Russell](https://github.com/mrusse). **Libraries**: [beets](https://beets.io/), [psycopg2](https://www.psycopg.org/), [msgspec](https://jcristharif.com/msgspec/), [music-tag](https://github.com/KristoforMaynworWormo/music-tag).

## License

[MIT](LICENSE)
