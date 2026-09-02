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
      |  (next cycle)              |                       |
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
- **Async, parallel operation** — searches fan out concurrently; downloads span cycles without blocking (cycles run back-to-back, each starting seconds after the last completes)
- **Persisted search plans** with escalation (wildcarded queries → exact → per-track) and long-tail "unfindable" triage
- **External Beets contract** — the deployment owns the package, immutable effective config, catalog, library, state, secrets, and plain operator `beet`; Cratedigger consumes one compatible runtime and admits its safety invariants intrinsically at every application startup
- **Self-cleaning download workspace** — files cratedigger can positively prove it created (via its own write-ahead transfer ledger) are reaped after 7 days once no longer active; deletions are per-file with empty-dir pruning, never a folder guess. A file it can't attribute to itself — someone else's download, quarantined review material — is never touched, however old
- **Operator surface twice over** — every action exists as both a `pipeline-cli` subcommand and a web API endpoint
- **User cooldowns, force-import, wrong-match triage, YouTube rescue** for the long tail

## Ownership — read this before running it

Cratedigger keeps its authority separate from both neighbouring systems.

**Keep source and processing authority separate.** slskd's download directory is
an untrusted source tree, not Cratedigger's canonical workspace. The disk reaper
walks only that tree and removes only exact event-stamped files it can positively
prove Cratedigger downloaded once they age out; it never derives ownership from
canonical folders. Canonical albums and preview scratch live beneath the private
`processingDir` root. A shared slskd instance is safe for foreign files, which
are never reaped, but do not use the processing root for another service.

**Beets and the deployment own the installed library.** Supply one compatible
Beets Python package, immutable `BEETSDIR`, canonical catalog/root, separate
host-local state file, and token-only include to Cratedigger. The pipeline DB
owns exact acquisition requests, lifecycle, history, and durable capture proof;
it is not a holdings projection. Cratedigger mutates Beets only through its
serial importer harness and explicit exact-album delete child. Plain `beet` is
trusted operator authority outside that serialization: quiesce automation,
never raw-import or use `remove -d`, and treat path-affecting maintenance as a
high-risk librarian operation. Full contract: [docs/beets-primer.md](docs/beets-primer.md).

## Running it (NixOS)

Cratedigger is a Nix flake with a NixOS module. It builds its application
environment from **its own flake.lock** and requires the deployment-supplied
Beets package to use that same Python package set.

You need: **NixOS**, a slskd instance (`services.slskd` is in nixpkgs), an
externally owned Beets runtime/library, and disk for music. PostgreSQL can be
provisioned on the same host. Deployments upgrading from the removed
`beets.package` / `beets.config` interface must supply the external
`beets.runtime.*` capability in the same change; there are no compatibility
aliases.

```nix
{
  inputs = {
    cratedigger.url = "github:abl030/cratedigger";
    nixpkgs.follows = "cratedigger/nixpkgs";
  };

  outputs = { nixpkgs, cratedigger, ... }: {
    nixosConfigurations.myhost = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        cratedigger.nixosModules.default
        (import "${cratedigger}/examples/cratedigger.nix")
      ];
    };
  };
}
```

The imported, commented example includes slskd plus the complete external Beets
composition: `nix/beets.nix` instantiation, immutable config, canonical
catalog/root, `/var/lib/beets/state.pickle`, token-only include, readiness
unit, exact `beets.runtime.*` options, and plain operator `beet`. Copy and adapt
it; do not use the example hostname or secret paths unchanged. Misconfigurations
fail at evaluation or intrinsic application startup with actionable categories.

Keep the PostgreSQL data directory on a filesystem PostgreSQL supports locally; do not put it on virtiofs, NFS, FUSE, or the shared music filesystem. The sample's `pipelineDb.createLocally = true` is the safe default: NixOS owns the local database, directory, and service ordering. An external PostgreSQL DSN is an advanced deployment. If that server is an nspawn container backed by a host bind mount, the host directory must retain the container PostgreSQL user's mapped numeric UID/GID on every switch. In particular, a `systemd.tmpfiles.rules` `d` entry is reapplied and must not reset that bind root to `root:root`; PostgreSQL can keep running on open files and then panic at its next checkpoint.

`slskd.downloadDir` must already exist and be usable by the Cratedigger identity **before the first switch** — `cratedigger`, `cratedigger-importer`, `cratedigger-import-preview-worker`, and `cratedigger-web` each prove they can read (and, for `cratedigger`, write) that directory at startup and refuse to start otherwise (`docs/nixos-module.md#startup-write-probe`); `cratedigger-youtube-ingest` never touches it, and `cratedigger-unfindable` is never gated by this probe at all. It is not created by any tmpfiles rule; provision it yourself or point it at the directory your `services.slskd` instance already manages. The same applies to `beets.validation.stagingDir` whenever `beets.validation.enable = true` OR `youtubeIngest.enable = true` — either flag independently forces `stagingDir` non-null via its own module assertion, and the rendered config value is unconditional either way.

The web UI requires exactly one of three mutually exclusive authorization modes, and evaluation fails if you select none or more than one. The example selects Basic, so before the first switch use a runtime secret manager such as sops-nix to provision `/run/secrets/cratedigger.htpasswd` as a non-empty bcrypt htpasswd file owned `root:nginx` with mode `0440`. Keep the file outside `/nix/store`; missing or invalid material blocks the nginx start rather than exposing the UI.

If you already run an identity provider, set `web.externalAuth = true` instead and provision no credential: your own reverse proxy authorizes every request before forwarding it to the loopback gateway, and Cratedigger performs no authorization and contacts no authorizer. [`examples/external-auth-nginx.nix`](examples/external-auth-nginx.nix) is a worked forward-auth configuration, and [`docs/nixos-module.md`](docs/nixos-module.md#external-authorization-mode) states the deployment contract that mode depends on. The third mode, `web.enableInsecure = true`, disables browser authentication deliberately and says so in the UI and the journal.

The deployment installs plain `beet` with the same immutable `BEETSDIR`; the
Cratedigger module does not install an operator wrapper. The pipeline operator
CLI is also available without installing anything:

```bash
nix run github:abl030/cratedigger#pipeline-cli -- --help
```

### Mirrors (optional, recommended for speed)

Cratedigger's default MusicBrainz endpoint is **public musicbrainz.org**
(works, rate-limited ~1 req/s), while **Discogs browse is off**. The external
Beets owner must configure its corresponding endpoint; drift is a startup
warning. Local mirrors remove both limits:

| Mirror | Without it | Option | Sample |
|---|---|---|---|
| MusicBrainz | Functional but ~1 req/s | `musicbrainz.apiBase` | [`examples/musicbrainz-mirror.nix`](examples/musicbrainz-mirror.nix) |
| Discogs | Discogs browse off; MB browse unaffected | `discogs.apiBase` + deployment `nix/beets.nix` `discogsMirrorUrl` | [`examples/discogs-mirror.nix`](examples/discogs-mirror.nix) |
| LRCLIB (lyrics) | Public lrclib.net | deployment `nix/beets.nix` `lrclibUrl` | — |

The full account (sizes, replication tokens, degraded-mode math) is in [`docs/mirrors.md`](docs/mirrors.md).

### Verifying before you trust it

```bash
nix flake check github:abl030/cratedigger
```

boots a NixOS VM with local PostgreSQL and an externally owned immutable Beets
capability. It proves package/config identity, token-only admission, role-
specific state access, and the import/path invariants that have historically
eaten libraries. Run it when changing the Nix package, flake, or NixOS module;
it is a scoped infrastructure check, not a universal push gate.

## Quality pipeline in one paragraph

Every download is validated against its exact target release (beets match distance ≤ 0.15), spectrally analysed (sox), converted (FLAC→V0 by default, or a configured `verifiedLosslessTarget` like `opus 128`), and compared against what's already on disk before beets imports it. All decisions are pure functions in `lib/quality/` with a CLI simulator (`pipeline-cli quality <id>`); every outcome lands as queryable JSONB in the pipeline DB. Details: [docs/quality-ranks.md](docs/quality-ranks.md), [docs/quality-verification.md](docs/quality-verification.md).

| Config value for `verifiedLosslessTarget` | Output | Notes |
|---|---|---|
| `opus 128` / `opus 96` | `.opus` | ~half V0's bitrate at equivalent quality |
| `mp3 v0` / `mp3 v2` / `mp3 192` | `.mp3` | LAME VBR/CBR |
| `aac 128` | `.m4a` | Apple ecosystem |
| *(empty)* | `.mp3` | keep V0 — the default |

## Request retry backoff

A request can be `wanted` while intentionally skipped for a few hours: retry-worthy failures (search miss, download timeout, rejected import) set a shared exponential `next_retry_after` (30 min base, 4 h cap — so the steady state is at most about six searches per release per day), and `get_wanted()` only returns rows that are due. Search, download, and validation attempts are counted separately; the retry clock is shared. The backoff is currently hardcoded (`BACKOFF_BASE_MINUTES = 30`, `BACKOFF_MAX_MINUTES = 240`), not module-tunable.

Every writer derives the interval from one function, `lib/pipeline_db/decisions.py::search_backoff_minutes`; the two SQL writers that compute it inside their own counter-incrementing `UPDATE` bound the doubling exponent with `SEARCH_BACKOFF_MAX_EXPONENT` from the same module, because PostgreSQL evaluates `POWER` in `double precision` and the product overflows long before an attempt counter realistically could. The clamp is value-identical: from the third doubling on, the 4 h cap already decides the result.

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
| Post-import notifiers (Plex / Jellyfin) | [docs/plex-primer.md](docs/plex-primer.md), [docs/jellyfin-primer.md](docs/jellyfin-primer.md) |

## Development

```bash
bash scripts/test.sh tests.test_X                        # target + adjacent/ambient gates
nix-shell --run "python3 scripts/run_pyright_checks.py"  # both typing contracts
nix-shell --run "bash scripts/run_tests.sh"              # complete suite
```

`scripts/test.sh` expands the requested unittest selector with its generated or
deterministic sibling, tests adjacent to every changed path, and every audit and
ratchet. It also runs JavaScript, both Pyright contracts, Ruff, and Vulture in
the same aggregate failure bundle. With no selector it derives targets from the
working-tree diff. A changed shared `tests/**.py` module (a fake, helper, or
other non-`test_*.py` file) with no registered mapping in
`scripts/targeted_test_selection.py` fails closed with exit code 2 before any
phase runs, JS and Pyright included — see that module's
`EXACT_PATH_NEIGHBOURS` / `SHARED_MODULES_WITHOUT_COVERAGE` for the mapping
and the admitted-gap registry. A changed `lib/**/*.py` module gets the same
fail-closed treatment on the other side of the same gap: one that resolves
zero test neighbours (the basename-only `tests.test_<stem>` probe misses real
coverage filed under a nested path, e.g. `lib/dispatch/core.py`) fails closed
too unless admitted in `LIB_MODULES_WITHOUT_SELECTION_COVERAGE`. A changed
`scripts/**/*.py` or `scripts/**/*.sh` file — the shell wrappers included —
is policed the same way through `SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE`.
`migrations/**/*.sql`, `nix/**/*.nix` and `*.json`, `web/**/*.py`,
`harness/**/*.py` and `*.sh`, and a top-level `*.py` file are policed the
same way too, each with its own admitted-gap registry (`web/js/*.js` is
the one deliberate exception — the JavaScript phase always runs in full
regardless of Python selection, so this mechanism does not police it).
One row per root lives in `ROOT_COVERAGE_RULES`, eight rows in total;
`tests/test_selection_coverage_audit.py` keeps every non-early-returning
registry exact in both directions and checks every registry's entries for
a rationale and a path that still exists. The `tests/` registry's own
exactness lives in `tests/test_targeted_test_selection.py` and
`tests/test_negative_coverage_audit.py` instead.

To see why a path selects what it selects, ask it:

```bash
nix-shell --run "python3 scripts/targeted_test_selection.py explain lib/download.py"
```

It names the mechanism behind every selected module: the hand-authored
`EXACT_PATH_NEIGHBOURS` entry, the self-selector, or one of the
`SELECTION_RULES` rows (five basename conventions, ten directory rules).
It also reports any module a rule looked for and did not find on disk, and
whether the fail-closed contract is watching the path at all. The path need
not exist yet, so a file can be explained before it is written.

`run_tests.sh` remains the one canonical complete suite.
`run_final_gate.sh` runs that exact suite on a clean commit and adds a receipt;
it does not select different checks. CI does not enforce this local workflow.

The dev shell resolves the same pinned nixpkgs as the default module package
set, and `tests/test_harness_beets2_contract.py` runs real Beets so runtime drift
fails the suite instead of production.

## Credits

This project grew out of [mrusse/soularr](https://github.com/mrusse/soularr) by [Michael Russell](https://github.com/mrusse). **Libraries**: [beets](https://beets.io/), [psycopg2](https://www.psycopg.org/), [msgspec](https://jcristharif.com/msgspec/), [music-tag](https://github.com/KristoforMaynworWormo/music-tag).

## License

[MIT](LICENSE)
