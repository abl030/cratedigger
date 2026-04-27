# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz via beets, auto-imports with spectral quality verification, or stages for manual review. Web UI at `music.ablz.au` browses the local MusicBrainz + Discogs mirrors and enqueues album requests.

Originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr) ([Ko-Fi](https://ko-fi.com/mrusse)). Has since diverged into its own project — the pipeline DB is the sole source of truth, and the web UI is the album picker.

## Critical invariants (read first — these will bite you)

1. **Run `hostname` at the start of every chat.** `proxmox-vm` = doc1, `doc2` = doc2, `framework` = Framework laptop. `DESKTOP-*` = Windows laptop. You are likely already on doc1 — do NOT ssh to doc1 from doc1.
2. **Windows laptop SSH access**: no native SSH key. A NixOS WSL2 instance has it via sops-nix. Run:
   ```
   mkdir -p ~/.ssh && wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2
   ssh -i ~/.ssh/id_doc2 abl030@doc2    # or abl030@proxmox-vm
   ```
   The key works for both machines. You may need `-o StrictHostKeyChecking=no` on first use.
3. **nixosconfig changes MUST be made on doc1.** The repo lives at `~/nixosconfig` on doc1. Doc1 has the git push credentials; doc2 and Windows do not. SSH to doc1 first, edit, commit, push, then deploy to doc2.
4. **Pipeline DB is PostgreSQL on doc2** (nspawn container at `192.168.100.11:5432`, migrated from SQLite 2026-03-25). Data lives at `/mnt/virtio/cratedigger/postgres`. Access via `pipeline-cli` on doc2's PATH, or from doc1 via `ssh doc2 'pipeline-cli ...'`. Request statuses: `wanted`, `downloading`, `imported`, `manual`. Import queue statuses: `queued`, `running`, `completed`, `failed`.
5. **This is a curated music collection.** Multiple editions/pressings of the same album are intentional. NEVER delete or merge duplicate albums — they are different MusicBrainz releases (countries, track counts, labels) and the user wants them all. Beets must disambiguate them into separate folders.

## Subsystems

- **Web UI** (`music.ablz.au`) — single-page app, stdlib `http.server`, vanilla JS, no build step. `cratedigger-web` systemd service on doc2. Browse tab toggles between MusicBrainz and Discogs mirror; Discogs releases flow through the same pipeline as MB. See `docs/webui-primer.md`.
- **Beets** (v2.5.1, Nix-managed on doc1) — library source of truth. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. See `docs/beets-primer.md`.
- **Meelo** — self-hosted music server on doc1 (podman), scans beets library. After every auto-import, cratedigger triggers a Meelo rescan so the new album appears immediately. See `docs/meelo-primer.md`.
- **Discogs mirror** (`discogs.ablz.au`) — ~19M releases, Rust JSON API, nspawn PostgreSQL on doc2. Beets' Discogs plugin is patched (Nix `substituteInPlace`) to hit this mirror, so numeric IDs route through it. See `docs/discogs-mirror.md`.
- **MusicBrainz mirror** (`http://192.168.1.35:5200`) — local MB mirror. See `docs/musicbrainz-mirror.md`.
- **Quality model** — codec-aware rank comparison (LOSSLESS > TRANSPARENT > EXCELLENT > ...). Every measurement classifies into a `QualityRank` band; gate compares against `cfg.quality_ranks.gate_min_rank` (default EXCELLENT). See `docs/quality-ranks.md` and `docs/quality-verification.md`.
- **User cooldowns** — global 3-day cooldowns for Soulseek users with 5 consecutive failures. See `docs/cooldowns.md`.

## Infrastructure

- **doc1** (`192.168.1.29`): runs beets (Home Manager); this repo lives at `/home/abl030/cratedigger`.
- **doc2** (`192.168.1.35`): runs cratedigger (systemd oneshot, 5-min timer), MusicBrainz mirror (`:5200`), slskd (`:5030`).
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB data, music library are all accessible from both.
- **Nix deployment**: cratedigger is a flake input (`cratedigger-src`) in `~/nixosconfig/flake.nix`. Downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports `inputs.cratedigger-src.nixosModules.default` and layers on sops + nspawn DB + redis + localProxy. See `docs/nixos-module.md` for the full option surface.

### Key paths

| Path | Machine | Purpose |
|------|---------|---------|
| `192.168.100.11:5432/cratedigger` | doc2 nspawn | Pipeline DB (PostgreSQL) |
| `/mnt/virtio/cratedigger/postgres` | shared | PostgreSQL data dir |
| `/mnt/virtio/Music/beets-library.db` | shared | Beets library DB |
| `/mnt/virtio/Music/Beets` | shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | shared | Staging root for validated downloads (`auto-import/` for request imports, `post-validation/` for manual redownload review) |
| `/mnt/virtio/Music/Re-download` | shared | READMEs for redownload targets |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/var/lib/cratedigger` | doc2 | Runtime state (config.ini, lock, denylists) |

### Accessing doc2

```bash
ssh doc2 'sudo journalctl -u cratedigger -f'                  # tail logs
ssh doc2 'sudo journalctl -u cratedigger --since "5 min ago"'
ssh doc2 'sudo systemctl is-active cratedigger'
ssh doc2 'sudo systemctl start cratedigger --no-block'        # trigger run
ssh doc2 'sudo cat /var/lib/cratedigger/config.ini'
```

**IMPORTANT for Claude Code**: `systemctl start cratedigger` blocks until the oneshot finishes (minutes). **Always use `--no-block`** when starting via SSH from a Bash tool call. Never use `&` inside SSH quotes to background systemctl — SSH keeps the connection open waiting for all child processes regardless.

## Repository layout

```
cratedigger.py          — Main loop + thin wrappers; delegates to lib/
album_source.py         — AlbumRecord, DatabaseSource abstraction
web/                    — Web UI (server.py, mb.py, discogs.py, index.html, js/)
lib/                    — Pipeline modules (see below)
harness/                — beets_harness.py (JSON protocol), import_one.py (one-shot)
migrations/             — Versioned SQL (NNN_name.sql), run by lib/migrator.py
scripts/                — pipeline_cli.py, migrate_db.py, run_tests.sh, populate_tracks.py
tests/                  — 1400+ tests; fakes.py + helpers.py shared infra
nix/                    — slskd-api build, package.nix, shell.nix, module.nix, VM check
flake.nix               — Outputs: slskd-api, devShell, nixosModules.default, checks.moduleVm
docs/                   — Subsystem docs referenced from this file
.claude/rules/          — Path-scoped auto-loaded rules (code-quality, nix-shell, deploy, ...)
```

Key `lib/` modules:
- `config.py` — typed `CratediggerConfig` (loaded from config.ini)
- `context.py` — `CratediggerContext` (replaces module globals; caches cooled_down_users)
- `pipeline_db.py` — PostgreSQL CRUD + advisory locks (see `docs/advisory-locks.md`)
- `migrator.py` — versioned schema migrator
- `quality.py` — pure decision functions + all typed dataclasses (`ImportResult`, `ValidationResult`, `DispatchAction`, `QualityRankConfig`, `CooldownConfig`, ...)
- `preimport.py` — shared pre-import gates (audio integrity + spectral) called by both auto and force/manual paths
- `download.py` — async polling + completion processing + slskd transfers
- `import_queue.py` — typed shared queue payload/result helpers
- `import_dispatch.py` — decision tree + quality gate + dispatch_import_from_db
- `import_service.py` — force-import / manual-import service layer
- `grab_list.py`, `search.py`, `beets.py`, `beets_db.py`, `spectral_check.py`, `util.py`

## Pipeline flow

```
Web UI (music.ablz.au)               CLI
      │                                │
      │ /api/add                       │ pipeline_cli.py add
      ▼                                ▼
┌──────────────────────────────────────────────┐
│           PostgreSQL (pipeline DB)            │
│  status: wanted → downloading → imported     │
│                                  manual      │
└──────────────────┬───────────────────────────┘
                   │
    ┌──────────────┴──────────────┐
    │ poll_active_downloads()     │ get_wanted()
    │ (resume previous)           │ (search new)
    ▼                             ▼
┌──────────────────────────────────────────────┐
│  Cratedigger (cratedigger.py + lib/download) │
│  Phase 1: poll → Phase 2: search + enqueue   │
└──────────────────┬───────────────────────────┘
                   │
         ┌─────────┴──────────┐
    source=request       source=redownload
    dist ≤ 0.15              │
         │              stage to /Incoming
         ▼              (manual review only)
    stage to /Incoming
         │
         ▼
    import_one.py
    (spectral → convert FLAC→V0 → quality compare → import)
         │
         ▼
      /Beets/       (cleanup /Incoming on success)
```

**All validated downloads stage under `/Incoming` first.** Request auto-imports stage under `/Incoming/auto-import`, while redownload/manual-review paths stage under `/Incoming/post-validation`. Don't assume a path under `/Incoming` is a redownload — request imports can be mid-move or mid-import there too.

### Two-track pipeline

- **Requests** (`source='request'`) — user-added via CLI or web UI. Auto-imported to beets if validation passes at distance ≤ 0.15. Converts FLAC→V0 (or target format), imports from `/Incoming/auto-import`, and cleans up.
- **Redownloads** (`source='redownload'`) — replacing bad source material. Always staged to `/Incoming/post-validation` for manual review, never auto-imported.

Schema fields, JSONB audit blobs, search_log outcomes, and the force-import flow live in `docs/pipeline-db-schema.md`.

## Decision architecture

Quality policy should stay pure where possible: facts in, decision out, no I/O,
database, or filesystem side effects. Key entries in `lib/quality.py` include
`spectral_import_decision`, `import_quality_decision`,
`transcode_detection`, `quality_gate_decision`,
`determine_verified_lossless`, `dispatch_action`,
`compute_effective_override_bitrate`, `verify_filetype`, `should_cooldown`,
and `get_decision_tree` (feeds the web UI Decisions tab).

The importer queue is the beets-mutating ownership boundary. Web, CLI, and the
automation poller enqueue import jobs; `cratedigger-importer` drains them
serially. Keep pure quality decisions; avoid adding new direct beets-mutating
entry points outside the importer worker. On startup the importer immediately
requeues any `running` import job left by a previous worker process, then
retries it; the worker holds a DB advisory singleton lock so only one importer
can drain the queue.

Wire-boundary types (harness, JSONB, subprocess stdout) are `msgspec.Struct`,
not `@dataclass` — see `.claude/rules/code-quality.md` § "Wire-boundary types".

## Deploying changes

Flake input updates MUST happen on doc1. Doc2 has no git push credentials.

```bash
# 1. commit + push code (anywhere)
git add <files> && git commit -m "..." && git push

# 2. bump flake input (on doc1)
ssh doc1 'cd ~/nixosconfig && nix flake update cratedigger-src && git add flake.lock && git commit -m "cratedigger: ..." && git push'

# 3. rebuild doc2 — runs cratedigger-db-migrate AND restarts cratedigger-web
ssh doc2 'sudo nixos-rebuild switch --flake github:abl030/nixosconfig#doc2 --refresh'
```

`cratedigger.service` has `restartIfChanged = false` — deploys don't restart it. The 5-min timer picks up new code. `cratedigger-web` and `cratedigger-db-migrate` use the systemd default and restart on switch. Before deploying `nix/module.nix` changes, run the VM check: `nix build .#checks.x86_64-linux.moduleVm`. Full flow + verification in `.claude/rules/deploy.md`; `/deploy` command runs the whole sequence.

## Database migrations

Schema lives in `migrations/NNN_name.sql`. `cratedigger-db-migrate.service` (oneshot, `restartIfChanged = true`) runs on every `nixos-rebuild switch` BEFORE `cratedigger.service`, `cratedigger-web.service`, and `cratedigger-importer.service`. These services require the migrate unit, so a failed migration blocks the app from coming up.

To add a schema change: drop a new numbered SQL file in `migrations/`, test with `nix-shell --run "python3 -m unittest tests.test_migrator -v"`, commit, deploy. No manual psql. **Never** edit an already-shipped migration — frozen history. **Never** add DDL inside `PipelineDB` methods. For destructive changes, back up first: `ssh doc2 'pg_dump -h 192.168.100.11 -U cratedigger cratedigger' > /tmp/backup.sql`. Verify after deploy: `ssh doc2 'pipeline-cli query "SELECT * FROM schema_migrations ORDER BY version DESC LIMIT 5"'`.

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"                # full suite (~2 min), saves to /tmp/cratedigger-test-output.txt
grep "^FAIL\|^ERROR" /tmp/cratedigger-test-output.txt      # check after the fact
nix-shell --run "python3 -m unittest tests.test_X -v"      # single module
```

**ALWAYS use `nix-shell --run` for Python** — the dev shell provides psycopg2, sox, ffmpeg, music-tag, slskd-api. `.claude/rules/nix-shell.md` enforces this on `.py` edits. `.claude/rules/code-quality.md` covers test taxonomy (pure / seam / orchestration / slice), shared fakes/builders in `tests/fakes.py` + `tests/helpers.py`, and the new-work checklist that maps each kind of change to the tests you owe.

**Never re-run the full suite just to grep output differently.** Read `/tmp/cratedigger-test-output.txt`.

### Pre-commit hook

Install with: `ln -sf ../../scripts/pre-commit .git/hooks/pre-commit`. Runs pyright on staged `.py` files.

### Claude Code commands

- `/deploy` — full push → flake update → rebuild → verify sequence
- `/debug-download <id>` — query both JSONB audit blobs for a download_log entry
- `/check` — pyright + full test suite pre-commit quality gate
- `/refactor`, `/fix-bug` — guided pipelines

### Claude Code rules (auto-loaded when editing matching files)

- `code-quality.md` — type safety, TDD, logging, decision purity (always loaded)
- `nix-shell.md` — always use nix-shell for Python (`*.py`)
- `harness.md` — never discard harness data, typed dataclasses (`harness/`, `lib/beets.py`)
- `web.md` — vanilla JS, no build step (`web/`)
- `pipeline-db.md` — autocommit, migration discipline (`lib/pipeline_db.py`)
- `deploy.md` — flake flow, verify deployed code (always loaded)
- `scope.md` — clean-as-you-go (always loaded)

## Playwright MCP

Browser automation for testing `music.ablz.au`. Configured per-machine in `.mcp.json` (gitignored — platform paths differ). Always use HTTPS (http times out). See `docs/playwright-mcp.md` for setup.

## Debugging quality decisions

```bash
pipeline-cli show <request_id>               # quality columns + download history with import decisions
pipeline-cli quality <request_id>            # simulate gate for genuine FLAC / V0 / CBR 320 / suspect FLAC
pipeline-cli debug-download <download_log_id>  # raw JSONB audit for one attempt
pipeline-cli query "SELECT ..."              # ad-hoc read-only SQL (add --json for machine output)
pipeline-cli query - <<'SQL'                 # multi-line SQL without shell quoting
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC LIMIT 10
SQL
```

`pipeline-cli query` sets `default_transaction_read_only = on` — safe for diagnostics. When debugging pipeline behavior, start with the simulator (`pipeline-cli quality`) and add scenarios that expose the bug FIRST — see `.claude/rules/code-quality.md` § "Pipeline Decision Debugging — Simulator-First TDD".

## Critical rules

1. **NEVER use `beet remove -d`** — deletes files permanently (exception: ban-source endpoint, explicit user action).
2. **NEVER import without inspecting the match** — always through the harness, never pipe blind input to `beet`.
3. **NEVER match by `candidate_index`** — always by MB release ID. Candidate ordering is not stable.
4. **NEVER match by release group** — always exact MB release ID. Release groups conflate pressings.
5. **Auto-import only for `source='request'`** — redownloads always stage for manual review.
6. **All scripts deploy via Nix** — no manual `cp` to virtiofs. Change code → push → flake update → rebuild.
7. **PostgreSQL must use `autocommit=True`** — prevents idle-in-transaction deadlocks. DDL migrations use separate short-lived connections with `lock_timeout` (commit ca579e3).

## Known issues

- **Track name matching**: `album_match()` uses fuzzy filename matching — can match wrong pressings with same title. Track title cross-check added as post-match gate but won't catch all cases.
- **Discogs analysis tab**: disambiguate/analysis tab requires MusicBrainz recording IDs; not available for Discogs-browsed artists (#81).
- **Discogs cover art**: the CC0 dump has no images. Discogs-only releases have no cover art in browse UI (#82).

## Resolved — canonical RCs (don't re-investigate)

- **2026-04-20 Palo Santo data loss**: NOT a beets upstream bug. The user's `duplicate_keys` block was at the top level of `~/.config/beets/config.yaml` instead of under `import:`. Beets reads strictly from `config["import"]["duplicate_keys"]["album"]` (`beets/importer/tasks.py:385`); the misplaced block was silently ignored and beets fell back to the default `[albumartist, album]` — no `mb_albumid`. `find_duplicates()` then matched cross-MBID siblings on album title alone, the harness sent `{"action":"remove"}` thinking it was a same-MBID stale entry, and beets' `task.should_remove_duplicates` blast radius wiped the sibling. Fixed by `beets.nix` YAML relocation + harness startup assertion in `_assert_duplicate_keys_include_mb_albumid`, then superseded by guarded Beets-owned replacement: Cratedigger answers `remove` only when Beets reports exactly one same-release duplicate and otherwise fails before mutation. The `03bfc63` Cratedigger-owned replacement state machine (pre-flight surgical remove + always-keep + post-import sibling `beet move`) has been removed; do not reintroduce it as fallback architecture.
- **2026-04-14 Lucksmiths MBID drift**: NOT a bug. `tagging-workspace/scripts/fix_reissues.py` deliberately retagged "First Tape" to its cassette sibling via `harness --search-id`. The drift was invisible to cratedigger's audit trail because the harness was driven out-of-band. Mitigated by the harness MBID-swap audit log at `/mnt/virtio/Music/.harness-mutations.jsonl` (see `_mbid_swap_event`).

## Secrets

- slskd API key: sops-managed, injected into `config.ini` at runtime via the `cratedigger-secrets-split` oneshot (see `docs/nixos-module.md`).
- Discogs token: `~/.config/beets/secrets.yaml` on doc1 (not used by cratedigger directly).
