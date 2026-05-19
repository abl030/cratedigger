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
4. **Pipeline DB is PostgreSQL on doc2** (nspawn container at `192.168.100.11:5432`, migrated from SQLite 2026-03-25). Data lives at `/mnt/virtio/cratedigger/postgres`. Access via `pipeline-cli` on doc2's PATH; for non-root SSH sessions, source `/run/secrets/cratedigger-pgpass` and export `PGPASSWORD` before running `pipeline-cli` (example below). Raw TCP reachability to `192.168.100.11:5432` exists on doc1/doc2, not on the Framework laptop by default. Request statuses: `wanted`, `downloading`, `imported`, `manual`. Import queue statuses: `queued`, `running`, `completed`, `failed`.
5. **This is a curated music collection.** Multiple editions/pressings of the same album are intentional. NEVER delete or merge duplicate albums — they are different MusicBrainz releases (countries, track counts, labels) and the user wants them all. Beets must disambiguate them into separate folders.
6. **The pipeline self-heals — the request is the source of truth, everything else is derived.** Files, beets entries, wrong-matches folders, search plans, denylist, overrides, evidence — all derived state. Operator actions that touch identity supersede the row rather than mutate it, and let the pipeline rebuild from the new row. Audit trail (the frozen old row and its content-addressed child rows) is preserved by virtue of the old row never being mutated or deleted. The Replace operator action (`lib/mbid_replace_service.py`) is the canonical example: it flips the old row to `status='replaced'`, inserts a new row with `replaces_request_id` pointing back, and lets the next 5-minute cycle re-source from the new MBID. Request statuses also include `replaced` (terminal, frozen audit row).
7. **Don't duplicate convergence — reuse the cleanup paths that already exist.** When an operator action could leave behind orphans (in-flight slskd transfers, stale staging, dangling rows), prefer letting existing convergence pick them up over adding bespoke teardown to the action. Where convergence does not yet exist, file an issue and ship the closest direct cleanup in the action itself. Replace deliberately leaves in-flight slskd transfers running (cleanup tracked at issue #278) rather than building a partial cancellation path that would duplicate that work.

## Subsystems

- **Web UI** (`music.ablz.au`) — single-page app, stdlib `http.server`, vanilla JS, no build step. `cratedigger-web` systemd service on doc2. Browse tab toggles between MusicBrainz and Discogs mirror; Discogs releases flow through the same pipeline as MB. See `docs/webui-primer.md`.
- **Beets** (Nix-managed via Home Manager, colocated with cratedigger — currently doc2) — library source of truth. All automated imports go through the JSON harness (`harness/beets_harness.py` via `run_beets_harness.sh`), never raw `beet import`. The `musicbrainz` plugin MUST be in the plugins list or beets returns 0 candidates. Always match by `candidate_id` (MB release UUID), never `candidate_index`. See `docs/beets-primer.md`.
- **Meelo** — self-hosted music server on doc1 (podman), scans beets library. After every auto-import, cratedigger triggers a Meelo rescan so the new album appears immediately. See `docs/meelo-primer.md`.
- **Plex** — second music browser, Docker container on Unraid (`tower`), reads the beets library via SMB. Cratedigger triggers a partial scan after each import via `lib/util.py::trigger_plex_scan`. **Note: Plex's refresh endpoint returns HTTP 200 for any path including invalid ones — HTTP 200 is not evidence the scan ran.** See `docs/plex-primer.md`.
- **Discogs mirror** (`discogs.ablz.au`) — ~19M releases, Rust JSON API, nspawn PostgreSQL on doc2. Beets' Discogs plugin is patched (Nix `substituteInPlace`) to hit this mirror, so numeric IDs route through it. See `docs/discogs-mirror.md`.
- **MusicBrainz mirror** (`http://192.168.1.35:5200`) — local MB mirror. See `docs/musicbrainz-mirror.md`.
- **Quality model** — codec-aware rank comparison (LOSSLESS > TRANSPARENT > EXCELLENT > ...). Every measurement classifies into a `QualityRank` band; gate compares against `cfg.quality_ranks.gate_min_rank` (default EXCELLENT). See `docs/quality-ranks.md` and `docs/quality-verification.md`.
- **User cooldowns** — global 3-day cooldowns for Soulseek users with 5 consecutive failures. See `docs/cooldowns.md`.
- **Persisted search plans** — every `wanted` request carries a deterministic plan generated by `lib.search.generate_search_plan` and persisted via `lib.search_plan_service.SearchPlanService`. Phase 2 reads `get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID, ...)` and consumes plan-items by ordinal; the executor never recomputes variants. Atomic consumed-attempt writes guard against stale completions after mid-flight regeneration. New `outcome='exhausted'` rows are no longer emitted (cycle wrap replaces them); historical rows remain. Bump `SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` whenever generator output changes. See `docs/persisted-search-plans-rollout.md`, `docs/pipeline-db-schema.md`.

## Infrastructure

- **doc1** (`192.168.1.29`): this repo lives at `/home/abl030/cratedigger`; primary interactive dev host.
- **doc2** (`192.168.1.35`): runs cratedigger (systemd oneshot, 5-min timer) and beets (Home Manager), plus MusicBrainz mirror (`:5200`) and slskd (`:5030`). Beets is colocated with cratedigger so the harness can invoke `beet import` locally.
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

### Web dev server

Use `scripts/web_dev_server.py` in two layers:

- `--data live-db` runs local route code against a real read-only PostgreSQL
  session and the backend host's filesystem.
- `--data prod-api` serves your checked-out frontend files locally while
  proxying `/api/*` to another read-only backend. Despite the name, it can
  target any remote base URL, not just prod.

For Wrong Matches, `live-db` must run on a host that can see the rejected
folders on disk. DB reachability alone is not enough because
`/api/wrong-matches/explorer` and `/api/wrong-matches/audio` open real files.
In this homelab, `doc1` and `doc2` qualify as backend hosts; Framework and
Windows do not unless the relevant paths are mounted locally.

Canonical remote-dev flow from any machine with SSH access:

1. Start a `live-db` backend on a host that can see the files. If that host
   does not have direct DB reachability, tunnel PostgreSQL first:
   ```bash
   ssh -N -L 15432:192.168.100.11:5432 doc2
   PIPELINE_DB_DSN=postgresql://cratedigger@127.0.0.1:15432/cratedigger \
     nix-shell --run "python3 scripts/web_dev_server.py --data live-db --host 127.0.0.1 --port 8096"
   ```
2. Tunnel that backend to your local machine if `8096` is not already reachable:
   ```bash
   ssh -N -L 18096:127.0.0.1:8096 <backend-host>
   ```
3. On your local checkout, serve the frontend against the tunneled backend:
   ```bash
   nix-shell --run "python3 scripts/web_dev_server.py --data prod-api --prod-base-url http://127.0.0.1:18096 --host 127.0.0.1 --port 8096"
   ```

Open `http://127.0.0.1:8096`. This gives live reload for local `web/` edits
without exposing the Postgres port to the laptop. The proxy forwards `Range`
headers, so Wrong Matches audio playback and scrubbing still work through the
tunnel.

`--beets-db` is optional in this flow. Wrong Matches does not need it; only
beets-backed library badges and lookups do.

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
docs/solutions/         — Compounding lessons from past bugs/decisions, organized by category with YAML frontmatter (module, tags, problem_type). Worth a grep when debugging in a documented area.
.claude/rules/          — Path-scoped auto-loaded rules (code-quality, nix-shell, deploy, ...)
```

Key `lib/` modules:
- `config.py` — typed `CratediggerConfig` (loaded from config.ini)
- `context.py` — `CratediggerContext` (replaces module globals; caches cooled_down_users)
- `pipeline_db.py` — PostgreSQL CRUD + advisory locks (see `docs/advisory-locks.md`)
- `migrator.py` — versioned schema migrator
- `quality.py` — pure decision functions + all typed dataclasses (`ImportResult`, `ValidationResult`, `DispatchAction`, `QualityRankConfig`, `CooldownConfig`, ...)
- `measurement.py` — pure measurement helpers (`measure_preimport_state`, `inspect_local_files`, `spectral_analyze`, bad-audio-hash gate). No decision logic — the importer reads persisted evidence and decides via `full_pipeline_decision_from_evidence`.
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

## CLI ⇄ API surface symmetry

Operator capabilities should appear on **both** `pipeline-cli` and the web API. When you add an operator action to one, add it to the other in the same PR. Both surfaces wrap the same service-layer method (e.g. `SearchPlanService.advance_for_request`); the CLI command and the HTTP endpoint are thin adapters with matching exit-code/status-code mappings.

Why: operators frequently start in the web UI, escalate to CLI when they want a script, or vice versa. A capability that exists in only one surface is a trap — the team learns to expect parity, then trips when it isn't there. A drifted contract (CLI returns one shape, API returns another) is worse than no parity, because tests usually catch only one side.

Concrete pattern (see `search-plan advance` for a worked example):

| Layer | File | Responsibility |
|-------|------|----------------|
| Service | `lib/<thing>_service.py` | Holds logic; returns a typed `Result` dataclass |
| DB | `lib/pipeline_db.py` | Atomic mutations + `FOR UPDATE` row locks |
| CLI | `scripts/pipeline_cli.py` | Wraps service; maps `Result.outcome` to exit code |
| API | `web/routes/<thing>.py` | Wraps service; maps `Result.outcome` to HTTP status |
| Tests | `tests/test_<thing>_service.py` + `tests/test_pipeline_cli.py` + `tests/test_web_server.py` | Service tests are authoritative; CLI + API tests check the wrapper mapping |
| Audit | `tests/test_web_server.py::TestRouteContractAudit::CLASSIFIED_ROUTES` | Every new route must be added — guard test fails otherwise |

Status/exit code mapping should follow the existing convention:
- 200 / exit 0 — success
- 400 / exit 3 — input validation error (API only — CLI argparse catches this)
- 404 / exit 2 — not found
- 409 / exit 4 — wrong state (e.g. no active plan when one is required)
- 422 / exit 3 — semantic validation error (e.g. forward-only violated)
- 503 / exit 5 — transient (lock contention, retry)

If you only need the capability from one surface in this PR, expose it on both anyway. The cost of adding the second surface is small; the cost of explaining "why is X CLI-only?" to a future operator is larger.

## Decision architecture

**Quality decisions live in ONE place.** `full_pipeline_decision_from_evidence`
in `lib/quality.py` (and its flat-kwargs simulator twin `full_pipeline_decision`)
is the single source of truth for every importer decision — folder/audio
integrity (audio_corrupt, bad_audio_hash, nested_layout, empty_fileset) AND
quality (spectral, codec rank, V0 probe, provisional lossless, verified
lossless, transcode detection, quality gate). **Never re-create import
decisions elsewhere.** If a code path needs to know "should this be
imported", it must call the full pipeline — not invent its own narrower
check.

This bit us twice. First (PR #257): a parallel `preimport_decide` spectral
branch fell back to existing container bitrate when spectral evidence was
missing, rejecting legitimate FLAC provisional-lossless upgrades. Fixed by
deleting the parallel decision. Second (the evidence-canonical-cleanup
refactor, PR landing #258 + this PR): `preimport_decide` still owned four
folder/audio-integrity branches alongside `full_pipeline_decision_from_evidence`.
That asterisk on "quality decisions live in ONE place" — "except these four
facts, which live in `preimport_decide`" — was hair-splitting. The four
branches were folded into `full_pipeline_decision_from_evidence` as early
exits at the top of the function (U11). One decider, one rejection helper,
one denylist policy.

**Preview produces evidence. Importer decides.** The two-worker contract:

- **Preview worker** (`lib/import_preview.py`): measures via
  `measure_preimport_state` + `run_import_one`, persists
  `AlbumQualityEvidence`, marks the job `evidence_ready` (or
  `measurement_failed`). Never emits a verdict. Never decides accept/reject.
  Never writes the denylist.
- **Importer worker** (`lib/import_dispatch.py::dispatch_import_from_db`):
  reads persisted evidence and decides via
  `full_pipeline_decision_from_evidence`. The single function makes every
  import decision — the four folder/audio-integrity early branches
  (`audio_corrupt`, `bad_audio_hash`, `nested_layout`, `empty_fileset`) and
  the quality branches (spectral, codec rank, V0, gate). Rejects route
  through one helper (`_reject_import_from_evidence_decision`) with one
  denylist policy (`dispatch_action` returns `denylist=True` for source-
  quality rejects and the two integrity reasons that warrant peer
  denylisting). The "always self-heal on four-fact reject" invariant is
  enforced via `_PREIMPORT_FACT_REJECT_DECISIONS` inside the unified
  helper.

If you find yourself writing a new function that compares spectral / bitrate
/ codec ranks, stop. Either call `full_pipeline_decision_from_evidence` or
extend the pure decision helpers it already composes (`spectral_import_decision`,
`measured_import_decision`, `provisional_lossless_decision`,
`quality_gate_decision`). The function does NOT accept container-bitrate
fallback — spectral compares to spectral evidence only (invariant of #257).

**The album test set is what defines behavior.** Live-bug scenarios go in
`tests/test_quality_classification.py::TestLiveBugReproductions` (Bride,
Flux, Taboo, Tyler Lambert, BoC, Heretic Pride, etc.) and the four-fact
scenarios go in `TestFourFactPreimportRejects` (same file). Every scenario
MUST also be exercised through the production decider via
`TestLiveBugReproductionsThroughEvidencePipeline` — the parity contract is
that the simulator and the evidence pipeline reach the same outcome on the
same album. If you change import policy, update the album test set first;
the live code follows.

**Evidence is content-addressed.** `album_quality_evidence` rows are keyed
by `(mb_release_id, snapshot_fingerprint)`; addressing entities reference
them via `import_jobs.candidate_evidence_id`,
`download_log.candidate_evidence_id`, and
`album_requests.current_evidence_id`. Triage walks the FK chain (direct →
cross-walk via `request_id` → measure as last resort). Evidence is never
deleted unless the files actually change.

**Evidence survives the candidate → library transition (lossless-source
gated).** After a successful import, `propagate_candidate_evidence_to_current`
(U10) inherits the candidate's measurement payload onto the library
evidence row. `verified_lossless_proof`, `verified_lossless`, and
`was_converted_from` propagate in **all** cases. `spectral_grade`,
`spectral_bitrate_kbps`, `v0_metric`, and `matched_bad_audio_hash_*`
propagate when the import is renamed-only OR when the candidate source
codec is lossless (FLAC / ALAC / WAV) — `LOSSLESS_CODECS` in `lib/quality.py`
is the canonical set. Non-lossless transcoded imports (MP3 → Opus etc.)
strip those fields onto NULL because a lossy source's spectral / V0
lineage is not meaningfully comparable against future candidates and
storing it on the library row would mislead triage.

For lossless-source-transcoded library rows, the propagated fields
describe the upstream source audio at import time, not the on-disk
file. Wrong-match cleanup triage compares future candidates against
this evidence to reject same-source duplicates.

**Search narrowing companion.** When `lossless_source_locked` fires —
in the importer (`lib/import_dispatch.py`) or wrong-match cleanup
triage (`lib/wrong_match_cleanup_service.py`) — the request's
`search_filetype_override` is narrowed to `"lossless"` via
`narrow_override_on_lossless_source_lock` (`lib/quality.py`). Future
search cycles only ask Soulseek for lossless tiers, so the lock
doesn't fire repeatedly against new peers serving the same lossy
file. No plan-generator change is needed — `generate_search_plan`
produces query strategies, and the filetype filter is applied
downstream in `enqueue.py::effective_search_tiers` from the request's
override column.

Known wart: library rows imported before this policy landed (2026-05-17)
have NULL spectral / V0 / bad-hash fields and may have lossy
`search_filetype_override` values. They keep the old behaviour —
wrong-match triage cannot reject same-source duplicates against them
and the search-narrowing only fires on new `lossless_source_locked`
events — until each row is re-imported or force-imported. Forward-only
by design; no backfill. See
`docs/brainstorms/2026-05-17-propagate-source-evidence-on-transcode-requirements.md`.

Pure decision helpers in `lib/quality.py`: `spectral_import_decision`,
`import_quality_decision`, `transcode_detection`, `quality_gate_decision`,
`determine_verified_lossless`, `dispatch_action`,
`compute_effective_override_bitrate`, `verify_filetype`, `should_cooldown`,
`provisional_lossless_decision`, `measured_import_decision`, and
`get_decision_tree` (feeds the web UI Decisions tab).

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

## GitHub PR merges

Use GitHub **Create a merge commit** for this repo's PRs. Do not use
**Rebase and merge** or **Squash and merge**.

This keeps the PR attached to mainline history on GitHub while preserving the
individual commits that landed in the PR.

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
- `/refactor` — guided structural refactor pipeline

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
pipeline-cli search-plan show <request_id>   # active plan + cursor + per-slot usefulness stats (--json for machine output)
pipeline-cli search-plan regenerate <request_id>  # operator repair path; resets cursor on success, preserves old plan on failure
pipeline-cli query "SELECT ..."              # ad-hoc read-only SQL (add --json for machine output)
pipeline-cli query - <<'SQL'                 # multi-line SQL without shell quoting
SELECT id, artist_name, album_title, min_bitrate, current_spectral_bitrate
FROM album_requests
WHERE current_spectral_bitrate IS NOT NULL
ORDER BY updated_at DESC LIMIT 10
SQL
```

From doc1, use the same CLI over SSH without sudo by loading doc2's
world-readable PG dotenv and mapping it to libpq's `PGPASSWORD`:

```bash
ssh doc2 'set -a; . /run/secrets/cratedigger-pgpass; set +a; export PGPASSWORD="${PGPASSWORD:-${PIPELINE_DB_PASSWORD:-${POSTGRES_PASSWORD:-}}}"; pipeline-cli query --json "SELECT 1 AS ok"'
```

`pipeline-cli query` sets `default_transaction_read_only = on` — safe for diagnostics. When debugging pipeline behavior, start with the simulator (`pipeline-cli quality`) and add scenarios that expose the bug FIRST — see `.claude/rules/code-quality.md` § "Pipeline Decision Debugging — Simulator-First TDD".

## Critical rules

1. **NEVER use `beet remove -d`** — deletes files permanently (exceptions: ban-source endpoint and Replace action, both explicit user actions composed via `lib/release_cleanup.py::remove_and_reset_release`).
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
- **2026-05-18 asciify_paths Plex mass-split**: Enabling `asciify_paths = true` in beets + running a full-library `beet move` renamed thousands of file paths through unidecode but left ID3 tags untouched (paths-only by design). Plex's scanner doesn't reconcile mass renames — it created ghost album rows for the renamed files alongside the original rows (still pointing at dead curly-path tracks). Affected ~12% of the library (1,178 albums split). Empty Trash + Clean Bundles made it WORSE (surfaced additional splits the prior partial scans had missed on the remote SMB mount). Fixed via the Plex metadata merge API (`scripts/plex_dupes_audit.py` + `plex_dupes_merge.py`). See `docs/solutions/runtime-errors/plex-asciify-paths-album-split.md`. **Footgun:** any future beets change that mutates rendered paths (asciify, `paths:` template, `path_sep_replace`) followed by `beet move` will re-trigger this — plan to run the merge scripts after the next full Plex scan.

## Secrets

- slskd API key: sops-managed, injected into `config.ini` at runtime via the `cratedigger-secrets-split` oneshot (see `docs/nixos-module.md`).
- Discogs token: `~/.config/beets/secrets.yaml` on the beets host (doc2; not used by cratedigger directly).
