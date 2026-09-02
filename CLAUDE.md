# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz/Discogs via Beets, auto-imports with spectral quality verification, or stages for manual review. Web UI: `music.ablz.au`. The pipeline DB is acquisition-request/lifecycle authority; Beets owns current library facts.

## Session start

Before doing anything else, silently run `hostname` and `date`. Then read
`.claude/memory/MEMORY.md` unless the client already injected it. This establishes
the current machine, time, and shared cross-agent memory.

Do not use Compound Engineering (`ce-*`, `compound-engineering:*`, or `lfg`) in
this repository. Native agent planning, implementation, debugging, and review
are sufficient.

Work in an isolated git worktree, and remove it when your task ends — durable
work lives on pushed branches, never in worktrees. A stale worktree that is
clean and whose commits are on `main` may be swept by any session, via
`git worktree remove` without `--force` so the tool itself refuses dirty
trees (#1208); never disturb another agent's live work. Keep the shared
checkout on `main` and reasonably current; after merging to `main`, pull it
forward.

`.claude/memory/` is the exception: it is written in the shared checkout, not
task worktrees. Notice and preserve those changes when cleaning up or advancing
`main`; commit and push them separately when appropriate.

## Why this exists — the archivist frame

Cratedigger is a **music archival tool first, an acquisition pipeline second**. The operator is an archivist: most of the long-tail music here is genuinely vanishing — niche pressings, Australian indie, demos that lived on one peer who logged off years ago. This frame is load-bearing; these invariants flow from it:

- **Strict pressing identity.** A request points at a specific MB release MBID or Discogs release ID. The matcher NEVER substitutes a sibling pressing. Different pressings ARE different releases.
- **The system never stops searching.** Cadence is constant forever; never auto-throttled based on apparent unfindability. Surfacing the unfindable cohort is right; throttling search on it is a product violation.
- **The system never auto-decides anything irreversible.** Surfacing is fine; replace/remove/accept-different-pressing decisions belong to the operator. **This governs identity and destruction, NOT failure handling** — a failure never waits for a human. Reading it as "when in doubt, stop and ask" is the drift that produces parked work; see invariant 11.
- **No adapter code between MB and Discogs.** Both feed the same columns in the same shape.
- **Long-tail rescue is a celebrated event** — `rescued_at` + `prior_unfindable_category` on `album_requests` are first-class audit data.
- **Single-operator, no backwards-compat.** One user, forward-only migrations, no compat shims, no committed backfill scripts, no one-shot machinery kept "in case". Full rules in `.claude/rules/scope.md` (always loaded).

If a design drifts toward "good enough" matches, "smart" defaults, or auto-throttling — that drift is a bug. Push back.

## Critical invariants (read first — these will bite you)

1. **Run `hostname` at the start of every chat.** `proxmox-vm` = doc1, `doc2` = doc2, `framework` = Framework laptop, `DESKTOP-*` = Windows. You are likely already on doc1 — do NOT ssh to doc1 from doc1.
2. **Windows laptop SSH**: no native key. Extract via WSL: `wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2`, then `ssh -i ~/.ssh/id_doc2 abl030@doc2` (works for doc1 too).
3. **nixosconfig changes MUST be made on doc1** (`~/nixosconfig`; it has the Forgejo token + signing key). Edit, commit (signed), push, then deploy to doc2.
4. **Pipeline DB is PostgreSQL on doc2** (nspawn `cratedigger-db` at `10.20.0.11:5432`; DSN is in the deployed immutable runtime config). The `10.20.0.0/24` subnet is doc2-local — query via `pipeline-cli` over SSH to doc2, never raw TCP elsewhere. Request statuses: `initializing` (provisional/non-runnable), `wanted`, `downloading`, `processing`, `imported`, `unsearchable`, `replaced` (terminal audit). `processing` means one exact automation-import job owns the request, not download/transfer ownership. `unsearchable` is a reversible operator search stop, orthogonal to Bad Rip/ban-source cleanup. Import jobs: `queued`, `running`, historical-readable `recovery_required`, `completed`, `failed`; no current writer creates `recovery_required`.
5. **This is a curated collection.** Multiple editions/pressings of the same album are intentional. NEVER delete or merge duplicate albums — beets disambiguates them into separate folders.
6. **The pipeline self-heals — the acquisition request is authority; pipeline work is derived.** Identity actions supersede rather than mutate it (Replace: old row → `replaced`, new row links via `replaces_request_id`, next cycle rebuilds). This does not make PostgreSQL current-library authority; Beets owns installed holdings.
7. **Don't duplicate convergence — reuse the cleanup paths that already exist.** Prefer letting existing convergence (e.g. `lib/slskd_transfers.py::converge_slskd_orphans`) reap orphans over adding bespoke teardown to an action.
8. **Wildcard-all-artist-tokens stays.** `lib/search.py::wildcard_artist_tokens` wildcards EVERY artist token (bypasses Soulseek server-side artist-name bans, which are exact-string keyed). Deliberate; do not "optimize" to first-token-only.
9. **Canonical processing albums are exact media manifests** (#853/#859): trust transitions ONCE at atomic publication into `processing/albums/`, after which the owned album may be normalized in place — but its directory must contain exactly the downloaded manifest, nothing else. Control-plane artifacts (quality-evidence action sidecars) are tempfiles via `lib/evidence_action_file.py`, never written inside album dirs; `_canonical_manifest_complete` enforces exact equality and must not be weakened to allowlist our own debris. Patrolled by `tests/test_preview_manifest_generated.py`.
10. **Processing has one exact durable owner** (#898): `album_requests.status='processing'` iff `active_automation_import_job_id` names that request's active `automation_import` job. The immutable download `enqueued_at` witnesses only the atomic `downloading → processing` handoff; every later mutation is fenced by the exact owner job and a pinned `IMPORT(request_id) → RELEASE(release_id)` session. The execution lease proves liveness, never ownership. Cleanup is journaled and completed while the owner remains attached; the terminal transaction consumes that receipt, writes audit/policy/job outcome, clears owner/state, and moves to `wanted|imported` last. Never add `processing` to slskd event, transfer, ledger, or reaper status sets.
11. **Broken worlds surface and restart; nothing is parked.** On failed moves/subprocesses, ambiguous acknowledgements, DB blips, or partial cleanup: record Recents audit evidence, return the request to a runnable state, and let the next cycle re-derive truth. Never add a status/flag/`*_required` marker whose only exit is human action, return from failure with the request unsearchable, or “preserve state for investigation.” The request is authority; derived state can restart safely, including ambiguity about completed irreversible work. Retained counters and growing backoff control cadence. Authority: "we surfsce broken worlds to the user and tje jsut restsrt, the pipeline keeps moving. never parked." — https://github.com/abl030/cratedigger/issues/898#issuecomment-5124769384
12. **Evidence is observational, not atomic.** The two databases and filesystems share no transaction. Preview caches; import reauthorizes each side and fails closed before launch. See `docs/quality-verification.md`.

## Subsystems (one line + the doc that owns it)

- **Web UI** — SPA, stdlib `http.server`, vanilla JS, no build step; MB/Discogs browse toggle. `docs/webui-primer.md`.
- **Beets** — deployment owns the compatible package, immutable effective config, SQLite catalog, files, state, secrets, current identity/path/tags, and plain operator `beet`; Cratedigger owns exact acquisition requests/history/proof. Its mutation lanes are the serial JSON importer harness, the explicit exact-album delete child (Bad Rip/Replace/library-delete, and its metadata-only recovery-side crash-debris mode, #1089), and the import-time merge retag (`beet modify -a -M -W -y`, one album, identity only, never moves files, reached by both the automation and force import lanes; see Decision architecture for the exact primary-key-plus-identity selection guard, #1087, #1093), and the one-album file-tag sync (#1260, `lib/beets_tag_sync.py`: guarded `beet write` DB→file healing the retag's `-W` residual — census-card button, `pipeline-cli sync-file-tags`, best-effort merge-seam call); all other access is observational (never raw-import or `remove -d` — Critical rules 1-3). `docs/beets-primer.md`.
- **Plex / Jellyfin** — post-import scan notifiers. **Plex's refresh endpoint returns HTTP 200 for any path, including invalid ones — 200 is not evidence the scan ran.** Upgrades are kept out of "Recently Added" by pin reconcilers (Plex `addedAt`, migration 040; Jellyfin `DateCreated` incl. audio children, migration 046 — waits for the rescan to be observable, no fixed settle window). Jellyfin item identity is a hash of the path, so a path-changing upgrade mints new items: capture falls back to the replaced beets albums' old paths (`postflight.replaced_albums`), else writes a floor pin from the pipeline's own first-known date (migration 053). `docs/plex-primer.md`, `docs/jellyfin-primer.md`.
- **Mirrors** — MB mirror + Discogs mirror (Rust JSON API) + LRCLIB. `musicbrainz.apiBase` governs Cratedigger consumers; external Beets config selects its matching endpoint. Public MB is supported-but-slow; Discogs browse is mirror-required. `docs/mirrors.md`, `docs/musicbrainz-mirror.md`, `docs/discogs-mirror.md`.
- **Quality model** — quality decides imports, proof decides names, config decides formats. `docs/quality-ranks.md`, `docs/quality-verification.md`, `docs/research/spectral-calibration-findings.md`.
- **slskd client + event ingestion** — all HTTP uses `lib/slskd_client.py`; the event feed is the ONLY completed-file location authority (unstamped materialization hard-fails then self-heals to `wanted`). Delete only ledger-owned event-stamped paths with empty-dir pruning, never inferred-folder rmtree; attempt cleanup (`cancel_and_delete`) is ownership-scoped exactly as convergence is, and `local_path` implies acceptance at the schema (migration 083). **Good-citizen ownership (#571): destroy only positively proven Cratedigger state/files; a shared slskd's FOREIGN keys are safe.** Not a cross-instance boundary: a second pipeline DB on the same slskd can dual-claim a key and reap it (#1254, accepted — self-healing waste only). Accepted transfer-ledger rows authorize cancellation only for their exact `(username, filename)` queue key when no `downloading` row backs it; pending/foreign keys are never cancelled. Disk reaping after 7 days requires an event `local_path`; unowned files are never deleted. Protect `failed_imports/`, `wrong_matches/`, and active downloading paths regardless. Searches and enqueue intents are write-ahead ledgered; only accepted enqueue promotes destructive ownership, and completion events cannot promote rejected/unknown intent. One ingestion pass has ONE ownership rule (#1278): both the `active_download_state` stamp and the ledger stamp require an accepted-POST row for that exact queue key, per key rather than per attempt. Terminal `Completed,*` records are purged by confirmed queue key/current ID; transfer cleanup alone never authorizes disk deletion. Pending intents age out; accepted evidence retention exceeds the reaper threshold, so pruning makes files unowned, not reapable. `lib/slskd_events.py`, `lib/slskd_transfers.py`, `lib/slskd_searches.py`, `lib/slskd_transfer_ledger.py`.
- **Persisted search plans** — deterministic per-request plans; the executor consumes plan-items by ordinal, never recomputes. **Bump `SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` whenever generator output changes.** `docs/persisted-search-plans-rollout.md`.
- **Unfindable detection** — its own daily oneshot unit (`cratedigger-unfindable.service`), deliberately NOT in the main pipeline loop so the never-stop-searching invariant is enforceable at the systemd level. `docs/search-plan-iter2-deploy.md`.
- **Triage** — `pipeline-cli triage show/list/quarantine` + `/api/triage/*`; composes unfindable + field-resolution + search forensics and surfaces unreferenced `failed_imports/`/`wrong_matches/` folders under the slskd download dir and processing tree (`<processing_dir>/albums/`; beets-staging's pair: known unscanned gap). `quarantine` relays its API route, since the processing tree's 0700 permission blocks direct CLI reads (#1122 F1). Read-only. `docs/search-plan-iter2-deploy.md`, `docs/pipeline-db-schema.md`.
- **Wrong Matches** — a **DB queue, not a folder listing**. `PipelineDB.get_wrong_matches()` collapses `download_log` to the newest row per `(request_id, validation_result->>'failed_path')`, one row per actionable folder. **The folder path lives in the `validation_result` JSONB under `failed_path`; `download_path` and `staged_path` are NULL on these rows** — so searching those columns, or the filesystem, finds nothing and the queue looks empty when it is not. Live folders sit under `<processing_root>/albums/wrong_matches/`, NOT the same root as `Incoming/auto-import/wrong_matches` or `slskd/wrong_matches`; enumerate the queue, never the disk. Operator actions key on the **`download_log_id`** (`pipeline-cli force-import <id>`, `wrong-match-delete <id>`) or the request (`wrong-match-delete-group`, `wrong-match-converge`). Force-importing a Wrong Matches row is the fastest way to smoke-test the validation/import seam against a real album without waiting for a download. Routing is settled by issue #1077: kept iff its contributing peers are denylisted, kept implies visible in the worklist, and only an explicit four-scenario allowlist (`extra_tracks`/`high_distance`/`mbid_not_found`/`no_choose_match`) may ever reach the delete-eligible cleanup reducer — `docs/rejection-routing.md` is the full producer-by-producer table.
- **YouTube resolver + rescue ingest** — resolver maps a release ID to YT Music albums with beets distances; rescue runs yt-dlp into the existing preview→importer chain (own systemd unit for network-namespacing). `album_requests.status` is never touched by rescue code — only `mark_imported_with_rescue` writes it. `docs/plans/2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md`.
- **API discoverability** — `GET /api/_index` / `pipeline-cli routes`; every route needs a description (route-audit test enforces).

## Infrastructure

- **doc1** (`192.168.1.29`): this repo at `/home/abl030/cratedigger`; primary dev host.
- **doc2** (`192.168.1.35`): runs cratedigger (back-to-back timer, ~4-5 min/cycle) and deployment-owned Beets. Its dedicated dependencies are MusicBrainz + LRCLIB at `192.168.1.43`, Discogs at `192.168.1.44`, and the slskd microVM at `192.168.21.2`.
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB data, music library reachable from both.
- **Nix deployment**: cratedigger is a flake input (`cratedigger-src`) in `~/nixosconfig/flake.nix`; downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports `nixosModules.default`. `docs/nixos-module.md`.

### Key paths

| Path | Machine | Purpose |
|------|---------|---------|
| `10.20.0.11:5432/cratedigger` | doc2 nspawn | Pipeline DB (PostgreSQL) |
| `/mnt/virtio/cratedigger/beets-db/beets-library.db` | shared | Beets library DB, journals, import log, and harness audit |
| `/mnt/virtio/Music/Beets` | shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | shared | Staging root (`auto-import/` requests, `post-validation/` manual review) |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/mnt/virtio/cratedigger/processing/albums` | shared | Canonical processing albums; its `wrong_matches/` subdir holds the live Wrong Matches folders (several other `wrong_matches/` dirs exist elsewhere and are NOT the queue) |
| `/var/lib/cratedigger` | doc2 | Mutable runtime state (lock, denylists, processing metadata) |
| deployment Nix-store `BEETSDIR` | doc1/doc2 | Immutable external Beets config |
| `/var/lib/beets/state.pickle` | each host | External host-local Beets importer/operator state |

### Accessing doc2

```bash
ssh doc2 'sudo journalctl -u cratedigger --since "5 min ago"'
ssh doc2 'sudo systemctl start cratedigger --no-block'        # ALWAYS --no-block (oneshot blocks for minutes)
ssh doc2 'sudo systemctl show cratedigger.service -p ExecStart --no-pager'
```

Never background systemctl with `&` inside SSH quotes — SSH waits on all children anyway.

#### Querying the pipeline DB (do this, in this order)

1. **Run the query ON doc2** (`pipeline-cli` is the writable operator/agent control plane). Raw `pipeline-cli query` is read-only by default; intentional raw SQL writes require `pipeline-cli query --write --confirm WRITE -`. That is a safety/intent boundary, not authentication: the operator connection is otherwise full-privilege. Routine actions belong in typed `pipeline-cli` subcommands, not raw SQL. See `docs/debugging-cli.md` for the authoritative command and access reference.
2. **Pull the live schema first — never guess column names** (query `information_schema.columns`; the schema is deliberately not transcribed here).
3. Then write your query.

Gotchas that cost a lot of time once:
- The pgpass secret is **env-format** (`PGPASSWORD=...`) — extract with `grep '^PGPASSWORD=' | cut -d= -f2`, not `cut -d:`.
- **Pass SQL via stdin heredoc, not argv** — `$$` dollar-quoting expands to the shell PID in argv.

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query -' <<'SQL'
SELECT column_name FROM information_schema.columns
WHERE table_name = 'album_requests' ORDER BY ordinal_position;
SQL
```

### Web dev server

`scripts/web_dev_server.py`: `--data live-db` (real read-only PG + local routes) or `--data prod-api` (local frontend, proxied API). Wrong Matches needs `live-db` on a host that sees the rejected folders (doc1/doc2). Full remote-dev flow in `docs/web-dev-server.md`.

## Repository layout

`ls` shows the tree. The two non-obvious directories: `docs/solutions/` holds
compounding lessons (grep it when debugging), and `.claude/rules/` holds the
shared rules (Claude auto-loads them; Codex reads as directed below).

`lib/config.py`/`lib/context.py` hold the typed `CratediggerConfig`/`CratediggerContext` — never construct a partial config; always `CratediggerConfig.from_ini()`.

## Pipeline flow

```
Web UI / CLI → PostgreSQL (wanted → downloading → processing → imported; wanted ↔ unsearchable)
   Phase 1: poll_active_downloads()   Phase 2: get_wanted() → search + enqueue
   completed download → validate vs exact release ID (dist ≤ 0.15)
   source=request    → keep exact processing owner path → import_one.py (spectral → convert → quality gate) → /Beets
   force import      → same path, Beets distance overridden (validation + merge redirect included)
   local import      → operator path copied into private scratch, same path, strict distance (rejects to Wrong Matches)
   source=redownload → stage /Incoming/post-validation (manual review only, never auto-imported)
```

**Don't assume a path under `/Incoming` is a redownload** — YouTube rescues
still enter through `/Incoming/auto-import`. Schema fields, JSONB audit blobs,
and the force-import flow: `docs/pipeline-db-schema.md`.

## CLI ⇄ API surface symmetry

Every operator action lives on **both** `pipeline-cli` and the web API through exactly one canonical execution path: normally both are thin adapters over one service; an existing canonical web mutation route may instead be called by a thin CLI HTTP adapter. Either shape preserves matched exit-code/status-code mappings and never gains a duplicate direct-DB fallback. Full pattern table in `.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry" (always loaded).

## Decision architecture

**Quality decisions live in ONE place** — `full_pipeline_decision_from_evidence` in `lib/quality/pipeline.py` (simulator twin `full_pipeline_decision`; the `lib/quality/` package is split by concern per issue #477, `__init__.py` re-exports the names callers ask for by that path, trimmed to measured demand and held there by `tests/test_quality_reexport_audit.py`; import anything else from its submodule). Preview measures and persists evidence; the importer reads evidence and decides. Never re-create an import decision elsewhere or add a narrower check upstream — full rules in `.claude/rules/code-quality.md` (always loaded). Evidence addressing/propagation policy (content-addressed rows, lossless-source-gated propagation to library rows): `docs/quality-verification.md` § "Evidence addressing, propagation, and ownership".

The importer queue is the automatic Beets-mutation boundary: web/CLI/poller enqueue; `cratedigger-importer` drains serially under an advisory singleton lock. A request-backed automation job owns its request for the complete `processing` lifetime. Any world failure, including a launched-but-unacknowledged child, records audit evidence and returns the request to `wanted` for re-derivation. The other Cratedigger mutation lanes are the explicit exact-album delete child (destructive/operator-authorized, or metadata-only recovery-side crash-debris, #1089) and the import-time MusicBrainz merge retag (`beet modify -a -M -W -y`, pinned by the guard-resolved primary key AND the identity value, identity only, never moves files, both `RELEASE` locks held, under whichever exact import claim the caller took — the automation processing owner or a claimed `force_import` job, #1080 — `lib/beets_retag.py`; #1087 replaced `beet mbsync -M`, which cannot follow a release-only merge; #1093 unified the query's selection with the guard's own read and closed a TOCTOU race) and the one-album file-tag sync (#1260 — `lib/beets_tag_sync.py`, tags only, never chooses an identity, verdict from re-read files, outcome-inert at the seam); add no fifth lane. Authority: "this was supposed to be just at import time. we go, oh, re=direct. re-tag and then import." — https://github.com/abl030/cratedigger/issues/1059; lane 4: issue #1260 (operator decision quoted there). Plain operator `beet` remains external authority and is not serialized by Cratedigger.

Wire-boundary types (harness, JSONB, subprocess stdout) are `msgspec.Struct`, not `@dataclass` — `.claude/rules/code-quality.md` § "Wire-boundary types".

## Deploying changes

Push cratedigger (GitHub) → pin nixosconfig's `cratedigger-src` input to that exact revision on doc1 (`scripts/pin_nixosconfig.sh`, an `--override-input` pin — never a bare `nix flake update`, which only follows the input's branch tip) → signed commit + push nixosconfig to **Forgejo** (`git.ablz.au`; GitHub nixosconfig is a frozen fallback) → from doc1 run `fleet-deploy doc2` through the locked-sibling trigger, then poll and verify the exact fleet anchor. `cratedigger.service` has `restartIfChanged = false` (the timer picks up new code next cycle); web/migrate restart on switch. Before `nix/module.nix` changes, run `nix build .#checks.x86_64-linux.moduleVm`. Full sequence + verification in `.claude/rules/deploy.md`; the `deploy` skill runs it end-to-end.

**PR merges: use GitHub "Create a merge commit"** — never rebase- or squash-merge.

## Database migrations

Schema lives in `migrations/NNN_name.sql`; the migrate oneshot runs them on every switch. Add a numbered SQL file — no manual psql, **never** edit a shipped migration, **never** add DDL inside `PipelineDB` methods. The unit-ordering contract (why `cratedigger`/`cratedigger-unfindable` only `wants`+`after` the migrate unit and gate on `assert_schema_current` instead) and the full workflow are in `.claude/rules/deploy.md` § "Database migrations".

## Running tests

Always use `nix-shell --run` for Python:

```bash
bash scripts/test.sh tests.test_X                       # explicit target + adjacent/ambient gates
bash scripts/test.sh                                    # derive targets from the current diff
nix-shell --run "python3 -m unittest tests.test_X -v"    # isolated test debugging only
nix-shell --run "bash scripts/run_tests.sh"              # exhaustive suite
```

`scripts/test.sh` is normal development validation; direct `unittest` is for
debugging an isolated failure, never local convergence evidence; the `check`
skill owns the one receipt-backed canonical-suite confirmation before
independent-review handoff.

The complete operational contract — selection rules and the exit-code-2 refusal,
suite bundle, the `check` receipt, the shared-tmpfs admission lock (#1111),
specialized evidence, generated/fuzz testing, no-skips policy, and hooks — is in
`.claude/rules/code-quality.md` § "Test execution, evidence, and hooks", which
is always loaded alongside this file. Do not restate it here.

## Shared AI surfaces

One authored source exists for each concept; client-specific formats are adapters:

- Instructions: `CLAUDE.md`; `AGENTS.md` is its symlink.
- Skills: `.claude/skills/`; `.agents/skills` is the Codex discovery symlink.
- Shared rules: `.claude/rules/`; Claude auto-loads them and Codex follows the
  loading rule below.
- Specialist agents: `.claude/agents/*.md`; `.codex/agents/*.toml` is generated.
- Project MCP: `.mcp.json`; `.codex/config.toml` is generated.
- Durable learning: `.claude/memory/`, `docs/`, and GitHub issues/PRs.

After editing an agent or `.mcp.json`, run:

```bash
nix-shell --run "python3 tools/generate-ai-adapters.py"
nix-shell --run "python3 tools/generate-ai-adapters.py --check"
```

Never edit generated `.codex/agents/*.toml` or `.codex/config.toml` directly.
Author skills in the common `SKILL.md` format and keep platform-specific tool
names out of workflows where a normal shell/read/edit instruction suffices.

Claude auto-memory and Codex native memory are client-local recall caches, not
project truth. Promote durable discoveries to the shared memory index, docs, or
issue/PR surfaces so either client can recover them. Do not duplicate rationale
across client-local memory stores.

### Shared skills

`deploy` (full deploy sequence) · `debug-download` (live audit trail) · `check`
(receipt-backed canonical suite) · `beets-docs` (pinned upstream reference) ·
`orchestrate-issue` (isolated multi-PR implementation/review/deploy loop) ·
`meta-orchestrate` (register-scale parallel agent dispatch) · `unslop` (cut AI
tells from prose)

### Shared rule loading

Both clients must follow `code-quality.md`, `deploy.md`, `scope.md`, and
`test-fidelity.md` for repository work. Also read the matching path-scoped rule
before touching its surface: `nix-shell.md` (`*.py`, tests, shell), `harness.md`
(`harness/`, `lib/beets.py`, `lib/quality/`), `web.md` (`web/`), and
`pipeline-db.md` (pipeline DB, CLI DB code, migrations). The YAML `paths` lists
inside those files are authoritative.

## Playwright MCP

Browser automation for `music.ablz.au` is authored in the tracked `.mcp.json`;
Codex consumes its generated adapter. Always use HTTPS (HTTP times out).
`docs/playwright-mcp.md`.

## Hunting production bugs — generated-first (the house method)

**Production bugs are hunted with generated tests, not log-trawling** — write the invariant down, drive the REAL code path over generated worlds, let Hypothesis find and shrink the reproduction, ship RED → fix → GREEN in one PR with the shrunk world pinned forever. Test infrastructure is deterministic-only. Proven on #550. Full workflow: `.claude/rules/code-quality.md` § "Production Bug Hunting — Generated-First" + `docs/generated-testing.md`.

For quality-decision bugs the simulator is the tool within the method: `pipeline-cli show / quality / debug-download / search-plan show / query` are the diagnostic entry points; add the failing scenario to the album test set and verify against real albums in the live DB. Command reference + triage signals in `docs/debugging-cli.md`.

## Finding dead code

`nix-shell --run "bash scripts/run_ruff.sh"` checks every Python surface with
the exact Ruff version/config; `nix-shell --run "bash scripts/find_dead_code.sh"`
runs the production-only aggregate Vulture sweep against
`tools/vulture/whitelist.py`. After deleting, regenerate the Vulture whitelist
and watch for **cascading orphans** (deleting one helper exposes its callees).
Full workflow: `docs/dead-code.md`.

## Critical rules

1. **NEVER `beet remove -d`** — deletes files permanently. Explicit Bad Rip, Replace, library-delete, and recovery-side crash-debris-removal (#1089, metadata-only, never touches a file) actions use the admitted exact-album child in `lib/beets_delete.py`; selector-based deletion is retired.
2. **NEVER import without inspecting the match** — always through the harness.
3. **NEVER match by `candidate_index`** — always by MB release ID.
4. **NEVER match by release group** — release groups conflate pressings.
5. **Auto-import only for `source='request'`** — redownloads always stage for manual review.
6. **All code deploys via Nix** — no manual `cp` to virtiofs.
7. **PostgreSQL uses `autocommit=True`** — prevents idle-in-transaction deadlocks; migrations use separate short-lived connections with `lock_timeout`.

## Resolved — canonical RCs (don't re-investigate)

Settled; read the solution doc instead of reopening.

- **Palo Santo data loss** (2026-04-20) — misplaced `duplicate_keys` (top-level, silently ignored) let a cross-MBID sibling be wiped. NOT a beets bug. `docs/solutions/runtime-errors/palo-santo-duplicate-keys-data-loss.md`.
- **Lucksmiths MBID drift** (2026-04-14) — deliberate out-of-band retag; NOT a bug. `docs/solutions/runtime-errors/lucksmiths-mbid-drift-out-of-band-harness.md`.
- **asciify_paths Plex mass-split** (2026-05-18) — path-affecting beets change + `beet move` split 1,178 Plex albums; fix is the Plex merge API. **Footgun: any beets change that mutates rendered paths + `beet move` re-triggers this.** `docs/solutions/runtime-errors/plex-asciify-paths-album-split.md`.

## Secrets

- slskd API key + notifier creds: sops-managed `*File` paths (issue #117 pattern), materialized by the wrapper's secrets-split oneshot. See `docs/nixos-module.md`.
- Beets Discogs token: deployment-owned scalar secret, encoded at runtime into the designated token-only include as `root:cratedigger-ops 0440`; never stored in the Nix closure or Cratedigger output. `docs/beets-primer.md`.
