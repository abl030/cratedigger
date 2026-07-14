# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz/Discogs via beets, auto-imports with spectral quality verification, or stages for manual review. Web UI at `music.ablz.au`. Originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr); long since its own project — the pipeline DB is the sole source of truth, the web UI is the album picker.

## Session start

Before doing anything else, silently run `hostname` and `date`. Then read
`.claude/memory/MEMORY.md` unless the client already injected it. This establishes
the current machine, time, and shared cross-agent memory.

Do not use Compound Engineering (`ce-*`, `compound-engineering:*`, or `lfg`) in
this repository. Native agent planning, implementation, debugging, and review
are sufficient.

## Why this exists — the archivist frame

Cratedigger is a **music archival tool first, an acquisition pipeline second**. The operator is an archivist: most of the long-tail music here is genuinely vanishing — niche pressings, Australian indie, demos that lived on one peer who logged off years ago. This frame is load-bearing; these invariants flow from it:

- **Strict pressing identity.** A request points at a specific MB release MBID or Discogs release ID. The matcher NEVER substitutes a sibling pressing. Different pressings ARE different releases.
- **The system never stops searching.** Cadence is constant forever; never auto-throttled based on apparent unfindability. Surfacing the unfindable cohort is right; throttling search on it is a product violation.
- **The system never auto-decides anything irreversible.** Surfacing is fine; replace/remove/accept-different-pressing decisions belong to the operator.
- **No adapter code between MB and Discogs.** Both feed the same columns in the same shape.
- **Long-tail rescue is a celebrated event** — `rescued_at` + `prior_unfindable_category` on `album_requests` are first-class audit data.
- **Single-operator, no backwards-compat.** One user, forward-only migrations, no compat shims, no committed backfill scripts, no one-shot machinery kept "in case". Full rules in `.claude/rules/scope.md` (always loaded).

If a design drifts toward "good enough" matches, "smart" defaults, or auto-throttling — that drift is a bug. Push back.

## Critical invariants (read first — these will bite you)

1. **Run `hostname` at the start of every chat.** `proxmox-vm` = doc1, `doc2` = doc2, `framework` = Framework laptop, `DESKTOP-*` = Windows. You are likely already on doc1 — do NOT ssh to doc1 from doc1.
2. **Windows laptop SSH**: no native key. Extract via WSL: `wsl -d NixOS -- bash -c 'cat /run/secrets/ssh_key_abl030' > ~/.ssh/id_doc2 && chmod 600 ~/.ssh/id_doc2`, then `ssh -i ~/.ssh/id_doc2 abl030@doc2` (works for doc1 too).
3. **nixosconfig changes MUST be made on doc1** (`~/nixosconfig`; it has the Forgejo token + signing key). Edit, commit (signed), push, then deploy to doc2.
4. **Pipeline DB is PostgreSQL on doc2** (nspawn `cratedigger-db` at `10.20.0.11:5432`; DSN in `/var/lib/cratedigger/config.ini`). The `10.20.0.0/24` subnet is doc2-local — query via `pipeline-cli` over SSH to doc2, never raw TCP from elsewhere. Request statuses: `wanted`, `downloading`, `imported`, `manual`, `replaced` (terminal, frozen audit). Import queue: `queued`, `running`, `completed`, `failed`.
5. **This is a curated collection.** Multiple editions/pressings of the same album are intentional. NEVER delete or merge duplicate albums — beets disambiguates them into separate folders.
6. **The pipeline self-heals — the request is the source of truth, everything else is derived.** Operator actions that touch identity supersede the row rather than mutate it (canonical example: Replace, `lib/mbid_replace_service.py` — old row flips to `replaced`, new row points back via `replaces_request_id`, next cycle rebuilds).
7. **Don't duplicate convergence — reuse the cleanup paths that already exist.** Prefer letting existing convergence (e.g. `lib/slskd_transfers.py::converge_slskd_orphans`) reap orphans over adding bespoke teardown to an action.
8. **Wildcard-all-artist-tokens stays.** `lib/search.py::wildcard_artist_tokens` wildcards EVERY artist token (bypasses Soulseek server-side artist-name bans, which are exact-string keyed). Deliberate; do not "optimize" to first-token-only.

## Subsystems (one line + the doc that owns it)

- **Web UI** — SPA, stdlib `http.server`, vanilla JS, no build step; MB/Discogs browse toggle. `docs/webui-primer.md`.
- **Beets** — cratedigger-owned end-to-end: pinned package (`nix/beets.nix`), module-rendered config at `${stateDir}/beets` (BEETSDIR), `cratedigger-beet` (run with sudo) for manual ops. **The shipped beets is the ONLY thing that may touch the library** — a foreign beets version/config against the same library DB risks schema migration and path rewrites (the beets-2.12 and asciify_paths incidents). All automated imports go through the JSON harness — never raw `beet import`. `musicbrainz` MUST be in the plugins list (else 0 candidates); always match by `candidate_id`, never `candidate_index`. Runs as a non-root service user with a setgid group-`users` library (media-server-readable art via the `permissions` plugin, issue #570). `docs/beets-primer.md`.
- **Plex / Jellyfin** — post-import scan notifiers. **Plex's refresh endpoint returns HTTP 200 for any path, including invalid ones — 200 is not evidence the scan ran.** Upgrades are kept out of "Recently Added" by pin reconcilers (Plex `addedAt`, migration 040; Jellyfin `DateCreated` incl. audio children, migration 046 — waits for the rescan to be observable, no fixed settle window). `docs/plex-primer.md`, `docs/jellyfin-primer.md`.
- **Mirrors** — MB mirror + Discogs mirror (Rust JSON API) + LRCLIB, all optional config (`musicbrainz.apiBase` is ONE value threaded to web, CLI, and beets; public MB is the supported-but-slow fallback; Discogs browse is mirror-required). `docs/mirrors.md`, `docs/musicbrainz-mirror.md`, `docs/discogs-mirror.md`.
- **Quality model** — codec-aware rank comparison; gate vs `cfg.quality_ranks.gate_min_rank`. `docs/quality-ranks.md`, `docs/quality-verification.md`.
- **slskd client + event ingestion** — all slskd HTTP via the in-repo typed client `lib/slskd_client.py`. Event ingestion (`lib/slskd_events.py`) stamps completed-file locations from the slskd events feed; **the event is the ONLY source of completed-file locations** (unstamped file at materialize = hard failure, then self-heal to `wanted`). Deletions are event-driven paths + empty-dir pruning — never an inferred-folder rmtree. **Good-citizen ownership (#571, reversing #550's own-the-instance doctrine): cratedigger only destroys slskd state/files it can positively prove it created — a shared instance is safe.** `converge_slskd_orphans` cancels STRAY transfers only when an accepted ledger row confirms Cratedigger created that `(username, filename)` queue key and no `downloading` row still backs it; pending intents and foreign keys are NEVER cancelled, whatever their state or age. `reap_disk_orphans` (Phase 0b, `lib/slskd_transfers.py`) deletes files older than 7 days that it can positively prove it created — good-citizen doctrine (#571 PR 4): an event-stamped `local_path`, or under a canonical folder derived from an accepted ledger row's `attempt_fingerprint` (past OR present attempts). A file with no positive ownership signal is NEVER deleted however old (pre-#571 debris from before migration 045 shipped included — an operator one-shot clears that at deploy, not the reaper's job going forward); the `failed_imports/` quarantine tree (cratedigger's own tree by construction, but protected — never reap-eligible) and a currently-`downloading` row's active canonical folder/stamped paths stay protected regardless of ledger ownership or age. Never park files there. Searches cratedigger submits are write-ahead ledgered (migration 044) and reaped by `converge_slskd_searches` (Phase 0c, `lib/slskd_searches.py`); unledgered searches are never touched (#576). Every slskd file Cratedigger enqueues is likewise write-ahead ledgered by `(username, filename)` before the POST (migration 045, `slskd_enqueue_with_outcome` in `lib/slskd_transfers.py`), but that intent becomes destructive ownership only after slskd accepts the POST; completion events add paths only to already-confirmed rows and never promote rejected/unknown intent. slskd may assign fresh transfer IDs while retrying that queue entry, so the end-of-cycle purge removes every terminal `Completed,*` record with a confirmed queue key using the record's current ID and `remove=true`; pending, foreign, and nonterminal rows are untouched. Transfer cleanup does not authorize disk deletion: only the event-stamped `local_path` or another positive file-ownership signal does. Migration 051 adds the acceptance boundary and removes the obsolete attempt-ID and terminal-stamp columns. Phase 0d (`lib/slskd_transfer_ledger.py`) prunes ledger rows once old AND their request is inactive (retention strictly exceeds the reaper's age threshold — a pruned row's file becomes unowned and is never reaped after that).
- **Persisted search plans** — deterministic per-request plans; the executor consumes plan-items by ordinal, never recomputes. **Bump `SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` whenever generator output changes.** `docs/persisted-search-plans-rollout.md`.
- **Unfindable detection** — its own daily oneshot unit (`cratedigger-unfindable.service`), deliberately NOT in the main pipeline loop so the never-stop-searching invariant is enforceable at the systemd level. `docs/search-plan-iter2-deploy.md`.
- **Triage** — `pipeline-cli triage show/list/quarantine` + `/api/triage/*`; composes unfindable + field-resolution + search forensics and surfaces unreferenced immediate `failed_imports/` album folders. Read-only. `docs/search-plan-iter2-deploy.md`, `docs/pipeline-db-schema.md`.
- **YouTube resolver + rescue ingest** — resolver maps a release ID to YT Music albums with beets distances; rescue runs yt-dlp into the existing preview→importer chain (own systemd unit for network-namespacing). `album_requests.status` is never touched by rescue code — only `mark_imported_with_rescue` writes it. `docs/plans/2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md`.
- **API discoverability** — `GET /api/_index` / `pipeline-cli routes`; every route needs a description (route-audit test enforces).

## Infrastructure

- **doc1** (`192.168.1.29`): this repo at `/home/abl030/cratedigger`; primary dev host.
- **doc2** (`192.168.1.35`): runs cratedigger (systemd oneshot; the timer starts the next cycle 1s after the last completes — `OnUnitInactiveSec`, cycles run back-to-back, ~4-5 min each) + module-owned beets, MB mirror (`:5200`), slskd (`:5030`).
- **Shared storage**: `/mnt/virtio` (virtiofs) — beets DB, pipeline DB data, music library reachable from both.
- **Nix deployment**: cratedigger is a flake input (`cratedigger-src`) in `~/nixosconfig/flake.nix`; downstream wrapper at `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports `nixosModules.default`. `docs/nixos-module.md`.

### Key paths

| Path | Machine | Purpose |
|------|---------|---------|
| `10.20.0.11:5432/cratedigger` | doc2 nspawn | Pipeline DB (PostgreSQL) |
| `/mnt/virtio/Music/beets-library.db` | shared | Beets library DB |
| `/mnt/virtio/Music/Beets` | shared | Beets library (tagged files) |
| `/mnt/virtio/Music/Incoming` | shared | Staging root (`auto-import/` requests, `post-validation/` manual review) |
| `/mnt/virtio/music/slskd` | doc2 | slskd download directory |
| `/var/lib/cratedigger` | doc2 | Runtime state (config.ini, lock, denylists) |
| `/var/lib/cratedigger/beets` | doc2 | BEETSDIR — module-rendered beets config.yaml (+ secrets.yaml) |

### Accessing doc2

```bash
ssh doc2 'sudo journalctl -u cratedigger --since "5 min ago"'
ssh doc2 'sudo systemctl start cratedigger --no-block'        # ALWAYS --no-block (oneshot blocks for minutes)
ssh doc2 'sudo cat /var/lib/cratedigger/config.ini'
```

Never background systemctl with `&` inside SSH quotes — SSH waits on all children anyway.

#### Querying the pipeline DB (do this, in this order)

1. **Run the query ON doc2** (`pipeline-cli` is on its PATH). For **write** SQL, `pipeline-cli query` won't work (read-only session) — use `psql "postgresql://cratedigger@10.20.0.11:5432/cratedigger"` on doc2 with `PGPASSWORD` exported.
2. **Pull the live schema first — never guess column names** (query `information_schema.columns`; the schema is deliberately not transcribed here).
3. Then write your query.

Gotchas that cost a lot of time once:
- The pgpass secret is **env-format** (`PGPASSWORD=...`) — extract with `grep '^PGPASSWORD=' | cut -d= -f2`, not `cut -d:`.
- **Pass SQL via stdin heredoc, not argv** — `$$` dollar-quoting expands to the shell PID in argv.

```bash
ssh doc2 'export PGPASSWORD=$(sudo cat /run/secrets/cratedigger-pgpass | grep "^PGPASSWORD=" | cut -d= -f2); pipeline-cli query "$(cat)"' <<'SQL'
SELECT column_name FROM information_schema.columns
WHERE table_name = 'album_requests' ORDER BY ordinal_position;
SQL
```

### Web dev server

`scripts/web_dev_server.py`: `--data live-db` (real read-only PG + local routes) or `--data prod-api` (local frontend, proxied API). Wrong Matches needs `live-db` on a host that sees the rejected folders (doc1/doc2). Full remote-dev flow in `docs/web-dev-server.md`.

## Repository layout

```
cratedigger.py    — Main loop + thin wrappers; delegates to lib/
album_source.py   — AlbumRecord, DatabaseSource abstraction
web/              — Web UI (server.py, routes/, mb.py, discogs.py, js/)
lib/              — Pipeline modules (quality/ package = pure decisions, split by concern; pipeline_db.py = PG CRUD + advisory locks)
harness/          — beets_harness.py (JSON protocol), import_one.py
migrations/       — Versioned SQL (NNN_name.sql), run by lib/migrator.py
scripts/          — pipeline_cli/ (operator CLI package, split by command family) + dev/ops scripts
tests/            — shared infra in fakes.py + helpers.py
nix/              — package.nix, beets.nix, shell.nix, module.nix, VM check
examples/         — sample consumer + mirror NixOS configs
docs/             — subsystem docs; docs/solutions/ = compounding lessons (grep when debugging)
.claude/rules/    — shared rules (Claude auto-loads; Codex reads as directed below)
```

`lib/config.py`/`lib/context.py` hold the typed `CratediggerConfig`/`CratediggerContext` — never construct a partial config; always `CratediggerConfig.from_ini()`.

## Pipeline flow

```
Web UI / CLI → PostgreSQL (wanted → downloading → imported | manual)
   Phase 1: poll_active_downloads()   Phase 2: get_wanted() → search + enqueue
   completed download → validate vs exact release ID (dist ≤ 0.15)
   source=request    → stage /Incoming/auto-import  → import_one.py (spectral → convert → quality gate) → /Beets
   source=redownload → stage /Incoming/post-validation (manual review only, never auto-imported)
```

**Don't assume a path under `/Incoming` is a redownload** — request imports can be mid-move or mid-import there too. Schema fields, JSONB audit blobs, and the force-import flow: `docs/pipeline-db-schema.md`.

## CLI ⇄ API surface symmetry

Every operator action lives on **both** `pipeline-cli` and the web API, wrapping the same service-layer method; the two are thin adapters with matched exit-code/status-code mappings. Full pattern table in `.claude/rules/code-quality.md` § "CLI ⇄ API Surface Symmetry" (always loaded).

## Decision architecture

**Quality decisions live in ONE place** — `full_pipeline_decision_from_evidence` in `lib/quality/pipeline.py` (simulator twin `full_pipeline_decision`; the `lib/quality/` package is split by concern per issue #477, `__init__.py` re-exports the full historical surface so `from lib.quality import X` still works everywhere). Preview measures and persists evidence; the importer reads evidence and decides. Never re-create an import decision elsewhere or add a narrower check upstream — full rules in `.claude/rules/code-quality.md` (always loaded). Evidence addressing/propagation policy (content-addressed rows, lossless-source-gated propagation to library rows): `docs/quality-verification.md` § "Evidence addressing, propagation, and ownership".

The importer queue is the beets-mutating ownership boundary: web/CLI/poller enqueue; `cratedigger-importer` drains serially under an advisory singleton lock (startup requeues any `running` job). No new direct beets-mutating entry points outside the importer worker.

Wire-boundary types (harness, JSONB, subprocess stdout) are `msgspec.Struct`, not `@dataclass` — `.claude/rules/code-quality.md` § "Wire-boundary types".

## Deploying changes

Push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → signed commit + push nixosconfig to **Forgejo** (`git.ablz.au` — GitHub nixosconfig is a frozen fallback, never deploy from it) → from doc1 run `fleet-deploy doc2` through the locked-sibling trigger, then poll and verify the exact fleet anchor. `cratedigger.service` has `restartIfChanged = false` (the back-to-back timer picks up new code on the next cycle); web/migrate restart on switch. Before `nix/module.nix` changes, run `nix build .#checks.x86_64-linux.moduleVm`. Full sequence + verification in `.claude/rules/deploy.md`; the `deploy` skill runs it end-to-end.

**PR merges: use GitHub "Create a merge commit"** — never rebase- or squash-merge.

## Database migrations

Schema lives in `migrations/NNN_name.sql`; the migrate oneshot runs them on every switch. `cratedigger-web` and the other long-running workers `requires` the migrate unit and start after it; `cratedigger` and `cratedigger-unfindable` are timer-driven (`restartIfChanged = false`) so they only `wants`+`after` it — a `requires` edge would let the migrate unit's every-deploy restart SIGTERM a mid-flight cycle — and instead gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`). Add a numbered SQL file — no manual psql, **never** edit a shipped migration, **never** add DDL inside `PipelineDB` methods. Full workflow in `.claude/rules/deploy.md`.

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"    # prints its unique artifact directory
export ARTIFACT=/tmp/cratedigger-...            # copy the directory printed above
grep "^FAIL\|^ERROR" "$ARTIFACT/output.log"
nix-shell --run 'python3 scripts/test_artifact.py verify --artifact \
  "$ARTIFACT" --expected-head "$(git rev-parse HEAD)"'
nix-shell --run "python3 -m unittest tests.test_X -v"
```

Export `ARTIFACT` as the directory printed at the start and completion of that
invocation. It contains the full gate stream in `output.log` and exact
worktree/HEAD/count provenance plus the completed output's byte count and
SHA-256 in `summary.json`; there is deliberately no global "latest output"
alias. The verifier only accepts a completed green run that started and ended
clean at the expected HEAD and whose output still matches that integrity
record. To cite it during a push, run
`CRATEDIGGER_TEST_ARTIFACT="$ARTIFACT" git push`; the hook checks it against every
distinct peeled target commit before running its other gates.

**ALWAYS `nix-shell --run` for Python** (`.claude/rules/nix-shell.md`). **Never re-run the full suite just to grep differently** — read the printed artifact's `output.log`. The suite gates: JS syntax + JS tests, Ruff's source-local `F401`/`F811` import check, the aggregate vulture sweep, then unittest discovery — which includes `tests/test_docs_audit.py`, so the suite **fails if a new beets plugin, module option, or `pipeline-cli` subcommand ships undocumented** (or a doc link goes dead); docs are part of "done". `.claude/rules/code-quality.md` covers the test taxonomy, shared fakes/builders, the new-work checklist, and the docs-freshness rule.

**Generated (property-based) tests** (`tests/test_*_generated.py`, Hypothesis) run deterministically in the suite; after changing quality policy, run the randomized fuzz burst: `CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz` on those modules. Failures shrink to minimal worlds — promote them to named `@example` pins or album-test-set scenarios, never JSON artifacts. **New features start by writing their invariants down, and every invariant ships as a PAIR — deterministic pin + generated property — in the same PR, with known-bad self-tests** (`.claude/rules/code-quality.md` § Red/Green TDD). When in doubt that the harness constrains anything, qualify it by fault injection. `docs/generated-testing.md`.

### Skipped tests are an anti-pattern

**A test either runs or it doesn't exist.** No skip decorators, no env-gated tests, no "fixtures must be generated first" — every test runs on every `run_tests.sh` in a fresh dev shell. A skipped test is either irrelevant (delete it) or mis-designed (make it run: Nix-provided binaries, synthetic fixtures in `setUp`, or fakes). `tests/test_skip_audit.py` fails the suite on any skip; there is no allowlist. (History: the suite once said `OK (skipped=56)` for months while 56 tests had never executed once.)

### Hooks

- Pre-commit (`ln -sf ../../scripts/pre-commit .git/hooks/pre-commit`): pyright on staged `.py`.
- Pre-push (`git config core.hooksPath scripts`, once per clone): the relative tracked hooks path makes every linked worktree invoke its own `scripts/pre-push`; when `CRATEDIGGER_TEST_ARTIFACT` is set it first verifies that exact suite artifact against every pushed commit, then runs the randomized generated-test burst (push profile, fresh entropy each push — `docs/generated-testing.md`) and `nix flake check` (VM boot gate + eval guards + CLI bundle). Escape hatch: `git push --no-verify`.
- **Tag convention:** `vYYYY.MM.DD` (suffix `-N`) cut AFTER live verification on doc2.

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
(pyright + suite) · `beets-docs` (pinned upstream reference) ·
`orchestrate-issue` (isolated multi-PR implementation/review/deploy loop)

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

## Hunting bugs — generated-first (the house method)

**Bugs are hunted with generated tests, not log-trawling.** Write down the invariant the symptom violates, probe the cheapest suspicious seam with real production functions, then build a generated harness (`tests/test_*_generated.py`) that drives the REAL code path over generated worlds and let Hypothesis find + shrink the reproduction — RED → fix → GREEN in one PR, with the shrunk world pinned forever. Proven on #550: a live bug that static analysis and disk forensics could not reproduce fell to this method in one session. Full workflow: `.claude/rules/code-quality.md` § "Bug Hunting — Generated-First" + `docs/generated-testing.md`.

For quality-decision bugs the simulator is the tool within the method: `pipeline-cli show / quality / debug-download / search-plan show / query` are the diagnostic entry points; add the failing scenario to the album test set and verify against real albums in the live DB. Command reference + triage signals in `docs/debugging-cli.md`.

## Finding dead code

`nix-shell --run "bash scripts/find_dead_code.sh"` (source-local Ruff `F401`/`F811`, then aggregate vulture vs `tools/vulture/whitelist.py`). After deleting, regenerate the vulture whitelist and watch for **cascading orphans** (deleting one helper exposes its callees). Full workflow: `docs/dead-code.md`.

## Critical rules

1. **NEVER `beet remove -d`** — deletes files permanently (exceptions: ban-source and Replace, both explicit operator actions via `lib/release_cleanup.py::remove_and_reset_release`).
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
- Discogs token: `/var/lib/cratedigger/secrets/discogs-token` on doc2 (`root:cratedigger-ops 0440` — group-readable so the non-root cratedigger service can read it, via a durable one-time `chown` since tmpfiles can't manage it across the state-dir's non-root ownership transition), rendered into the beets `secrets.yaml` by the module's preStart.
