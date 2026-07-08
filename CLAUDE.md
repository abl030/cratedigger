# Cratedigger — Music Acquisition Pipeline

A quality-obsessed music acquisition pipeline. Searches Soulseek via slskd, validates downloads against MusicBrainz/Discogs via beets, auto-imports with spectral quality verification, or stages for manual review. Web UI at `music.ablz.au`. Originally inspired by [mrusse/soularr](https://github.com/mrusse/soularr); long since its own project — the pipeline DB is the sole source of truth, the web UI is the album picker.

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
- **Beets** — cratedigger-owned end-to-end: pinned package (`nix/beets.nix`), module-rendered config at `${stateDir}/beets` (BEETSDIR), `cratedigger-beet` (run with sudo) for manual ops. All automated imports go through the JSON harness — never raw `beet import`. `musicbrainz` MUST be in the plugins list (else 0 candidates); always match by `candidate_id`, never `candidate_index`. `docs/beets-primer.md`.
- **Meelo / Plex / Jellyfin** — post-import scan notifiers. **Plex's refresh endpoint returns HTTP 200 for any path, including invalid ones — 200 is not evidence the scan ran.** `docs/meelo-primer.md`, `docs/plex-primer.md`.
- **Mirrors** — MB mirror + Discogs mirror (Rust JSON API) + LRCLIB, all optional config (`musicbrainz.apiBase` is ONE value threaded to web, CLI, and beets; public MB is the supported-but-slow fallback; Discogs browse is mirror-required). `docs/mirrors.md`, `docs/musicbrainz-mirror.md`, `docs/discogs-mirror.md`.
- **Quality model** — codec-aware rank comparison; gate vs `cfg.quality_ranks.gate_min_rank`. `docs/quality-ranks.md`, `docs/quality-verification.md`.
- **slskd client + event ingestion** — all slskd HTTP via the in-repo typed client `lib/slskd_client.py`. Event ingestion (`lib/slskd_events.py`) stamps completed-file locations from the slskd events feed; **the stamp is the ONLY source of completed-file locations** (unstamped file at materialize = hard failure, then self-heal to `wanted`). Deletions are event-driven paths + empty-dir pruning — never an inferred-folder rmtree.
- **Persisted search plans** — deterministic per-request plans; the executor consumes plan-items by ordinal, never recomputes. **Bump `SEARCH_PLAN_GENERATOR_ID` in `lib/search.py` whenever generator output changes.** `docs/persisted-search-plans-rollout.md`.
- **Unfindable detection** — its own daily oneshot unit (`cratedigger-unfindable.service`), deliberately NOT in the 5-min loop so the never-stop-searching invariant is enforceable at the systemd level. `docs/search-plan-iter2-deploy.md`.
- **Triage** — `pipeline-cli triage show/list` + `/api/triage/*`; composes unfindable + field-resolution + search forensics. Read-only. `docs/search-plan-iter2-deploy.md`.
- **YouTube resolver + rescue ingest** — resolver maps a release ID to YT Music albums with beets distances; rescue runs yt-dlp into the existing preview→importer chain (own systemd unit for network-namespacing). `album_requests.status` is never touched by rescue code — only `mark_imported_with_rescue` writes it. `docs/plans/2026-05-28-001-feat-youtube-rescue-ingest-api-plan.md`.
- **API discoverability** — `GET /api/_index` / `pipeline-cli routes`; every route needs a description (route-audit test enforces).

## Infrastructure

- **doc1** (`192.168.1.29`): this repo at `/home/abl030/cratedigger`; primary dev host.
- **doc2** (`192.168.1.35`): runs cratedigger (systemd oneshot, 5-min timer) + module-owned beets, MB mirror (`:5200`), slskd (`:5030`).
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
.claude/rules/    — path-scoped auto-loaded rules
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

Push cratedigger (GitHub) → `nix flake update cratedigger-src` on doc1 → signed commit + push nixosconfig to **Forgejo** (`git.ablz.au` — GitHub nixosconfig is a frozen fallback, never deploy from it) → `ssh doc2 'sudo fleet-update'`. `cratedigger.service` has `restartIfChanged = false` (the 5-min timer picks up new code); web/migrate restart on switch. Before `nix/module.nix` changes, run `nix build .#checks.x86_64-linux.moduleVm`. Full sequence + verification in `.claude/rules/deploy.md` (always loaded); `/deploy` runs it end-to-end.

**PR merges: use GitHub "Create a merge commit"** — never rebase- or squash-merge.

## Database migrations

Schema lives in `migrations/NNN_name.sql`; the migrate oneshot runs them on every switch before app services start. Add a numbered SQL file — no manual psql, **never** edit a shipped migration, **never** add DDL inside `PipelineDB` methods. Full workflow in `.claude/rules/deploy.md`.

## Running tests

```bash
nix-shell --run "bash scripts/run_tests.sh"    # full suite (~2 min) → /tmp/cratedigger-test-output.txt
grep "^FAIL\|^ERROR" /tmp/cratedigger-test-output.txt
nix-shell --run "python3 -m unittest tests.test_X -v"
```

**ALWAYS `nix-shell --run` for Python** (`.claude/rules/nix-shell.md`). **Never re-run the full suite just to grep differently** — read the output file. The suite gates: JS syntax + JS tests, the vulture dead-code sweep, then unittest discovery. `.claude/rules/code-quality.md` covers the test taxonomy, shared fakes/builders, and the new-work checklist.

**Generated (property-based) tests** (`tests/test_*_generated.py`, Hypothesis) run deterministically in the suite; after changing quality policy, run the randomized fuzz burst: `CRATEDIGGER_HYPOTHESIS_PROFILE=fuzz` on those modules. Failures shrink to minimal worlds — promote them to named `@example` pins or album-test-set scenarios, never JSON artifacts. **New features start by writing their invariants down, and every invariant ships as a PAIR — deterministic pin + generated property — in the same PR, with known-bad self-tests** (`.claude/rules/code-quality.md` § Red/Green TDD). When in doubt that the harness constrains anything, qualify it by fault injection. `docs/generated-testing.md`.

### Skipped tests are an anti-pattern

**A test either runs or it doesn't exist.** No skip decorators, no env-gated tests, no "fixtures must be generated first" — every test runs on every `run_tests.sh` in a fresh dev shell. A skipped test is either irrelevant (delete it) or mis-designed (make it run: Nix-provided binaries, synthetic fixtures in `setUp`, or fakes). `tests/test_skip_audit.py` fails the suite on any skip; there is no allowlist. (History: the suite once said `OK (skipped=56)` for months while 56 tests had never executed once.)

### Hooks

- Pre-commit (`ln -sf ../../scripts/pre-commit .git/hooks/pre-commit`): pyright on staged `.py`.
- Pre-push (`ln -sf ../../scripts/pre-push .git/hooks/pre-push`): randomized generated-test burst (push profile, fresh entropy each push — `docs/generated-testing.md`), then `nix flake check` (VM boot gate + eval guards + CLI bundle). Escape hatch: `git push --no-verify`.
- **Tag convention:** `vYYYY.MM.DD` (suffix `-N`) cut AFTER live verification on doc2.

### Claude Code commands

`/deploy` (full deploy sequence) · `/debug-download <id>` · `/check` (pyright + suite) · `/refactor`

### Claude Code rules (auto-loaded)

`code-quality.md`, `deploy.md`, `scope.md`, `test-fidelity.md` (always loaded) · `nix-shell.md` (`*.py`) · `harness.md` (`harness/`, `lib/beets.py`) · `web.md` (`web/`) · `pipeline-db.md` (`lib/pipeline_db.py`)

## Playwright MCP

Browser automation for `music.ablz.au` — per-machine `.mcp.json` (gitignored). Always HTTPS (http times out). `docs/playwright-mcp.md`.

## Hunting bugs — generated-first (the house method)

**Bugs are hunted with generated tests, not log-trawling.** Write down the invariant the symptom violates, probe the cheapest suspicious seam with real production functions, then build a generated harness (`tests/test_*_generated.py`) that drives the REAL code path over generated worlds and let Hypothesis find + shrink the reproduction — RED → fix → GREEN in one PR, with the shrunk world pinned forever. Proven on #550: a live bug that static analysis and disk forensics could not reproduce fell to this method in one session. Full workflow: `.claude/rules/code-quality.md` § "Bug Hunting — Generated-First" + `docs/generated-testing.md`.

For quality-decision bugs the simulator is the tool within the method: `pipeline-cli show / quality / debug-download / search-plan show / query` are the diagnostic entry points; add the failing scenario to the album test set and verify against real albums in the live DB. Command reference + triage signals in `docs/debugging-cli.md`.

## Finding dead code

`nix-shell --run "bash scripts/find_dead_code.sh"` (vulture vs `tools/vulture/whitelist.py`). After deleting, regenerate the whitelist and watch for **cascading orphans** (deleting one helper exposes its callees). Full workflow: `docs/dead-code.md`.

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
- Discogs token: `/var/lib/cratedigger/secrets/discogs-token` on doc2 (root 0400), rendered into the beets `secrets.yaml` by the module's preStart.
