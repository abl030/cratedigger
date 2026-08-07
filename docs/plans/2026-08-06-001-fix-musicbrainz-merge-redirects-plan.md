---
title: MusicBrainz Merge Redirects - Plan
type: fix
date: 2026-08-06
topic: musicbrainz-merge-redirects
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: issue-1059
execution: code
---

# MusicBrainz Merge Redirects - Plan

## Goal Capsule

- Objective: make the request↔album correspondence survive a MusicBrainz
  release merge, on both the read (join) and write (match) paths, without
  asking MusicBrainz on any read.
- Product authority: issue #1059 (rewritten 2026-08-06).
  Authority: `"teh reconciler should do the whole library, every day. it ain't
  no big deal for people who run local mirrors."` —
  https://github.com/abl030/cratedigger/issues/1059
  Authority: `"double sided merge is so so so rare, it could be a one time
  fix."` — https://github.com/abl030/cratedigger/issues/1059
- Open blockers: none.

---

## Problem Frame

MusicBrainz editors merge two release entries. The loser's MBID becomes a
permanent `301`. `mbsync` retags local files onto the survivor. The request
still stores the loser. Two surfaces break, differently:

- **The join** — `album_requests.mb_release_id = albums.mb_albumid`, re-derived
  on every read, misses after the local retag.
- **The match** — beets follows the `301` and returns a candidate wearing the
  survivor's ID; `lib/beets.py:265` and `harness/import_one.py:174` demand
  string equality with the stored loser. `rc=4`, forever.

Six live rows, all verified against the mirror on 2026-08-06. Four of the six
still have beets holding the **loser** (mbsync has not retagged them), so a
canonical-only join would fix two rows and break four. The derivation must be a
union.

---

## Key Decisions

- **KD1. One new column on the request, not a new key on the album.**
  `album_requests.canonical_release_id`. `mb_release_id` is frozen acquisition
  history. Rejected: per-album request-ID flexattr stamp — costs a Beets
  mutation lane, an 8.4k-album backfill and a twelve-consumer contract split
  for a case with zero live instances. See #1059 § "Known unfixed case".
- **KD2. The union lives in one new module, never inside
  `resolve_current_releases`.** The resolver keeps its exact release-keyed
  contract; browse and add-path callers are untouched. Merge-following inside
  the resolver is precisely what broke four consumers across two review rounds
  on `feat/mb-canonical-redirects`.
- **KD3. One definition of "acceptable release IDs for this request",** shared
  by the join, validation, candidate matching, the duplicate query, the remove
  guard and post-import verification. Divergence between any two of those is
  the defect class this issue exists to close.
- **KD4. The match relaxation and the duplicate widening ship in the same PR.**
  Splitting them opens the data-integrity hazard (below). Non-negotiable
  sequencing constraint.
- **KD5. Whole-library daily sweep, its own systemd unit.** Not
  `cratedigger-unfindable.service`: it gates on slskd reachability, so a slskd
  outage would silently stop reconciliation, and it is deliberately isolated so
  the never-stop-searching invariant stays systemd-enforceable.
- **KD6. Fail-open by contract.** Mirror unavailable, `4xx`, or disagreement ⇒
  behaviour identical to today. Only an **observed `301`** ever writes a
  canonical. A `4xx` is never read as "this release is gone".

---

## The hazard that governs sequencing

`harness/beets_harness.py::_find_duplicates_with_mapped_release_ids` builds
beets' duplicate query from the **candidate's** `mb_albumid` — the survivor.
When beets holds the loser it queries `mb_albumid = survivor`, finds nothing,
concludes the import is not a duplicate, and **keeps both albums**. Post-import
verification then names the stale album, which drives the sidecar write, the
current-evidence refresh and the Plex/Jellyfin pins at the wrong target.

Requests 346 and 8712 are the live fuse: `wanted`, beets holds the loser, so
the first successful download triggers it. Today they fail closed at `rc=4` and
mutate nothing — which is only true because the match is still strict. **The
moment the match accepts the canonical without the duplicate query being
widened, the hazard is live.** They ship together or not at all (KD4).

This seam is identical under any design: beets `duplicate_keys` are release-ID
keyed, so a request-ID flexattr would not participate in that query either.

---

## Invariants

Each ships a deterministic pin **and** a generated property in the same PR,
each checker with a known-bad self-test.

| # | Invariant | PR |
|---|---|---|
| I1 | `mb_release_id` is never mutated. | 1 |
| I2 | `canonical_release_id` is written only from an observed `301` — never a body field, never metadata, never a release-group relative. | 1 |
| I3 | The join resolves the union {canonical, acquisition}, canonical first. Both resolving to *different* albums is ambiguous and fails closed — never a silent pick. | 1 |
| I4 | Candidate acceptance is the acquisition ID or the canonical ID. Never a sibling, never a release-group relative. | 2 |
| I5 | The duplicate query and the remove guard use the **same** acceptable-identity set as candidate acceptance. | 2 |
| I6 | Post-import verification resolves the union, not an only-on-empty fallback. | 2 |
| I7 | A canonical may be updated on a further merge; it is never cleared by a failed or unavailable lookup. | 1 |
| I8 | Mirror unavailable / `4xx` / disagreement ⇒ behaviour identical to today. | 1 |

**Composition requirement.** Per `.claude/rules/code-quality.md` § "Invariants
live at the widest boundary", the pin+property pair for I3, I5 and I6 must
compose the **real** writer with the **real** consumer over the same real
resource — the reconciler writing a canonical and a real consumer reading it;
the real harness duplicate query against a real temp beets library. The first
attempt shipped green twice because the pin and the property both stopped at
the resolver. A property that stops at `acceptable_identities()` does not
discharge these.

---

## PR 1 — Read path: column, resolver union, reconciler

### Schema

`migrations/074_canonical_release_id.sql`

- `ALTER TABLE album_requests ADD COLUMN canonical_release_id text` (nullable).
- `ADD COLUMN canonical_resolved_at timestamptz` — when the `301` was observed.
  Provenance for I2/I7; also lets the sweep report staleness.
- Partial index on `canonical_release_id WHERE canonical_release_id IS NOT NULL`
  (the populated set is single digits; the index is for the join, not the scan).
- ~~No CHECK forbidding `canonical = acquisition`.~~ **Superseded during
  implementation**: the migration DOES add that CHECK. A self-referential
  canonical would collapse the union resolver's two-identity probe to one and
  silently hide a real split, which is worse than a COMMIT failure — and
  nothing does an in-place `SET mb_release_id`, so the "future MB correction"
  the original text feared cannot arise. The writer guards it too, so the
  constraint is a second line, not the only one.

### `lib/mb_canonical.py` — salvaged from `feat/mb-canonical-redirects`

Lift as-is. Fail-open by contract, inert until a process wires a base, MB-only,
byte-capped, redirect-proof rule (a merge is proven by the observed `301`,
never by a body field). Its only behavioural change: it is wired by the
reconciler process, not by the web process.

Do **not** carry over `web/api_bases.py`'s canonical wiring into the web
process — no read path calls it.

### `lib/request_identity.py` — new, the one union site (KD2, KD3)

```
acceptable_identities(row) -> tuple[ReleaseIdentity, ...]   # canonical first, then acquisition
resolve_current_for_request(beets, row) -> CurrentBeetsResolution
resolve_current_for_requests(beets, rows) -> dict[int, CurrentBeetsResolution]
```

Merge rule over the per-identity resolutions:

| canonical | acquisition | result |
|---|---|---|
| missing | missing | `CurrentBeetsMissing(identity=acquisition)` |
| unique | missing | that unique |
| missing | unique | that unique |
| unique | unique, same `album_id` | that unique |
| unique | unique, **different** `album_id` | `CurrentBeetsAmbiguous(reason="merged_identity_split")` |
| any ambiguous | — | propagate that ambiguous |

`merged_identity_split` is a new member of `CurrentBeetsAmbiguityReason`. It is
the double-sided merge, and it is the *only* new failure state this work
introduces. Zero live instances.

The batch variant issues **one** `resolve_current_releases` call over the
flattened identity set — no per-row fan-out, no N+1.

### Consumer switchover

Every caller that has a request row moves to `resolve_current_for_request(s)`:

| file | note |
|---|---|
| `lib/world_audit_service.py:319` | has rows |
| `lib/current_library_display.py:137` | has the row already |
| `lib/destructive_release_service.py:267,727` | has the request |
| `lib/mbid_replace_service.py:911` | old request |
| `lib/import_job_recovery_service.py:531` | has the request |
| `lib/quality_evidence.py:2046` | takes `mb_release_id`; add the canonical as a parameter |
| `scripts/cleanup_ghost_imported.py:66` | has the request |
| `lib/banding.py::resolve_current_release_bands` | widen from `Iterable[str]` to identity-pair inputs; long-tail rows already carry their pipeline columns |

**Unchanged and deliberately so:** `web/routes/_overlay.py::band_release_ids`
when serving the browse overlay, and the add-path collision check. Those ask
"do I hold *this release*", which is release-keyed and correct as-is.

### Request-row-derived Beets consumer inventory (current candidate state)

The request-row inventory records the current candidate state. A request row
must use `resolve_current_for_request(s)` whenever its
Beets answer changes a request lifecycle or quality outcome; a release-keyed
caller remains exact only when it genuinely asks whether that exact release is
held.

| consumer family | disposition |
|---|---|
| direct request-row display, audit, destructive, recovery, cleanup and banding consumers listed above | switched in PR 1 |
| `lib/beets.py` validation and all `harness/import_one.py` target, duplicate-guard/query and postflight exact lookups | deferred write-path work; retain every exact lookup in the inventory |
| `harness/beets_harness.py` duplicate query | deferred write-path work; retain its exact lookup inventory |
| preview HAVE spectral audit (`lib/measurement.py`, both `lib/import_preview.py` paths and the preview-worker reused-evidence front gate) | U1: request-aware union; missing/ambiguous/omitted authority fails closed |
| requeue retained override/minimum bitrate (`web/routes/pipeline_mutations.py::post_pipeline_update`) | U2: only `CurrentBeetsUnique` supplies the retained floor |
| manual imported quality floor (`web/routes/pipeline_mutations.py::post_pipeline_set_quality`) | U3: only `CurrentBeetsUnique` supplies the survivor-held average bitrate |
| `lib/import_evidence.py::load_current_evidence_for_action` | deferred indirect action path; retain its exact lookup inventory |
| `lib/sidecar_service.py::write_sidecar_for_request` | deferred action seam; retain its exact lookup inventory |
| `lib/wrong_match_cleanup_service.py` through `load_current_evidence_for_action` | deferred indirect action path; do not mistake it for a release-keyed browse read |
| `lib/disk_coverage_service.py::check_mbids` | raw-union, presence-only reporting disposition: it deliberately flattens canonical + acquisition ids for one non-destructive coverage query and cannot authorize lifecycle, quality, or deletion work |
| Library artist inverse association, Wrong Matches display, and destructive no-pipeline-id inverse association | deferred to [#1066](https://github.com/abl030/cratedigger/issues/1066): deriving a request from a survivor-filed Beets album needs its own inverse-association contract; it remains a pre-deployment batch blocker and is not PR 1 work |

This ledger records the direct request-row consumers switched in this
candidate and the deferred match/action and inverse-association work. The
deferred rows above are not landed work. It is an explicit consumer ledger,
not a semantic scanner: future request-aware callers are reviewed at their
production adapter when they are introduced.

### Reconciler

Per the CLI ⇄ API symmetry rule, one canonical execution path with two thin
adapters:

- `lib/canonical_release_service.py` — `reconcile_request(request_id)` and
  `reconcile_all()`, typed `Result` with one outcome per branch:
  `resolved` / `unchanged` / `no_redirect` / `invalid_identity` /
  `mirror_unavailable` / `not_found`.
- `scripts/pipeline_cli/canonical.py` — `pipeline-cli canonical reconcile
  [--id N | --all]`, `pipeline-cli canonical show <id>`. Register in `cli.py`
  dispatch + `routes_meta.py` subparser.
- `web/routes/canonical.py` — `POST /api/canonical/reconcile`,
  `GET /api/canonical/<request_id>`. `classified=True`, contract test with
  `REQUIRED_FIELDS`, production-shaped mock rows.
- Exit/status mapping: `200/0`, `404/2`, `409/4`, `422/3`, `503/5`.
- `scripts/run_canonical_reconciliation.py` — the oneshot driver. Calls
  `assert_schema_current` at startup (it is timer-driven with
  `restartIfChanged = false`, so it `wants`+`after` the migrate unit, never
  `requires` — same reasoning as `cratedigger-unfindable`).

Only MusicBrainz-source identities are swept. Discogs rows have no merge
semantics and nothing retags them; skipping them is a positive statement, with
a test.

### systemd (`nix/module.nix`)

`cratedigger-canonical-reconcile.service` + `.timer`:

- `Type = oneshot`, `restartIfChanged = false`, `wants` + `after` the migrate
  unit and `network.target`.
- **No slskd health gate** (KD5).
- `OnCalendar = daily`, `Persistent = true`, `RandomizedDelaySec = 30min`.
- `TimeoutStartSec = 1h` — the sweep is ~10 min at 72ms × 8,500; an hour gives
  headroom for a slow mirror while still surfacing a genuinely stuck run.
- New module option for enable/disable, documented in `docs/nixos-module.md`.
- Run `nix build .#checks.x86_64-linux.moduleVm` before deploying (module change).
- **Check the downstream wrapper** at
  `~/nixosconfig/modules/nixos/services/cratedigger.nix` — a new option is
  latent breakage there until the next fleet-update.

### Tests owed (PR 1)

- Pin + generated property for I1, I2, I3, I7, I8, each with a known-bad
  self-test.
- I3's property composes the real reconciler write with a real consumer read
  (composition requirement above).
- Rule A: real-PG round trip asserting every key of the canonical write is
  readable back; `tests/test_pipeline_db_column_contract.py` entry.
- Rule B: the mirror fake raises the real exception classes — `HTTPError`,
  `URLError` — never `lambda: None`.
- `FakePipelineDB` gains the new column and its self-test in `tests/test_fakes.py`.
- Service-layer test covering every outcome branch; CLI exit-code test; route
  contract test.
- **Rule D live-corpus render differential is owed** — the join feeds the
  library tab, Recents "currently have" and long-tail bands. Expected: 2 rows
  change (316 and 8832, missing → present). Run `scripts/render_differential.py`
  against `origin/main` and put the numbers in the PR body, zeros included.

### Docs (PR 1)

`docs/pipeline-db-schema.md` (new columns), `docs/nixos-module.md` (new unit +
option), `docs/mirrors.md` / `docs/musicbrainz-mirror.md` (the reconciler is a
new mirror consumer with a stated daily budget), `docs/debugging-cli.md` (new
subcommands), CLAUDE.md § subsystems (one line).

---

## PR 2 — Write path: the acceptable-identity set at every match seam

Every seam consumes the same ordered tuple from
`lib/request_identity.py::acceptable_identities` (KD3).

| site | change |
|---|---|
| `lib/beets.py::beets_validate` (`cand.mbid == mb_release_id`, :265) | accept membership in the set; pass **both** ids to the already-repeatable `--search-id` |
| `harness/import_one.py::_find_target_candidate` (:174) | accept membership in the set; deterministic preference order canonical → acquisition when both are offered |
| `harness/beets_harness.py::_find_duplicates_with_mapped_release_ids` | run the duplicates query once per acceptable id, union by `album.id`; new repeatable `--acceptable-release-id` arg carries the set into the harness |
| `harness/import_one.py::_duplicate_remove_guard_failure` (:276) | compare `candidate_identity.key` against the acceptable **set**, not one `target_identity.key`; otherwise the guard refuses the very duplicate it exists to remove |
| post-import verification (`import_one.py:1848`, `:2746`) | `get_all_album_ids_for_release` over the **union**, never an only-on-empty fallback — this is carried-over defect #1 from the aborted branch |
| `scripts/import_preview_worker.py` | wire the canonical; today it reports `uncertain / mbid_missing` for a release the importer would take cleanly — carried-over defect #2 |

Both harness launchers (`lib/download_validation.py`, `lib/import_preview.py`)
forward the new arg.

### Tests owed (PR 2)

- Pin + generated property for I4, I5, I6, each with a known-bad self-test.
- **I5's composition test runs the real harness duplicate query against a real
  temp beets library** holding a loser-tagged album, with a survivor-tagged
  candidate, and asserts exactly one duplicate is found. A module-scope test of
  `acceptable_identities` does not discharge this — that is the #853/#859
  lesson verbatim.
- An orchestration test proving the loser-installed upgrade replaces rather
  than duplicates: assert one album remains, carrying the survivor.
- A must-still-work guard: a non-merged request still matches exactly one
  candidate and still refuses a sibling (I4 fail-closed direction).
- Integration slice in `tests/test_integration_slices.py` for the
  loser-installed upgrade through real serialization.

---

## Deploy window (operator one-shots, not committed)

Per the single-operator rule these are agent-run during the controlled window
and then discarded — no `scripts/backfill_*.py`.

1. **After PR 1 deploys:** run the reconciler once immediately rather than
   waiting for the timer, so the six known rows resolve before PR 2 relaxes any
   match. Verify `canonical_release_id` on 316, 346, 1838, 8712, 8815, 8832.
2. **Orphan sweep:** delete the nine orphan albums — `3630, 5163, 6312, 7159,
   10204, 10278, 14527, 18664`, plus `6612`, which has already drifted onto live
   request 8792's ID. Use the admitted exact-album delete child
   (`lib/beets_delete.py`); never `beet remove -d`. This takes
   `current_beets_ambiguous` 1 → 0. The Replace defect behind them is fixed at
   `f283baad`, so the population is closed.
3. **Re-run `cratedigger-live-world-audit`** and confirm bucket B 634 → 629,
   with `current_beets_missing`, `evidence_link_without_album` and
   `current_beets_ambiguous` all at zero.

---

## Verification

- `bash scripts/test.sh <targets>` during development; `check` skill for the
  one receipt-backed canonical suite on the clean committed tree before review
  handoff.
- `nix-shell --run "bash scripts/fuzz_burst.sh"` after the generated modules
  land.
- `nix build .#checks.x86_64-linux.moduleVm` before the module change deploys.
- Rule D render differential in the PR 1 body.
- `deploy` skill for the full sequence; live cycle verification via
  `scripts/verify_cratedigger_cycle.sh`.

---

## Explicitly out of scope

- Mutating the stored acquisition ID.
- The per-album request-ID stamp (#1059 § "Known unfixed case"). It remains
  buildable on top of this work without unwinding any of it — a different key
  in a different store, sitting in front of the column in the same derivation.
- Merge-following inside `resolve_current_releases`.
- Canonicalizing the add path's collision check.
- Any operator-facing badge for the drift.
- The 629 `current_evidence_missing` / `evidence_fingerprint_mismatch` bucket-B
  members. Operator-declared closed by current evidence engineering; this work
  takes bucket B to 629 and does not green the debt gate.

---

## Residual risk

- **Double-sided merge** — permanent, fails closed as
  `merged_identity_split`, zero live instances, stamp is its future exit.
- **≤24h window** — between an upstream merge and the next daily sweep, a row
  retagged by `mbsync` in that interval has a missing join and fails `rc=4` on
  download. Both fail closed and self-heal on the next sweep.
- **Mirror replication lag** — a `200` is not proof of currency for ~25h. We
  act only on an observed `301`, so lag delays a fix; it cannot cause a wrong
  write.
