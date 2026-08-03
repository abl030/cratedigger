---
title: Beets Tip Canary and Rolling Release Compatibility - Plan
type: feat
date: 2026-08-03
issue: 992
artifact_readiness: implementation-ready
execution: code
---

# Beets Tip Canary and Rolling Release Compatibility - Plan

## Goal Capsule

- **Objective:** Close [#992](https://github.com/abl030/cratedigger/issues/992) by detecting upstream Beets drift before release while maintaining the Cratedigger-owned boundary across every final Beets release from the previous 730 days.
- **Production authority:** The admitted Nixpkgs input remains the only source of the production Beets package. The exported NixOS module and ordinary development shell must not resolve the tip input or a historical release package.
- **Canary authority:** One non-flake `beets-tip` input locks upstream `beetbox/beets` `master` for checks only. Automation may advance that one lock node after its focused runtime and whole-repository typing gates pass.
- **Release authority:** A committed manifest records immutable revisions and NAR hashes for final upstream releases selected by a deterministic 730-day-window refresher. Pure Nix evaluation consumes the manifest without network or wall-clock access.
- **Compatibility boundary:** Only behavior Cratedigger owns and executes is promised: harness protocol, import session hooks, configured library construction, exact duplicate handling, provider-ID normalization, distance/candidate data, pretend source purity, and exact-album deletion against disposable authority.
- **Explicit exclusion:** LRCLIB and other optional third-party plugin behavior are external Beets configuration. This work will neither backport nor emulate them.
- **Execution profile:** Preserve this plan as the first branch commit, delegate implementation to Terra, commission an independent read-only Sol review, then obtain a receipt-backed clean-tree gate before first push.
- **Tail ownership:** The orchestrator owns review convergence, PR landing with a merge commit, the signed downstream pin and scheduler composition, exact-source/live successor proof, issue closure, and worktree cleanup.

---

## Product Contract

### Problem

The existing daily gate advances the entire flake lock. Consequently Cratedigger first sees a Beets incompatibility when Nixpkgs admits the relevant Beets release. Beets 2.13 made three different late-breaking changes that the current stable-only contract cannot expose ahead of that point:

1. upstream changed its Python build backend from Poetry to Hatchling;
2. Beets startup or migration output can reach stdout and corrupt the harness's newline-delimited JSON protocol; and
3. Beets' album IDs became `int | None` in type information, exposing fixtures that assume an ID always exists.

Cratedigger needs advance warning without weakening the production pin, taking ownership of external plugins, or pretending historical versions are safe against the live library.

### Supported Cohorts

The compatibility surface has three cohorts with different meanings:

| Cohort | Source | Qualification | Meaning |
|---|---|---|---|
| Production | `pkgs.python3Packages.beets` from locked Nixpkgs | full suite, Pyright, flake checks, deploy proof | the only Beets package production may run |
| Tip | locked non-flake `beets-tip` input | build, focused real boundary, whole-repository Pyright | advance warning only; never automatically promoted |
| Releases | immutable entries in the generated 730-day manifest | build plus focused real boundary per release | supported Cratedigger interface history; never live downgrade proof |

As of 2026-08-03 the manifest contains 19 final releases: `v2.1.0`, `v2.2.0`, `v2.3.0`, `v2.3.1`, `v2.4.0`, `v2.5.0`, `v2.5.1`, `v2.6.0`, `v2.6.1`, `v2.6.2`, `v2.7.0`, `v2.7.1`, `v2.8.0`, `v2.9.0`, `v2.10.0`, `v2.11.0`, `v2.12.0`, `v2.13.0`, and `v2.13.1`.

The checked-in list is a reproducible snapshot. The refresher, not flake evaluation, applies the moving calendar window and proposes an ordinary reviewed manifest diff.

### Requirements

#### Isolation and production safety

- R1. The production package, exported module, package set, wrappers, and default development shell must continue to resolve Beets exclusively from the locked Nixpkgs input.
- R2. Upstream tip must be a locked non-flake input consumed only by named checks and canary tooling.
- R3. Every tip and historical check must create its own temporary `BEETSDIR`, SQLite catalog, import source, and library root. Ambient `BEETSDIR`, live database paths, and live library roots must be scrubbed and rejected.
- R4. A passing disposable check makes no claim that a live catalog can be upgraded or downgraded safely.

#### Compatibility adapters

- R5. Beets importer imports must work across the monolithic 2.1-2.3 API and split 2.4+ modules through one localized capability adapter.
- R6. Library construction must preserve configured path formats and replacements in both the legacy four-argument API and the 2.12+ derived two-argument API.
- R7. Duplicate decisions must use one Cratedigger-owned internal outcome and adapt it to legacy `resolve_duplicate` / `should_remove_duplicates` and modern `get_duplicate_action` / `DuplicateAction` hooks.
- R8. Capability detection must be structural. Scattered version comparisons and a general compatibility framework are forbidden.
- R9. Unknown or partially supported capability combinations fail loudly before mutation.

#### Harness protocol and owned behavior

- R10. After argument parsing, the harness must reserve the original stdout file descriptor for protocol frames and redirect Python and raw-fd third-party stdout to stderr. `--help` must remain ordinary argparse stdout and exit zero.
- R11. Every protocol line emitted after the boundary is valid JSON; Beets diagnostics never share the protocol stream.
- R12. The real contract must retain provider-ID normalization, candidate distance serialization, exact release duplicate lookup, remove/skip semantics, configured paths/replacements, and completed `--pretend` source purity.
- R13. Exact-album deletion must be exercised against a disposable catalog and library, including foreign-file preservation. Selector-based deletion remains forbidden.
- R14. Optional album IDs are narrowed with explicit fail-loud assertions at fixture/contract boundaries; no cast may hide a missing ID.

#### Matrix and automation

- R15. Every manifest release builds with the admitted Python/dependency set and only the plugin closure required to exercise the Cratedigger boundary. Build backend selection is explicit for the upstream packaging era.
- R16. The matrix runs one focused real-Beets contract per release in independently schedulable Nix checks. It does not multiply the complete Cratedigger suite by the release count.
- R17. Tip runs the same focused runtime contract plus whole-repository Pyright in a tip-backed test environment.
- R18. The existing Nixpkgs candidate runner updates only `nixpkgs`; a separate tip runner updates only `beets-tip`. Both serialize repository mutation through one state-directory lock.
- R19. A red tip candidate is reported but never blocks, rewrites, or rolls back a green Nixpkgs candidate. A candidate commit may contain only the lockfile and must name the input it advanced.
- R20. The release refresher selects non-draft, non-prerelease GitHub releases whose publication timestamps fall inside `[as_of - 730 days, as_of]`, resolves each tag to an immutable revision and NAR hash, sorts deterministically, and supports an explicit fixed `--as-of` date.
- R21. Pure flake checks do not query GitHub or read the wall clock. Refresher tests use fixture data and fixed dates, including threshold, draft, prerelease, duplicate, ordering, and known-bad cases.

#### Documentation and operations

- R22. Documentation distinguishes production support, tip canary coverage, release-boundary coverage, and live-catalog safety.
- R23. Documentation explicitly excludes LRCLIB and unrelated plugin feature parity.
- R24. Downstream scheduling runs the Nixpkgs and tip candidates at separate times or services while sharing the serialization state. A failed canary reaches the existing negative-alert path.

### Non-Goals

- No LRCLIB source shim, compatibility patch, substitute URL, test server, or old-release backport.
- No compatibility promise for every built-in or external Beets plugin.
- No automatic promotion of upstream tip into production.
- No mutation, migration, copy, or backup of the live Beets catalog or music library.
- No full Cratedigger suite once per historical release.
- No broad semver abstraction, dependency solver, or version-conditioned branches throughout the harness.
- No change to strict pressing identity, quality policy, importer ownership, or operator-approved destructive boundaries.

### Acceptance Examples

- AE1. Given Beets 2.3.1, importing the harness succeeds through the monolithic importer API, configured paths and replacements are present, and a `remove` duplicate decision sets the legacy remove flag.
- AE2. Given Beets 2.11.0, the split importer modules load, legacy duplicate resolution works, and the same internal decision contract is observed.
- AE3. Given Beets 2.13.1 or locked tip, migration text written through Python stdout or raw file descriptor 1 appears on stderr while every protocol stdout line parses as JSON.
- AE4. Given `--help`, the harness emits argparse help on normal stdout without installing the protocol redirection.
- AE5. Given a configured non-default path template and replacement map, both the oldest and newest supported Library constructors render the expected disposable destination.
- AE6. Given duplicate `remove`, `skip`, and missing decisions, each API era performs the same Cratedigger-owned outcome and defaults fail-closed to skip.
- AE7. Given a Discogs candidate, the real Beets metadata mapping cannot write numeric provider IDs into MusicBrainz fields; a MusicBrainz UUID remains unchanged.
- AE8. Given a disposable album, exact-album deletion removes its owned media and derived clutter but preserves an unrelated sentinel and its containing directory.
- AE9. Given ambient live-looking `BEETSDIR` or library variables, the matrix runner scrubs them and proves its opened paths stay under its temporary root.
- AE10. Given one failing matrix member, its named check fails with the Beets version and retained stderr while sibling releases remain independently buildable.
- AE11. Given a failed tip candidate, no commit or push occurs; the Nixpkgs updater remains independently runnable and updates only `nixpkgs`.
- AE12. Given releases exactly on, just inside, and just outside the 730-day threshold plus a draft and prerelease, fixed-clock refresh includes only the two qualifying final releases in deterministic order.

---

## Technical Design

### 1. Flake topology

Add this input shape:

```nix
beets-tip = {
  url = "github:beetbox/beets/master";
  flake = false;
};
```

Thread `beets-tip` only into `checks`. Do not pass it to `packages`, `devShells`, `apps`, or `nixosModules.default`. Add an evaluation guard that compares the production Beets source/version against the Nixpkgs package and proves it differs from the canary source identity when tip is ahead.

Introduce a small checks-only package builder, expected at `nix/beets-compat-package.nix`, which takes:

- the admitted `pkgs` and Python package set;
- an immutable source and display version;
- the declared build backend era; and
- the bounded plugin set needed by Cratedigger's boundary.

The builder reuses the admitted Nixpkgs Beets dependency recipe and overrides source/backend intentionally. It disables the all-plugin closure and enables only Cratedigger-relevant built-ins so the 19-way check does not pull unrelated scientific/audio-analysis dependencies. It must not patch plugin behavior. Upstream package tests are not the compatibility oracle; the dedicated Cratedigger boundary contract is.

`nix/beets-compat-releases.json` is generated data with one entry per release:

```json
{
  "version": "2.13.1",
  "tag": "v2.13.1",
  "publishedAt": "2026-07-29T10:47:07Z",
  "rev": "<immutable commit>",
  "narHash": "sha256-...",
  "buildBackend": "hatchling"
}
```

The manifest is the evaluation input; its filename and schema are structurally tested. Nix creates sanitized named checks such as `beets-release-2_13_1-contract`, plus `beets-tip-build`, `beets-tip-contract`, and `beets-tip-pyright`.

### 2. Localized runtime compatibility

Add a narrow `harness/beets_compat.py` module. It owns only these seams:

1. importer type imports and aliases;
2. configured Library construction;
3. conversion between one internal duplicate decision (`remove` or `skip`) and the active upstream hook contract; and
4. explicit capability validation with actionable errors.

The harness class may expose both upstream hook names because different Beets eras call different names, but both delegate immediately to one protocol decision helper. No controller-visible JSON shape changes. The compatibility module detects symbols/imports, not versions.

The legacy Library path calls Beets' own legacy path-format/replacement helpers and supplies all four arguments. The modern path uses the two-argument constructor and verifies derived configuration. Detection must distinguish an unavailable helper from an unrelated import failure.

### 3. Protocol isolation

Keep argparse construction and `parse_args()` before protocol setup. Immediately afterward:

1. flush stdout;
2. duplicate original fd 1 into a private protocol fd;
3. duplicate fd 2 over fd 1 so all later ordinary stdout reaches stderr;
4. bind `_send` to a text stream opened on the private fd; and
5. close the stream exactly once at session completion or failure.

This mirrors the already-shipped exact-delete JSON child boundary in `harness/delete_album.py`. Tests must inject output through both `print()` and `os.write(1, ...)`, and must prove help output stays untouched.

### 4. Focused real-Beets contract

Refactor `tests/test_harness_beets2_contract.py` so its assertions describe capabilities rather than Beets 2.12 specifically. Keep the real fresh-interpreter and real wrapper paths. The reusable focused entry point must cover:

- import/load and capability report;
- configured Library construction and destination rendering;
- duplicate remove/skip/default behavior for the active API era;
- provider-ID neutralization through real `AlbumInfo` mapping;
- candidate distance/identity serialization;
- release-ID duplicate lookup;
- exact-album deletion and foreign sentinel behavior;
- real wrapper `--pretend` source-manifest equality; and
- JSON-only protocol under injected third-party stdout.

It must print a compact success summary containing the running Beets version and capability era. Failures retain stdout/stderr and the matrix check name.

The stable suite continues to discover this test normally. Each Nix matrix derivation runs only this focused contract in a fresh temporary root with ambient database/config variables unset. Where test fixtures consume `Album.id`, assert non-`None` before constructing an integer-bearing request or expectation.

### 5. Release manifest refresher

Add `scripts/refresh_beets_compat_releases.py` as an operator/automation tool, not a flake evaluator. Its pure core accepts release records plus `as_of`, validates UTC timestamps, applies the inclusive 730-day window, rejects drafts/prereleases, resolves tags through injected GitHub/Nix-prefetch edges, and renders canonical JSON.

The command defaults `as_of` to the current UTC date for ordinary use but accepts required deterministic override in tests. Network calls use `gh api`/GitHub archive metadata outside the Nix sandbox; no token is written to the manifest. It writes only after every tag has an immutable revision and valid SRI NAR hash. Existing manifest entries may be reused only when tag, publication time, revision, and hash agree.

Unit tests drive the pure selector/rendering core with fixed fixtures. A known-bad selector that admits a prerelease or stale boundary item must be rejected by the invariant checker.

### 6. Independent update runners

Change `scripts/daily_flake_update.sh` from bare `nix flake update` to `nix flake update nixpkgs`. Preserve its full candidate qualification and lock-only commit behavior.

Add `scripts/daily_beets_tip_update.sh` with the same clone/fail-closed/push discipline but:

- acquire the shared state-directory update lock;
- run `nix flake update beets-tip` only;
- run the tip build, tip focused contract, and tip whole-repository Pyright checks;
- commit only `flake.lock` with a tip-specific message and `Refs #992`; and
- leave the checkout unpushed on any red stage.

The Nixpkgs runner acquires the same lock. Test fakes record the exact update argv, lock acquisition, check set, commit path, and push behavior for changed, unchanged, and failed candidates. Tests prove each runner preserves the other input's lock node.

The release manifest refresh remains a deliberate reviewed source change rather than an unattended direct-to-main push: the refresher produces the diff and its matrix must go green before it lands.

### 7. Documentation and downstream composition

Update `docs/beets-primer.md`, `docs/generated-testing.md`, and the relevant deployment rule text to describe the three cohorts and their different guarantees. Amend wording that says every flake update changes all inputs; production updates remain `nixpkgs`-specific. State the LRCLIB exclusion without ambiguity.

After the Cratedigger PR merges, update the downstream doc1 scheduler in `nixosconfig` so the tip runner is a separate oneshot/timer (staggered from the long Nixpkgs job), shares the state directory/serialization lock and GitHub credentials, and uses the existing negative-alert pattern. This downstream composition is deployment plumbing: it does not change which Beets package production runs.

---

## Implementation Units

### U1 — Pin tip and generate the release manifest

- **Files:** `flake.nix`, `flake.lock`, `nix/beets-compat-releases.json`, `nix/beets-compat-package.nix`, focused Nix tests.
- **Work:** Add the checks-only input, package builder, current 19-release immutable manifest, named build/runtime checks, and production-source isolation assertion.
- **Proof:** Pure eval succeeds offline; all names enumerate; production outputs do not depend on `beets-tip`; v2.1.0 and tip packages build before expanding the full matrix.

### U2 — Add capability adapters with red/green era pins

- **Files:** `harness/beets_compat.py`, `harness/beets_harness.py`, harness unit tests.
- **Work:** Write deterministic pins for monolithic/split imports, legacy/modern Library construction, legacy/modern duplicate actions, and unknown-capability failure; then implement the localized adapter.
- **Proof:** Oldest, transition (2.4/2.12), newest, and tip representatives pass before running all 19 releases.

### U3 — Harden stdout as a protocol boundary

- **Files:** `harness/beets_harness.py`, protocol/wrapper tests.
- **Work:** Reserve original stdout after argparse, redirect third-party fd 1 to stderr, and route `_send` over the private stream with exact lifecycle ownership.
- **Proof:** Python and raw-fd contamination tests fail on the old harness and pass after the fix; `--help` and controller disconnect behavior remain correct.

### U4 — Generalize the real boundary contract and typing proof

- **Files:** `tests/test_harness_beets2_contract.py` (rename only if it improves the contract), affected fixture tests, Nix check wiring.
- **Work:** Remove 2.12-only assertions, exercise the active capability era, add exact delete/distance/protocol cases, and fail loudly on optional IDs.
- **Proof:** Stable full test discovery remains green; tip Pyright catches the historical optional-ID mutant; the focused contract passes v2.1.0, v2.3.1, v2.4.0, v2.11.0, v2.12.0, v2.13.1, then the complete manifest.

### U5 — Add deterministic manifest refresh

- **Files:** `scripts/refresh_beets_compat_releases.py`, `tests/test_beets_compat_releases.py`, docs.
- **Work:** Implement fixed-clock selection, immutable resolution/prefetch, canonical rendering, validation, and dry-run/check modes.
- **Proof:** Threshold/draft/prerelease/order tests, known-bad checker self-test, and a check that regenerating at `2026-08-03` is byte-identical.

### U6 — Isolate Nixpkgs and tip automation

- **Files:** `scripts/daily_flake_update.sh`, `scripts/daily_beets_tip_update.sh`, `tests/fakes/daily_flake_update.py`, `tests/test_daily_flake_update.py`, new tip-runner tests as needed.
- **Work:** Update exact input argv, shared locking, tip-only gates, lock-only commits, and independent red/green semantics.
- **Proof:** Fake command transcript establishes exact input isolation, failure behavior, no-change behavior, serialized mutation, and no authority leakage.

### U7 — Document, review, ship, and schedule

- **Files:** `docs/beets-primer.md`, `docs/generated-testing.md`, `.claude/rules/deploy.md`, downstream `nixosconfig` scheduler after merge.
- **Work:** Document the support contract and operator refresh flow; obtain Sol review; run the receipt-backed clean-tree gate; merge; pin and deploy; add the staggered downstream canary timer.
- **Proof:** Exact reviewed commit is active on doc2, production `beet --version` remains the admitted Nixpkgs version, tip source is absent from the production closure, the new timer is enabled, one canary invocation has a fresh successful InvocationID, and a natural Cratedigger successor cycle runs the merged harness revision.

---

## Validation Strategy

### Focused development order

1. Pure manifest selector/render tests.
2. Harness adapter unit pins with fake capability modules only at the third-party import boundary.
3. Stable real-Beets contract.
4. Representative old/mid/new/tip Nix checks.
5. Complete 19-release matrix.
6. Whole-repository Pyright in both stable and tip environments.
7. Complete deterministic suite under the admitted production shell.
8. `nix flake check --print-build-logs` including all named compatibility checks.
9. Receipt-backed `check` skill on the final reviewed, committed, clean tree before first push.

### Required review cross-checks

- Trace every `beets.importer` import and every `Library(...)` construction to ensure compatibility logic is not duplicated outside the adapter.
- Trace every harness stdout write, including raw child/third-party fd behavior, to ensure the protocol stream has one owner.
- Inspect the flake dependency graph to prove production packages/modules do not close over `beets-tip` or release sources.
- Inspect update scripts and their fakes to prove exact lock-input isolation and serialization.
- Confirm no test path can resolve `/mnt/virtio`, `/var/lib/cratedigger`, an ambient `BEETSDIR`, or the live catalog.

### First-push gate

No branch push occurs until:

- all implementation and review fixes are committed;
- the worktree is clean;
- an uninvolved Sol reviewer has returned no unresolved material findings;
- the branch is current with `origin/main`; and
- the repository `check` skill has issued a same-HEAD Pyright/full-suite receipt.

---

## Rollout and Closure

1. Push the reviewed branch and open a PR using `Refs #992`, not an auto-closing keyword.
2. Wait for GitHub checks/review state; address any real failure and repeat the independent review/final receipt if the tree changes materially.
3. Merge using GitHub **Create a merge commit**.
4. Use the repository deploy workflow to create and push the signed `nixosconfig` Cratedigger pin and deploy through the locked fleet trigger.
5. Add/enable the staggered doc1 Beets-tip canary service/timer in the signed downstream configuration and deploy it.
6. Verify on doc2 that the exact merged Cratedigger source is active, production resolves the admitted Nixpkgs Beets package, migrations/readiness are green, and one natural successor pipeline cycle completes.
7. Verify on doc1 that the tip timer is enabled and one manually triggered or naturally due canary invocation succeeds without changing the production Beets version.
8. Comment on #992 with PR, receipt, signed pin, fleet anchor, exact active source, production Beets version, canary InvocationID, and successor-cycle evidence.
9. Close #992 deliberately, fast-forward the shared checkout while preserving `.claude/memory/`, and remove the clean task worktree.

### Rollback

- Revert the Cratedigger merge commit and deploy the reverted signed pin if runtime harness behavior regresses.
- Disable/revert only the downstream tip timer if canary scheduling or resource use is faulty; this does not affect production acquisition.
- Never point production at a historical or tip package as a rollback mechanism.
- Manifest/check failures require fixing the adapter or explicitly revising the support contract in a reviewed issue; never silently delete a still-in-window release.

---

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| The 19-way matrix becomes an enormous closure | Disable unrelated plugin dependencies, use one focused contract per release, and keep full-suite qualification single-shot on production. |
| Current Nixpkgs recipe assumptions do not fit old/new source layouts | Make backend/source overrides explicit, qualify oldest and tip first, and fail named builds rather than patching upstream behavior silently. |
| Compatibility code hides unsupported behavior | Limit it to three enumerated seams, detect capabilities structurally, and fail unknown combinations before mutation. |
| A test touches live Beets authority | Scrub ambient variables, allocate all paths under a private temporary root, add path assertions, and run in Nix sandboxes. |
| Third-party stdout corrupts protocol before redirection | Keep `--help` before protocol mode, then reserve fd 1 before config read/plugin load/Library construction. |
| Tip failure freezes ordinary dependency updates | Use distinct input-specific runners, commits, and schedules sharing only a serialization lock and alert path. |
| A moving 730-day calculation makes evaluation unreproducible | Compute the window only in the explicit refresher with fixed-clock tests; commit immutable manifest results. |
| “Compatibility” is mistaken for safe live downgrade | Repeat the disposable-boundary disclaimer in issue, docs, check output, and rollout evidence. |
| LRCLIB grows into Cratedigger scope | Do not test or patch its service behavior; state it as an external plugin/config exclusion and reject scope creep in review. |

---

## Definition of Done

- The plan exists as the branch's first dedicated commit.
- All 19 current release entries are immutable, reproducible, and green on the focused real boundary.
- Locked tip is green for build, focused runtime behavior, and whole-repository Pyright.
- Production evaluation cannot resolve tip or historical packages.
- The known Beets 2.13 backend, stdout, and optional-ID regressions are pinned and green.
- Nixpkgs and tip updaters are independently mutable, serialized, fail-closed, and covered by executable fakes.
- Release refresh is deterministic and tested without network or wall-clock access in the pure core.
- LRCLIB and non-owned plugin behavior are explicitly outside the promise.
- Terra implementation and independent Sol review have converged with no unresolved material findings.
- The same clean committed HEAD has a receipt-backed final gate before first push.
- The PR is merged with a merge commit, the signed downstream pin is deployed, production remains on admitted Beets, the canary timer is live, a successor Cratedigger cycle succeeds, and #992 is closed with exact evidence.
