---
date: 2026-04-27
topic: guarded-beets-replacement
---

# Guarded Beets Replacement Requirements

## Problem Frame

The Palo Santo incident was ultimately caused by Beets reading an unsafe
duplicate configuration: `duplicate_keys` was placed outside `import:`, so
Beets fell back to matching duplicates by `[albumartist, album]` instead of
including `mb_albumid`. Cratedigger responded by making Beets' destructive
duplicate replacement path unrepresentable and adding its own post-import stale
row cleanup plus `beet move` canonicalization state machine.

That state machine now has real product cost. Album upgrades no longer behave
like Beets' original atomic delete-and-replace flow, folder names drift, and
Plex can retain old missing albums beside newly imported albums. The desired
direction is to restore Beets-owned replacement while keeping Cratedigger as a
guardrail: Beets owns the atomic operation, Cratedigger verifies the exact
delete set before allowing it.

The new invariant is:

```text
Beets owns atomic replacement.
Cratedigger validates Beets' would-remove set before answering remove.
```

---

## Actors

- A1. Operator: Wants upgrades to replace albums cleanly without creating
  lingering missing folders in Plex.
- A2. Cratedigger importer: Drives the Beets harness and records import
  outcomes in the pipeline audit trail.
- A3. Beets harness: Receives Beets duplicate callbacks and decides whether to
  allow Beets to remove duplicates.
- A4. Acquisition pipeline: Should keep searching for a valid source when one
  source trips the duplicate-remove guard.
- A5. Downstream library scanners: Plex and Meelo observe final Beets folder
  state and should not be handed avoidable path churn.

---

## Key Flows

- F1. Guarded same-release replacement
  - **Trigger:** A request import reaches Beets and Beets reports a duplicate
    during `resolve_duplicate`.
  - **Actors:** A2, A3
  - **Steps:** The harness serializes Beets' `found_duplicates` set, verifies
    it contains exactly one album, verifies that album matches the target
    release identity, then answers `remove` so Beets performs its normal
    duplicate replacement.
  - **Outcome:** Beets atomically replaces the old copy with the new copy, and
    Cratedigger records the successful import without running its old
    post-import move state machine.
  - **Covered by:** R1, R2, R3, R4, R5, R11

- F2. Unsafe duplicate set
  - **Trigger:** Beets reports zero duplicates, multiple duplicates, or a
    duplicate whose release identity does not match the target replacement.
  - **Actors:** A2, A3, A4
  - **Steps:** Cratedigger refuses to answer `remove`, fails the import attempt
    with `duplicate_remove_guard_failed`, deny-lists the source/user for the
    request, moves the staged files to a separate quarantine folder under the
    configured Incoming root, and persists the would-remove set for logs and
    Recents.
  - **Outcome:** The Beets library is untouched, the unsafe source is not
    retried, the request itself is left alone, and another source can be tried
    later.
  - **Covered by:** R6, R7, R8, R9, R10

- F3. One-release transition
  - **Trigger:** The guarded Beets-owned replacement path is deployed.
  - **Actors:** A1, A2
  - **Steps:** Keep the old Cratedigger stale-row cleanup and sibling
    canonicalization code in place for one release as explicitly temporary
    fallback code, but skip post-import `beet move` on successful guarded
    replacement. After one successful deployed release, remove the old state
    machine.
  - **Outcome:** The transition is reversible for one release, but the codebase
    has a loud and concrete removal trigger.
  - **Covered by:** R11, R12, R13

---

## Requirements

**Beets-owned replacement**

- R1. Cratedigger must restore Beets-owned duplicate replacement for safe
  same-release upgrades instead of always answering `keep`.
- R2. Before answering `remove`, Cratedigger must inspect the exact
  `found_duplicates` set Beets passed to the duplicate-resolution callback.
- R3. Cratedigger may answer `remove` only when the would-remove set contains
  exactly one album.
- R4. Cratedigger may answer `remove` only when that one album matches the
  target release identity for the import.
- R5. The Beets config guard requiring `import.duplicate_keys.album` to include
  `mb_albumid` must remain in place.

**Guard failure behavior**

- R6. If the duplicate-remove guard fails, Cratedigger must stop before Beets
  applies the destructive replacement.
- R7. Guard failure must fail the import attempt or import job with a specific
  reason, `duplicate_remove_guard_failed`.
- R8. Guard failure must not change the request's type, status, or long-lived
  intent; the request should remain eligible for normal acquisition from other
  sources.
- R9. Guard failure must deny-list the source/user that produced the risky
  staged candidate so the same source does not loop.
- R10. Guard failure must move the staged files out of active staging into a
  separate quarantine folder under the configured Incoming root, distinct from
  Wrong Matches.
- R11. Guard failure must persist and log the Beets would-remove set, including
  album ids, release ids where available, album paths, and item counts.

**State machine removal**

- R12. On successful guarded Beets-owned replacement, Cratedigger must not run
  its post-import `beet move` disambiguation or sibling canonicalization.
- R13. The existing Cratedigger stale-row cleanup and sibling canonicalization
  code may remain for one release only, marked loudly as temporary in code,
  docs, tests, and commit messaging.
- R14. After one deployed release where guarded Beets-owned replacement has
  imported at least one upgrade successfully and no unresolved guard failure
  requires fallback behavior, remove the old Cratedigger replacement state
  machine.

---

## Acceptance Examples

- AE1. **Covers R1, R2, R3, R4, R12.** Given Beets reports exactly one
  duplicate album whose release id matches the target import, when the harness
  resolves the duplicate, Cratedigger answers `remove`, Beets performs the
  replacement, and Cratedigger does not run post-import `beet move`.
- AE2. **Covers R3, R6, R7, R9, R10, R11.** Given Beets reports two duplicate
  albums before removal, when the harness evaluates the set, the import attempt
  fails with `duplicate_remove_guard_failed`, the source is deny-listed, staged
  files move to `Incoming/duplicate-remove-guard/`, and the Beets library is
  not mutated.
- AE3. **Covers R4, R6, R7, R11.** Given Beets reports exactly one duplicate
  album but its release identity does not match the target import, when the
  guard runs, Cratedigger fails closed and records the mismatched album id,
  release id, and path.
- AE4. **Covers R8, R9.** Given a guard failure on one source for a request,
  when the import job finishes, the request itself is not converted to manual
  or otherwise retyped, but the failed source is excluded from future retries.
- AE5. **Covers R13, R14.** Given the first release with guarded Beets
  replacement has been deployed and tested through one successful upgrade, when
  the follow-up cleanup happens, the temporary Cratedigger stale cleanup and
  sibling move code is deleted rather than kept as permanent fallback.

---

## Success Criteria

- Album upgrades behave like Beets' original atomic replacement flow again.
- Plex no longer sees avoidable old missing albums caused by Cratedigger's
  delete-later and move-later behavior.
- A Palo Santo-class unsafe duplicate set fails before any library mutation,
  with enough audit detail to diagnose the Beets delete set.
- The old Cratedigger replacement state machine has a concrete removal trigger
  and does not become permanent architecture by inertia.

---

## Scope Boundaries

- This does not remove the Beets duplicate-key startup/config guard.
- This does not change quality policy, import matching thresholds, or source
  selection policy.
- This does not add UI for the guard failure quarantine path in v1.
- This does not change the request to manual on guard failure.
- This does not reuse Wrong Matches storage; guarded duplicate failures get a
  separate Incoming quarantine folder.
- This does not run whole-library path repair as part of the code change. A
  separate operator step can run `beet move -p -a` and then real `beet move -a`
  after preview.

---

## Key Decisions

- Beets should own replacement again because the original atomic behavior is
  the desired product behavior when Beets' duplicate configuration is correct.
- Cratedigger should guard the delete set, not reimplement replacement.
- Guard failures are source failures, not request-shape changes.
- Staged files from guard failures should be preserved but removed from active
  staging so they do not retry.
- The old state machine gets one release of temporary overlap only.

---

## Dependencies / Assumptions

- `harness/beets_harness.py` can serialize the `found_duplicates` set before
  choosing `remove`.
- Beets' duplicate callback set is the same album set Beets will use for
  duplicate removal when `task.should_remove_duplicates = True`.
- Existing denylist behavior can represent this as a source/user failure for a
  request.
- Existing import job failure/audit paths can carry the guard payload without a
  new UI surface.
- The configured Incoming root is the right parent for
  `duplicate-remove-guard/` quarantine.

---

## Outstanding Questions

### Resolve Before Planning

- None.

### Deferred to Planning

- [Affects R4][Technical] Define exact release-identity matching for
  MusicBrainz and Discogs rows, including empty `mb_albumid` cases.
- [Affects R10][Technical] Choose the exact quarantine folder naming scheme
  under `Incoming/duplicate-remove-guard/`.
- [Affects R11][Technical] Decide whether item paths are persisted in full or
  capped in the DB payload while full detail goes to logs.
- [Affects R13, R14][Technical] Decide the exact feature flag or code branch
  that skips post-import `beet move` during the one-release overlap.

---

## Next Steps

-> /ce-plan for structured implementation planning.
