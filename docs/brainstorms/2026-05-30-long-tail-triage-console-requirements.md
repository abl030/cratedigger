---
date: 2026-05-30
topic: long-tail-triage-console
---

# Long-Tail Triage Console

## Summary

A new long-tail worklist under the Pipeline tab that opens on the `wanted` set, with
quality-band filter tabs (`Missing`, `Transparent`, `Excellent`, …) and a search box.
Selecting a release opens an action console that explains why the request is stuck —
the unfindable buckets, the Soulseek peers actually seen and why each was rejected,
which sibling pressings exist, and what YouTube Music has — and lets the operator act on
it inline: rescue off YouTube, accept a sibling pressing, set quality intent, or
re-search. This is the first consumer of three already-built backends (search-analytics
/ triage, the YouTube resolver, and the YouTube rescue ingest API); no new pipeline
behavior is introduced.

---

## Problem Frame

The operator has built three backends — the triage / search-analytics surface, the
YouTube Music resolver, and the YouTube rescue ingest API — but none of them has a UI
consumer yet. They answer powerful per-request questions ("why can't we find this?",
"what does YouTube Music have for this MBID?", "rescue it off YouTube") but only from the
CLI or via raw HTTP. There is no place in the app where the operator can stand in front
of the long tail as a *cohort* and work it down.

And the long tail is real and measurable. Of 820 `wanted` requests, 391 currently carry
an unfindable category and 454 already have a provisional copy on disk at some quality
band — meaning more than half the `wanted` set is "have something, want better" rather
than "have nothing." The operator's instinct is not to rank these on one forced ladder
but to *select* a band — start with `Missing`, then climb the quality levels — and focus
attention where it pays off most: the things that are missing or lowest-quality on disk.

The pain is that answering "what's missing, and what can I do about each one" today means
bouncing between CLI commands (`pipeline-cli triage show`, `youtube-album`,
`youtube-rescue`), the pipeline detail panel, and the Replace picker — with no single
surface that shows the evidence and offers the verbs in one place. The long-tail-rescue
narrative this product is built around (a `wanted`-for-weeks request finally importing
because the operator found it on YouTube Music and clicked rescue) has no front door.

---

## Key Decisions

- **A cohort worklist, not a per-release popup.** The brainstorm started as "click any
  release anywhere → popup explorer" and deliberately pivoted. Discoverability of the
  long tail is a cohort problem — the operator wants to stand in front of "everything I
  can't find" and triage it, not open one release at a time from scattered contexts. The
  per-release explorer survives only as the in-place detail that expands when a row in
  this view is selected.

- **Scoped to the `wanted` set.** The view opens on the ~820 `wanted` requests, not the
  ~3,500 `imported` "done" pile. This keeps it a focused worklist instead of a
  whole-library health dashboard. The imported collection re-ranked by quality is a
  separate, deferred idea.

- **Quality bands are selectable filters, not a forced ranking.** Filter tabs across the
  top — `Missing` plus the on-disk `QualityRank` bands — let the operator pick a band and
  see only that cohort. `Missing` is the floor (a `wanted` request with nothing on disk);
  the higher tabs are `wanted` requests whose best on-disk copy lands in that band but is
  still open because the operator wants better.

- **Action console, not a read-only lens.** Selecting a release shows the evidence *and*
  the verbs inline. This is where all three backends finally pay off in one place — the
  YouTube rescue button is only useful here. The operator accepted the larger build over
  a read-only triage lens because reading-without-acting would just relocate the
  CLI-bouncing problem into the UI.

- **The quality gate does not block first acquisition; rescue needs no bar-drop.** The
  gate is comparative (never replace a better copy with a worse one), not an absolute
  floor applied to an empty slot. Per `docs/quality-ranks.md`, unconstrained Opus 128
  classifies as `TRANSPARENT` (rank 60), which sits *above* the default `EXCELLENT` (rank
  50) gate floor. A YouTube rescue therefore imports cleanly and lands in the
  `Transparent` band — it needs no "lower the bar" step. This explicitly **corrects** the
  assumption recorded in `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md`
  ("the default EXCELLENT gate rejects it … auto-import requires the operator to pre-lower
  min_bitrate"), which was wrong.

- **Evidence in, operator decides.** Every console action is operator-initiated and the
  view never auto-substitutes a pressing or auto-rescues. This inherits the archivist
  invariants (strict pressing identity; the system never auto-decides anything
  irreversible; the system never stops searching).

---

## Actors

- A1. Operator — opens the worklist, filters by band, searches, selects a release, and
  fires console actions.
- A2. Search-analytics / triage backend — supplies the per-request unfindable category,
  field-resolution telemetry, and search forensics (which Soulseek peers were seen and
  why each was rejected).
- A3. YouTube resolver — supplies the (YT sibling × MB pressing) distance matrix for a
  release group. Must have been run for a release group before a rescue is possible.
- A4. YouTube rescue ingest — executes a rescue against an operator-chosen `browse_id`
  from the resolver matrix.
- A5. Importer worker — drains the resulting import and owns every grind-up / quality /
  wrong-matches decision downstream. Unchanged by this work.

---

## Key Flows

- F1. Open the worklist and triage by band
  - **Trigger:** operator opens the long-tail sub-view under the Pipeline tab
  - **Actors:** A1, A2
  - **Steps:** the view loads the `wanted` set; band filter tabs are shown (`Missing` +
    on-disk quality bands); operator selects a band and the list filters to that cohort
  - **Outcome:** the operator sees only the cohort they want to work (e.g. everything
    `Missing` on disk)
  - **Covers:** R1, R2, R3, R4

- F2. Find a specific release
  - **Trigger:** operator types into the search box
  - **Actors:** A1
  - **Steps:** the list filters to releases matching the query within the current scope
  - **Outcome:** the operator jumps straight to a known release without scrolling the
    cohort
  - **Covers:** R5

- F3. Select a release — the console renders evidence
  - **Trigger:** operator selects a row
  - **Actors:** A1, A2, A3
  - **Steps:** the row expands into the action console; the console shows the
    band-appropriate evidence — for a `Missing` / unfindable item: the unfindable
    bucket(s), the Soulseek peers actually seen and their rejection reasons, the sibling
    pressings that exist, and (if resolved) the YouTube Music matrix; for an
    on-disk-below-intent item: current band vs. intent plus what better is available
  - **Outcome:** the operator understands *why* the item is where it is, in one place
  - **Covers:** R6, R7, R8

- F4. Rescue off YouTube (two-step)
  - **Trigger:** operator wants the YouTube copy of a `Missing` item
  - **Actors:** A1, A3, A4, A5
  - **Steps:** if no resolver matrix is cached for the release group, the console offers a
    "check YouTube" step that runs the resolver; the operator then picks a YT sibling from
    the matrix and confirms rescue; the rescue ingest runs; on success the importer
    imports it (clean, no bar-drop) and the item moves to the `Transparent` band
  - **Outcome:** a long-tail item the operator could not find on Soulseek is imported from
    YouTube Music
  - **Covers:** R9, R10, R11, R12

- F5. Accept a sibling pressing
  - **Trigger:** the exact pressing is unfindable but a sibling is available, and the
    operator chooses to accept it
  - **Actors:** A1
  - **Steps:** the operator picks an available sibling pressing from the console and
    confirms; the request is replaced to point at the chosen pressing (the existing
    Replace operator action)
  - **Outcome:** the request re-sources against a pressing the network actually has, by
    explicit operator choice — never an automatic substitution
  - **Covers:** R13

- F6. Set quality intent / re-search
  - **Trigger:** operator wants to change how hard the system keeps grinding, or to kick a
    search now
  - **Actors:** A1
  - **Steps:** the operator sets the request's quality intent (keep grinding toward
    lossless, or accept the current band as the floor) and/or triggers an immediate
    re-search from the console
  - **Outcome:** the operator steers the watch loop without leaving the console
  - **Covers:** R14, R15

---

## Requirements

**The worklist view**

- R1. A new sub-view exists under the Pipeline tab dedicated to the long tail. It is
  reachable from the existing tab/sub-view navigation.
- R2. The view's default scope is the `wanted` set. `imported`, `manual`, `downloading`,
  and `replaced` requests are not part of this worklist.
- R3. The view presents quality-band filter tabs: `Missing` plus each on-disk
  `QualityRank` band present in the cohort. Selecting a tab filters the list to that
  cohort. Bands are selectable filters, not a forced sort order.
- R4. `Missing` means a `wanted` request with no acceptable audio on disk. Each on-disk
  band means a `wanted` request whose current best on-disk copy classifies into that band
  (still open because the operator wants better).
- R5. The view has a search box that filters the current cohort to releases matching the
  query (by artist / album).

**The action console (release detail)**

- R6. Selecting a release expands it in place into an action console (reusing the app's
  existing in-place detail / modal substrate, not a new bespoke overlay paradigm).
- R7. For a `Missing` / unfindable release, the console surfaces: the unfindable
  category and its reason (the 4-bucket taxonomy), the Soulseek peers actually seen for
  recent searches and why each was rejected, the sibling pressings that exist for the
  release group, and — when the YouTube resolver has been run — the YouTube Music distance
  matrix.
- R8. For an on-disk-below-intent release, the console surfaces the current band versus
  the operator's intent and what better is available (peers seen, sibling pressings,
  YouTube). The console adapts its emphasis to the release's state rather than showing an
  identical panel for every row.

**Console actions**

- R9. The console offers a YouTube rescue action for a release. When no resolver matrix
  is cached for the release group, the console first offers a step that runs the resolver;
  rescue is only offered against a resolver-supplied `browse_id`.
- R10. The rescue action targets a specific operator-chosen YouTube sibling from the
  matrix. The view never auto-picks a sibling.
- R11. A YouTube rescue requires no quality-bar adjustment to import. The view must not
  add a "lower the bar before rescue" step.
- R12. After a successful rescue + import, the release reflects its new on-disk band
  (e.g. moves into the `Transparent` tab). The long-tail-rescue audit (`rescued_at` +
  `prior_unfindable_category`) is populated by the existing import path, not by this view.
- R13. The console offers an "accept a sibling pressing" action that replaces the request
  to target an operator-chosen available sibling, via the existing Replace operator
  action. The choice is always explicit; no sibling is ever auto-substituted.
- R14. The console offers a quality-intent control for the request (keep grinding toward
  lossless vs. accept the current band as the floor).
- R15. The console offers a re-search action that triggers a search for the request
  without waiting for the next watch-loop cycle.

**Surface symmetry**

- R16. Any new backend list/filter or read surface introduced specifically for this view
  follows the project's CLI ⇄ API symmetry convention. The console's mutating actions
  (rescue, replace, set-intent, re-search) already have CLI counterparts and must continue
  to route through the same service-layer methods rather than gaining a parallel path.

---

## Acceptance Examples

- AE1. Covers R2, R3, R4. Given the worklist is open, when the operator selects the
  `Missing` tab, the list shows only `wanted` requests with no acceptable audio on disk —
  not `imported` albums and not `wanted` requests that already have an on-disk copy.

- AE2. Covers R3, R4. Given the worklist is open, when the operator selects the
  `Transparent` tab, the list shows only `wanted` requests whose current best on-disk copy
  classifies as `Transparent` (the "have a decent copy, still want lossless" cohort).

- AE3. Covers R6, R7. Given a `Missing` request that has been categorised
  `wrong_pressing_available`, when the operator selects it, the console shows that category
  and reason, the Soulseek peers seen with their rejection reasons, and the sibling
  pressings that exist — making it obvious that the exact pressing is unavailable but a
  sibling is.

- AE4. Covers R9. Given a release whose release group has never been run through the
  YouTube resolver, when the operator opens the console, the YouTube section offers a
  "check YouTube" step rather than a one-click rescue. After the resolver runs, the matrix
  of YT siblings is shown and each becomes a rescue target.

- AE5. Covers R10, R11, R12. Given a 10-track `Missing` request whose release group is
  resolved and has a matching YT sibling, when the operator picks that sibling and confirms
  rescue, the rescue ingest runs and the album imports with no bar-drop step; afterward the
  release appears under the `Transparent` tab.

- AE6. Covers R13. Given a `Missing` request whose exact pressing is unfindable but a
  sibling pressing exists, when the operator picks the sibling and confirms "accept this
  pressing," the request is replaced to target the chosen sibling MBID and re-sources on
  the next cycle. No substitution happens without that explicit confirmation.

---

## Scope Boundaries

- The ~3,500 `imported` albums re-ranked by quality (a whole-collection health
  dashboard) is deferred. This view is the `wanted` worklist only.
- The original "click any release anywhere → popup explorer" framing is dropped. The
  only detail surface this work adds is the in-place console that expands when a row in
  this view is selected.
- No new pipeline behavior. The three backends (triage / search analytics, resolver,
  rescue ingest) and the importer are consumed as-is. The only possible backend addition
  is a thin band-filterable `wanted` list surface if one does not already exist (see
  Dependencies).
- No "lower the quality bar before rescue" step — the gate does not block first
  acquisition and Opus 128 is `Transparent`.
- No automatic / background rescue. Rescue stays operator-initiated, consistent with the
  rescue ingest API's own scope.
- The watch loop is never throttled by this view. Surfacing the unfindable cohort is the
  point; search cadence is unchanged.

---

## Dependencies / Assumptions

- D1. The triage / search-analytics surface exists and supplies the console's evidence:
  unfindable category, field-resolution telemetry, and search forensics (peers seen +
  rejection reasons). See `lib/triage_service.py` and the `GET /api/triage/<id>` endpoint.
- D2. The YouTube resolver exists (`GET /api/youtube-album`, `lib/youtube_album_service.py`)
  and must be run for a release group before a rescue is possible — the rescue API rejects
  submissions with no resolver mapping.
- D3. The YouTube rescue ingest API exists (`POST /api/pipeline/<id>/youtube-rescue`,
  `lib/youtube_ingest_service.py`) and is the only path the rescue action calls.
- D4. The Replace operator action exists (`lib/mbid_replace_service.py`, the
  `web/js/replace_picker.js` modal) and is the only path "accept a sibling pressing"
  calls. Sibling enumeration is available via `GET /api/release-group/<rg>`.
- D5. The `QualityRank` classifier and band table exist (`docs/quality-ranks.md`,
  `lib/quality.py`). Per-request on-disk quality fields (`min_bitrate`,
  `current_spectral_grade`, `current_spectral_bitrate`) already exist on `album_requests`,
  so banding the `wanted` set is feasible from existing data.
- D6. Whether a band-filterable `wanted` *list* endpoint already exists is unverified. The
  raw quality fields are present per request, but a surface that returns the `wanted`
  cohort filtered/grouped by computed band may be a thin new addition or a client-side
  banding over the existing pipeline list. Planning verifies this against the codebase
  early, since R3/R4 depend on it.
- D7. Set-intent (the request `target_format` toggle) and re-search both have existing
  operator surfaces; the exact CLI/web entry points are confirmed during planning.
- D8. The existing in-place detail pattern and the Promise-based Replace-picker modal are
  the UI substrate for the console; no generic modal primitive exists, so the console
  follows the Replace-picker pattern.

---

## Outstanding Questions

### Deferred to planning

- Q1. [Affects R3, R4, R6] Does a band-filterable `wanted` list surface already exist, or
  is banding done client-side over the existing pipeline list, or is a thin new endpoint
  warranted? Verify against the codebase before building the view. If a new endpoint is
  added, R16 (CLI ⇄ API symmetry) applies.
- Q2. [Affects R3] Exact band tab set and whether each tab shows a live count of its
  cohort. The band names come from `QualityRank`; whether to show counts is a UI call.
- Q3. [Affects R1] Default band on open — show the whole `wanted` set with bands as
  optional filters, or default to `Missing` first. The dialogue leaned toward "opens on
  the `wanted` set, then click a band."
- Q4. [Affects R15] The exact mechanism behind "re-search now" (trigger a cycle vs. a
  search-plan advance/regenerate) and what feedback the console shows while it runs.
- Q5. [Affects R6, R7, R8] Console layout — how the unfindable reason, the Soulseek
  peers-seen list, the sibling pressings, and the YouTube matrix coexist without
  overwhelming the panel, and how the panel adapts between the `Missing` and
  on-disk-below-intent cases.

---

## Sources / Research

- `docs/brainstorms/2026-05-27-youtube-music-album-resolver-requirements.md` — the
  resolver this console drives (matrix shape, cache semantics, "surface evidence, decide
  nothing").
- `docs/brainstorms/2026-05-28-youtube-rescue-ingest-api-requirements.md` — the rescue
  ingest API this console fires. Note: its stated gate assumption (EXCELLENT gate rejects
  Opus 128, must pre-lower `min_bitrate`) is corrected by this brainstorm; see Key
  Decisions.
- `docs/quality-ranks.md` — the `QualityRank` band table. `TRANSPARENT` (60) > `EXCELLENT`
  (50); `Opus 128+` → `TRANSPARENT`. Grounds both the band tabs and the no-bar-drop
  decision.
- Triage / unfindable subsystem — `lib/triage_service.py`, `lib/unfindable_detection_service.py`,
  and the `CLAUDE.md` "Triage subsystem" / "Unfindable detection" entries. Supplies the
  4-bucket taxonomy and search forensics the console renders.
- Search forensics — `search_log.candidates` (top-20 peers seen, with filetype /
  matched-tracks / avg-ratio), `search_log.rejection_reason`, and the
  `request_search_summary` view. The raw material for "what's actually on the Soulseek
  network for this album."
- Existing UI substrate — `web/js/replace_picker.js` (the only reusable modal pattern),
  the in-place `.p-detail` / `.release-detail` expansion pattern, `web/js/pipeline.js`
  (the Pipeline tab this sub-view joins), and `web/routes/_overlay.py` (the on-disk /
  quality overlay that stamps release rows).
- Live data grounding (2026-05-30): 820 `wanted` requests — 454 (55%) with audio on disk,
  391 with an unfindable category — versus 3,561 `imported`. Confirms the `wanted` set
  genuinely spans `Missing` plus on-disk bands, and that the worklist is a tractable size.
