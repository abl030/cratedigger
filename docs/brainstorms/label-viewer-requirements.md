---
date: 2026-04-29
topic: label-viewer
---

# Label Viewer

## Problem Frame

When the user finds a release they like — e.g. Gridlock on Hymen Records — they want to see what else came out on that label, and quickly tell which of those they already own and which they haven't heard. Today the cratedigger web UI surfaces labels only as plain-text fields on Discogs release payloads (`web/discogs.py:231`, `web/discogs.py:282`); there is no way to navigate from a release to the rest of its label, no way to search by label name, and no library overlay to mark "already mine". The workaround is leaving cratedigger and using Discogs.com directly, which loses the library overlay that makes the local mirror valuable in the first place.

This is fundamentally a discovery surface, not a navigation surface — the goal is "what I haven't heard but probably want to", not "let me look up Warp's catalogue". That framing drives the filter priorities below.

Origin: GitHub issue #183.

---

## Key Flows

- F1. Drill-in from a release
  - **Trigger:** User is viewing a release in the browse tab or library and clicks the label name.
  - **Steps:** Label page opens with the label's name, optional metadata (country, parent label if applicable), and the full release list. Library overlay marks each release as `in library` / `in pipeline` / `not held`. Year and format filters available; sort newest-first by default.
  - **Outcome:** User can scan the label end-to-end and instantly see which releases are new to them.
  - **Covered by:** R1, R2, R5, R6, R7, R9, R10

- F2. Top-down label search
  - **Trigger:** User types a label name (e.g. "Hymen") into a search box in the browse tab.
  - **Steps:** Search returns a disambiguation list of matching label entities with enough context to pick the right one (release count, country, parent label where present). User clicks a result to land on the same label page from F1.
  - **Outcome:** User reaches the label page without first needing to find a release on it.
  - **Covered by:** R3, R4, R5

- F3. Era-bounded browse on a large label
  - **Trigger:** User is on a label page for a high-volume label (Warp, Blue Note) where the flat list is unwieldy.
  - **Steps:** User applies year filter and/or format filter (album / EP / single, optionally physical format) to narrow the list. Library overlay still applies.
  - **Outcome:** User sees a scoped, scannable list for that era / format.
  - **Covered by:** R6, R7, R9

---

## Requirements

**Search and navigation**
- R1. A release detail view that today shows label names as plain text MUST expose each label as a clickable link to that label's page.
- R2. The label page MUST be reachable directly by URL (deep-linkable, shareable, refresh-safe).
- R3. The browse tab MUST offer label search alongside the existing artist search, returning a disambiguation list of matching label entities.
- R4. Label search results MUST include enough context to disambiguate similarly-named labels: at minimum release count and country; parent label name shown when the result is a sub-label.

**Label page content**
- R5. The label page MUST list every release on the label that the data source knows about, paginated or virtualized for labels with hundreds or thousands of entries.
- R6. Each release row MUST show release title, primary artist, year, and format/type, and link to the existing release detail page.
- R7. The label page MUST split releases by primary type the same way the artist discography view does (album / EP / single / other), preserving the existing visual conventions.

**Library overlay (primary discovery filter)**
- R8. Each release on the label page MUST be marked with its current library status: `in beets library`, `in pipeline (any non-imported state)`, or `not held`. The marking MUST be visually scannable, not require hover.
- R9. The label page MUST allow filtering to "not held" releases as a single-click toggle, since this is the dominant discovery move.

**Secondary filters**
- R10. The label page MUST provide year and format/type filters that compose with the library overlay (all three layered).
- R11. Default sort MUST be year descending (newest first); within the same year, secondary sort is stable but unspecified.

**Source-agnostic plumbing (enables Phase B without rework)**
- R12. The label entity model exposed to the UI MUST be source-tagged (e.g. `{source: "discogs" | "musicbrainz", id, name, ...}`), not Discogs-shaped, even though only Discogs is wired in v1.
- R13. The label page route MUST accept a source-qualified identifier, so adding MusicBrainz later is a new source adapter behind the same route shape rather than a parallel route.
- R14. The release-list rendering on the label page MUST work from a source-agnostic release shape compatible with the artist discography view's existing release shape, so the same component renders MB and Discogs label release lists when MB is added.
- R15. The "add MusicBrainz label support" follow-up MUST NOT require schema, route, or component changes to anything shipped in v1 — only a new source adapter implementation and the search-side wiring.
- R16. The source-agnostic shape MUST be defined to accommodate what the upstream MusicBrainz API actually returns, not what we'd prefer it to return. The MusicBrainz mirror runs the upstream MusicBrainz Server codebase and we cannot modify its API surface; the Discogs mirror is ours and we can extend its endpoints freely. This asymmetry MUST be reflected in the v1 design: any normalization or denormalization required to hit the source-agnostic shape happens in our Python web layer (per-source adapter), not server-side.

---

## Acceptance Examples

- AE1. **Covers R1, R8, R9.** Given the user is viewing a Gridlock release on Hymen Records, when they click "Hymen Records" in the labels field, then they land on the Hymen label page showing every Hymen release, with their owned Gridlock releases marked `in library`, the rest marked `not held`, and a one-click "hide held" toggle visible.
- AE2. **Covers R3, R4.** Given the user types "warp" in label search, when results return, then they see "Warp Records (UK)", "Warp Singles (UK, sub-label of Warp Records)", and any other near-name labels with their release counts, sufficient to pick the intended entity.
- AE3. **Covers R6, R10.** Given the user is on the Warp Records label page, when they apply year filter `2001-2003` and format filter `EP`, then the list shows only Warp EPs released in that range, with library overlay still applied.
- AE4. **Covers R12, R13, R14, R15.** Given Phase B (MusicBrainz label support) is later attempted, when the developer adds an MB label adapter, then no v1 route, page component, or release-list rendering needs to be modified — the addition is wiring, not redesign.

---

## Success Criteria

- The user reaches an unfamiliar release on a label they like and, within one click + a quick scan, can identify what else exists on that label and which of those they don't own.
- For the Hymen / Gridlock concrete case from this brainstorm, the full label catalogue is visible on a single page with library overlay correct.
- A planner picking this up can implement v1 (Approach A) without inventing product behavior, filter semantics, or scope boundaries.
- When Phase B (MusicBrainz labels) is later planned, it is clearly a wiring task — the v1 brainstorm doc and code make obvious where the MB adapter plugs in.

---

## Scope Boundaries

- **No labelmate-recommendation sort in v1.** The "rank releases by labelmates of artists in your library first" surface (Approach D in brainstorm) is rejected for v1. May revisit if flat list + filters proves thin in practice.
- **No MusicBrainz label support in v1.** Phase B is a separate follow-up. The v1 plumbing must be source-agnostic enough that B is wiring, but B itself is out of scope here.
- **No cover art on Discogs labels in v1.** The Discogs CC0 dump has no images (already a known limitation, issue #82). Any image on the label page is best-effort or absent.
- **No editorial label metadata.** Bios, discography summaries, "label history" prose — not in scope. v1 is a release index, not a label profile.
- **No multi-label intersection ("releases on Hymen AND in genre X").** Genre/style filtering is not a v1 axis even though Discogs has the data.
- **No cross-label artist navigation enrichment.** "What other labels has this artist been on?" is the artist view's job, not the label viewer's.

---

## Key Decisions

- **Discogs first, MB next, no toggle in v1**: Discogs has materially better label data (sub-labels, catalog numbers, format detail) and the user's driving use cases (Hymen, Gridlock, IDM/electronic boutique labels) are Discogs-native. Symmetry with the artist-view source toggle is deferred to Phase B rather than blocking v1.
- **Library overlay is the load-bearing filter, not year**: The actual user goal is "what I haven't heard", not "what came out in year X". Year and format are secondary refinements that compose with the overlay; the overlay is what makes flat lists tolerable on big labels.
- **Source-agnostic plumbing from day one**: Even though only Discogs is wired in v1, the entity model, route shape, and rendering components are designed so MB is a source adapter, not a re-architecture. This is a directive from the user, not a speculative future-proofing call.
- **API ownership asymmetry shapes the design**: We own the Discogs mirror API (Rust, can add `/api/labels` and any other endpoints freely) but we do NOT own the MusicBrainz API — the local mirror runs upstream MusicBrainz Server and we consume whatever its public API gives us. v1 should add whatever endpoints we want to the Discogs mirror but design the source-agnostic shape to fit what the MB API actually returns when Phase B comes around. Adapter logic lives in our Python web layer (`web/discogs.py`, future `web/mb.py` label code), not in either mirror.
- **No labelmate recommendations**: Considered as Approach D, dropped. Library overlay + year + format are sufficient for the discovery workflow on the user's library scale; the recommendation surface added design complexity for a benefit that isn't yet observed to be missing.

---

## Dependencies / Assumptions

- The Discogs mirror at `discogs.ablz.au` already exposes label data on every release (verified — `web/discogs.py:231`, `web/discogs.py:282`). A new label search endpoint and label-detail endpoint on the Rust mirror will need to be added — the existing `web/discogs.py` only exposes release and artist search (verified by grep). The Rust mirror is ours; we extend it as needed.
- The beets library DB join needed for the library-overlay marking is already used elsewhere (e.g. `web/routes/library.py`) and can be reused.
- The local MusicBrainz mirror at `192.168.1.35:5200` runs the upstream MusicBrainz Server codebase. **We do not own the MB API and cannot modify it.** Phase B must consume whatever MB's stock label endpoints return (label search by name, label-detail with releases). It is **assumed but not verified** that those endpoints exist and return enough data to populate the label page; verifying this is a Phase B planning task. v1 does not depend on it.
- Cardinality assumption: small/boutique labels (the dominant case in this user's collection) have <500 releases; large labels (Warp, Blue Note) may have thousands. v1 design must remain usable at both scales — pagination/virtualization is a planning concern, not a product one.

---

## Outstanding Questions

### Resolve Before Planning

(none — all product decisions resolved.)

### Deferred to Planning

- [Affects R5][Technical] How to paginate / virtualize the release list for high-volume labels (Warp, Blue Note) — page size, infinite scroll vs paged, default visible count. Not a product decision; pick whatever matches existing artist-view conventions.
- [Affects R3, R4][Needs research] Whether the Discogs mirror's existing search infrastructure can serve label search, or whether label search needs its own indexed endpoint on the Rust API.
- [Affects R5, R7][Needs research] Sub-label rollup behavior: when the user lands on a parent label like "Warp Records", should the release list include sub-label releases by default with a sub-label badge, or only direct-parent releases with sub-labels reachable via a separate link? Recommended default: include sub-label releases with a badge, since the discovery workflow benefits from breadth — but verify against Discogs data shape during planning.
- [Affects R12-R15][Technical] What exact source-agnostic shape the label entity should take. Pick a shape that minimally generalizes Discogs label payloads while leaving room for MB label fields (e.g. label MBID, MB label-type, MB area). Should not block v1; can be settled at planning time.

---

## Next Steps

-> `/ce-plan` for structured implementation planning of v1 (Approach A: Discogs-first label viewer).
