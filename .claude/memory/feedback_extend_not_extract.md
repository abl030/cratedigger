---
name: feedback-extend-not-extract
description: "When integrating a new caller against an existing service API, default to minimal extension (one new parameter / alternative input mode), not helper extraction or restructuring"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 740dc87f-bc63-476a-b5b2-3cb425f27455
---

When a new feature needs the same logic an existing service-layer API already provides, the default move is to extend that API minimally — add an optional parameter, support an alternative input mode — not to extract a shared helper and refactor the existing caller.

**Why:** On 2026-05-27, planning the YouTube Music album API, the user pointed out that `lib/beets_distance.py::compute_beets_distance` is already the Replace picker's scoring API (CLI ⇄ API symmetry already in place, typed result, cache protocol, mb_get_release injection). I proposed extracting a `score_items_against_release` helper so both callers (download_log path + YT Music path) could feed it items from different sources. The user pushed back: "why are you trying to reinvent the wheel? we extend the api if we need to ... can't we just do that?" The minimal extension is one new optional parameter (`items_override: list[ItemRecord] | None`) that lets the caller pass items directly instead of loading them from a download_log's on-disk folder. The current caller's behavior is unchanged when the parameter is absent.

**How to apply:** When a new feature wants the logic an existing service already implements, sketch the smallest extension to that service's *signature* before sketching any internal restructuring. If the extension is one optional parameter or one alternative input branch, that's the answer — ship it. Reach for helper extraction only when the extension genuinely doesn't fit (two truly orthogonal scoring shapes, etc.) AND the existing caller would benefit from the extracted form. Connects to [[feedback-single-operator-no-backfill-scripts]] — same instinct, different domain: don't accumulate structural changes the use case doesn't demand.
