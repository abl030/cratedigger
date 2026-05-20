---
name: no-bridge-do-backfill
description: "When changing what gets persisted, don't write adapter/bridge code that handles both shapes. Backfill old rows to the new shape when possible."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c26c0916-3d19-47dc-b8d8-33bdff7f7404
---

When a change adds or broadens what we persist (a new column, a wider helper that captures more data per row, a new audit field), do **not** write code that lets old-shape and new-shape rows coexist behind a compat layer. Instead, **backfill** the old rows so the whole DB looks like the new world, and write the producer/consumer code as if only the new shape ever existed.

**Why:** Bridge/compat code is an attractive nuisance — it doubles the surface area of the change, hides bugs behind a "tolerant" layer, and becomes load-bearing for years after it should have been deleted. A one-shot backfill (a migration, a script, or a re-derive pass) is cleaner: the bridge exists for one commit, then dies. The producer code, the consumer code, and every downstream reader only ever see one shape.

**How to apply:**
- Producer change → make the producer wider. Don't `COALESCE(new_col, derive_from_old_col)` in the read path. Don't `if row.has_new_field: ... else: ...` in the consumer.
- Then, if the data can be recomputed or re-derived for pre-change rows, **do it**. Examples: re-classify from JSONB blobs, re-probe from on-disk files, run a migration that fills the new column from the same source the producer would.
- The backfill is its own thing — a script, a migration, a one-off SQL — not a runtime fallback. Once it runs, delete it (or leave it gathering dust; it won't run again).
- The consumer code still has to tolerate `None`/missing for rows that *legitimately* can't be derived (e.g. files no longer on disk, evidence lost), but that's the only concession. Not a compat layer; just normal nullability.
- One exception: the CLAUDE.md "lossless-source-gated propagation" policy is explicitly forward-only (no backfill) because re-deriving would require re-measuring transcoded audio against vanished source material. When backfill is impossible, say so and move on.

Related: [[finish-the-job]] (this is its dual — finish wiring forward, then drag the legacy data along with one decisive backfill).
