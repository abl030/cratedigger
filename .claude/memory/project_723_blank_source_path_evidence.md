---
name: project-723-blank-source-path-evidence
description: PR
metadata: 
  node_type: memory
  type: project
  originSessionId: 202984fb-8406-4d4f-a5a4-f5a6dfcbcf61
---

**RCA (dl 37206, French Quarter, req 3150):** legacy 2026-05 backfill current-evidence rows with `source_path=''` could never be enriched (every persist guard compares scanned path vs recorded path), and a HAVE side with no spectral silently disables ALL THREE spectral protections (stage-1 reject, shared clamp, transcode-rank-regression) → ~96k transcode imported over ~160k copy as "better" avg tiebreak. Force imports DO honor downgrade verdicts — the verdict itself was blind. The reuse fast path also scanned the HAVE for audit only, never persisting.

**Fix shipped v2026.07.16 (PR #723):** `policy_incomplete_reasons()` rejects blank source_path (decider refuses via `_require_evidence_ready`; action+preview loaders rebuild from beets; content-address upsert repairs `source_path` in place); reuse fast path persists its HAVE scan pre-decision. One-shot rebuilt 57 wanted rows on doc2 (1 `replaced` row left frozen).

**Arcade Fire B-Sides (dl 37201) is the mirror image and NOT a bug:** fake CBR-320 (likely_transcode/96) correctly demoted by the transcode override once HAVE enrichment (#717) filled its spectral; honest genuine 156k avg replaced it ("Replaced unverified CBR"). Its June "downgrade" rejections were the wrong (blind) verdicts. Operator confirmed this behavior is desired.

**Why:** blind-HAVE comparisons corrupt verdicts in BOTH directions (let bad files in; protect counterfeits).

**How to apply:** French Quarter req 3150 is `wanted` with the 96k floor — when the better 186/194 rip lands in failed_imports again, force import will now correctly rank it an upgrade. Residual seam (scan-error → still-blind decision, no spectral-attempted marker) is noted on issue #711. Doc2 one-shot recipe: `sudo env PGPASSWORD=... PYTHONPATH=<store-source> <store-python-env>/bin/python - <<EOF` with `read_runtime_config("/var/lib/cratedigger/config.ini")` + `PipelineDB(dsn)` + production loader. Related: [[project-571-576-slskd-good-citizen]].
