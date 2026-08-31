---
name: 1278-pt5-classify-render-interface
description: "2026-08-31: #1278 point 5 shipped live — classify_log_entry demoted private, tests pin build_recents_download_log_rows; PR #1283"
metadata:
  type: project
---

2026-08-31: Issue #1278 point 5 shipped and live-verified (PR #1283, main
47258cef, nixosconfig e689fcb7). `web/classify.py::_classify_log_entry` is
module-private; `build_recents_download_log_rows` is the render interface
~160 test drive points pin. Six files deliberately keep driving the private
classifier (render-differential parity pin, track-length patrol,
failure-presentation parity, producer audits) — do not "finish" migrating
them; the seam is their subject. Rule D differential: 38,689 rows, 0 changed.

Review lessons that paid: the mutant runner found `proof_gate_projection`'s
`candidate_evidence_id` short-circuit unfalsified across 876 tests (every
generated world hardcoded a joined candidate) — fixed with a twin pin +
property in the same PR. The correction round again minted a fresh
unqualified universal (caught by the follow-up independent read, per
[[correction-rounds-mint-false-claims]]). `msgspec.ValidationError` in an
except tuple is inert next to `ValueError` (subclass) — a fault-injection
trap. The deploy also swept up PR #1282 (point 4, beets child module),
which had been merged but never pinned — the #1203 drop-detector shape.
