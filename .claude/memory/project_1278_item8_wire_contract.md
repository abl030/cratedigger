---
name: 1278-item8-wire-contract-shipped
description: "#1278 item 8 (harness wire contract) shipped + live-verified 2026-08-31; key-set audit + consumer-grounded required fields"
metadata:
  type: project
---

**#1278 item 8 COMPLETE 2026-08-31**: PR #1292 merged (3cd0a874), deployed via
nixosconfig 80aa21dd, cycle-verified from the exact new store. The pin also
swept the merged-but-undeployed test-infra series #1288-#1291 (items 6+7 —
another #1203-shape drop caught by the deploy drop-detector).

What exists now: `tests/test_harness_wire_contract_audit.py` executes the REAL
`beets_harness` serializers + `HarnessImportSession.choose_match` under the
shared `tests/harness_test_support.py::beets_module_mocks` factory and asserts
key-set equality with the wire Structs both directions (carve-outs `type`/
`index`/`is_target`; stale carve-out = violation). Required wire fields are
consumer-grounded: all of `ChooseMatchMessage`, `CandidateSummary`
album_id/distance/data_source/mapping/extra_items/extra_tracks, both
`TrackMapping` halves, `HarnessItem.path`. `tracks` and leaf metadata stay
defaulted deliberately (no decision consumer / component-count-gated).

**Why (non-obvious):** the reader round proved "required" claims must be
re-derived per field — `tracks` was required on a nonexistent consumer while
`path` (the actual `candidate_audio_coverage` key) was optional; and
`is_target` DOES ride the ValidationResult JSONB wire into web/classify even
though it never rides the harness wire. The runner's survivor showed
`harness/import_one.py`'s choose_match schema-violation arm had zero coverage
until this series pinned it.

Residual noted on the issue: five local complete-wire-fixture spellings across
test modules; consolidate opportunistically. Pairs with
[[mutant-restore-git-checkout-trap]] (bit again this session).
