---
name: project-813-819-822-series
description: "2026-07-22: #813/#819/#822 all shipped via orchestrate-issue (PRs #823-#827), deployed 9018db3b, cycle-verified; reflection issue #828"
metadata: 
  node_type: memory
  type: project
  originSessionId: 79896fa5-25d5-4857-904c-185bcb8da540
  modified: 2026-07-22T11:43:18.060Z
---

2026-07-22: Issues #813, #819, #822 closed in one orchestrated series — 5 PRs (#823 rules, #824 matcher contract, #825 fail-closed+cooldown, #826 denylist single-source + quality replay tier, #827 stage parity), all merged via merge commits, deployed together (nixosconfig 9018db3b, cycle invocation 80d16381, merged-tree fuzz burst 66/66 green), issues closed with live evidence.

Key outcomes to not re-derive:
- Stage 1 (`spectral_import_decision`) STAYS — load-bearing for no-spectral and self-inconsistent-evidence worlds; parity property `test_stage1_never_contradicts_stage2` patrols the both-bound domain. Unpatrolled classes tracked in [[issue-828]] (#828 item 1).
- #827's property found 2 real clamp bugs; the fable review then found the fixes over-reached into bound/unbound worlds — both now gated on `both_spectral_bound`.
- `decision_denylists()` / `_finalize_denylist` is the single denylist derivation (production write AND simulator display); wire-through was chosen over delete because dispatch_action and post_import_search_action genuinely disagree for retained-import decisions.
- #822 item 4 cooldown was operator-authorized in-thread (issue #822 comment 5042163957).
- `pipeline-cli quality <id>` now has a real-candidate replay tier (needs PGPASSWORD exported like `query`).
- Fuzz burst long pole: `test_dispatch_outcomes_generated` ~45 min single-threaded dominates burst wall-clock on doc1's 8 cores; operator considering a Proxmox vCPU bump ([[project-doc1-nested-virt-kvm]]).
