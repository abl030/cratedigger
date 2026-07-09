---
name: doc1-nested-virt-kvm
description: "doc1 got nested virt (KVM) on 2026-07-08 — moduleVm check dropped from ~5 min (TCG emulation) to ~54s; if /dev/kvm vanishes, check Proxmox CPU type first"
metadata: 
  node_type: memory
  type: project
  originSessionId: e8da54d7-0db6-4cc3-afe6-66783b64cc20
---

2026-07-08: doc1 (Proxmox guest, hostname `proxmox-vm`) had NO `/dev/kvm` — the
NixOS moduleVm flake check silently fell back to QEMU TCG software emulation
(~5 min per run, the "5-minute VM test" pain). Operator enabled nested
virtualization on the Proxmox host (guest CPU type `host`, AMD/svm) and
rebooted; `/dev/kvm` now exists and the same check runs in ~54s.

**Why:** NixOS test driver uses `accel=kvm:tcg` — it degrades silently, no
error. A suddenly-slow VM check is the symptom.

**How to apply:** If moduleVm/`nix flake check` gets slow again on doc1, check
`ls /dev/kvm` FIRST before profiling anything else; the fix is on the Proxmox
console, not in the repo. Related: PR #563 made push-time checks cheap
(content-addressed runtimeSrc + sharded fuzz burst) — see [[548-generated-testing]]
for the burst design.
