---
name: feedback-heavy-gates-serial-not-epi
description: Heavy gates run serially on doc1. Never run test suites on epi — unstable under load.
metadata:
  type: feedback
---

doc1 OOM'd running the canonical suite (~12 PG clusters) concurrently with
moduleVm (2026-08-13). epi is **not** the overflow host: it is unstable under
load and rebooted under a `run_final_gate.sh` (2026-08-14, uptime reset to
0:02). Do not send suites, final gates, fuzz bursts, or moduleVm there.

**How to apply:** heavy gates run SERIALLY, one at a time, on doc1. Never
overlap a suite with moduleVm or a second `run_suite`. A second concurrent
`run_suite` waits on the #1111 admission lock. Never auto-relaunch a died
gate — diagnose why it died first; repeated harness death IS the
memory-pressure signal. Subagents run gates in the FOREGROUND on doc1.

Wake-on-LAN still applies when checking whether epi itself is reachable.
That is not permission to park a test suite on it.
