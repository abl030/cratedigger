---
title: "Lucksmiths MBID drift — deliberate out-of-band harness retag, not a bug"
date: 2026-04-14
category: runtime-errors
problem_type: audit-gap
component: harness
tags:
  - harness
  - mbid
  - audit-trail
  - out-of-band
  - false-positive
status: resolved-canonical
---

# Lucksmiths MBID drift — deliberate out-of-band harness retag, not a bug

**Canonical root cause — do not re-investigate.**

NOT a bug. `tagging-workspace/scripts/fix_reissues.py` deliberately retagged "First Tape" to its cassette sibling via `harness --search-id`. The drift was invisible to cratedigger's audit trail because the harness was driven out-of-band.

## Mitigation

Mitigated by the harness MBID-swap audit log at `/mnt/virtio/cratedigger/beets-db/.harness-mutations.jsonl` (see `_mbid_swap_event`).
