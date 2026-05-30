---
title: "Palo Santo data loss — misplaced duplicate_keys block wiped a cross-MBID sibling"
date: 2026-04-20
category: runtime-errors
problem_type: data-integrity
component: beets
tags:
  - beets
  - duplicate-keys
  - harness
  - data-loss
  - mbid
  - config
status: resolved-canonical
---

# Palo Santo data loss — misplaced duplicate_keys block wiped a cross-MBID sibling

**Canonical root cause — do not re-investigate.**

NOT a beets upstream bug. The user's `duplicate_keys` block was at the top level of `~/.config/beets/config.yaml` instead of under `import:`. Beets reads strictly from `config["import"]["duplicate_keys"]["album"]` (`beets/importer/tasks.py:385`); the misplaced block was silently ignored and beets fell back to the default `[albumartist, album]` — no `mb_albumid`. `find_duplicates()` then matched cross-MBID siblings on album title alone, the harness sent `{"action":"remove"}` thinking it was a same-MBID stale entry, and beets' `task.should_remove_duplicates` blast radius wiped the sibling.

## Fix

Fixed by `beets.nix` YAML relocation + harness startup assertion in `_assert_duplicate_keys_include_mb_albumid`, then superseded by guarded Beets-owned replacement: Cratedigger answers `remove` only when Beets reports exactly one same-release duplicate and otherwise fails before mutation.

The `03bfc63` Cratedigger-owned replacement state machine (pre-flight surgical remove + always-keep + post-import sibling `beet move`) has been removed; **do not reintroduce it as fallback architecture.**
