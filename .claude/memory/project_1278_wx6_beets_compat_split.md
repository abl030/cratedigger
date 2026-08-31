---
name: 1278-wx6-beets-compat-split
description: "#1278 wx6 shipped 2026-08-31: beets_compat split + duplicates-query era boundary, live-verified; completes the Worth exploring list"
metadata:
  type: project
---

Issue #1278 "Worth exploring" item 6 SHIPPED and live-verified 2026-08-31 (PR
#1310, merge 8393dba2, nixosconfig pin 6dfd1c40, cycle-verified from store
bg49jj8j…-source). `harness/beets_compat.py` (~300 L) is the beets-core era
boundary alone; `harness/discogs_patches.py` holds the Discogs plugin
patching, moved byte-for-byte with dual package/script-mode import and
TYPE_CHECKING-typed aliases. The `beets_harness.py` inline
`Album.duplicates_query` probe became a `duplicates_query_era` capability
(exactly-one ambiguity check) + `beets_compat.album_duplicates_query` seam.
**This completed the "Worth exploring" list — all six items shipped.**

Lessons earned:

- **An era-probe premise must be measured against the release manifest, not
  inferred from API intuition.** The first commit shipped "all_fields_query
  is present in BOTH eras" — false; the reader's tarball greps showed 2.3.0
  swapped one builder for the other, so the sibling exactly-one check was
  right all along. The independent read caught it, as
  [[correction-rounds-mint-false-claims]] predicts.
- **MagicMock fixture stubs pinned on `sys.modules["beets.X"]` are inert for
  `from beets import X`** — that statement resolves the PARENT mock's
  divergent auto-child. Every mocked-harness fixture rode auto-attributes,
  not its stubs, until the exactly-one check exposed it; fixtures now bind
  `mocks["beets"].config/library/plugins` to the entries
  (`tests/harness_test_support.py::beets_module_mocks`).
- **A purge/restore-list pin is vacuous in a fresh process** — pre-seed the
  stale-module world before asserting eviction (the mutant round's one
  survivor).

Open on the issue: absorb-or-leave for `lib/beets_delete.py`'s
plugin-loading era probe (the one beets-core era decision outside the
boundary, named in docstring + primer); four fixtures still hand-roll
`beets_module_mocks()`'s dict (next-touch cleanup).
