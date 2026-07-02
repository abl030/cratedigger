---
name: project_beets_2_12_migration
description: 2026-06-29 beets 2.11→2.12 bump broke ALL imports two ways; harness tests mock beets so the drift shipped undetected — real-beets contract test now guards it
metadata: 
  node_type: memory
  type: project
  originSessionId: 2b258c72-ccf2-44cb-829a-f3f40fbeb038
---

On 2026-06-29 a nixpkgs closure bump moved beets `2.11.0 → 2.12.0` on doc2 and
broke **every** cratedigger import. There were TWO independent beets-2.x API
breakages — fixing the first made the harness run again but upgrades still
failed, which read as "still broken":

1. **Library / import API.** `beets.ui.get_path_formats` / `get_replacements`
   were removed (get_path_formats → `beets.util.pathformats`, get_replacements →
   `Library` staticmethod) and `Library.__init__` dropped its
   `path_formats`/`replacements` positional args. Fix: open with 2-arg
   `library.Library(path, dir)` — 2.x derives both from `config["paths"]` /
   `config["replace"]`. Symptom: `ImportError ... get_path_formats` at harness
   import → every import/validate crashed.

2. **Duplicate-resolution hook** (only breaks UPGRADE imports of
   already-in-library albums). beets 2.x removed `ImportSession.resolve_duplicate`
   + `task.should_remove_duplicates`; it now calls
   `session.get_duplicate_action(task, found_duplicates) -> DuplicateAction` and
   removes the old album in `manipulate_files` iff the action is `REMOVE`. The
   harness's stale `resolve_duplicate` override was silently never called, so
   every upgrade created a SECOND beets album row and tripped the post-import
   guard: `decision=import_failed … multiple beets album rows [X, Y]`. Fix:
   override `get_duplicate_action`, return `DuplicateAction.REMOVE`/`SKIP`, keep
   the JSON `resolve_duplicate` wire message unchanged.

Also fixed `lib/beets_distance.py`: 2.x re-exports the `distance` *function*
from `beets.autotag`, shadowing the submodule, so `_mod.distance(...)` was an
AttributeError (swallowed as `distance_failed` in the YouTube resolver).

**Why it shipped undetected:** the harness unit tests
(`tests/test_harness_*.py`) MOCK beets via `sys.modules.setdefault("beets…",
MagicMock())`, so they never exercise the real beets API and can't catch
version drift. The dev shell DOES have real beets (now 2.12). Guard added:
`tests/test_harness_beets2_contract.py` runs the real harness against real beets
2.12 in a subprocess (isolated from the mock pollution). This is an instance of
[[feedback_test_fidelity_meta_pattern]].

**How to apply:** when a closure/nixpkgs bump touches beets, assume the harness
(`harness/beets_harness.py`) may break — it monkeypatches beets internals
(`find_duplicates`, the duplicate hook, `Library`). Run
`tests/test_harness_beets2_contract.py` against the new beets first. The fix +
full RC are in cratedigger PR #462 and the wiki incident note
`docs/wiki/services/cratedigger.md` (nixosconfig). Deploy was the standard
flow: cratedigger fix → GitHub PR/merge → nixosconfig flake bump
`cratedigger-src` + drop the stopgap Nix patch → Forgejo → fleet-update.
