---
name: mutant-restore-git-checkout-trap
description: git checkout <file> after a planted mutant wipes uncommitted edits sharing that file — restore by inverse edit instead
metadata:
  type: feedback
---

Restoring a planted mutant with `git checkout <file>` restores from HEAD, which
also silently discards any UNCOMMITTED work in the same file. Bit the #1258
session (2026-08-25): the item-2 transitions inversion (unstaged) was wiped
mid-mutant-round and had to be re-applied from context.

**Why:** mutant planting and real editing often share a file within one
convergence loop; `git checkout` cannot distinguish the mutant from the work.

**How to apply:** when a file carries uncommitted edits, plant mutants with a
scripted exact-string replace and restore with the inverse replace (assert
`count == 1` both ways); `git checkout <file>` is safe only when the file is
committed-clean. Commit your own work before any mutant round when possible.
Pairs with [[mutation-testing-pycache-trap]] (always
`PYTHONDONTWRITEBYTECODE=1`). Encoded for the repo in issue #1270 item 2.
