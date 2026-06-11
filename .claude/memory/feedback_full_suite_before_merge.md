---
name: feedback-full-suite-before-merge
description: "Re-run the FULL suite + pyright after review-fixer edits, before commit/push/merge — a partial run shipped a broken import to main"
metadata: 
  node_type: memory
  type: feedback
  originSessionId: 5148477e-f19b-49d8-ad0e-2f8f9c392fe2
---

After applying review findings (ce-code-review's fixer, or any edit pass), re-run the **full** `nix-shell --run "bash scripts/run_tests.sh"` AND `nix-shell --run "pyright"` before committing — running only the touched module is not enough.

**Why:** On 2026-05-31, applying a review finding ("move the lazy `from lib.quality import native_codec_format_label` to top-level") deleted the in-function import but the top-level add never landed. The symbol was referenced but undefined → NameError on every native-lossy import. I committed, pushed, AND merged PR #403 to main with it broken, trusting an earlier partial module run instead of a final full-suite gate. Broken `main` then got deployed to doc2; the YouTube rescue of request 4679 had already failed once with "Rejected by persisted quality evidence: downgrade" on the pre-fix code. Fixed in follow-up commit 75056b5.

**Compounding traps seen the same session:**
- `cd` is intercepted by zoxide and silently lands in the wrong dir / "no match found" — use absolute paths (`git -C`, full paths to nix-shell cwd) or `builtin cd` and verify `pwd`.
- A worktree torn down by `/exit` took an uncommitted re-fix with it. Don't leave a fix uncommitted in a worktree across a session boundary; commit before exiting.
- A flake bump can lock to a phantom local-only commit SHA never pushed (`upload-pack: not our ref`). Always confirm the locked rev == `git rev-parse origin/main` before rebuilding doc2.
- Big parallel SSH/`$(...)` batches error out ("API killing us in agents mode") — run doc2 DB/ops commands one at a time.
- `test_beets_album_op::test_no_file_outside_allowlist_constructs_beet_argv` greps the whole repo tree and FALSE-fails on stale `.claude/worktrees/*` copies. Check `git worktree list` before trusting that one failure.

**How to apply:** review edits → `pyright` (0 errors) → full `run_tests.sh` (ends `OK`) → read `/tmp/cratedigger-test-output.txt`, count only `FAIL/ERROR: test_*` lines (ignore `ERROR: cratedigger` logger lines) → THEN commit/push/merge. See [[feedback-deploy-via-master-worktree]].
