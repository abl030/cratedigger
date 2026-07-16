---
name: feedback-pyright-full-repo
description: "Always run pyright on the full repo, never on a subset. Pre-existing errors are still your problem to fix."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c8cfb77a-e80b-4c12-8e68-6924d9827a7e
---

When the user asks for pyright cleanup (or at final validation after any
non-trivial change), run `nix-shell --run "pyright --threads 4"` on the **whole
repo**. Never scope the final check to the files you touched. If there are 39
errors and only 5 are "yours", fix all 39.

**Why:** The user noticed I drift over time — I refuse to fix pyright errors outside my immediate changes, and the repo accumulates errors I could have caught cheaply. Triaging "is this mine or pre-existing?" via `git checkout` to compare costs more tokens than just fixing it. Every individual pyright fix is cheap. The repo is either 0-errors or it is not.

**How to apply:**
- When the user asks to fix pyright, treat it as a whole-repo job from the first command. Don't filter to touched files.
- When making any change in this repo, the final Pyright check is
  `nix-shell --run "pyright --threads 4"` with no path filter. If new errors
  appear anywhere — even in files you didn't touch — fix them in the same pass.
- The codified rule lives in `.claude/rules/code-quality.md` under the `# PYRIGHT CLEAN ALWAYS` banner.
- Don't push back with "those errors were pre-existing" or "scope creep". They aren't and it isn't. Just fix them.
