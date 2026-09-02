# Mutation testing — the implementer's catalog breadth pass

Catalog mutation testing (mutmut) is part of the IMPLEMENTER's convergence
loop, not the review gate. Adopted by operator decision in issue #1317 after
the 2026-09-01 evaluation: 460 mutants against `web/wrong_match_queue_view.py`
ran in 20.5s wall (399 killed, 60 survived) and surfaced real test-lattice
gaps the manual 40-mutant review round of PR #1304 did not aim at — while the
catalog provably could not express that round's one real survivor
(`job.to_json_dict() → {}`). Catalog breadth and aimed mutants are
complementary, not substitutes; the review-gate mutant runner's aimed
obligations are unchanged (`.claude/rules/code-quality.md` § "Pre-Commit
Review Gate").

## What it does

mutmut copies the source tree into a `mutants/` directory with every function
rewritten to carry catalog mutants (operator swaps, literal changes, argument
`None`-ing, string mutations) behind an env-var trampoline, then runs pytest
per mutant, selecting only the tests whose call stacks reach the mutated
function. It never rewrites your source files (all writes land under
`mutants/`), so the `__pycache__` same-second trap and the `git checkout`
restore trap of hand-planted mutants do not apply. Results are cached in
`mutants/` and re-runs are incremental.

**The cache is keyed on the SOURCE, not on the tests.** Fixing a survivor
by strengthening a test and re-running `mutmut run` re-executes nothing:
it reports the identical tally at `0.00 mutations/second`, which reads
exactly like a run that confirmed your fix did not work. Delete
`mutants/` to re-measure. Issue #1313 hit this on the first survivor
round of `scripts/phase_parsers/` — a stale 261/7 where the clean re-run
said 266/2.

## Running it

The whole test tree is pytest-runnable: `tests/conftest.py` boots the
ephemeral PostgreSQL and applies migrations at import, and pytest collects
`unittest.TestCase` natively.

1. Materialize a root `pyproject.toml` in YOUR OWN worktree (never commit
   it; delete it and `mutants/` when done):

   ```toml
   [tool.mutmut]
   # The package root(s) the diff touches. Everything under a root is
   # copied into mutants/; only_mutate narrows actual mutation.
   source_paths = ["web"]
   only_mutate = ["web/wrong_match_queue_view.py"]
   # The changed/adjacent DETERMINISTIC test modules for this diff —
   # roughly what `scripts/test.sh` would select. Exclude
   # test_*_generated.py: Hypothesis per-mutant is slow and the fuzz
   # profile is nondeterministic under mutation.
   pytest_add_cli_args_test_selection = [
       "tests/web/test_wrong_match_queue_view.py",
       "tests/web/test_routes_imports.py",
   ]
   # Every other package the selected tests import, plus migrations
   # (conftest.py applies them inside mutants/).
   also_copy = ["lib", "scripts", "harness", "migrations"]
   ```

2. Run:

   ```bash
   nix-shell nix/mutmut-shell.nix --run "mutmut run"
   nix-shell nix/mutmut-shell.nix --run "mutmut results"
   nix-shell nix/mutmut-shell.nix --run "mutmut show <mutant-name>"
   ```

3. Triage every survivor (see below), then delete `pyproject.toml` and
   `mutants/` before committing.

## What it silently does not mutate

**A decorated class body is skipped whole, and so is a decorated
function.** `mutmut/mutation/file_mutation.py` returns "ignore" for any
`cst.ClassDef` carrying a decorator, and for any `cst.FunctionDef`
carrying one other than a lone `@staticmethod` / `@classmethod`. Nothing
in the output says so: the run reports a tally over whatever it did
mutate, and a file whose every method sits inside `@dataclass` scores a
clean sheet by mutating nothing.

That covers a lot of this repository. Measured 2026-09-01 across `lib/`,
`web/`, `scripts/`, and `harness/`: 350 decorated classes holding 307
functions, plus 75 more decorated functions outside them — 382 in all,
about 9% of the 4,169 production functions, and it lands on exactly the
frozen dataclasses and `msgspec.Struct`s the house style prefers. The
decomposition, since it is easy to double-count: 234 functions carry a
decorator at all, 98 of those carry a lone `@staticmethod` or
`@classmethod` and ARE mutated, leaving 136 skipped for their own
decorator — 61 of which already sit inside the 350 classes, 75 outside.
Issue #1313's
batch A hit it head on: the breadth pass over `web/runtime.py` mutated
`runtime()` and nothing else, because every method under test belongs to
`@dataclass(frozen=True) class WebRuntime`.

So before reading a tally, check `mutants/<file>.py` for the function you
changed. If it is there verbatim with no `__mutmut_` trampoline, the
breadth pass said nothing about it and the aimed mutants are your only
mutation evidence. Say which in the PR.

## Hard rules

- **Don't list a `source_paths` member in `also_copy` — it's redundant, not
  a trap.** In the pinned 3.7.0, `_run` copies the source roots
  (`copy_src_dir`) and the `also_copy` entries FIRST and generates mutants
  LAST, so a duplicate entry cannot neutralize mutants (measured end-to-end
  with a deliberately overlapping config: mutants intact both runs). Note
  mutmut auto-appends `tests/`, `test/`, `setup.cfg`, `pyproject.toml`,
  lockfiles, and any root-level `test*.py` to `also_copy` (mutmut's
  `configuration.py`) — the template above relies on that for `tests/`.
- **Real-PG test selections need `--max-children 1`** (`mutmut run
  --max-children 1`): mutmut forks parallel children that all inherit the
  ONE ephemeral PostgreSQL from `tests/conftest.py`, and the fixtures
  DELETE between tests. Selections that only use `FakePipelineDB` are safe
  at full parallelism.
- **mutmut runs outside the suite admission lock** (#1111). Do not run it
  concurrently with a canonical suite or targeted run on the same host.
- **Per-run artifacts are never committed, and must not outlive the run**:
  the committed tooling is `nix/mutmut-shell.nix` alone. Run the breadth
  pass BEFORE the `check` receipt: a leftover `pyproject.toml` or
  `mutants/` dirties the tree and the final gate refuses untracked files
  (loudly and immediately, before running anything — not a silent hazard). `ruff.toml` and both
  pyright configs exclude `mutants/` as defense-in-depth against the
  #1208 shape (a root-planted tree making whole-repo passes crawl a
  foreign corpus), but deletion is the rule, not the excludes. One more
  reason not to leave the config behind: `tests/ruff_lsp_worker.py`
  hands a root `pyproject.toml` to `ruff server --config` whenever one
  exists — measured inert for that worker's temp-workspace documents on
  ruff 0.16, but a future ruff need not keep it so.
- **debug = true** in `[tool.mutmut]` surfaces the real pytest error when
  the stats pass dies with `BadTestExecutionCommandsException` — usually a
  missing `also_copy` entry (the founding example: `migrations/` missing,
  so conftest's `apply_migrations` raised inside `mutants/`).

## Triage — a survivor is a finding, never a footnote

For each surviving mutant, decide with `mutmut show <name>`:

- **Real gap** → write the missing assertion/pin now, in this PR, and
  re-run (`mutmut run` is incremental; the new test must kill it).
- **Equivalent or unreachable** → dismiss with a one-line rationale.
  Known noise shapes from the evaluation: `float("inf") → float("INF")`
  (parses identically); degenerate sort-key fallbacks distinguishable only
  between already-degenerate entries; the string-literal operators
  inflating ONE unasserted field into ~5 survivor rows (XX-wrapping,
  case-flips, `.get(None)`) — triage those as one question, not five.
- **A survivor inside a log-only branch** is decided by the house rule in
  `.claude/rules/code-quality.md` § "Testing — Red/Green TDD": a log line earns
  a pin iff it is the sole operator-visible evidence of a decision or failure
  the operator would act on, such as Recents audit evidence or a refusal
  reason. Progress and trace logging doesn't, so those are dismissals with the
  rule cited. Operator ruling, issue #1313 residual 1336-3:
  https://github.com/abl030/cratedigger/issues/1313#issuecomment-5503374358

Report the machine-generated tally (mutants run / killed / survived) and
the dismissal list with rationales in the PR's Fault injection section.
The table is machine evidence — but the DISMISSALS are your claims, and
the review reader audits them like any other claim.

What the catalog CANNOT do — still owed where the rules demand it:
aimed mutants (revert a real past fix, break a specific adapter
derivation, swap two arguments of one call), anything in JavaScript, and
the runner's two-aimed-mutants-per-new-test obligation — which covers the
tests you just added to kill survivors, because no round certifies its
own pins. The aimed procedure is `docs/generated-testing.md`
§ "Qualifying the harness — fault injection"; its driver stays an
uncommitted one-shot.

## Maintenance

`nix/mutmut-shell.nix` derives nixpkgs from `flake.lock`, so ordinary lock
refreshes need no edits here — but nothing CI-builds this shell (no flake
check, no daily gate), so a nixpkgs bump that moves `uv-build`, `libcst`,
or the interpreter surfaces only when someone next runs mutmut. The mutmut
version is pinned by source hash in that file; bump deliberately. nixpkgs'
own `mutmut` package (3.2.0 at adoption time) is NOT usable for this repo
— it hard-guesses `lib/` as the mutate root and has no test-selection
config.
