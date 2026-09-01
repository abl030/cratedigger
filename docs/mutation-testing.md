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
function. It never touches the working tree, so the `__pycache__` same-second
trap and the `git checkout` restore trap of hand-planted mutants do not
apply. Results are cached in `mutants/` and re-runs are incremental.

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

## Hard rules

- **Never list a `source_paths` member in `also_copy`.** `also_copy` runs
  after mutant generation and copies verbatim — it silently overwrites the
  mutated file, neutralizing every mutant. (mutmut's forced-fail check
  catches total neutralization loudly, but do not lean on it.)
- **Real-PG test selections need `--max-children 1`** (`mutmut run
  --max-children 1`): mutmut forks parallel children that all inherit the
  ONE ephemeral PostgreSQL from `tests/conftest.py`, and the fixtures
  DELETE between tests. Selections that only use `FakePipelineDB` are safe
  at full parallelism.
- **mutmut runs outside the suite admission lock** (#1111). Do not run it
  concurrently with a canonical suite or targeted run on the same host.
- **Per-run artifacts are never committed, and the `pyproject.toml` must
  not outlive the run**: the committed tooling is `nix/mutmut-shell.nix`
  alone. A stray root `pyproject.toml` is NOT inert:
  `tests/ruff_lsp_worker.py` passes a root `pyproject.toml` to
  `ruff server --config` whenever one exists, displacing `ruff.toml` for
  the LSP-backed tests — so a leftover file changes suite behavior, not
  just mutmut's next run. Materialize it, run mutmut, delete it (and
  `mutants/`) before running anything else.
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

Report the machine-generated tally (mutants run / killed / survived) and
the dismissal list with rationales in the PR's Fault injection section.
The table is machine evidence — but the DISMISSALS are your claims, and
the review reader audits them like any other claim.

What the catalog CANNOT do — still owed where the rules demand it:
aimed mutants (revert a real past fix, break a specific adapter
derivation, swap two arguments of one call), anything in JavaScript, and
the runner's two-aimed-mutants-per-new-test obligation — which covers the
tests you just added to kill survivors, because no round certifies its
own pins.

## Maintenance

`nix/mutmut-shell.nix` derives nixpkgs from `flake.lock`, so ordinary lock
refreshes need no edits here. The mutmut version is pinned by source hash
in that file; bump deliberately. nixpkgs' own `mutmut` package (3.2.0 at
adoption time) is NOT usable for this repo — it hard-guesses `lib/` as the
mutate root and has no test-selection config.
