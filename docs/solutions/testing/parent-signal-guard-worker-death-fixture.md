---
title: "A test fixture's own os.kill(os.getppid(), ...) SIGKILLed the developer's whole systemd session — twice"
date: 2026-08-23
category: testing
problem_type: production-hardening
component: testing-infrastructure
tags:
  - testing
  - process-pool
  - signals
  - systemd
  - subreaper
  - fault-injection
related_issues:
  - "#1250"
  - "#1156"
---

# A test fixture's own os.kill(os.getppid(), ...) SIGKILLed the developer's whole systemd session — twice

## Context

`tests/test_parallel_test_runner.py`'s worker-death fixture (`_run_worker_death_fixture`, issue #1156 item 2) exists to prove that a killed `ProcessPoolExecutor` worker's `BrokenProcessPool` folds into ONE named failure marker instead of N disguised per-target failures — the same exception shape a real OOM kill produces. To simulate that, it writes `count` (default 2) throwaway unittest fixture files, each containing:

```python
def test_worker_dies(self):
    os.kill(os.getppid(), signal.SIGKILL)
```

The intent: `os.getppid()` inside the nested `--_run-target` subprocess resolves to the pool worker's PID, so killing it reproduces a real worker death. This is documented and reasonable — and it fired the wrong signal at the wrong target twice on doc1 (2026-08-22, 2026-08-23), each time SIGKILLing the developer's entire `systemd --user` session: tmux, every running agent, dbus, the works. Both incidents were indistinguishable at the console from the known 55-minute idle-session reap, which sent the first investigation down the wrong path entirely — the distinguishing evidence turned out to be that logind logs `is idle, stopping` for a real reap and logs nothing at all for this.

It was only pinned down after a SIGKILL audit trap (`homelab.killTrace` in nixosconfig) was deployed on doc1 and caught the exact syscall:

```
OBJ_PID   opid=1285 oauid=abl030 ouid=abl030 oses=1 ocomm=systemd
SYSCALL   a0=0x505 a1=SIGKILL ppid=1285 pid=2604483 auid=abl030 ses=1
          comm=python3.14
```

`a0` (the target) is `0x505` = 1285, `ocomm=systemd` — the target was `systemd --user`, the session's own init. The sender's `ppid` is *also* 1285: it killed its own parent.

## Root cause

`os.getppid()` is evaluated **at kill time**, not at spawn time, and the fixture deliberately runs `count` of these concurrently (`--jobs count`). The moment ONE instance's own pool worker exits first, that instance is orphaned. `systemd --user` sets `PR_SET_CHILD_SUBREAPER`, so an orphan in a user session reparents to **the user manager**, not PID 1. The surviving instance's `os.getppid()` then returns the manager's PID, and the raw call SIGKILLs it — the whole session dies with it.

Two things make this genuinely hard to catch by review or by CI:

1. **It's invisible off doc1.** In a bare container init (no subreaper), an orphan reparents to PID 1, and PID 1 refuses arbitrary signals from a non-root sender (EPERM) — a completely silent no-op. CI never showed a symptom because CI never runs under a `systemd --user` session. The bug was real, dormant, and load-bearing for two days before doc1 (an interactive dev host with tmux + agents living in a user session) actually exercised the reparenting path.
2. **It's genuinely non-deterministic.** Whether it fires depends on which sibling instance's pool worker dies first — a race against the OTHER instances' own teardown, not against anything the test itself controls.

### The CPython mechanism that makes even a single-instance run risky

CPython's `concurrent.futures.process._ExecutorManagerThread._terminate_broken` (verified on 3.14.7, the interpreter this repo runs) does:

```python
for p in self.processes.values():
    p.terminate()
```

The first fixture instance's SIGKILL breaks the pool. The executor's own manager thread then reacts by terminating **every remaining pool worker** as part of tearing the broken pool down — which can orphan a sibling instance's nested `--_run-target` child before that child ever reaches its own `os.kill(os.getppid(), ...)` line. Measured over 11 runs with a record-and-guard probe: **5 of 20 instance records (25%) observed a live parent that was NOT their own pool worker** at signal time (4× reparented to `ppid=1, comm=systemd`; 1× reparented to an unrelated `python3.14`). In 2 of the 11 runs, a child never reached the test body at all — its own pool worker was terminated first by the cascade.

## The gotcha: the issue's own suggested guard #2 silently disables the fixture

Issue #1250 suggested, in order of preference: (1) pass the intended PID in explicitly at spawn (removes the race entirely), (2) verify the parent looks like a pool worker before signalling — e.g. require `/proc/<ppid>/cmdline` to contain the runner's own path, (3) at minimum refuse PID 1 / a systemd `comm`.

**Option 2 as literally suggested does not work.** A real pool worker's `/proc/<pid>/cmdline` does **not** contain this repository's runner path at all. Measured full cmdline of a real `ProcessPoolExecutor` worker on CPython 3.14.7:

```
/nix/store/…/bin/python3 -s -c "from multiprocessing.spawn import spawn_main; spawn_main(tracker_fd=8, pipe_handle=12)" --multiprocessing-fork
```

A guard checking for the runner's own path was implemented and measured directly: it produced **zero kills, return code 0, no failure markers** — the fixture silently stopped exercising anything at all, and nothing failed loudly to say so. A plausible-looking, textbook version of the suggested fix turns the regression test into a no-op. The actual signature to check for is the interpreter's own `--multiprocessing-fork` argv flag, not the caller's script path.

## The fix

`tests/parent_signal_guard.py` is the one shared implementation. Two ideas, both required together — neither is sufficient alone:

1. **Capture the intended parent PID once, at spawn/import time** (`capture_intended_parent_pid()`), never re-read it at signal time. This alone narrows the race window but is **not sufficient**: a captured PID can, in principle, be recycled by the OS before the signal fires, silently naming an unrelated process.
2. **Re-verify live identity immediately before signalling** (`guard_refusal_reason` / `guard_and_signal_parent`): refuse unless `os.getppid()` still equals the captured value AND (when a signature is expected) the live parent's `/proc/<pid>/cmdline` still contains it. Belt-and-braces regardless of the above: never signal PID 1, never signal a process whose `/proc/<pid>/comm` is `systemd`.

A refusal is always a plain returned reason string, never an exception — a guard that could itself crash the fixture would defeat the point.

Two call shapes cover every real site, because not every generated child can `import tests.parent_signal_guard`:

- **Importable form** (`guard_and_signal_parent`) for a child that CAN import the repo. This is less restrictive than it first looks: `scripts/run_python_tests.py` inserts the real repository root onto `sys.path` unconditionally at its own module top, before it ever loads a discovered test — so even a throwaway unittest fixture file, written to a scratch tempdir and loaded by name inside the nested `--_run-target` subprocess, can `import tests.parent_signal_guard` successfully. (`sys.path[0]` for a script-mode `python -c` or a `python -S` shim is a different story — see below.)
- **Source-emitting form** (`guard_source_prelude` / `guard_kill_statement`) for a child that CANNOT import the repo: an inline `python -c "..."` argv element (`sys.path[0]` there resolves to the process's cwd, not reliably the repo root), or a stdlib-only `-S` shim (`tests/fakes/deploy_pin.py`'s fake git/nix/hostname commands run under `python3 -S`, whose `sys.path[0]` is the fixture's own throwaway bin directory). Both functions only ever concatenate text; they reimplement the same clauses as plain source, never evaluate anything at define time, and the shim variant adds **zero new imports** (no `import signal`, matching that file's own existing budget — it already hardcodes `SIGTERM = 15` to avoid the per-process cost of importing `signal` across ~28 subprocess spawns) and at most one `/proc` read (`expected_signature=None` skips the cmdline check entirely for a caller with no meaningful "expected parent shape," since its real parent is never a pool worker).

All six real `os.kill(os.getppid(), ...)` sites in the tree (the SIGKILL fixture above, two `SIGTERM` sites in `tests/test_suite_coordinator.py`'s synthetic interrupt-handling phases, and three `SIGTERM` sites in `tests/fakes/deploy_pin.py`'s fault-injection shim) now route through this guard.

**Measured drop-in fidelity**: with the guard active, 5/5 real end-to-end runs of the worker-death fixture still produced return code 5 (`TEST_HOST_MEMORY_EXHAUSTED_EXIT_CODE`), exactly ONE failure marker, and both expected test IDs folded into it — including runs where one instance correctly refused to signal a reparented `pid=1` parent instead of escalating. The fixture's existing assertions did not need weakening.

## Why the fixture's own claim needed correcting too

Before this fix, the fixture's docstring claimed the two-instance run produced "TWO REAL, INDEPENDENT `BrokenProcessPool` deaths." With the guard active, that phrasing is no longer exactly true: whichever instance's kill fires first can still orphan the other *before* the sibling's own guarded kill runs — the sibling then correctly refuses, and its own pool worker dies anyway, via the same `_terminate_broken` cascade CPython already runs while tearing the broken pool down, not via a second fixture signal. Both instances still produce a genuine, distinct `BrokenProcessPool` for their own target — which is what the test actually asserts and folds into one marker — but the docstring's claim about *how* the second one died was no longer accurate, so it was rewritten rather than left standing (`.claude/rules/code-quality.md` § "No round catches its own false claims"). Arguably this is *better* production fidelity, not worse: a real OOM kill hits one worker directly, and the executor's own teardown cascades `.terminate()` to the rest — exactly this shape.

## Why a bounded text scan, not an AST walk, is the right audit here

Every one of the six real occurrences of the hazardous shape lived inside a **Python string literal** — a generated unittest fixture body, a `python -c` argv element, or a stdlib-only shim's source text — never as a literal call expression in the file that spelled it. An AST-`Call`-node audit (the pattern `tests/test_generated_node_worker_audit.py` uses for a real `subprocess.run(["node", ...])` call site) would see only one big string constant in each of these files, not a nested call node, and would silently miss all six. `tests/test_parent_signal_guard_audit.py` instead scans raw file text for the literal `os.kill(os.getppid()` substring shape (flexible whitespace, so a call split across lines is still caught) — the text is what actually decides the eventual child process's behavior, whether or not it happens to be "real code" in the scanning file's own AST.

## When to apply

Any fixture — present or future — that signals "the parent process" (or any other identity resolved via a syscall re-read at the moment of use, not captured once and verified) needs the same two-part treatment: capture early, then re-verify live identity immediately before acting. The mechanism generalizes beyond `os.getppid()`/`os.kill()`; the specific shape banned by the audit is scoped narrowly to what has actually bitten this repository.

## Action

- Never remove or bypass the guard on `tests/test_parallel_test_runner.py`'s worker-death fixture, even "just once," to reproduce this bug directly — the hazard is real regardless of how confident the reproduction attempt feels. Mutant/known-bad evidence for the guard belongs at the helper-unit level (`tests/test_parent_signal_guard.py`, fully dependency-injected, zero real signals), never by running the real fixture unguarded.
- Before running the real end-to-end fixture on an interactive host, sanity-check process ancestry with a throwaway `os.fork()` + `os.getppid()` one-liner. If the reparented-to PID is not 1 (i.e., you're under a real `systemd --user` session or similar subreaper), any residual guard failure is not an EPERM no-op — stop and investigate rather than proceeding.
