---
title: "The daily gate is a different environment — fixtures that encode local path or version budgets fail only there"
date: 2026-08-15
category: testing
problem_type: environment-drift
component: testing
tags:
  - testing
  - daily-gate
  - fixtures
  - af-unix
  - pyright
  - nixpkgs-unstable
related_solutions:
  - docs/solutions/testing/idealized-destructive-tests-missed-the-beets-runtime-envelope.md
---

# The daily gate is a different environment — fixtures that encode local path or version budgets fail only there

## Context

The 2026-08-15 `cratedigger-daily-checks` run failed two of five stages while
the identical tree was green on every developer run:

```
FAIL deterministic full suite (exit 1)
PASS stable Nix and Beets-release checks
PASS world-model burst
FAIL generated fuzz burst (exit 1)
PASS mirror-harness smoke
```

Three defects. One was ordinary entropy — the fuzz burst generated
`inside_segment=".."` into a world whose premise was "inside the library
root", and `check_library_root_containment` correctly reported the escape a
local run had simply never generated. The other two are the subject of this
document: neither is reachable from a local `scripts/test.sh` or
`scripts/run_tests.sh` at any depth or example count, because what differs is
not the code and not the entropy. It is the environment.

## What differs, exactly

`modules/nixos/ci/cratedigger-daily-checks.nix` sets
`XDG_RUNTIME_DIR = /run/cratedigger-daily-checks/scratch`, and
`scripts/test_tmpfs.sh` roots the suite's `TMPDIR` under it. Compare:

| Run | Suite `TMPDIR` | Length |
| --- | --- | --- |
| interactive | `/run/user/1000/cratedigger-tests.XXXXXX` | 39 |
| daily gate | `/run/cratedigger-daily-checks/scratch/cratedigger-tests.XXXXXX` | 62 |

Every generated world is built 23 bytes deeper than it is locally. The daily
gate also runs `nix flake update nixpkgs` before the suite, so its Python,
pyright, and every library are a moving target the repository lock does not
pin — that is the entire point of the gate.

## Defect 1 — 23 bytes overran `sun_path`

Socket worlds were planted with `socket.socket(AF_UNIX).bind(path)`. `bind`
enforces `sun_path`'s ~107-byte ceiling, which has nothing to do with the
filesystem's own limits: the same path is a perfectly legal file. A socket
planted inside a generated album folder
(`<tmp>/failed_imports/Album/00 track.flac`) fit locally and overran on the
gate, so 3 deterministic IDs and 24 fuzz shards died with `OSError: AF_UNIX
path too long`.

This was the class's third recurrence. The previous two were patched
per-site by shortening one leaf name — which weakened the world (a generated
leaf name is often the thing under test) and only moved the ceiling.

The fix is `tests/helpers.py::make_socket_file`, which plants the same
`S_ISSOCK` inode with `os.mknod(path, stat.S_IFSOCK | 0o600)`: no path
ceiling, no descriptor to keep alive, and `open` still answers ENXIO exactly
as it does for a bound socket — the only property these worlds assert. An
unprivileged caller may create a socket or a FIFO; only device nodes need
`CAP_MKNOD`. A test that needs a real LISTENER still binds a real socket.

## Defect 2 — a suppression comment that flips with the library version

`tests/test_audio_hash.py` carried
`from mutagen.id3 import ID3, TIT2  # type: ignore[import-untyped]`.
`mutagen` ships `py.typed`, so the comment's stated reason was already false;
the diagnostic it actually suppressed was `reportPrivateImportUsage`. The
mechanism, measured in both store paths rather than assumed:

| mutagen | `id3/__init__.py` spells | PEP 561 verdict |
| --- | --- | --- |
| pinned (1.47.x) | `from ._frames import (..., TIT2, ...)` | not re-exported → diagnostic |
| gate (1.48.1) | `from ._frames import ... TIT2 as TIT2 ...` | re-exported → no diagnostic |

A py.typed module re-exports a symbol only via the redundant `X as X` form
(or `__all__`). Upstream converted to that form between the two versions, so
the suppression became unnecessary and `reportUnnecessaryTypeIgnoreComment`
(an `error` in both pyright configs) failed the phase. Watch for that
conversion in any library this repo suppresses an import diagnostic against —
it is what flips a required comment into a forbidden one.

There is no comment-based form that survives this: a scoped
`# pyright: ignore[reportPrivateImportUsage]` is reported by that same
`reportUnnecessaryTypeIgnoreComment` rule the moment it stops being needed
(measured, not assumed — probe a clean line with one and pyright says
`Unnecessary "# pyright: ignore" rule`). The version-robust
fix is to remove the need for a comment — import from the defining module,
`from mutagen.id3._frames import TIT2`, which is what pyright's own message
asks for and is clean under both versions.

## Reproducing gate-only path failures locally

Point the suite's RAM root at a deeper directory. It must be tmpfs, and no
ancestor may be group/other-writable (`scripts/test_tmpfs.sh` enforces both):

```bash
mkdir -p /run/user/1000/simulated-daily-checks-scratch-root
CRATEDIGGER_TEST_RAM_ROOT=/run/user/1000/simulated-daily-checks-scratch-root \
  nix-shell --run "python3 -m unittest tests.test_protected_path_truth_generated"
```

That reproduces the exact `OSError: AF_UNIX path too long` traceback the gate
reported, in seconds, with no clone and no flake update.

## The lesson

A green local suite is not evidence for the daily gate, and the gate's
failures are not flakes. When it reports something the suite cannot, ask what
differs in the ENVIRONMENT before reading the diff: scratch-path depth, and
every version the repository lock is deliberately not pinning. A fixture that
silently depends on either has an undeclared budget, and the honest fix
removes the budget rather than buying headroom under it.
