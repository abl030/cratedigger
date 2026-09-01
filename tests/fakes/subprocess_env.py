"""The environment a fake-command fixture hands its subprocesses.

Three fixtures here put a directory of fake commands on ``PATH`` and let
a real Bash entrypoint drive them: ``daily_flake_update`` (fake
``git``/``nix``/``nix-shell`` for ``scripts/daily_flake_update.sh``),
``deploy_pin`` (the same three for ``scripts/pin_nixosconfig.sh``), and
``deploy_cycle`` (a fake ``ssh`` for
``scripts/verify_cratedigger_cycle.sh``). Each fake command is a two-line
Python stub importing one shared ``_shim`` module, so CPython caches the
shim's bytecode once and reuses it for every later invocation (issue
#1156 items 4 and 5). That caching is what keeps a runner test firing
dozens of fake commands down to a few seconds.

Whoever ran the tests decided whether it happened at all, though, and
that is the problem this module exists for. Every mutant runner in this
repository must export ``PYTHONDONTWRITEBYTECODE=1``
(`.claude/rules/code-quality.md`, the two-reviewer split), which each
fixture then copied wholesale out of ``os.environ`` into the environment
its stubs ran under. The shim recompiled from source on every invocation
and all three tests asserting the cache failed. Two house rules in direct
collision, paid on every review of those files (issue #1313 residual
1329-2).

Production settles it. All three Bash entrypoints run from systemd or an
operator's shell with neither variable set, so a fixture that drops them
models the real world more closely than one inheriting the reviewer's.
The narrow scope matters too: nothing under these fixtures imports a
repository module, only the ``_shim.py`` each writes fresh into its own
temporary directory, so no stale ``.pyc`` of ours can survive here to
mask a reverted mutant. That hazard is what the mutant-runner rule exists
for, and it is not reachable from this environment.
"""

from __future__ import annotations

import os

#: Interpreter settings that stop CPython writing ``__pycache__`` beside a
#: fixture's shared shim, and so silently defeat its bytecode cache.
#: ``PYTHONDONTWRITEBYTECODE`` disables the write outright;
#: ``PYTHONPYCACHEPREFIX`` redirects it to a tree outside the fixture,
#: where the tests looking for it never find it.
BYTECODE_CACHE_OPT_OUT_VARS = ("PYTHONDONTWRITEBYTECODE", "PYTHONPYCACHEPREFIX")


def inherited_environment() -> dict[str, str]:
    """``os.environ`` without the bytecode-cache opt-outs above."""
    environment = os.environ.copy()
    for name in BYTECODE_CACHE_OPT_OUT_VARS:
        environment.pop(name, None)
    return environment
