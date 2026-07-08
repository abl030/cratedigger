"""Hypothesis profile selection for generated tests (issue #548).

Importing this module registers three profiles and loads the one selected
by ``CRATEDIGGER_HYPOTHESIS_PROFILE``:

* ``suite`` (default) — deterministic tier. ``derandomize=True`` makes
  generation a fixed pseudo-random sweep, ``database=None`` keeps results
  independent of local ``.hypothesis/`` state, so every ``run_tests.sh``
  run behaves identically on every machine. This is the tier that gates
  merges.
* ``push`` — quick randomized burst the pre-push hook runs on every
  ``git push`` (scripts/pre-push). Fresh entropy per push accumulates
  exploration over time; the local example database makes any push-found
  failure replay first in dev. Sized to seconds, not minutes.
* ``fuzz`` — deep randomized burst for local exploration when quality
  policy changes. Fresh entropy per run plus the local Hypothesis example
  database (``.hypothesis/``, gitignored), so failures found in one burst
  replay first on the next. ``print_blob=True`` prints a
  ``@reproduce_failure`` blob for exact replay.

Deadlines are disabled in every tier: wall-clock-per-example limits flake
under load and none of the generated tests do I/O worth bounding.

Promotion policy: a failure found by the push/fuzz tiers is shrunk by
Hypothesis to a minimal world — commit that world as a named
``@example(...)`` pin or as a scenario in the album test set. Never check
in opaque artifacts. See docs/generated-testing.md.
"""

import os

from hypothesis import HealthCheck, settings

settings.register_profile(
    "suite",
    derandomize=True,
    max_examples=150,
    database=None,
    deadline=None,
)
settings.register_profile(
    "push",
    max_examples=2_000,
    deadline=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.register_profile(
    "fuzz",
    max_examples=20_000,
    deadline=None,
    print_blob=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile(os.environ.get("CRATEDIGGER_HYPOTHESIS_PROFILE", "suite"))
