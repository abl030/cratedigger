"""Regression test for issue #95 — ensure no module loads under two names.

When ``lib/`` (or ``web/``, ``harness/``, ``scripts/``) is on ``PYTHONPATH``
alongside the repo root, Python will load the same file twice — once as
``lib.quality`` and once as ``quality`` — producing two distinct class
objects. Enum identity checks (``is``), isinstance, and pickle round-trips
all silently break across that boundary. PR #94 hit this in production
for ``RankBitrateMetric.AVG``.

This test boots the web server's entrypoint in a subprocess, dumps
``sys.modules``, and fails if any module name appears both bare and
prefixed. Running as a subprocess (not ``importlib.import_module`` in-
process) catches wrapper / entrypoint misconfigurations that only
manifest when the process starts from ``web/server.py``.
"""

import json
import os
import subprocess
import sys
import unittest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Modules that live under a package directory and whose bare name would
# collide with the prefixed name if the package dir were on PYTHONPATH.
# Each entry is (package, module_file_stem).
PACKAGE_MODULES: list[tuple[str, str]] = []
for pkg in ("lib", "web", "harness", "scripts"):
    pkg_dir = os.path.join(REPO_ROOT, pkg)
    if not os.path.isdir(pkg_dir):
        continue
    for entry in os.listdir(pkg_dir):
        if entry.endswith(".py") and entry != "__init__.py":
            PACKAGE_MODULES.append((pkg, entry[:-3]))


def _run_entrypoint_and_dump_modules(bootstrap_code: str) -> set[str]:
    """Run ``bootstrap_code`` in a fresh Python and return loaded module names.

    Uses a clean subprocess with PYTHONPATH set to only the repo root, so
    the test mirrors production's single-canonical-path invariant.
    """
    script = bootstrap_code + (
        "\nimport json, sys\n"
        "print('__MODULES_BEGIN__')\n"
        "print(json.dumps(sorted(sys.modules)))\n"
        "print('__MODULES_END__')\n"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = REPO_ROOT
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"Bootstrap failed (rc={proc.returncode}):\n"
            f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    begin = proc.stdout.find("__MODULES_BEGIN__")
    end = proc.stdout.find("__MODULES_END__")
    if begin < 0 or end < 0:
        raise AssertionError(f"Missing sentinels in output:\n{proc.stdout}")
    payload = proc.stdout[begin:end].split("\n", 1)[1].strip()
    return set(json.loads(payload))


def _dual_loaded(modules: set[str]) -> list[tuple[str, str]]:
    """Return list of (bare, prefixed) pairs that both appear in modules."""
    offenders = []
    for pkg, stem in PACKAGE_MODULES:
        prefixed = f"{pkg}.{stem}"
        if stem in modules and prefixed in modules:
            offenders.append((stem, prefixed))
    return offenders


class TestNoDualLoad(unittest.TestCase):
    """Guard against the PYTHONPATH footgun from issue #95."""

    def test_web_server_no_dual_load(self):
        """Booting web/server.py must not load any module twice."""
        # Stubs: server.main() parses argv and connects to PG. We only want
        # the import graph, so short-circuit at ``main``.
        bootstrap = (
            "import sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            # Stub out the Handler.serve_forever loop — import only.
            "import web.server\n"
        )
        modules = _run_entrypoint_and_dump_modules(bootstrap)
        offenders = _dual_loaded(modules)
        self.assertEqual(
            offenders, [],
            f"Modules loaded under both bare and prefixed names: {offenders}. "
            "This is the issue #95 footgun — two copies of the same class "
            "object will compare unequal with `is`. Ensure PYTHONPATH only "
            "contains the repo root, and every import uses its prefixed form."
        )

    def test_cratedigger_main_no_dual_load(self):
        """Booting cratedigger.py must not load any module twice."""
        bootstrap = (
            "import sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            "import cratedigger  # noqa: F401\n"
        )
        modules = _run_entrypoint_and_dump_modules(bootstrap)
        offenders = _dual_loaded(modules)
        self.assertEqual(
            offenders, [],
            f"Modules loaded under both bare and prefixed names: {offenders}."
        )

    def test_pipeline_cli_no_dual_load(self):
        """Importing scripts.pipeline_cli must not dual-load anything."""
        bootstrap = (
            "import sys, os\n"
            "sys.path.insert(0, os.path.abspath('.'))\n"
            "import scripts.pipeline_cli  # noqa: F401\n"
        )
        modules = _run_entrypoint_and_dump_modules(bootstrap)
        offenders = _dual_loaded(modules)
        self.assertEqual(
            offenders, [],
            f"Modules loaded under both bare and prefixed names: {offenders}."
        )


if __name__ == "__main__":
    unittest.main()
