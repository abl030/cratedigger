"""Guard .coveragerc against the deployed-path drift that broke prod coverage.

Production services run from a Nix store path whose name is owned by the
downstream nixosconfig wrapper — currently
``/nix/store/<hash>-cratedigger-src-group-permissions`` (the wrapper renames
the flake input when it patches group permissions). The include globs in
.coveragerc are matched against that absolute path at collection time, and
the [paths] aliases are matched against it again at ``coverage combine``
time on the dev box.

This has silently broken twice (2026-05-28: ``source = .`` vs prod CWD;
2026-06-11: globs anchored on ``*-cratedigger-src`` missing the
``-group-permissions`` suffix — two weeks of empty shards). Both failures
were invisible: coverage runs happily and writes data files that track zero
files. These tests pin the contract with coverage.py's own matchers so a
rename that escapes the globs fails the suite instead of the deploy.
"""

import configparser
import unittest
from pathlib import Path

from coverage.files import GlobMatcher, PathAliases

REPO_ROOT = Path(__file__).resolve().parent.parent

# Real production store path as deployed on doc2 (hash is illustrative; the
# name shape — flake input renamed by the nixosconfig wrapper — is the part
# under test). If the wrapper renames the derivation again, update this AND
# make sure .coveragerc still matches it.
DEPLOYED_SRC = (
    "/nix/store/blqiv3zg77zklql3zm5vczqi32vckdww-cratedigger-src-group-permissions"
)
# The unrenamed flake-input shape (`src = ./.;` convention) — what prod would
# use if the downstream wrapper ever stops patching permissions.
FLAKE_INPUT_SRC = "/nix/store/8a1k2j3h4g5f6d7s8a9p0o1i2u3y4t5r-source"
LOCAL_SRC = str(REPO_ROOT)


def _read_coveragerc() -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    parser.read(REPO_ROOT / ".coveragerc")
    return parser


def _multiline_list(value: str) -> list[str]:
    return [line.strip() for line in value.splitlines() if line.strip()]


class TestCoverageIncludeGlobs(unittest.TestCase):
    """[run] include must match production source files, omit must still bite."""

    def setUp(self):
        cfg = _read_coveragerc()
        self.include = GlobMatcher(_multiline_list(cfg["run"]["include"]))
        self.omit = GlobMatcher(_multiline_list(cfg["run"]["omit"]))

    def test_include_matches_deployed_prod_paths(self):
        for rel in ("lib/quality.py", "web/server.py", "scripts/importer.py",
                    "cratedigger.py", "harness/import_one.py"):
            for root in (DEPLOYED_SRC, FLAKE_INPUT_SRC):
                path = f"{root}/{rel}"
                with self.subTest(path=path):
                    self.assertTrue(
                        self.include.match(path),
                        f"include globs miss the production path {path} — "
                        "prod coverage would silently collect nothing",
                    )

    def test_include_matches_local_repo_paths(self):
        self.assertTrue(self.include.match(f"{LOCAL_SRC}/lib/quality.py"))

    def test_omit_still_excludes_tests_and_site_packages(self):
        for path in (
            f"{DEPLOYED_SRC}/tests/test_quality_decisions.py",
            f"{LOCAL_SRC}/tests/test_quality_decisions.py",
            "/nix/store/abc-cratedigger-python-env/lib/python3.13/site-packages/msgspec/__init__.py",
        ):
            with self.subTest(path=path):
                self.assertTrue(self.omit.match(path))


class TestCoveragePathAliases(unittest.TestCase):
    """[paths] must fold prod store paths onto the local repo at combine time."""

    def test_deployed_paths_alias_to_canonical(self):
        cfg = _read_coveragerc()
        patterns = _multiline_list(cfg["paths"]["source"])
        canonical = patterns[0]
        aliases = PathAliases(relative=True)
        for pattern in patterns[1:]:
            aliases.add(pattern, canonical)
        for root in (DEPLOYED_SRC, FLAKE_INPUT_SRC):
            mapped = aliases.map(f"{root}/lib/quality.py")
            with self.subTest(root=root):
                self.assertNotEqual(
                    mapped, f"{root}/lib/quality.py",
                    f"[paths] aliases do not rewrite {root} — the prod-vs-test "
                    "diff would treat prod files as a disjoint tree",
                )
                self.assertTrue(mapped.endswith("lib/quality.py"))


if __name__ == "__main__":
    unittest.main()
