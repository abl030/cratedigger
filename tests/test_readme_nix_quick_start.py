"""Evaluate the README's copy-paste NixOS quick-start."""

from __future__ import annotations

import json
import re
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

REPO_ROOT = Path(__file__).resolve().parent.parent
README = REPO_ROOT / "README.md"

#: Resolve the repository to a store path, never handing Nix the live tree.
#:
#: This test overrides the README flake's ``cratedigger`` input with the
#: repository itself. Naming it as ``path:<root>`` made Nix copy and hash the
#: WHOLE working directory. Measured 2026-09-09 by planting probe files and
#: reading the resulting store copy: it carried untracked paths (a file under
#: ``tests/_harness_fixtures/``), ``.gitignore``d ones (under ``build/`` and
#: ``.hypothesis/``) and ``.git`` itself, and its store hash moved whenever
#: any of them changed — so the walk repeats every run in a tree a suite is
#: writing to. Nothing is excluded, so the root a developer runs this from
#: decides the size: this repository's own shared checkout measured 2.2 GB
#: that day, 1.8 GB of it agent worktrees under ``.claude/worktrees/``. The
#: plain ``.`` ref carried none of the probes.
#:
#: That is the same walk #1378 removed from ``tests/test_nix_module.py`` and
#: ``tests/test_web_auth_mode_generated.py`` after one died mid-walk with
#: "path .../tests/_harness_fixtures does not exist" — the JavaScript phase
#: runs concurrently with this one. The flake ref on the command line was
#: the live-tree walker those two left behind (#1394 item 2).
#:
#: So the override is a store path, resolved through the same expression
#: those two modules use: the git snapshot when ``.git`` exists (tracked
#: working-tree content, no untracked path), else #1248's filtered copy for a
#: ``git archive`` snapshot, which cannot be fetched as ``git+file``.
#: ``builtins.path``'s result carries a store-path string context that
#: ``getFlake`` refuses, hence ``unsafeDiscardStringContext``; the copy is
#: already realized on disk by then. Cost, measured in a 52 MB worktree at
#: host load ~30: 0.04s for this call, against 0.55s to re-walk the live tree
#: after a one-file change.
_SOURCE_EXPRESSION = r"""
  (builtins.getFlake (
    if builtins.pathExists ./.git
    then "git+file://" + toString ./.
    else builtins.unsafeDiscardStringContext (toString (builtins.path {
      path = toString ./.;
      filter = path: type:
        baseNameOf path != "__pycache__"
        && baseNameOf path != "_harness_fixtures";
      name = "cratedigger-nix-eval-source";
    }))
  )).outPath
"""


def _cratedigger_source_flake_ref(root: Path = REPO_ROOT) -> str:
    """The flake ref the README's ``cratedigger`` input is overridden with."""
    result = subprocess.run(
        ["nix", "eval", "--impure", "--raw", "--expr", _SOURCE_EXPRESSION],
        cwd=root,
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr)
    store_path = result.stdout.strip()
    if not store_path.startswith("/nix/store/"):
        raise AssertionError(f"source did not resolve to a store path: {store_path!r}")
    return f"path:{store_path}"


def _extract_nix_quick_start(readme: str) -> str:
    section = readme.partition("## Running it (NixOS)")[2]
    if not section:
        raise AssertionError("README has no 'Running it (NixOS)' section")
    section = section.partition("\n## ")[0]
    matches = re.findall(r"```nix\n(?P<body>.*?)\n```", section, re.DOTALL)
    if len(matches) != 1:
        raise AssertionError(
            "README NixOS section must contain exactly one fenced Nix example"
        )
    return matches[0]


def _read_nix_quick_start() -> str:
    return _extract_nix_quick_start(README.read_text(encoding="utf-8"))


class TestCratediggerSourceFlakeRef(unittest.TestCase):
    """#1394: the ``cratedigger`` input override never names the live tree.

    Each branch is driven against a throwaway tree this class builds, one
    with ``.git`` and one without, so both hold wherever the module runs.
    Asserting the git branch against THIS repository would have proved
    nothing in a ``git archive`` snapshot, which has no ``.git`` of its own
    and sends every call down the fallback — and that snapshot is exactly
    where the mutant runner works.
    """

    @staticmethod
    def _flake(root: Path) -> None:
        (root / "flake.nix").write_text("{ outputs = _: {}; }\n", encoding="utf-8")

    @staticmethod
    def _git(root: Path, *args: str) -> None:
        subprocess.run(
            ["git", "-C", str(root), *args],
            capture_output=True,
            check=True,
            text=True,
        )

    def test_this_repository_resolves_to_a_store_path(self) -> None:
        ref = _cratedigger_source_flake_ref()
        self.assertTrue(ref.startswith("path:/nix/store/"), ref)
        self.assertNotIn(str(REPO_ROOT), ref)
        self.assertTrue((Path(ref.removeprefix("path:")) / "flake.nix").is_file(), ref)

    def test_a_tree_with_git_resolves_to_the_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._flake(root)
            (root / "tracked.txt").write_text("tracked\n", encoding="utf-8")
            (root / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
            self._git(root, "init", "-q", "-b", "main")
            self._git(root, "add", "flake.nix", "tracked.txt", ".gitignore")
            self._git(
                root,
                "-c", "user.email=fixture@example.invalid",
                "-c", "user.name=fixture",
                "commit", "-qm", "fixture",
            )
            (root / "untracked.txt").write_text("untracked\n", encoding="utf-8")
            (root / "ignored.txt").write_text("ignored\n", encoding="utf-8")

            ref = _cratedigger_source_flake_ref(root)

        self.assertTrue(ref.startswith("path:/nix/store/"), ref)
        source = Path(ref.removeprefix("path:"))
        self.assertTrue((source / "tracked.txt").is_file(), source)
        # The three a `path:` ref would have carried (measured 2026-09-09).
        self.assertFalse((source / "untracked.txt").exists(), source)
        self.assertFalse((source / "ignored.txt").exists(), source)
        self.assertFalse((source / ".git").exists(), source)

    def test_a_tree_with_no_git_falls_back_to_the_filtered_copy(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            self._flake(root)
            (root / "kept.txt").write_text("kept\n", encoding="utf-8")
            for churn in ("_harness_fixtures", "__pycache__"):
                (root / churn).mkdir()
                (root / churn / "transient.txt").write_text("x\n", encoding="utf-8")

            ref = _cratedigger_source_flake_ref(root)

        self.assertTrue(ref.startswith("path:/nix/store/"), ref)
        source = Path(ref.removeprefix("path:"))
        self.assertTrue((source / "kept.txt").is_file(), source)
        self.assertFalse((source / "_harness_fixtures").exists(), source)
        self.assertFalse((source / "__pycache__").exists(), source)


class TestReadmeNixQuickStart(unittest.TestCase):
    def test_extractor_rejects_a_nix_fence_only_in_a_later_section(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "exactly one fenced Nix example",
        ):
            _extract_nix_quick_start(
                "## Running it (NixOS)\nNo example.\n"
                "## Later\n```nix\n{ outputs = _: {}; }\n```\n"
            )

    def test_extractor_rejects_multiple_nix_fences_in_section(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "exactly one fenced Nix example",
        ):
            _extract_nix_quick_start(
                "## Running it (NixOS)\n"
                "```nix\n{ outputs = _: {}; }\n```\n"
                "```nix\n{ outputs = _: {}; }\n```\n"
                "## Later\n"
            )

    def test_evaluates_as_non_root_basic_auth_install(self) -> None:
        with TemporaryDirectory() as temp_dir:
            Path(temp_dir, "flake.nix").write_text(
                _read_nix_quick_start(),
                encoding="utf-8",
            )
            projection = r"""
              config:
              let
                service = config.services.cratedigger;
                accessGroup = service.web.accessGroup;
                failures = map (assertion: assertion.message)
                  (builtins.filter
                    (assertion:
                      !assertion.assertion
                      && builtins.match
                        "services[.]cratedigger.*"
                        assertion.message != null)
                    config.assertions);
              in {
                inherit failures;
                user = service.user;
                group = service.group;
                isSystemUser = config.users.users.${service.user}.isSystemUser;
                hostName = service.web.hostName;
                gatewayPort = service.web.gatewayPort;
                inherit accessGroup;
                accessGroupDeclared =
                  builtins.hasAttr accessGroup config.users.groups;
                serviceHasAccess =
                  builtins.elem accessGroup
                    config.users.users.${service.user}.extraGroups;
                basicAuthFile = service.web.basicAuthFile;
                insecure = service.web.enableInsecure;
              }
            """
            result = subprocess.run(
                [
                    "nix",
                    "eval",
                    "--impure",
                    "--json",
                    "--no-write-lock-file",
                    "--override-input",
                    "cratedigger",
                    _cratedigger_source_flake_ref(),
                    "--apply",
                    projection,
                    f"path:{temp_dir}#nixosConfigurations.myhost.config",
                ],
                capture_output=True,
                check=False,
                text=True,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            json.loads(result.stdout),
            {
                "failures": [],
                "user": "cratedigger",
                "group": "users",
                "isSystemUser": True,
                "hostName": "music.example.net",
                "gatewayPort": 8086,
                "accessGroup": "cratedigger-web",
                "accessGroupDeclared": True,
                "serviceHasAccess": True,
                "basicAuthFile": "/run/secrets/cratedigger.htpasswd",
                "insecure": False,
            },
        )


if __name__ == "__main__":
    unittest.main()
