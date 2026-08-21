"""Contract tests for the shared suite-launch memory containment helper.

Deterministic only. `scripts/memory_scope.sh` is test infrastructure, and
`.claude/rules/code-quality.md` reserves Hypothesis for production behaviour.

What these tests pin is the ARGV the helper emits, not the kernel's
enforcement of it. Whether `MemoryMax` actually kills a process is a property
of cgroup v2, not of this repository; driving a real systemd scope from the
suite would make the whole suite depend on a live user D-Bus session. The
enforcement evidence is measured and recorded in the PR body and in the
helper's own header comment (a 200 MiB allocation under a 64 MiB cap: exit 0
without `MemorySwapMax=0`, exit 137 with it).

That split is why `test_prefix_caps_swap` exists and matters: `MemorySwapMax=0`
is the single non-obvious load-bearing flag -- without it `MemoryMax` bounds
only resident memory and a runaway spills into swap instead of dying -- so it
gets its own pin rather than riding along inside a whole-argv assertion.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
HELPER = REPO_ROOT / "scripts" / "memory_scope.sh"

#: Resolved once, absolutely, so a test that narrows PATH to prove the
#: missing-systemd-run branch cannot also make the interpreter unresolvable.
BASH = shutil.which("bash") or "/bin/bash"

#: 8 GiB expressed the way /proc/meminfo does (kB), so the expected
#: MemoryMax below is arithmetic a reader can check by hand rather than a
#: number copied out of a previous run.
_FIXTURE_MEM_TOTAL_KB = 8 * 1024 * 1024
_FIXTURE_MEM_TOTAL_BYTES = _FIXTURE_MEM_TOTAL_KB * 1024
_EXPECTED_MAX_BYTES = _FIXTURE_MEM_TOTAL_BYTES * 70 // 100


class _HelperCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)

        self.meminfo = self.root / "meminfo"
        self.meminfo.write_text(
            f"MemTotal:       {_FIXTURE_MEM_TOTAL_KB} kB\n"
            "MemFree:         1000000 kB\n"
            "MemAvailable:    2000000 kB\n",
            encoding="utf-8",
        )

        # A delegated-memory-controller cgroup tree, at the exact path shape
        # the helper derives from the current uid.
        self.cgroup_root = self.root / "cgroup"
        uid = os.getuid()
        self.user_service = (
            self.cgroup_root
            / "user.slice"
            / f"user-{uid}.slice"
            / f"user@{uid}.service"
        )
        self.user_service.mkdir(parents=True)
        self.controllers = self.user_service / "cgroup.controllers"
        self.controllers.write_text("cpu io memory pids\n", encoding="utf-8")

        # A stub `systemd-run` so the helper's availability probe passes
        # without this test depending on a real one being installed.
        self.fake_bin = self.root / "bin"
        self.fake_bin.mkdir()
        stub = self.fake_bin / "systemd-run"
        stub.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        stub.chmod(0o755)

        # The helper shells out to `id` and nothing else. Giving it its own
        # minimal directory lets the missing-systemd-run test drop `fake_bin`
        # from PATH without also removing the tools the helper legitimately
        # needs -- and, critically, without hiding the REAL systemd-run that
        # the host PATH would otherwise still supply, which would make that
        # test silently assert nothing.
        self.sysbin = self.root / "sysbin"
        self.sysbin.mkdir()
        real_id = shutil.which("id")
        assert real_id is not None, "coreutils `id` must be available"
        (self.sysbin / "id").symlink_to(real_id)

    def run_helper(self, **env_overrides: str) -> subprocess.CompletedProcess[str]:
        """Source the helper, call it, print one argv element per line.

        The `rc=` line is printed before the argv so a failing return is
        visible even when the array is empty.
        """
        script = textwrap.dedent(
            f"""
            source {HELPER}
            cratedigger_memory_scope_prefix
            printf 'rc=%s\\n' "$?"
            printf '%s\\n' "${{CRATEDIGGER_MEMORY_SCOPE_ARGV[@]}}"
            """
        )
        env = dict(os.environ)
        # Drop any inherited real values so a test never reads the host's.
        for leaked in (
            "CRATEDIGGER_TEST_MEMORY_MAX_BYTES",
            "CRATEDIGGER_MEMORY_SCOPE_ACTIVE",
        ):
            env.pop(leaked, None)
        env.update(
            {
                "PATH": f"{self.fake_bin}:{self.sysbin}",
                "_CRATEDIGGER_MEMINFO_PATH": str(self.meminfo),
                "_CRATEDIGGER_CGROUP_ROOT": str(self.cgroup_root),
            }
        )
        env.update(env_overrides)
        return subprocess.run(
            [BASH, "-c", script],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )

    def argv_of(self, result: subprocess.CompletedProcess[str]) -> list[str]:
        lines = [line for line in result.stdout.splitlines() if line]
        self.assertTrue(lines, f"helper printed nothing; stderr={result.stderr}")
        self.assertEqual(lines[0], "rc=0", f"helper failed: {result.stderr}")
        return lines[1:]


class TestPrefixConstruction(_HelperCase):
    def test_default_limit_is_seventy_percent_of_mem_total(self) -> None:
        argv = self.argv_of(self.run_helper())
        self.assertIn(f"MemoryMax={_EXPECTED_MAX_BYTES}", argv)

    def test_prefix_caps_swap(self) -> None:
        """`MemorySwapMax=0` is what makes `MemoryMax` a real bound.

        Without it the kernel reclaims the overage to swap instead of
        refusing it, and a runaway thrashes indefinitely rather than dying
        (measured; see this module's docstring). A mutant dropping this flag
        leaves every other assertion here green.
        """
        argv = self.argv_of(self.run_helper())
        self.assertIn("MemorySwapMax=0", argv)

    def test_prefix_is_a_transient_collected_user_scope(self) -> None:
        argv = self.argv_of(self.run_helper())
        self.assertEqual(argv[0], "systemd-run")
        for flag in ("--user", "--scope", "--collect"):
            self.assertIn(flag, argv)

    def test_prefix_marks_the_scope_active_to_prevent_nesting(self) -> None:
        argv = self.argv_of(self.run_helper())
        self.assertIn("--setenv=CRATEDIGGER_MEMORY_SCOPE_ACTIVE=1", argv)

    def test_does_not_set_memory_high(self) -> None:
        """MemoryHigh stalls a runaway in reclaim instead of killing it.

        Measured at over 120s with nothing to reclaim, which turns an
        unattended gate into a hang. Re-adding it must fail here.
        """
        argv = self.argv_of(self.run_helper())
        self.assertFalse(
            [item for item in argv if item.startswith("MemoryHigh=")],
            f"MemoryHigh must not be set: {argv}",
        )


class TestExplicitOverride(_HelperCase):
    def test_override_is_used_verbatim(self) -> None:
        argv = self.argv_of(
            self.run_helper(CRATEDIGGER_TEST_MEMORY_MAX_BYTES="123456789")
        )
        self.assertIn("MemoryMax=123456789", argv)

    def test_zero_disables_containment(self) -> None:
        result = self.run_helper(CRATEDIGGER_TEST_MEMORY_MAX_BYTES="0")
        self.assertEqual(self.argv_of(result), [])

    def test_invalid_override_fails_closed(self) -> None:
        """An operator who typo'd a limit must not silently get a different
        one. Contrast with the fail-OPEN infrastructure branches below."""
        result = self.run_helper(CRATEDIGGER_TEST_MEMORY_MAX_BYTES="16GiB")
        self.assertIn("rc=1", result.stdout)
        self.assertIn("must be a non-negative integer", result.stderr)

    def test_negative_override_fails_closed(self) -> None:
        result = self.run_helper(CRATEDIGGER_TEST_MEMORY_MAX_BYTES="-1")
        self.assertIn("rc=1", result.stdout)
        self.assertIn("must be a non-negative integer", result.stderr)


class TestFailOpenBranches(_HelperCase):
    """Missing infrastructure degrades to an uncontained run, loudly.

    Each branch is driven to its own condition with the EARLIER branches
    passing, so a short-circuit cannot let one assertion stand in for
    another, and each asserts its own distinct message.
    """

    def test_already_scoped_run_does_not_nest(self) -> None:
        result = self.run_helper(CRATEDIGGER_MEMORY_SCOPE_ACTIVE="1")
        self.assertEqual(self.argv_of(result), [])
        self.assertEqual(result.stderr, "")

    def test_missing_systemd_run_warns_and_continues(self) -> None:
        result = self.run_helper(PATH=str(self.sysbin))
        self.assertEqual(self.argv_of(result), [])
        self.assertIn("systemd-run is not available", result.stderr)
        self.assertIn("running uncontained", result.stderr)

    def test_undelegated_memory_controller_warns_and_continues(self) -> None:
        self.controllers.write_text("cpu io pids\n", encoding="utf-8")
        result = self.run_helper()
        self.assertEqual(self.argv_of(result), [])
        self.assertIn("memory controller is not delegated", result.stderr)

    def test_unreadable_cgroup_warns_and_continues(self) -> None:
        self.controllers.unlink()
        result = self.run_helper()
        self.assertEqual(self.argv_of(result), [])
        self.assertIn("cgroup is unreadable", result.stderr)

    def test_unreadable_meminfo_warns_and_continues(self) -> None:
        self.meminfo.unlink()
        result = self.run_helper()
        self.assertEqual(self.argv_of(result), [])
        self.assertIn("cannot read MemTotal", result.stderr)

    def test_substring_controller_name_is_not_a_match(self) -> None:
        """`memory` must match a whole controller, not a prefix of one.

        A naive `[[ $controllers == *memory* ]]` would accept a future
        controller merely containing the word and enable a limit the kernel
        then ignores.
        """
        self.controllers.write_text("cpu io memoryfoo pids\n", encoding="utf-8")
        result = self.run_helper()
        self.assertEqual(self.argv_of(result), [])
        self.assertIn("memory controller is not delegated", result.stderr)


class TestLaunchersUseTheHelper(unittest.TestCase):
    """The helper is worthless if a launcher stops calling it.

    Pinned by reading each launcher's source rather than by executing it:
    running them for real means running the whole suite.
    """

    LAUNCHERS = ("run_final_gate.sh", "test.sh")

    def test_every_suite_launcher_sources_and_applies_the_prefix(self) -> None:
        for name in self.LAUNCHERS:
            with self.subTest(launcher=name):
                source = (REPO_ROOT / "scripts" / name).read_text(encoding="utf-8")
                self.assertIn("memory_scope.sh", source)
                self.assertIn("cratedigger_memory_scope_prefix", source)
                self.assertIn('"${CRATEDIGGER_MEMORY_SCOPE_ARGV[@]}"', source)

    def test_nightly_runners_document_why_they_opt_out(self) -> None:
        """The two nightly runners deliberately do NOT use the helper: they
        run under nixosconfig SYSTEM units with no reliable user bus, and
        carry a declarative unit-level MemoryMax instead. An undocumented
        absence is indistinguishable from an oversight."""
        for name in ("daily_flake_update.sh", "daily_beets_tip_update.sh"):
            with self.subTest(runner=name):
                source = (REPO_ROOT / "scripts" / name).read_text(encoding="utf-8")
                self.assertIn("memory_scope.sh", source)


if __name__ == "__main__":
    unittest.main()
