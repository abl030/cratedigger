"""Generated exhaustion contract for the full-suite coordinator."""

from __future__ import annotations

import io
import os
import shutil
import unittest
from pathlib import Path

import msgspec
from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.run_test_suite import (
    CheckSummary,
    PhaseExecution,
    PhaseSpec,
    run_suite,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PHASE_NAMES = (
    "js-syntax",
    "js-unit",
    "pyright",
    "pyright-production-strict",
    "ruff",
    "vulture",
    "python",
)


def decode_summary(path: Path) -> CheckSummary:
    return msgspec.json.decode(path.read_bytes(), type=CheckSummary)


def assert_generated_outcome(
    *,
    exit_codes: tuple[int, ...],
    executed: tuple[str, ...],
    phase_states: tuple[str, ...],
    suite_state: str,
    exit_code: int,
) -> None:
    if executed != PHASE_NAMES:
        raise AssertionError(f"not every phase executed once: {executed!r}")
    expected_states = tuple("passed" if code == 0 else "failed" for code in exit_codes)
    if phase_states != expected_states:
        raise AssertionError(
            f"phase states {phase_states!r} do not match {expected_states!r}"
        )
    expected_suite_state = "passed" if all(code == 0 for code in exit_codes) else "failed"
    expected_exit = 0 if expected_suite_state == "passed" else 1
    if suite_state != expected_suite_state or exit_code != expected_exit:
        raise AssertionError(
            f"root outcome {(suite_state, exit_code)!r} does not match "
            f"{(expected_suite_state, expected_exit)!r}"
        )


class TestSuiteCoordinatorGenerated(unittest.TestCase):
    @given(
        exit_codes=st.lists(
            st.integers(min_value=0, max_value=1),
            min_size=len(PHASE_NAMES),
            max_size=len(PHASE_NAMES),
        )
    )
    def test_every_phase_contributes_to_the_single_terminal_outcome(
        self,
        exit_codes: list[int],
    ) -> None:
        outcomes = tuple(exit_codes)
        runtime = Path(
            os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
        )
        executed: list[str] = []
        phases = tuple(
            PhaseSpec(
                name,
                ("unused",),
                f"rerun-{name}",
                "generic",
            )
            for name in PHASE_NAMES
        )

        def execute(
            phase: PhaseSpec,
            _command: tuple[str, ...],
            log_path: Path,
        ) -> PhaseExecution:
            index = PHASE_NAMES.index(phase.name)
            executed.append(phase.name)
            log_path.write_text(
                "ok\n" if outcomes[index] == 0 else "known bad\n",
                encoding="utf-8",
            )
            return PhaseExecution(
                exit_code=outcomes[index],
                elapsed_seconds=0.01,
            )

        result = run_suite(
            repo_root=REPO_ROOT,
            phases=phases,
            runtime_dir=runtime,
            executor=execute,
            stream=io.StringIO(),
        )
        self.addCleanup(shutil.rmtree, result.bundle, True)
        summary = decode_summary(result.bundle / "summary.json")

        assert_generated_outcome(
            exit_codes=outcomes,
            executed=tuple(executed),
            phase_states=tuple(phase.state for phase in summary.phases),
            suite_state=summary.state,
            exit_code=result.exit_code,
        )


class TestSuiteCoordinatorGeneratedKnownBad(unittest.TestCase):
    def test_checker_rejects_the_old_fail_fast_shape(self) -> None:
        with self.assertRaisesRegex(AssertionError, "not every phase"):
            assert_generated_outcome(
                exit_codes=(1, 1, 1, 1, 1, 1),
                executed=("js-syntax",),
                phase_states=("failed",),
                suite_state="failed",
                exit_code=1,
            )

    def test_checker_rejects_false_green_after_a_failed_phase(self) -> None:
        with self.assertRaisesRegex(AssertionError, "root outcome"):
            assert_generated_outcome(
                exit_codes=(0, 0, 1, 0, 0, 0),
                executed=PHASE_NAMES,
                phase_states=(
                    "passed",
                    "passed",
                    "failed",
                    "passed",
                    "passed",
                    "passed",
                ),
                suite_state="passed",
                exit_code=0,
            )


if __name__ == "__main__":
    unittest.main()
