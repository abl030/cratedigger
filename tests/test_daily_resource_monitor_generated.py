"""Generated timeline coverage for the daily resource receipt."""

from __future__ import annotations

import subprocess
import unittest

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from tests.test_daily_resource_monitor import (
    PHASE_PREFIX,
    RECEIPT_PREFIX,
    parsed_valid_summary,
    sample,
    summarize_samples,
)


@st.composite
def resource_timelines(draw: st.DrawFn) -> list[dict[str, int | str]]:
    phases = draw(
        st.lists(
            st.sampled_from(("stable_nix", "generated_fuzz", "world_model")),
            min_size=1,
            max_size=12,
        )
    )
    currents = draw(
        st.lists(
            st.integers(min_value=1, max_value=100_000),
            min_size=len(phases),
            max_size=len(phases),
        )
    )
    scratch_values = draw(
        st.lists(
            st.integers(min_value=0, max_value=50_000),
            min_size=len(phases),
            max_size=len(phases),
        )
    )
    inode_values = draw(
        st.lists(
            st.integers(min_value=0, max_value=5_000),
            min_size=len(phases),
            max_size=len(phases),
        )
    )
    rows: list[dict[str, int | str]] = []
    memory_peak = 0
    swap_peak = 0
    for index, (phase, current, scratch_bytes, scratch_inodes) in enumerate(
        zip(phases, currents, scratch_values, inode_values, strict=True), start=1
    ):
        memory_peak = draw(
            st.integers(
                min_value=max(memory_peak, current),
                max_value=250_000,
            )
        )
        swap_current = draw(st.integers(min_value=0, max_value=2_000))
        swap_peak = draw(
            st.integers(
                min_value=max(swap_peak, swap_current),
                max_value=5_000,
            )
        )
        shmem = draw(st.integers(min_value=0, max_value=current))
        remaining = current - shmem
        anon = draw(st.integers(min_value=0, max_value=remaining))
        file_bytes = draw(st.integers(min_value=shmem, max_value=shmem + remaining))
        kernel = max(0, current - anon - file_bytes)
        rows.append(
            {
                "timestamp_ns": index,
                "phase": phase,
                "memory_current": current,
                "memory_peak": memory_peak,
                "swap_current": swap_current,
                "swap_peak": swap_peak,
                "anon": anon,
                "file": file_bytes,
                "shmem": shmem,
                "kernel": kernel,
                "scratch_bytes": scratch_bytes,
                "scratch_inodes": scratch_inodes,
            }
        )
    return rows


def assert_summary_matches_timeline(
    rows: list[dict[str, int | str]],
    phases: dict[str, dict[str, str]],
    receipt: dict[str, str],
) -> None:
    phase_names = list(dict.fromkeys(str(row["phase"]) for row in rows))
    if set(phases) != set(phase_names):
        raise AssertionError("phase membership drifted")
    for phase in phase_names:
        phase_samples = [row for row in rows if row["phase"] == phase]
        peak_row = max(
            phase_samples,
            key=lambda row: int(row["memory_current"]),
        )
        actual = phases[phase]
        if int(actual["samples"]) != len(phase_samples):
            raise AssertionError(f"{phase} sample count drifted")
        if int(actual["memory_current_peak_bytes"]) != int(
            peak_row["memory_current"]
        ):
            raise AssertionError(f"{phase} memory-current peak drifted")
        for field in ("anon", "file", "shmem", "kernel"):
            if int(actual[f"{field}_at_memory_current_peak_bytes"]) != int(
                peak_row[field]
            ):
                raise AssertionError(f"{phase} {field} companion sample drifted")
        if int(actual["non_shmem_at_memory_current_peak_bytes"]) != (
            int(peak_row["memory_current"]) - int(peak_row["shmem"])
        ):
            raise AssertionError(f"{phase} non-shmem companion sample drifted")
        if int(actual["scratch_at_memory_current_peak_bytes"]) != int(
            peak_row["scratch_bytes"]
        ):
            raise AssertionError(f"{phase} scratch companion sample drifted")
        if int(actual["scratch_byte_peak"]) != max(
            int(row["scratch_bytes"]) for row in phase_samples
        ):
            raise AssertionError(f"{phase} scratch byte peak drifted")
        if int(actual["scratch_inode_peak"]) != max(
            int(row["scratch_inodes"]) for row in phase_samples
        ):
            raise AssertionError(f"{phase} scratch inode peak drifted")
        if int(actual["memory_peak_start_bytes"]) != int(
            phase_samples[0]["memory_peak"]
        ):
            raise AssertionError(f"{phase} memory peak start drifted")
        if int(actual["memory_peak_end_bytes"]) != int(
            phase_samples[-1]["memory_peak"]
        ):
            raise AssertionError(f"{phase} memory peak end drifted")
        if int(actual["swap_current_peak_bytes"]) != max(
            int(row["swap_current"]) for row in phase_samples
        ):
            raise AssertionError(f"{phase} swap-current peak drifted")
        if int(actual["swap_peak_start_bytes"]) != int(
            phase_samples[0]["swap_peak"]
        ):
            raise AssertionError(f"{phase} swap peak start drifted")
        if int(actual["swap_peak_end_bytes"]) != int(
            phase_samples[-1]["swap_peak"]
        ):
            raise AssertionError(f"{phase} swap peak end drifted")

    final_kernel_peak = max(int(row["memory_peak"]) for row in rows)
    expected_owner = next(
        str(row["phase"])
        for row in rows
        if int(row["memory_peak"]) == final_kernel_peak
    )
    if int(receipt["memory_peak_bytes"]) != final_kernel_peak:
        raise AssertionError("terminal kernel peak drifted")
    if receipt["memory_peak_owner"] != expected_owner:
        raise AssertionError("terminal peak owner drifted")


class TestDailyResourceSummaryGenerated(unittest.TestCase):
    @given(rows=resource_timelines())
    def test_every_phase_keeps_its_true_maximum_and_companion_sample(
        self, rows: list[dict[str, int | str]],
    ) -> None:
        completed = summarize_samples(
            [
                sample(
                    int(row["timestamp_ns"]),
                    str(row["phase"]),
                    memory_current=int(row["memory_current"]),
                    memory_peak=int(row["memory_peak"]),
                    swap_current=int(row["swap_current"]),
                    swap_peak=int(row["swap_peak"]),
                    anon=int(row["anon"]),
                    file=int(row["file"]),
                    shmem=int(row["shmem"]),
                    kernel=int(row["kernel"]),
                    scratch_bytes=int(row["scratch_bytes"]),
                    scratch_inodes=int(row["scratch_inodes"]),
                    scratch_byte_limit=50_001,
                    scratch_inode_limit=5_001,
                )
                for row in rows
            ]
        )
        phases, receipt = parsed_valid_summary(completed)
        assert_summary_matches_timeline(rows, phases, receipt)

    @given(
        memory_current=st.integers(min_value=0, max_value=100_000),
        excess=st.integers(min_value=1, max_value=100_000),
    )
    def test_impossible_shmem_worlds_fail_closed(
        self, memory_current: int, excess: int,
    ) -> None:
        shmem = memory_current + excess
        completed = summarize_samples(
            [
                sample(
                    1, "generated_fuzz",
                    memory_current=memory_current,
                    memory_peak=shmem,
                    file=shmem,
                    shmem=shmem,
                )
            ]
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("reason=memory_breakdown_invalid", completed.stdout)

    def test_checker_rejects_known_bad_aggregates_and_missing_terminal(self) -> None:
        rows: list[dict[str, int | str]] = [
            {
                "timestamp_ns": 1, "phase": "stable_nix",
                "memory_current": 100, "memory_peak": 500,
                "swap_current": 0, "swap_peak": 0,
                "anon": 40, "file": 50, "shmem": 30, "kernel": 10,
                "scratch_bytes": 70, "scratch_inodes": 7,
            },
            {
                "timestamp_ns": 2, "phase": "generated_fuzz",
                "memory_current": 400, "memory_peak": 500,
                "swap_current": 0, "swap_peak": 0,
                "anon": 200, "file": 150, "shmem": 100, "kernel": 50,
                "scratch_bytes": 90, "scratch_inodes": 9,
            },
        ]
        completed = summarize_samples(
            [
                sample(
                    int(row["timestamp_ns"]), str(row["phase"]),
                    memory_current=int(row["memory_current"]),
                    memory_peak=int(row["memory_peak"]),
                    anon=int(row["anon"]), file=int(row["file"]),
                    shmem=int(row["shmem"]), kernel=int(row["kernel"]),
                    scratch_bytes=int(row["scratch_bytes"]),
                    scratch_inodes=int(row["scratch_inodes"]),
                )
                for row in rows
            ]
        )
        phases, receipt = parsed_valid_summary(completed)

        bad_last_value = {key: value.copy() for key, value in phases.items()}
        bad_last_value["generated_fuzz"]["memory_current_peak_bytes"] = "50"
        with self.assertRaises(AssertionError):
            assert_summary_matches_timeline(rows, bad_last_value, receipt)

        bad_companion = {key: value.copy() for key, value in phases.items()}
        bad_companion["generated_fuzz"]["anon_at_memory_current_peak_bytes"] = "20"
        with self.assertRaises(AssertionError):
            assert_summary_matches_timeline(rows, bad_companion, receipt)

        bad_owner = receipt.copy()
        bad_owner["memory_peak_owner"] = "generated_fuzz"
        with self.assertRaises(AssertionError):
            assert_summary_matches_timeline(rows, phases, bad_owner)

        without_terminal = "\n".join(
            line for line in completed.stdout.splitlines()
            if line.startswith(PHASE_PREFIX) and not line.startswith(RECEIPT_PREFIX)
        )
        missing = subprocess.CompletedProcess(
            args=completed.args, returncode=0, stdout=without_terminal, stderr=""
        )
        with self.assertRaises(AssertionError):
            parsed_valid_summary(missing)


if __name__ == "__main__":
    unittest.main()
