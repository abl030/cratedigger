"""Contracts for the daily gate's phase-correlated resource receipt."""

from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MONITOR = REPO_ROOT / "scripts" / "daily_resource_monitor.sh"
PHASE_PREFIX = "CRATEDIGGER_DAILY_RESOURCE_PHASE "
RECEIPT_PREFIX = "CRATEDIGGER_DAILY_RESOURCE_RECEIPT "


def sample(
    timestamp_ns: int,
    phase: str,
    *,
    memory_current: int,
    memory_peak: int,
    swap_current: int = 0,
    swap_peak: int = 0,
    anon: int = 0,
    file: int = 0,
    shmem: int = 0,
    kernel: int = 0,
    scratch_bytes: int = 0,
    scratch_byte_limit: int = 16_000,
    scratch_inodes: int = 0,
    scratch_inode_limit: int = 1_000,
) -> str:
    return "\t".join(
        str(value)
        for value in (
            timestamp_ns,
            phase,
            memory_current,
            memory_peak,
            swap_current,
            swap_peak,
            anon,
            file,
            shmem,
            kernel,
            scratch_bytes,
            scratch_byte_limit,
            scratch_inodes,
            scratch_inode_limit,
        )
    )


def parse_fields(line: str, prefix: str) -> dict[str, str]:
    if not line.startswith(prefix):
        raise AssertionError(f"missing prefix {prefix!r}: {line!r}")
    return dict(field.split("=", 1) for field in line.removeprefix(prefix).split())


def summarize_samples(
    rows: list[str], dropped_samples: int = 0
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temporary:
        samples = Path(temporary) / "samples.tsv"
        samples.write_text("\n".join(rows) + "\n", encoding="utf-8")
        return subprocess.run(
            [
                "bash", str(MONITOR), "summarize", str(samples), str(dropped_samples),
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )


def parsed_summary(
    completed: subprocess.CompletedProcess[str], *, allowed_statuses: set[str]
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    if completed.returncode != 0:
        raise AssertionError(completed.stderr or completed.stdout)
    phase_rows: dict[str, dict[str, str]] = {}
    receipts: list[dict[str, str]] = []
    for line in completed.stdout.splitlines():
        if line.startswith(PHASE_PREFIX):
            fields = parse_fields(line, PHASE_PREFIX)
            phase_rows[fields["phase"]] = fields
        elif line.startswith(RECEIPT_PREFIX):
            receipts.append(parse_fields(line, RECEIPT_PREFIX))
    if len(receipts) != 1:
        raise AssertionError(f"expected one terminal receipt, got {len(receipts)}")
    receipt = receipts[0]
    if receipt.get("status") not in allowed_statuses:
        raise AssertionError(
            f"resource receipt status not in {allowed_statuses}: {receipt}"
        )
    return phase_rows, receipt


def parsed_valid_summary(
    completed: subprocess.CompletedProcess[str],
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    return parsed_summary(completed, allowed_statuses={"valid"})


class TestDailyResourceSummary(unittest.TestCase):
    def test_phase_transition_serializes_label_and_metric_sampling(self) -> None:
        harness = r'''
set -euo pipefail
source "$1"
state="$2"
_CRATEDIGGER_RESOURCE_DIR="$state"
_CRATEDIGGER_RESOURCE_SAMPLES="$state/samples.tsv"
_CRATEDIGGER_RESOURCE_PHASE="$state/phase"
_CRATEDIGGER_RESOURCE_FAILURE="$state/failure"
_CRATEDIGGER_RESOURCE_LOCK="$state/sample.lock"
_CRATEDIGGER_RESOURCE_STARTED=1
_CRATEDIGGER_RESOURCE_CURRENT_PHASE=old_phase
: > "$_CRATEDIGGER_RESOURCE_SAMPLES"
: > "$_CRATEDIGGER_RESOURCE_FAILURE"
: > "$_CRATEDIGGER_RESOURCE_LOCK"
printf '%s\n' old_phase > "$_CRATEDIGGER_RESOURCE_PHASE"
printf '%s\n' 100 > "$state/peak"

_daily_resource_record_sample_unlocked() {
    local phase="$1" peak timestamp
    timestamp="$(date +%s%N)"
    if [[ "$phase" == old_phase ]] && mkdir "$state/claim" 2>/dev/null; then
        : > "$state/entered"
        while [[ ! -e "$state/release" ]]; do
            sleep 0.01
        done
    fi
    peak="$(<"$state/peak")"
    printf '%s\t%s\t%s\t%s\t0\t0\t%s\t0\t0\t0\t0\t1000\t0\t1000\n' \
        "$timestamp" "$phase" "$peak" "$peak" "$peak" \
        >> "$_CRATEDIGGER_RESOURCE_SAMPLES"
}

_daily_resource_record_current_phase_locked &
background=$!
while [[ ! -e "$state/entered" ]]; do
    sleep 0.01
done
(sleep 0.2; : > "$state/release") &
daily_resource_monitor_set_phase new_phase
printf '%s\n' 200 > "$state/peak"
_daily_resource_record_sample_locked new_phase
wait "$background"
daily_resource_summarize_samples "$_CRATEDIGGER_RESOURCE_SAMPLES"
'''
        with tempfile.TemporaryDirectory() as temporary:
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), temporary],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=5,
            )
            samples = Path(temporary, "samples.tsv").read_text(
                encoding="utf-8"
            )

        phases, receipt = parsed_valid_summary(completed)
        old_peaks = [
            row.split("\t")[3]
            for row in samples.splitlines()
            if row.split("\t")[1] == "old_phase"
        ]
        self.assertTrue(old_peaks)
        self.assertEqual(set(old_peaks), {"100"})
        self.assertEqual(phases["new_phase"]["memory_peak_end_bytes"], "200")
        self.assertEqual(receipt["memory_peak_owner"], "new_phase")

    def test_phase_peak_retains_its_time_correlated_breakdown(self) -> None:
        completed = summarize_samples(
            [
                sample(
                    10, "stable_nix", memory_current=800, memory_peak=800,
                    anon=500, file=240, shmem=40, kernel=60,
                    scratch_bytes=100, scratch_inodes=10,
                ),
                sample(
                    20, "stable_nix", memory_current=600, memory_peak=800,
                    anon=300, file=230, shmem=80, kernel=70,
                    scratch_bytes=300, scratch_inodes=30,
                ),
                sample(
                    30, "generated_fuzz", memory_current=1_700, memory_peak=1_700,
                    swap_current=20, swap_peak=20,
                    anon=400, file=1_100, shmem=1_000, kernel=200,
                    scratch_bytes=1_200, scratch_inodes=120,
                ),
                sample(
                    40, "generated_fuzz", memory_current=1_200, memory_peak=1_700,
                    swap_current=5, swap_peak=20,
                    anon=700, file=300, shmem=100, kernel=200,
                    scratch_bytes=1_500, scratch_inodes=150,
                ),
            ]
        )

        phases, receipt = parsed_valid_summary(completed)
        fuzz = phases["generated_fuzz"]
        self.assertEqual(fuzz["memory_current_peak_bytes"], "1700")
        self.assertEqual(fuzz["anon_at_memory_current_peak_bytes"], "400")
        self.assertEqual(fuzz["shmem_at_memory_current_peak_bytes"], "1000")
        self.assertEqual(fuzz["non_shmem_at_memory_current_peak_bytes"], "700")
        self.assertEqual(fuzz["scratch_at_memory_current_peak_bytes"], "1200")
        self.assertEqual(fuzz["scratch_byte_peak"], "1500")
        self.assertEqual(fuzz["scratch_inode_peak"], "150")
        self.assertEqual(fuzz["memory_peak_start_bytes"], "1700")
        self.assertEqual(fuzz["memory_peak_end_bytes"], "1700")
        self.assertEqual(receipt["memory_peak_bytes"], "1700")
        self.assertEqual(receipt["memory_peak_owner"], "generated_fuzz")
        self.assertEqual(receipt["swap_peak_bytes"], "20")
        self.assertEqual(receipt["samples"], "4")
        self.assertEqual(receipt["phases"], "2")
        self.assertEqual(receipt["dropped_samples"], "0")

    def test_zero_scratch_limits_are_invalid_not_empty_evidence(self) -> None:
        completed = summarize_samples(
            [
                sample(
                    10, "generated_fuzz", memory_current=1, memory_peak=1,
                    scratch_byte_limit=0, scratch_inode_limit=0,
                )
            ]
        )

        self.assertNotEqual(completed.returncode, 0)
        receipts = [
            parse_fields(line, RECEIPT_PREFIX)
            for line in completed.stdout.splitlines()
            if line.startswith(RECEIPT_PREFIX)
        ]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["status"], "invalid")
        self.assertEqual(receipts[0]["reason"], "scratch_limit_invalid")
        self.assertNotIn(PHASE_PREFIX, completed.stdout)

    def test_impossible_memory_breakdown_is_invalid_not_negative_evidence(
        self,
    ) -> None:
        completed = summarize_samples(
            [
                sample(
                    10, "generated_fuzz", memory_current=10, memory_peak=20,
                    file=10, shmem=11,
                )
            ]
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("status=invalid", completed.stdout)
        self.assertIn("reason=memory_breakdown_invalid", completed.stdout)
        self.assertNotIn("non_shmem_at_memory_current_peak_bytes=-", completed.stdout)

    def test_non_monotonic_kernel_peak_is_rejected(self) -> None:
        completed = summarize_samples(
            [
                sample(10, "stable_nix", memory_current=80, memory_peak=100),
                sample(20, "stable_nix", memory_current=70, memory_peak=90),
            ]
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("status=invalid", completed.stdout)
        self.assertIn("reason=memory_peak_regressed", completed.stdout)

    def test_concurrent_append_order_is_sorted_by_sample_timestamp(self) -> None:
        completed = summarize_samples(
            [
                sample(20, "generated_fuzz", memory_current=200, memory_peak=200),
                sample(10, "stable_nix", memory_current=100, memory_peak=100),
            ]
        )

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(list(phases), ["stable_nix", "generated_fuzz"])
        self.assertEqual(receipt["memory_peak_owner"], "generated_fuzz")

    def test_degraded_receipt_still_carries_the_full_phase_breakdown(self) -> None:
        """A non-zero dropped_samples count marks the receipt distinguishably
        (status=degraded, reason=partial_sample_loss) while preserving every
        aggregate field a clean run has -- issue #1214 gap 2."""
        completed = summarize_samples(
            [
                sample(10, "generated_fuzz", memory_current=800, memory_peak=800),
                sample(20, "generated_fuzz", memory_current=600, memory_peak=800),
            ],
            dropped_samples=3,
        )

        phases, receipt = parsed_summary(completed, allowed_statuses={"degraded"})
        self.assertEqual(receipt["reason"], "partial_sample_loss")
        self.assertEqual(receipt["dropped_samples"], "3")
        self.assertEqual(receipt["samples"], "2")
        self.assertIn("generated_fuzz", phases)

    def test_summarize_rejects_a_malformed_dropped_samples_argument(self) -> None:
        """Known-bad self-test for the new dropped_samples argument guard:
        Q1, does the clause trip. The production caller always passes a
        wc -l count (always a clean non-negative integer, see
        daily_resource_monitor_finish); this guards the CLI surface a human
        or a future caller can still misuse."""
        with tempfile.TemporaryDirectory() as temporary:
            samples = Path(temporary) / "samples.tsv"
            samples.write_text(
                sample(10, "stable_nix", memory_current=1, memory_peak=1) + "\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["bash", str(MONITOR), "summarize", str(samples), "not-a-number"],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=summarize_arguments_invalid", completed.stdout
        )

    def test_state_root_sharing_the_scratch_filesystem_is_rejected(self) -> None:
        """Known-bad self-test for the new state-root isolation guard (issue
        #1214 gap 1): if the monitor's chosen state root ever resolves to
        the SAME filesystem as the tmpfs under measurement, that silently
        recreates the defect this issue exists to fix. Two distinct
        directories under /dev/shm are genuinely the same tmpfs mount (same
        filesystem id), not a synthetic collision."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"
daily_resource_monitor_start
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch, \
            tempfile.TemporaryDirectory(dir="/dev/shm") as state:
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), scratch, state],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=state_root_shares_scratch_filesystem",
            completed.stdout,
        )

    def test_state_root_that_does_not_exist_is_rejected(self) -> None:
        """Known-bad self-test: an unwritable/missing state root fails
        loudly and specifically rather than silently degrading (issue
        #1214 gap 1)."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"
daily_resource_monitor_start
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch:
            missing_state = str(Path(scratch) / "does-not-exist")
            completed = subprocess.run(
                [
                    "bash", "-c", harness, "bash", str(MONITOR), scratch,
                    missing_state,
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=state_root_unavailable", completed.stdout
        )

    def test_phase_state_write_failure_is_a_distinguishable_reason(self) -> None:
        """Known-bad self-test for issue #1214 gap 3: a mid-run failure of
        the monitor's OWN coordination file (the phase pointer) must be
        distinguishable from an ordinary lost sample -- unlike a lost
        sample, it cannot be shrugged off, since the shared phase pointer
        would otherwise mis-attribute every later sample. chmod 500 on the
        real state directory blocks creating the phase pointer's real
        rename-into-place temp file (a real EACCES, not a reimplementation)
        while leaving already-existing files (samples.tsv, the lock, the
        failure marker) writable, isolating exactly this failure mode."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
chmod 500 "$_CRATEDIGGER_RESOURCE_DIR"
if daily_resource_monitor_set_phase beta; then
    echo UNEXPECTED_SUCCESS
fi
chmod 700 "$_CRATEDIGGER_RESOURCE_DIR"
daily_resource_monitor_finish
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch, \
            tempfile.TemporaryDirectory() as state:
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), scratch, state],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertNotIn("UNEXPECTED_SUCCESS", completed.stdout)
        self.assertIn(
            "status=invalid reason=phase_state_write_failed", completed.stdout
        )

    def test_scratch_exhaustion_survives_with_full_phase_breakdown(self) -> None:
        """Central regression pin for issue #1214 gap 1. A real, small
        tmpfs stands in for the measured scratch and is filled to genuine
        ENOSPC -- the real script hitting the real errno the 2026-08-20
        incident hit, not a reimplementation. The monitor's state root is
        a genuinely separate filesystem (the host's real /tmp, inside the
        unshared private mount namespace), so the run must still finish
        with a complete per-phase breakdown despite the measured tmpfs
        sitting at 100% full.

        Mutant proof (both directions; run manually during review, not
        committed -- test infrastructure stays deterministic-only):
        reverting `state_root="${TMPDIR:-/tmp}"` in
        daily_resource_monitor_start back to
        `state_root="$_CRATEDIGGER_RESOURCE_SCRATCH"` (the pre-#1214 shape)
        makes this test fail: mktemp -d for the monitor's own state
        directory now fails once the tiny scratch tmpfs fills, so
        daily_resource_monitor_start itself reports
        status=invalid reason=monitor_state_unavailable and no phase
        breakdown is ever produced -- exactly the defect this issue
        exists to fix."""
        harness = r'''
set -euo pipefail
source "$1"
scratch="$2"
state="$3"

mkdir -p "$scratch"
mount -t tmpfs -o size=1500k,mode=0777 tmpfs "$scratch"
export XDG_RUNTIME_DIR="$scratch"
export TMPDIR="$state"

daily_resource_monitor_start
daily_resource_monitor_set_phase filling
i=0
while dd if=/dev/zero "of=$scratch/pad-$i" bs=1024 count=64 2>/dev/null; do
    i=$((i + 1))
    if ((i > 200)); then
        break
    fi
done
daily_resource_monitor_set_phase after_fill
sleep 0.6
daily_resource_monitor_finish
'''
        with tempfile.TemporaryDirectory() as base:
            scratch = str(Path(base) / "scratch")
            state = str(Path(base) / "state")
            Path(state).mkdir()
            completed = subprocess.run(
                [
                    "unshare", "--mount", "--map-root-user",
                    "--propagation", "private", "--",
                    "bash", "-c", harness, "bash", str(MONITOR), scratch, state,
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        phases, receipt = parsed_valid_summary(completed)
        self.assertIn("filling", phases)
        self.assertIn("after_fill", phases)
        self.assertGreater(int(phases["filling"]["scratch_byte_peak"]), 1_000_000)
        self.assertEqual(receipt["dropped_samples"], "0")

    def test_single_dropped_sample_degrades_without_losing_the_breakdown(
        self,
    ) -> None:
        """Central regression pin for issue #1214 gap 2. A boundary sample
        write is forced to fail with a real EACCES -- chmod 400 on the
        real samples file mid-run, restored afterward; 'a write-failing
        path', one of the injection techniques the issue names as
        legitimate for exercising the real script. The run must degrade,
        not discard the whole breakdown.

        Mutant proof (both directions; run manually during review, not
        committed): reverting daily_resource_monitor_set_phase to gate
        _daily_resource_write_phase on the FIRST boundary sample's success
        (the pre-#1214 shape) makes this test fail -- the shared phase
        pointer never advances past "one" while samples.tsv is chmod 400,
        so "two" and "three" never appear in the breakdown at all."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase one
sleep 0.3
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
daily_resource_monitor_set_phase two
chmod 644 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 0.3
daily_resource_monitor_set_phase three
daily_resource_monitor_finish
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch, \
            tempfile.TemporaryDirectory() as state:
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), scratch, state],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=15,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        phases, receipt = parsed_summary(completed, allowed_statuses={"degraded"})
        self.assertEqual(receipt["reason"], "partial_sample_loss")
        self.assertGreaterEqual(int(receipt["dropped_samples"]), 1)
        for phase in ("one", "two", "three"):
            self.assertIn(phase, phases)


if __name__ == "__main__":
    unittest.main()
