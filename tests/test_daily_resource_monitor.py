"""Contracts for the daily gate's phase-correlated resource receipt."""

from __future__ import annotations

import subprocess
import tempfile
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MONITOR = REPO_ROOT / "scripts" / "daily_resource_monitor.sh"
PHASE_PREFIX = "CRATEDIGGER_DAILY_RESOURCE_PHASE "
MISSING_PREFIX = "CRATEDIGGER_DAILY_RESOURCE_PHASE_MISSING "
RECEIPT_PREFIX = "CRATEDIGGER_DAILY_RESOURCE_RECEIPT "


def sample(
    seq: int,
    timestamp_ns: int,
    phase: str,
    *,
    writer: str = "parent",
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
            writer,
            seq,
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
    rows: list[str],
    phase_history: list[str] | None = None,
    *,
    parent_dropped: int = 0,
    loop_dropped: int = 0,
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temporary:
        samples = Path(temporary) / "samples.tsv"
        samples.write_text("\n".join(rows) + "\n", encoding="utf-8")
        if phase_history is None:
            # Mirror what a healthy run's own phase-history file holds:
            # every phase any (well-formed) row claims, first-seen order.
            seen: set[str] = set()
            phase_history = []
            for row in rows:
                fields = row.split("\t")
                if len(fields) < 4:
                    continue
                phase = fields[3]
                if phase not in seen:
                    seen.add(phase)
                    phase_history.append(phase)
        history_path = Path(temporary) / "phase-history"
        history_path.write_text(
            "".join(f"{p}\n" for p in phase_history), encoding="utf-8"
        )
        return subprocess.run(
            [
                "bash", str(MONITOR), "summarize", str(samples), str(history_path),
                str(parent_dropped), str(loop_dropped),
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )


def parsed_summary(
    completed: subprocess.CompletedProcess[str], *, allowed_statuses: set[str]
) -> tuple[dict[str, dict[str, str]], dict[str, str], list[str]]:
    if completed.returncode not in (0, 1):
        raise AssertionError(completed.stderr or completed.stdout)
    phase_rows: dict[str, dict[str, str]] = {}
    missing_phases: list[str] = []
    receipts: list[dict[str, str]] = []
    for line in completed.stdout.splitlines():
        if line.startswith(PHASE_PREFIX):
            fields = parse_fields(line, PHASE_PREFIX)
            phase_rows[fields["phase"]] = fields
        elif line.startswith(MISSING_PREFIX):
            fields = parse_fields(line, MISSING_PREFIX)
            missing_phases.append(fields["phase"])
        elif line.startswith(RECEIPT_PREFIX):
            receipts.append(parse_fields(line, RECEIPT_PREFIX))
    if len(receipts) != 1:
        raise AssertionError(f"expected one terminal receipt, got {len(receipts)}")
    receipt = receipts[0]
    if receipt.get("status") not in allowed_statuses:
        raise AssertionError(
            f"resource receipt status not in {allowed_statuses}: {receipt}"
        )
    # A valid summarize call returns 0 for status=valid, 1 for
    # status=degraded (issue #1214 review C5) -- both are legitimate
    # completions of daily_resource_summarize_samples, never a crash.
    expected_returncode = 1 if receipt["status"] == "degraded" else 0
    if completed.returncode != expected_returncode:
        raise AssertionError(
            f"status={receipt['status']} but returncode="
            f"{completed.returncode} (expected {expected_returncode})"
        )
    return phase_rows, receipt, missing_phases


def parsed_valid_summary(
    completed: subprocess.CompletedProcess[str],
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    phases, receipt, missing = parsed_summary(completed, allowed_statuses={"valid"})
    assert missing == []
    return phases, receipt



class TestDailyResourceSummary(unittest.TestCase):
    def test_phase_transition_serializes_label_and_metric_sampling(self) -> None:
        harness = r'''
set -euo pipefail
source "$1"
state="$2"
_CRATEDIGGER_RESOURCE_DIR="$state"
_CRATEDIGGER_RESOURCE_SAMPLES="$state/samples.tsv"
_CRATEDIGGER_RESOURCE_PHASE="$state/phase"
_CRATEDIGGER_RESOURCE_PHASE_HISTORY="$state/phase-history"
_CRATEDIGGER_RESOURCE_LOCK="$state/sample.lock"
_CRATEDIGGER_RESOURCE_STARTED=1
_CRATEDIGGER_RESOURCE_CURRENT_PHASE=old_phase
_CRATEDIGGER_RESOURCE_PARENT_SEQ=0
_CRATEDIGGER_RESOURCE_PARENT_DROPPED=0
_CRATEDIGGER_RESOURCE_FAILURE_REASON=""
: > "$_CRATEDIGGER_RESOURCE_SAMPLES"
: > "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY"
: > "$_CRATEDIGGER_RESOURCE_LOCK"
printf '%s\n' old_phase > "$_CRATEDIGGER_RESOURCE_PHASE"
printf '%s\n' old_phase >> "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY"
printf '%s\n' 100 > "$state/peak"

_daily_resource_record_sample_unlocked() {
    local phase="$1" writer="$2" seq="$3" peak timestamp
    timestamp="$(date +%s%N)"
    if [[ "$phase" == old_phase ]] && mkdir "$state/claim" 2>/dev/null; then
        : > "$state/entered"
        while [[ ! -e "$state/release" ]]; do
            sleep 0.01
        done
    fi
    peak="$(<"$state/peak")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t0\t0\t%s\t0\t0\t0\t0\t1000\t0\t1000\n' \
        "$writer" "$seq" "$timestamp" "$phase" "$peak" "$peak" "$peak" \
        >> "$_CRATEDIGGER_RESOURCE_SAMPLES"
}

_daily_resource_record_current_phase_locked loop 1 &
background=$!
while [[ ! -e "$state/entered" ]]; do
    sleep 0.01
done
(sleep 0.2; : > "$state/release") &
daily_resource_monitor_set_phase new_phase
printf '%s\n' 200 > "$state/peak"
_CRATEDIGGER_RESOURCE_PARENT_SEQ=$((_CRATEDIGGER_RESOURCE_PARENT_SEQ + 1))
_daily_resource_record_sample_locked new_phase parent "$_CRATEDIGGER_RESOURCE_PARENT_SEQ"
wait "$background"
daily_resource_summarize_samples "$_CRATEDIGGER_RESOURCE_SAMPLES" "$_CRATEDIGGER_RESOURCE_PHASE_HISTORY" 0 0
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
            row.split("\t")[4]
            for row in samples.splitlines()
            if row.split("\t")[3] == "old_phase"
        ]
        self.assertTrue(old_peaks)
        self.assertEqual(set(old_peaks), {"100"})
        self.assertEqual(phases["new_phase"]["memory_peak_end_bytes"], "200")
        self.assertEqual(receipt["memory_peak_owner"], "new_phase")
        self.assertEqual(receipt["dropped_samples"], "0")

    def test_phase_peak_retains_its_time_correlated_breakdown(self) -> None:
        completed = summarize_samples(
            [
                sample(
                    1, 10, "stable_nix", memory_current=800, memory_peak=800,
                    anon=500, file=240, shmem=40, kernel=60,
                    scratch_bytes=100, scratch_inodes=10,
                ),
                sample(
                    2, 20, "stable_nix", memory_current=600, memory_peak=800,
                    anon=300, file=230, shmem=80, kernel=70,
                    scratch_bytes=300, scratch_inodes=30,
                ),
                sample(
                    3, 30, "generated_fuzz", memory_current=1_700, memory_peak=1_700,
                    swap_current=20, swap_peak=20,
                    anon=400, file=1_100, shmem=1_000, kernel=200,
                    scratch_bytes=1_200, scratch_inodes=120,
                ),
                sample(
                    4, 40, "generated_fuzz", memory_current=1_200, memory_peak=1_700,
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
        self.assertEqual(receipt["missing_phases"], "0")
        self.assertEqual(receipt["corrupted_history_lines"], "0")

    def test_zero_scratch_limits_are_invalid_not_empty_evidence(self) -> None:
        completed = summarize_samples(
            [
                sample(
                    1, 10, "generated_fuzz", memory_current=1, memory_peak=1,
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
                    1, 10, "generated_fuzz", memory_current=10, memory_peak=20,
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
                sample(1, 10, "stable_nix", memory_current=80, memory_peak=100),
                sample(2, 20, "stable_nix", memory_current=70, memory_peak=90),
            ]
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("status=invalid", completed.stdout)
        self.assertIn("reason=memory_peak_regressed", completed.stdout)

    def test_concurrent_append_order_is_sorted_by_sample_timestamp(self) -> None:
        completed = summarize_samples(
            [
                sample(2, 20, "generated_fuzz", memory_current=200, memory_peak=200),
                sample(1, 10, "stable_nix", memory_current=100, memory_peak=100),
            ]
        )

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(list(phases), ["stable_nix", "generated_fuzz"])
        self.assertEqual(receipt["memory_peak_owner"], "generated_fuzz")

    def test_sequence_gap_alone_does_not_mark_degraded(self) -> None:
        """issue #1214 review C1/C3: loss is no longer INFERRED from a gap
        in the sequence numbers -- a permanent (never-recovered) outage
        cannot be seen that way, since there is no later surviving row to
        reveal the hole. Sequence numbers remain on the row purely as a
        corruption/reordering signal; a gap with nothing reported by
        either writer must NOT by itself flip the receipt to degraded --
        that would be re-introducing the exact defect C1 found."""
        completed = summarize_samples(
            [
                sample(1, 10, "generated_fuzz", memory_current=800, memory_peak=800),
                # seq 2 "missing" from the data, but nothing reported it.
                sample(3, 30, "generated_fuzz", memory_current=600, memory_peak=800),
            ]
        )

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(receipt["dropped_samples"], "0")
        self.assertIn("generated_fuzz", phases)

    def test_reported_parent_and_loop_drops_mark_degraded(self) -> None:
        """The drop count comes from what each writer reports about
        itself (issue #1214 review C1), not from the sample data."""
        completed = summarize_samples(
            [sample(1, 10, "generated_fuzz", memory_current=1, memory_peak=1)],
            parent_dropped=2,
            loop_dropped=3,
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["reason"], "partial_sample_loss")
        self.assertEqual(receipt["dropped_samples"], "5")
        self.assertEqual(receipt["missing_phases"], "0")
        self.assertEqual(missing, [])
        self.assertIn("generated_fuzz", phases)

    def test_corrupted_shape_row_is_skipped_not_fatal(self) -> None:
        """issue #1214 review F2: a real full filesystem does not reject a
        write atomically -- a partial page can land and the next append
        concatenates onto its unterminated tail, producing a row with the
        wrong shape. That row is skipped and counted; it must not discard
        every other row's evidence."""
        good = sample(1, 10, "generated_fuzz", memory_current=100, memory_peak=100)
        corrupted = "garbage\tnot\ta\tvalid\trow\tshape\tat\tall"
        completed = summarize_samples(
            [good, corrupted], phase_history=["generated_fuzz"]
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "1")
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)
        self.assertEqual(missing, [])

    def test_corrupted_value_row_is_skipped_not_fatal(self) -> None:
        good = sample(1, 10, "generated_fuzz", memory_current=100, memory_peak=100)
        corrupted = (
            "parent\t2\tNOTANUMBER\tgenerated_fuzz\t100\t100\t0\t0\t0\t0\t0\t0"
            "\t0\t16000\t0\t1000"
        )
        completed = summarize_samples(
            [good, corrupted], phase_history=["generated_fuzz"]
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "1")
        self.assertIn("generated_fuzz", phases)
        self.assertEqual(missing, [])

    def test_corrupted_writer_field_is_skipped_not_fatal(self) -> None:
        good = sample(1, 10, "generated_fuzz", memory_current=100, memory_peak=100)
        corrupted = sample(
            2, 20, "generated_fuzz", writer="bogus", memory_current=100, memory_peak=100
        )
        completed = summarize_samples(
            [good, corrupted], phase_history=["generated_fuzz"]
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "1")
        self.assertIn("generated_fuzz", phases)
        self.assertEqual(missing, [])

    def test_extra_trailing_fields_alone_is_rejected(self) -> None:
        """Known-bad self-test, per CLAUSE (issue #1214 review C7): a row
        with every field otherwise well-formed but two trailing extras --
        the exact shape a partial-write-then-concatenation produces
        (review F2) -- must trip the `extra`-non-empty clause specifically,
        not ride along behind an earlier one."""
        good = sample(1, 10, "generated_fuzz", memory_current=100, memory_peak=100)
        well_formed_plus_extra = (
            sample(2, 20, "generated_fuzz", memory_current=100, memory_peak=100)
            + "\textra1\textra2"
        )
        completed = summarize_samples(
            [good, well_formed_plus_extra], phase_history=["generated_fuzz"]
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "1")
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)
        self.assertEqual(missing, [])

    def test_malformed_phase_field_alone_is_rejected(self) -> None:
        """Known-bad self-test, per CLAUSE (issue #1214 review C7): every
        field well-formed except phase itself, which must trip the phase
        regex clause specifically."""
        good = sample(1, 10, "generated_fuzz", memory_current=100, memory_peak=100)
        bad_phase = sample(
            2, 20, "Not A Valid Phase!", memory_current=100, memory_peak=100
        )
        completed = summarize_samples(
            [good, bad_phase], phase_history=["generated_fuzz"]
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "1")
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)
        self.assertEqual(missing, [])

    def test_missing_phase_is_named_not_silently_dropped(self) -> None:
        """issue #1214 review F4: a phase whose every sample was lost must
        still be attributed by name, not silently absent from a receipt
        whose whole purpose is phase attribution."""
        completed = summarize_samples(
            [sample(1, 10, "stable_nix", memory_current=100, memory_peak=100)],
            phase_history=["stable_nix", "generated_fuzz"],
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(missing, ["generated_fuzz"])
        self.assertEqual(receipt["missing_phases"], "1")
        self.assertIn("stable_nix", phases)
        self.assertNotIn("generated_fuzz", phases)

    def test_corrupted_phase_history_line_is_skipped_not_fatal(self) -> None:
        """A malformed history line is skipped and counted rather than
        aborting the summary -- but it IS evidence that phase tracking for
        that transition is not fully trustworthy, so it still degrades
        the receipt. It is its own kind of loss (issue #1214 review C4/
        C9d), counted in corrupted_history_lines, never folded into
        dropped_samples (which counts sample attempts specifically)."""
        completed = summarize_samples(
            [sample(1, 10, "stable_nix", memory_current=1, memory_peak=1)],
            phase_history=["stable_nix", "Not A Valid Phase Name!"],
        )

        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "0")
        self.assertEqual(receipt["corrupted_history_lines"], "1")
        self.assertEqual(missing, [])
        self.assertIn("stable_nix", phases)

    def test_summarize_rejects_wrong_argument_count(self) -> None:
        """Known-bad self-test: the summarize subcommand now requires
        exactly four arguments (samples, phase history, parent-dropped,
        loop-dropped)."""
        with tempfile.TemporaryDirectory() as temporary:
            samples = Path(temporary) / "samples.tsv"
            samples.write_text(
                sample(1, 10, "stable_nix", memory_current=1, memory_peak=1) + "\n",
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["bash", str(MONITOR), "summarize", str(samples)],
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

    def test_summarize_rejects_an_unreadable_phase_history_file(self) -> None:
        """Known-bad self-test for the phase-history readability guard."""
        with tempfile.TemporaryDirectory() as temporary:
            samples = Path(temporary) / "samples.tsv"
            samples.write_text(
                sample(1, 10, "stable_nix", memory_current=1, memory_peak=1) + "\n",
                encoding="utf-8",
            )
            missing_history = Path(temporary) / "does-not-exist"
            completed = subprocess.run(
                [
                    "bash", str(MONITOR), "summarize", str(samples),
                    str(missing_history), "0", "0",
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=phase_history_unreadable", completed.stdout
        )

    def test_summarize_rejects_malformed_dropped_count_arguments(self) -> None:
        """Known-bad self-test for the new parent/loop-dropped input
        validation -- the production caller only ever passes clean
        non-negative integers, but a human or future caller could not."""
        with tempfile.TemporaryDirectory() as temporary:
            samples = Path(temporary) / "samples.tsv"
            samples.write_text(
                sample(1, 10, "stable_nix", memory_current=1, memory_peak=1) + "\n",
                encoding="utf-8",
            )
            history = Path(temporary) / "phase-history"
            history.write_text("stable_nix\n", encoding="utf-8")
            completed = subprocess.run(
                [
                    "bash", str(MONITOR), "summarize", str(samples), str(history),
                    "not-a-number", "0",
                ],
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

    def test_state_root_falls_back_to_tmp_when_tmpdir_collides(self) -> None:
        """Regression pin for issue #1214 review F9: the ordinary
        interactive path (this repo's own dev shell puts TMPDIR under
        XDG_RUNTIME_DIR) must not hard-refuse when a distinct, writable
        fallback (/tmp) is available. TMPDIR is pointed at the SAME real
        tmpfs as the scratch dir (both under /dev/shm); the monitor must
        still start by falling back past it."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"
daily_resource_monitor_start
daily_resource_monitor_finish
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch, \
            tempfile.TemporaryDirectory(dir="/dev/shm") as colliding_tmpdir:
            completed = subprocess.run(
                [
                    "bash", "-c", harness, "bash", str(MONITOR), scratch,
                    colliding_tmpdir,
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("status=valid", completed.stdout)
        self.assertNotIn("state_root_shares_scratch_filesystem", completed.stdout)

    def test_state_root_candidates_all_colliding_is_rejected(self) -> None:
        """Known-bad self-test: when EVERY candidate (TMPDIR and the /tmp
        fallback) shares the scratch filesystem, starting must still fail
        loudly and specifically. A real mount namespace makes /tmp itself
        alias the measured tmpfs -- not a reimplementation."""
        harness = r'''
set -euo pipefail
source "$1"
scratch="$2"
colliding_tmpdir="$3"

mkdir -p "$scratch" "$colliding_tmpdir"
mount -t tmpfs -o mode=0777 tmpfs "$scratch"
mount --bind "$scratch" "$colliding_tmpdir"
mount --bind "$scratch" /tmp
export XDG_RUNTIME_DIR="$scratch"
export TMPDIR="$colliding_tmpdir"
daily_resource_monitor_start
'''
        with tempfile.TemporaryDirectory() as base:
            scratch = str(Path(base) / "scratch")
            colliding_tmpdir = str(Path(base) / "tmpdir")
            completed = subprocess.run(
                [
                    "unshare", "--mount", "--map-root-user",
                    "--propagation", "private", "--",
                    "bash", "-c", harness, "bash", str(MONITOR), scratch,
                    colliding_tmpdir,
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=15,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=state_root_shares_scratch_filesystem",
            completed.stdout,
        )
        self.assertIn("no usable state root", completed.stderr)

    def test_state_root_candidates_all_unavailable_is_rejected(self) -> None:
        """Known-bad self-test: when no candidate is even reachable
        (TMPDIR unset and missing, /tmp itself read-only), the distinct
        state_root_unavailable reason fires rather than the
        filesystem-collision one -- a real read-only tmpfs replaces /tmp
        inside an isolated mount namespace, not a reimplementation."""
        harness = r'''
set -euo pipefail
source "$1"
scratch="$2"

mkdir -p "$scratch"
mount -t tmpfs -o mode=0777 tmpfs "$scratch"
mount -t tmpfs -o ro,mode=0555 tmpfs /tmp
unset TMPDIR
export XDG_RUNTIME_DIR="$scratch"
daily_resource_monitor_start
'''
        with tempfile.TemporaryDirectory() as base:
            scratch = str(Path(base) / "scratch")
            completed = subprocess.run(
                [
                    "unshare", "--mount", "--map-root-user",
                    "--propagation", "private", "--",
                    "bash", "-c", harness, "bash", str(MONITOR), scratch,
                ],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=15,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=state_root_unavailable", completed.stdout
        )
        self.assertIn("no usable state root", completed.stderr)

    def test_phase_state_write_failure_is_a_distinguishable_reason(self) -> None:
        """Known-bad self-test for issue #1214 gap 3: a mid-run failure of
        the monitor's OWN coordination file (the phase pointer) must be
        distinguishable from an ordinary lost sample -- unlike a lost
        sample, it cannot be shrugged off, since the shared phase pointer
        would otherwise mis-attribute every later sample. chmod 500 on the
        real state directory blocks creating the phase pointer's real
        rename-into-place temp file (a real EACCES, not a reimplementation)
        while leaving already-existing files (samples.tsv, the phase
        history, the lock, the loop report fifo) writable, isolating
        exactly this failure mode. (The failure REASON itself no longer
        touches the filesystem at all -- issue #1214 review C-F1 -- so
        this world only needs to isolate the phase-pointer write.)"""
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
        with a complete, VALID (zero drops -- nothing about the monitor's
        own writes ever touches the measured tmpfs) per-phase breakdown
        despite the measured tmpfs sitting at 100% full.

        Mutant proof (both directions; empirically run during review,
        not committed -- test infrastructure stays deterministic-only):
        reverting the state-root candidate list in
        daily_resource_monitor_start back to unconditionally
        `state_root="$_CRATEDIGGER_RESOURCE_SCRATCH"` (the pre-#1214
        shape) makes this test fail with a 30s subprocess.TimeoutExpired,
        not a clean invalid receipt. Observed mechanism: start and the
        first phase transition still succeed (the tmpfs starts empty), so
        the fill proceeds; once it is genuinely full,
        `daily_resource_monitor_finish`'s `: > "$_CRATEDIGGER_RESOURCE_STOP"`
        itself fails (ENOSPC, same broken filesystem), and under
        `set -e` the harness exits right there without ever reaching
        `wait "$_CRATEDIGGER_RESOURCE_PID"` -- the background loop is
        never signalled to stop, is orphaned still holding the captured
        stdout/stderr pipes open, and subprocess.communicate() hangs
        until the timeout. Confirmed live: no phase breakdown is
        produced either way, but by a deadlock, not a clean startup
        refusal."""
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

    def test_boundary_sample_loss_degrades_without_losing_the_breakdown(
        self,
    ) -> None:
        """Regression pin for issue #1214 gap 2 (boundary transitions). A
        boundary sample write is forced to fail with a real EACCES --
        chmod 400 on the real samples file mid-run, restored afterward;
        'a write-failing path', one of the injection techniques the
        original issue names as legitimate for exercising the real
        script. The run must degrade (its "parent" writer reports the two
        failed boundary attempts directly -- issue #1214 review C1), not
        discard the whole breakdown, and every phase the run actually
        entered must still be named. A degraded receipt now also flips
        daily_resource_monitor_finish's own exit code (review C5), so the
        harness -- which ends on a bare finish() call -- exits 1, not 0.

        Mutant proof (both directions; empirically run during review, not
        committed): reverting daily_resource_monitor_set_phase to gate
        _daily_resource_write_phase behind the boundary sample's own
        success (the pre-#1214 shape) makes this test fail with a 15s
        subprocess.TimeoutExpired, not a clean assertion failure. Observed
        mechanism: the reverted set_phase returns 1 under `set -e` while
        the harness still holds no EXIT trap, so it exits without ever
        calling daily_resource_monitor_finish -- the background loop is
        never signalled to stop, is orphaned still holding the captured
        stdout/stderr pipes open, and subprocess.communicate() hangs until
        the timeout. Same deadlock shape as the central gap-1 pin's
        mutant, different trigger."""
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

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["reason"], "partial_sample_loss")
        self.assertGreaterEqual(int(receipt["dropped_samples"]), 1)
        self.assertEqual(missing, [])
        for phase in ("one", "two", "three"):
            self.assertIn(phase, phases)

    def test_periodic_loop_survives_an_extended_write_outage(self) -> None:
        """Central regression pin for issue #1214 review F3: the loop is
        what samples ~99% of a real phase's duration, and the original
        fix's only loss test (a ~100ms boundary window) never actually
        drove it through a failure. This test forces the REAL background
        loop's own periodic sample writes to fail across an outage
        comfortably longer than several 0.25s ticks, then restores
        writability and proves MORE loop-writer samples land afterward --
        the loop kept running, it did not die on the first failure, and
        its own in-process count of the failed attempts is what the
        receipt reports (issue #1214 review C1), not an inference.

        Mutant proof (both directions; empirically run during review, not
        committed): reverting _daily_resource_monitor_loop to return 1 on
        the first failed `_daily_resource_record_current_phase_locked`
        call (the pre-#1214 fatal shape) makes this test fail -- the
        background subshell dies at the first failed tick inside the
        outage window without ever reaching its own report write, `wait`
        in daily_resource_monitor_finish then observes a nonzero exit and
        (since monitor_status != 0 skips the report read entirely)
        reports status=invalid reason=monitor_process_died, never even
        reaching a degraded receipt with a phase breakdown at all."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
sleep 0.6
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 1.5
chmod 644 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 0.6
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

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertGreaterEqual(int(phases["alpha"]["samples"]), 5)
        self.assertGreaterEqual(int(receipt["dropped_samples"]), 3)
        self.assertEqual(missing, [])

    def test_parent_drop_sites_are_each_individually_counted(self) -> None:
        """Regression pin for issue #1214 review C1: every prior test only
        lower-bounded dropped_samples (>=), so removing any of the three
        parent-writer drop-counting sites -- or all three together -- left
        the whole module green, including the exact site review C6 named
        as previously uncovered (the final sample write in finish()).
        _daily_resource_monitor_loop is stubbed to report 0 drops
        immediately: no real periodic sampling, no race against its first
        stop-file check. (Review C1's own suggested alternative -- touch
        the stop file right after start() so the loop's first check sees
        it -- was measured unreliable here: 7 of 8 runs still squeezed in
        one real loop tick before the flag was visible, an inherent fork
        race, not a flake in this test.) That isolates the count to
        exactly the three PARENT sites: the two boundary attempts in one
        set_phase call (close bootstrap + open alpha) plus the one final
        attempt in finish(). Asserts the EXACT count, not a floor -- and
        since alpha's every attempt fails under the same chmod, it also
        pins that a fully-lost phase is correctly named as missing rather
        than silently absent.

        Mutant proof (all four variants -- any ONE of the three sites
        alone, and all three together -- empirically run during review,
        not committed; issue #1214 review C-F3 corrected two successive
        false versions of this docstring's claim, both asserted from
        reading the diff rather than the actual observed output. This is
        the third version, and every claim in it was re-run fresh
        immediately before being written down). Every one of the four
        variants produces the IDENTICAL observable failure, not a
        shrinking count: each site's guard is a bare top-level statement
        of the form `command || _CRATEDIGGER_RESOURCE_PARENT_DROPPED=...`
        with no enclosing if/while/`&&` -- removing the `||` leaves a
        bare, unprotected statement whose nonzero exit (samples.tsv is
        chmod 400) trips this script's own `set -euo pipefail`
        immediately, at that exact line, regardless of which of the three
        sites it is. The caller that reaches it -- `set_phase` for the
        two boundary sites, `finish` for the final one -- dies mid-
        function without ever reaching the code that prints a receipt,
        and since the harness itself calls `daily_resource_monitor_set_phase
        alpha` and `daily_resource_monitor_finish` as bare (non-`if`-
        guarded) statements, that abort propagates to kill the WHOLE
        harness script the same way. Confirmed identical across all four
        variants: `completed.returncode == 1`, `completed.stdout == ""`
        (no receipt of any kind), `completed.stderr` holding "Permission
        denied" lines from the write's own retry loop (three for a
        single-site removal, three for a three-site removal -- one
        write's own retries, not one line per site), and the process
        exits quickly rather than hanging (the stubbed loop has no
        infinite `while` of its own to orphan, unlike the real loop other
        tests in this module rely on for their deadlock-shaped mutants).
        This test's own `parsed_summary()` call is what kills all four
        variants, failing closed with "expected one terminal receipt, got
        0" before the exact `== "3"` count assertion ever runs against
        mutated code -- that assertion still executes, and still matters,
        on every GREEN run against the real, unmutated three-site
        implementation; it is not exercised by this particular mutant
        class. A `>=` floor is still strictly weaker here: since no
        variant of this specific mutant class ever reaches a partial
        (nonzero, undercounted) receipt to floor-check in the first
        place, both assertion styles catch these four mutants equally --
        but only the exact assertion also catches a DIFFERENT class of
        bug this test does not separately mutate for: a counting-logic
        defect (off-by-one, double-count) that still produces a receipt,
        where a `>=` floor would silently pass a wrong number."""
        harness = r'''
set -euo pipefail
source "$1"
_daily_resource_monitor_loop() {
    printf "%d\n" 0 >&"${_CRATEDIGGER_RESOURCE_LOOP_REPORT_FD}"
}
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
daily_resource_monitor_set_phase alpha
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

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["dropped_samples"], "3")
        self.assertEqual(missing, ["alpha"])
        self.assertIn("bootstrap", phases)
        self.assertNotIn("alpha", phases)

    def test_persistent_write_failure_through_end_of_run_is_still_reported(
        self,
    ) -> None:
        """Central regression pin for issue #1214 review C1: gap-inference
        from the sample data cannot see loss that persists all the way to
        the end of a writer's own stream -- there is no later surviving
        row to reveal the hole. This is the exact reproduction the review
        gave: a real, NEVER-restored EACCES from partway through the run
        straight through daily_resource_monitor_finish's own final
        sample write (also review C6's previously-uncovered site). The
        run must still report an honest, non-zero dropped_samples count,
        never status=valid."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
sleep 0.3
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 1.5
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

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(receipt["reason"], "partial_sample_loss")
        self.assertGreaterEqual(int(receipt["dropped_samples"]), 4)
        self.assertEqual(missing, [])
        self.assertIn("alpha", phases)

    def test_set_phase_failure_reason_survives_total_state_dir_lockout(
        self,
    ) -> None:
        """Regression pin for issue #1214 review C-F1. The previous
        file-based failure marker was itself reachably unwritable: four
        bare `printf ... > $_CRATEDIGGER_RESOURCE_FAILURE` sites inside
        daily_resource_monitor_set_phase had no `|| ...` fallback of their
        own, so a compound world (new-file creation denied AND the marker
        file itself unwritable) let a lost phase transition report
        `status=valid dropped_samples=0 missing_phases=0` -- reproduced
        live by the reviewer. This test drives the now-strictly-stronger
        replacement: `alpha` is entered normally, then the ENTIRE real
        state directory is chmod 000'd -- not just blocking new-file
        creation, but removing the directory's own execute/search bit, so
        no path-based filesystem operation of any kind can succeed inside
        it, including opening the ALREADY-EXISTING lock file
        _daily_resource_lock needs for "beta". The failure reason must
        still surface correctly, because it now travels as a plain
        in-process bash variable (_CRATEDIGGER_RESOURCE_FAILURE_REASON)
        that never touches the filesystem at all -- a strictly harder
        world than the reviewer's own reproduction, and the old marker
        file mechanism no longer exists to chmod.

        Mutant proof (both directions; empirically run during review, not
        committed): reverting set_phase's
        `_CRATEDIGGER_RESOURCE_FAILURE_REASON=phase_lock_failed` back to
        the pre-fix `printf '%s\n' phase_lock_failed >
        "$_CRATEDIGGER_RESOURCE_FAILURE"` (no `||` guard, matching the
        original four call sites -- note `_CRATEDIGGER_RESOURCE_FAILURE`
        itself no longer has any assignment anywhere in the fixed script,
        so the mutant also reintroduces a reference to a variable nothing
        sets) makes this test fail, but NOT with the clean
        `status=valid` this docstring first predicted from reading the
        code -- verified empirically, not asserted. Observed mechanism:
        under this script's own `set -euo pipefail`, expanding the
        never-assigned `$_CRATEDIGGER_RESOURCE_FAILURE` trips `set -u`'s
        unbound-variable guard immediately, which aborts the harness
        script mid-function -- even though the failing printf sits inside
        an `if ... ; then` condition, `set -u`'s abort is not the same
        control-flow guard `set -e` respects there. The harness never
        reaches its own `chmod 700` / `daily_resource_monitor_finish`
        lines, so the background loop is never signalled to stop and is
        orphaned still holding the captured stdout/stderr pipes open --
        the same deadlock shape several other tests in this module name
        for their own mutants. `subprocess.communicate()` hangs until this
        test's own 10s timeout, raising `TimeoutExpired`: still a hard
        test failure, just a different one than a first read of the code
        suggests."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
chmod 000 "$_CRATEDIGGER_RESOURCE_DIR"
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
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=phase_lock_failed", completed.stdout
        )

    def test_missing_phase_via_real_write_path_is_named(self) -> None:
        """issue #1214 review C2: the unit-level missing-phase test hands
        the summarizer a HAND-WRITTEN history file, so it never exercises
        _daily_resource_write_phase's own production append at all -- a
        phase that vanished from BOTH the samples and the history would
        pass it silently. This test drives the real start/set_phase/
        finish API: "beta" is entered and left for real (its phase-pointer
        and phase-history writes both succeed normally) while every one of
        its own sample attempts is forced to fail with a real EACCES, so
        it ends up with a real, production-written history entry and zero
        samples -- the exact shape review F4/C2 requires the summarizer to
        catch. "alpha" and "gamma" bookend it with real surviving data,
        proving this is not just "everything broke".

        Mutant proof (both directions; empirically run during review, not
        committed): replacing the phase-history append in
        _daily_resource_write_phase (the `printf ... >> $..._PHASE_HISTORY`
        line) with a no-op `:` makes this test fail -- "beta" never lands
        in the production-written history file, so the summarizer's
        history-minus-samples check has nothing to compare against and
        reports missing_phases=0 / missing=[] instead of naming "beta"."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
sleep 0.3
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
daily_resource_monitor_set_phase beta
sleep 0.5
daily_resource_monitor_set_phase gamma
chmod 644 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 0.3
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

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt, missing = parsed_summary(
            completed, allowed_statuses={"degraded"}
        )
        self.assertEqual(missing, ["beta"])
        self.assertEqual(receipt["missing_phases"], "1")
        self.assertIn("alpha", phases)
        self.assertIn("gamma", phases)
        self.assertNotIn("beta", phases)

    def test_loop_report_unavailable_is_a_distinguishable_reason(self) -> None:
        """Known-bad self-test for issue #1214 review C4: if the loop
        somehow exits cleanly (wait sees 0) without ever writing its
        report, finish() must not silently treat that as "0 dropped" --
        it has to say so. The ORIGINAL loop is SIGKILLed and reaped (a
        real crash, not what this test is pinning), then
        _CRATEDIGGER_RESOURCE_PID is repointed at a genuinely fresh `sleep
        0.05 &` child: a real process that exits 0 on its own, without
        ever touching the fifo, standing in for "the loop returned
        normally but the report never arrived" without needing a
        production mutant to construct it. This exercises the
        `read -t 5` timeout for real (proven by wall-clock time, not
        just the reason string) -- removing that timeout would hang this
        exact scenario forever instead of degrading after 5s."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
kill -9 "$_CRATEDIGGER_RESOURCE_PID"
wait "$_CRATEDIGGER_RESOURCE_PID" 2>/dev/null || true
sleep 0.05 &
_CRATEDIGGER_RESOURCE_PID=$!
daily_resource_monitor_finish
'''
        with tempfile.TemporaryDirectory(dir="/dev/shm") as scratch, \
            tempfile.TemporaryDirectory() as state:
            started = time.monotonic()
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), scratch, state],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=15,
            )
            elapsed = time.monotonic() - started

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=loop_report_unavailable", completed.stdout
        )
        self.assertGreaterEqual(elapsed, 4.5)

    def test_state_store_bootstrap_failure_is_a_distinguishable_reason(
        self,
    ) -> None:
        """Known-bad self-test for state_store_bootstrap_failed: a state
        root just barely large enough for mktemp -d to create the empty
        directory, but not for the bootstrap writes that follow, forces
        this specific path via a real (small, fixed-size) tmpfs -- one of
        the legitimate ENOSPC-injection techniques -- not a
        reimplementation."""
        harness = r'''
set -euo pipefail
source "$1"
scratch="$2"
state="$3"
mkdir -p "$state"
mount -t tmpfs -o size=8k,mode=0777 tmpfs "$state"
export XDG_RUNTIME_DIR="$scratch"
export TMPDIR="$state"
daily_resource_monitor_start
'''
        with tempfile.TemporaryDirectory() as base:
            scratch = str(Path(base) / "scratch")
            state = str(Path(base) / "state")
            Path(scratch).mkdir()
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
                timeout=15,
            )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=state_store_bootstrap_failed", completed.stdout
        )

    def test_monitor_process_death_is_a_distinguishable_reason(self) -> None:
        """Known-bad self-test for monitor_process_died: SIGKILL the real
        background loop process directly (not a reimplementation of its
        failure) and prove daily_resource_monitor_finish still terminates
        (the report-fd read is skipped, not hung, when wait reports a
        genuine crash) with this specific, distinguishable reason."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
kill -9 "$_CRATEDIGGER_RESOURCE_PID"
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

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn(
            "status=invalid reason=monitor_process_died", completed.stdout
        )


if __name__ == "__main__":
    unittest.main()
