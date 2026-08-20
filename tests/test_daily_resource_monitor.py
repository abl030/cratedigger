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
    rows: list[str], reason: str = ""
) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory() as temporary:
        samples = Path(temporary) / "samples.tsv"
        samples.write_text("\n".join(rows) + "\n", encoding="utf-8")
        argv = ["bash", str(MONITOR), "summarize", str(samples)]
        if reason:
            argv.append(reason)
        return subprocess.run(
            argv,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )


def parsed_summary(
    completed: subprocess.CompletedProcess[str], *, allowed_statuses: set[str]
) -> tuple[dict[str, dict[str, str]], dict[str, str]]:
    if completed.returncode not in (0, 1):
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
    # status=valid returns 0; status=invalid returns 1 -- binary, no
    # third state (issue #1214 round-6 strip-back removed `degraded`).
    expected_returncode = 1 if receipt["status"] == "invalid" else 0
    if completed.returncode != expected_returncode:
        raise AssertionError(
            f"status={receipt['status']} but returncode="
            f"{completed.returncode} (expected {expected_returncode})"
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
_CRATEDIGGER_RESOURCE_LOCK="$state/sample.lock"
_CRATEDIGGER_RESOURCE_STARTED=1
_CRATEDIGGER_RESOURCE_CURRENT_PHASE=old_phase
_CRATEDIGGER_RESOURCE_FAILURE_REASON=""
: > "$_CRATEDIGGER_RESOURCE_SAMPLES"
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
        with tempfile.TemporaryDirectory() as state:
            completed = subprocess.run(
                ["bash", "-c", harness, "bash", str(MONITOR), state],
                cwd=REPO_ROOT,
                text=True,
                capture_output=True,
                check=False,
                timeout=10,
            )

        phases, receipt = parsed_valid_summary(completed)
        # The claiming write for old_phase is still in flight (blocked on
        # $state/release) when set_phase records new_phase's own boundary
        # samples and transitions the pointer -- proving the lock actually
        # serializes concurrent writers rather than merely labeling
        # whichever one happens to run first.
        self.assertIn("old_phase", phases)
        self.assertIn("new_phase", phases)
        self.assertEqual(receipt["phases"], "2")

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

    def test_corrupted_shape_row_is_skipped_not_fatal(self) -> None:
        """issue #1214 review F2: a real full filesystem does not reject a
        write atomically -- a partial page can land and the next append
        concatenates onto its unterminated tail, producing a row with the
        wrong shape. That row is skipped; it must not discard every other
        row's evidence."""
        good = sample(10, "generated_fuzz", memory_current=100, memory_peak=100)
        corrupted = "garbage\tnot\ta\tvalid\trow\tshape\tat\tall"
        completed = summarize_samples([good, corrupted])

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)

    def test_corrupted_value_row_is_skipped_not_fatal(self) -> None:
        good = sample(10, "generated_fuzz", memory_current=100, memory_peak=100)
        corrupted = (
            "NOTANUMBER\tgenerated_fuzz\t100\t100\t0\t0\t0\t0\t0\t0\t0\t16000\t0\t1000"
        )
        completed = summarize_samples([good, corrupted])

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)

    def test_extra_trailing_fields_alone_is_rejected(self) -> None:
        """Known-bad self-test, per CLAUSE (issue #1214 review C7): a row
        with every field otherwise well-formed but two trailing extras --
        the exact shape a partial-write-then-concatenation produces
        (review F2) -- must trip the `extra`-non-empty clause specifically,
        not ride along behind an earlier one."""
        good = sample(10, "generated_fuzz", memory_current=100, memory_peak=100)
        well_formed_plus_extra = (
            sample(20, "generated_fuzz", memory_current=100, memory_peak=100)
            + "\textra1\textra2"
        )
        completed = summarize_samples([good, well_formed_plus_extra])

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)

    def test_malformed_phase_field_alone_is_rejected(self) -> None:
        """Known-bad self-test, per CLAUSE (issue #1214 review C7): every
        field well-formed except phase itself, which must trip the phase
        regex clause specifically."""
        good = sample(10, "generated_fuzz", memory_current=100, memory_peak=100)
        bad_phase = sample(
            20, "Not A Valid Phase!", memory_current=100, memory_peak=100
        )
        completed = summarize_samples([good, bad_phase])

        phases, receipt = parsed_valid_summary(completed)
        self.assertEqual(receipt["samples"], "1")
        self.assertIn("generated_fuzz", phases)

    def test_summarize_rejects_wrong_argument_count(self) -> None:
        """Known-bad self-test: the summarize subcommand requires the
        samples file and accepts an optional trailing reason -- neither
        zero nor more than two positional arguments after the subcommand
        name is valid."""
        with tempfile.TemporaryDirectory() as temporary:
            samples = Path(temporary) / "samples.tsv"
            samples.write_text(
                sample(10, "stable_nix", memory_current=1, memory_peak=1) + "\n",
                encoding="utf-8",
            )
            too_few = subprocess.run(
                ["bash", str(MONITOR), "summarize"],
                cwd=REPO_ROOT, text=True, capture_output=True, check=False,
                timeout=10,
            )
            too_many = subprocess.run(
                ["bash", str(MONITOR), "summarize", str(samples), "reason", "extra"],
                cwd=REPO_ROOT, text=True, capture_output=True, check=False,
                timeout=10,
            )

        for completed in (too_few, too_many):
            self.assertNotEqual(completed.returncode, 0)
            self.assertIn(
                "status=invalid reason=summarize_arguments_invalid",
                completed.stdout,
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
        distinguishable from an ordinary lost sample. chmod 500 on the
        real state directory blocks creating the phase pointer's real
        rename-into-place temp file (a real EACCES, not a
        reimplementation) while leaving already-existing files
        (samples.tsv, the lock) writable, isolating exactly this failure
        mode."""
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
        with a complete, VALID per-phase breakdown despite the measured
        tmpfs sitting at 100% full -- nothing about the monitor's own
        writes ever touches the measured tmpfs."""
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
        phases, _receipt = parsed_valid_summary(completed)
        self.assertIn("filling", phases)
        self.assertIn("after_fill", phases)
        self.assertGreater(int(phases["filling"]["scratch_byte_peak"]), 1_000_000)

    def test_boundary_sample_write_failure_is_invalid_without_losing_the_breakdown(
        self,
    ) -> None:
        """Regression pin for issue #1214 round-6 strip-back. A boundary
        sample write is forced to fail with a real EACCES -- chmod 400 on
        the real samples file, restored immediately after -- and the
        periodic loop is stubbed to report cleanly with no real sampling
        of its own, isolating this to EXACTLY the set_phase-triggered
        boundary write (a real, unstubbed loop racing its own 0.25s tick
        against the same narrow chmod window would make which reason wins
        non-deterministic). The run must still be reported `invalid`
        (binary now, not a quantified `degraded` -- issue #1214's own
        accounting layer was cut in the round-6 strip-back), never
        discarding the phase breakdown for what DID survive."""
        harness = r'''
set -euo pipefail
source "$1"
_daily_resource_monitor_loop() { :; }
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase one
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
daily_resource_monitor_set_phase two
chmod 644 "$_CRATEDIGGER_RESOURCE_SAMPLES"
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
                timeout=10,
            )

        self.assertEqual(completed.returncode, 1, completed.stderr)
        phases, receipt = parsed_summary(completed, allowed_statuses={"invalid"})
        self.assertEqual(receipt["reason"], "sample_write_failed")
        for phase in ("one", "two", "three"):
            self.assertIn(phase, phases)

    def test_persistent_loop_write_failure_dies_and_is_reported(self) -> None:
        """Regression pin for issue #1214 round-6 strip-back: the periodic
        loop is deliberately fatal on its first sample-write failure
        (matching this file's own pre-accounting shape -- see the script
        header), not tolerant-and-counting. A real, never-restored-in-time
        EACCES on the real samples file, held across several of the
        loop's own 0.25s ticks, must make it die; `daily_resource_monitor_
        finish`'s ordinary `wait` on its PID observes that directly, with
        no report channel needed. Permissions are restored before
        `finish` so its own final sample write succeeds cleanly, isolating
        the reason to the loop's death specifically (`monitor_process_
        died`) rather than a second failure landing in `finish` itself.
        Whatever the loop already sampled for "alpha" before it died must
        still be in the printed breakdown."""
        harness = r'''
set -euo pipefail
source "$1"
export XDG_RUNTIME_DIR="$2"
export TMPDIR="$3"

daily_resource_monitor_start
daily_resource_monitor_set_phase alpha
sleep 0.4
chmod 400 "$_CRATEDIGGER_RESOURCE_SAMPLES"
sleep 1.0
chmod 644 "$_CRATEDIGGER_RESOURCE_SAMPLES"
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
        phases, receipt = parsed_summary(completed, allowed_statuses={"invalid"})
        self.assertEqual(receipt["reason"], "monitor_process_died")
        self.assertIn("alpha", phases)
        self.assertGreaterEqual(int(phases["alpha"]["samples"]), 1)

    def test_set_phase_failure_reason_survives_total_state_dir_lockout(
        self,
    ) -> None:
        """Regression pin for issue #1214 review C-F1: set_phase's own
        structural failures (an invalid phase name, a stuck lock, a failed
        phase-pointer write, a failed unlock) are reported through a plain
        in-process bash variable, never a marker file -- so they cannot
        fail to write the way a filesystem operation can. chmod 000 on the
        entire real state directory removes even its execute/search bit,
        so no path-based filesystem operation of any kind can succeed
        inside it, including opening the already-existing lock file
        _daily_resource_lock needs. The failure reason must still surface
        correctly."""
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

    def test_state_store_bootstrap_failure_is_a_distinguishable_reason(
        self,
    ) -> None:
        """Known-bad self-test for state_store_bootstrap_failed: a state
        root just barely large enough for mktemp -d to create the empty
        directory, but not for the bootstrap writes that follow, forces
        this specific path via a real (small, fixed-size) tmpfs -- one of
        the legitimate ENOSPC-injection techniques -- not a
        reimplementation. Measured fresh against the round-6 stripped-back
        bootstrap sequence (samples.tsv + the lock file + the phase
        pointer + one sample write, no more phase-history file or fifo):
        2k reliably exhausts it; 8k (the pre-strip-back size) now
        sometimes succeeds, since there is strictly less to write."""
        harness = r'''
set -euo pipefail
source "$1"
scratch="$2"
state="$3"
mkdir -p "$state"
mount -t tmpfs -o size=2k,mode=0777 tmpfs "$state"
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
        with this specific, distinguishable reason, still printing
        whatever the loop already sampled before it died."""
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
        self.assertIn("bootstrap", completed.stdout)


if __name__ == "__main__":
    unittest.main()
