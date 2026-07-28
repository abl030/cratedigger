"""Deterministic contracts for the tracked live-world audit debt gate."""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

import msgspec

from lib.world_audit_debt import (
    WORLD_AUDIT_DEBT_SCHEMA_VERSION,
    assess_world_audit_debt,
    initialize_world_audit_debt_state,
    load_world_audit_debt_state,
)
from lib.world_audit_service import WorldAuditCounts, WorldAuditReport
from lib.world_invariants import (
    EvidenceDiskSnapshot,
    WorldViolation,
    check_evidence_disk_coherence,
)
from scripts.world_audit_debt_gate import run


def _violation(
    request_id: int,
    *,
    code: str = "current_evidence_missing",
    detail: str | None = None,
) -> WorldViolation:
    return WorldViolation(
        code=code,
        detail=detail or f"request {request_id} violates {code}",
        request_id=request_id,
        release_id=f"release-{request_id}",
    )


def _report(*violations: WorldViolation) -> WorldAuditReport:
    return WorldAuditReport(
        status="clean" if not violations else "violations",
        counts=WorldAuditCounts(
            active_requests=20,
            beets_albums=18,
            linked_evidence=15,
            denylist_rows=3,
            violations=len(violations),
        ),
        audited_invariants=("evidence_disk_coherence",),
        temporal_invariants_not_auditable=(),
        violations=violations,
    )


def _encoded(report: WorldAuditReport) -> bytes:
    return msgspec.json.encode(report)


class TestWorldAuditDebtDecisions(unittest.TestCase):
    def test_initialized_state_contains_member_digests_not_raw_identity(self) -> None:
        state = initialize_world_audit_debt_state(_report(
            _violation(11),
            _violation(12, code="evidence_fingerprint_mismatch"),
        ))

        encoded = msgspec.json.encode(state)
        self.assertEqual(state.schema_version, WORLD_AUDIT_DEBT_SCHEMA_VERSION)
        self.assertEqual(state.approved_total, 2)
        self.assertEqual(len(state.remaining), 2)
        self.assertNotIn(b"release-11", encoded)
        self.assertNotIn(b"request 11", encoded)
        self.assertNotIn(b'"request_id"', encoded)

    def test_stable_known_cohort_is_green_without_state_change(self) -> None:
        report = _report(_violation(11), _violation(12))
        state = initialize_world_audit_debt_state(report)

        evaluation = assess_world_audit_debt(state, report)

        self.assertTrue(evaluation.passed)
        self.assertEqual(evaluation.report.status, "tracked_debt")
        self.assertEqual(evaluation.report.known_remaining, 2)
        self.assertEqual(evaluation.report.newly_converged, 0)
        self.assertEqual(evaluation.report.new_members, 0)
        self.assertEqual(evaluation.report.changed_members, 0)
        self.assertEqual(evaluation.next_state, state)

    def test_exact_subset_is_green_and_shrinks_remaining_debt(self) -> None:
        state = initialize_world_audit_debt_state(_report(
            _violation(11),
            _violation(12),
            _violation(13),
        ))
        current = _report(_violation(11), _violation(13))

        evaluation = assess_world_audit_debt(state, current)

        self.assertTrue(evaluation.passed)
        self.assertEqual(evaluation.report.known_remaining, 2)
        self.assertEqual(evaluation.report.newly_converged, 1)
        self.assertEqual(evaluation.report.converged_total, 1)
        self.assertIsNotNone(evaluation.next_state)
        assert evaluation.next_state is not None
        self.assertEqual(len(evaluation.next_state.remaining), 2)

    def test_same_count_member_replacement_is_red(self) -> None:
        state = initialize_world_audit_debt_state(_report(
            _violation(11),
            _violation(12),
        ))
        current = _report(_violation(11), _violation(13))

        evaluation = assess_world_audit_debt(state, current)

        self.assertFalse(evaluation.passed)
        self.assertEqual(evaluation.report.status, "unrecognized_violations")
        self.assertEqual(evaluation.report.known_remaining, 1)
        self.assertEqual(evaluation.report.new_members, 1)
        self.assertEqual(evaluation.report.changed_members, 0)
        self.assertEqual(evaluation.report.growth, 0)
        self.assertIsNone(evaluation.next_state)
        # A count-only baseline would incorrectly accept this world.
        self.assertEqual(len(state.remaining), len(current.violations))

    def test_changed_cause_for_known_identity_is_red(self) -> None:
        original = _violation(
            11,
            code="evidence_fingerprint_mismatch",
            detail="request 11 fingerprint old does not match disk first",
        )
        changed = _violation(
            11,
            code="evidence_fingerprint_mismatch",
            detail="request 11 fingerprint old does not match disk second",
        )
        state = initialize_world_audit_debt_state(_report(original))

        evaluation = assess_world_audit_debt(state, _report(changed))

        self.assertFalse(evaluation.passed)
        self.assertEqual(evaluation.report.new_members, 0)
        self.assertEqual(evaluation.report.changed_members, 1)
        self.assertIsNone(evaluation.next_state)

    def test_net_growth_is_red(self) -> None:
        state = initialize_world_audit_debt_state(_report(
            _violation(11),
            _violation(12),
        ))
        current = _report(_violation(11), _violation(12), _violation(13))

        evaluation = assess_world_audit_debt(state, current)

        self.assertFalse(evaluation.passed)
        self.assertEqual(evaluation.report.new_members, 1)
        self.assertEqual(evaluation.report.growth, 1)
        self.assertIsNone(evaluation.next_state)

    def test_clean_world_converges_every_remaining_member(self) -> None:
        state = initialize_world_audit_debt_state(_report(
            _violation(11),
            _violation(12),
        ))

        evaluation = assess_world_audit_debt(state, _report())

        self.assertTrue(evaluation.passed)
        self.assertEqual(evaluation.report.status, "clean")
        self.assertEqual(evaluation.report.known_remaining, 0)
        self.assertEqual(evaluation.report.newly_converged, 2)
        self.assertEqual(evaluation.report.converged_total, 2)

    def test_strict_invariant_still_reports_every_known_violation(self) -> None:
        strict = check_evidence_disk_coherence((
            EvidenceDiskSnapshot(
                request_id=11,
                release_id="release-11",
                status="wanted",
                album_path="/library/eleven",
                current_evidence_id=None,
                evidence_id=None,
                evidence_release_id=None,
                evidence_source_path=None,
                evidence_fingerprint=None,
                actual_fingerprint="sha256:current",
            ),
        ))
        state = initialize_world_audit_debt_state(_report(*strict))

        evaluation = assess_world_audit_debt(state, _report(*strict))

        self.assertEqual([item.code for item in strict], [
            "current_evidence_missing",
        ])
        self.assertTrue(evaluation.passed)
        self.assertEqual(evaluation.report.strict_violations, 1)


class TestWorldAuditDebtGateProcess(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.state_path = Path(self.tempdir.name) / "known-debt.json"

    def _run(
        self,
        report: WorldAuditReport,
        *args: str,
    ) -> tuple[int, dict[str, object], str]:
        stdout = io.StringIO()
        stderr = io.StringIO()
        rc = run(
            ["--state", str(self.state_path), *args],
            stdin=io.BytesIO(_encoded(report)),
            stdout=stdout,
            stderr=stderr,
        )
        payload = json.loads(stdout.getvalue()) if stdout.getvalue() else {}
        return rc, payload, stderr.getvalue()

    def test_explicit_initialization_creates_private_digest_state(self) -> None:
        rc, payload, stderr = self._run(
            _report(_violation(11), _violation(12)),
            "--initialize",
        )

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(payload["status"], "initialized")
        self.assertEqual(self.state_path.stat().st_mode & 0o777, 0o600)
        raw = self.state_path.read_bytes()
        self.assertNotIn(b"release-11", raw)
        self.assertNotIn(b"request 11", raw)

    def test_missing_state_fails_closed_without_auto_initializing(self) -> None:
        rc, payload, stderr = self._run(_report(_violation(11)))

        self.assertEqual(rc, 5)
        self.assertEqual(payload, {})
        self.assertIn("state is unavailable", stderr)
        self.assertFalse(self.state_path.exists())

    def test_initialization_refuses_to_replace_existing_authority(self) -> None:
        first_rc, _payload, first_stderr = self._run(
            _report(_violation(11)),
            "--initialize",
        )
        original = self.state_path.read_bytes()

        second_rc, payload, second_stderr = self._run(
            _report(_violation(12)),
            "--initialize",
        )

        self.assertEqual(first_rc, 0, first_stderr)
        self.assertEqual(second_rc, 5)
        self.assertEqual(payload, {})
        self.assertIn("already exists", second_stderr)
        self.assertEqual(self.state_path.read_bytes(), original)

    def test_successful_shrink_is_persisted_atomically(self) -> None:
        init_rc, _payload, init_stderr = self._run(
            _report(_violation(11), _violation(12)),
            "--initialize",
        )
        self.assertEqual(init_rc, 0, init_stderr)

        rc, payload, stderr = self._run(_report(_violation(11)))

        self.assertEqual(rc, 0, stderr)
        self.assertEqual(payload["status"], "tracked_debt")
        self.assertEqual(payload["newly_converged"], 1)
        persisted = load_world_audit_debt_state(self.state_path)
        self.assertEqual(len(persisted.remaining), 1)
        self.assertFalse(list(self.state_path.parent.glob("*.tmp")))

    def test_red_evaluation_never_mutates_authority_state(self) -> None:
        init_rc, _payload, init_stderr = self._run(
            _report(_violation(11), _violation(12)),
            "--initialize",
        )
        self.assertEqual(init_rc, 0, init_stderr)
        original = self.state_path.read_bytes()

        rc, payload, stderr = self._run(
            _report(_violation(11), _violation(13)),
        )

        self.assertEqual(rc, 1, stderr)
        self.assertEqual(payload["status"], "unrecognized_violations")
        self.assertEqual(payload["new_members"], 1)
        self.assertEqual(self.state_path.read_bytes(), original)

    def test_invalid_report_or_state_fails_closed(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        invalid_report_rc = run(
            ["--state", str(self.state_path), "--initialize"],
            stdin=io.BytesIO(b'{"status":"violations"}'),
            stdout=stdout,
            stderr=stderr,
        )
        self.assertEqual(invalid_report_rc, 5)
        self.assertIn("invalid audit report", stderr.getvalue())

        self.state_path.write_text(
            json.dumps({
                "schema_version": WORLD_AUDIT_DEBT_SCHEMA_VERSION + 1,
                "approved_total": 0,
                "approved_by_code": [],
                "remaining": [],
            }),
            encoding="utf-8",
        )
        self.state_path.chmod(0o600)
        rc, payload, state_stderr = self._run(_report())
        self.assertEqual(rc, 5)
        self.assertEqual(payload, {})
        self.assertIn("unsupported debt-state schema", state_stderr)

    def test_output_never_contains_member_identity_or_digest(self) -> None:
        init_rc, _payload, init_stderr = self._run(
            _report(_violation(11)),
            "--initialize",
        )
        self.assertEqual(init_rc, 0, init_stderr)

        stdout = io.StringIO()
        stderr = io.StringIO()
        rc = run(
            ["--state", str(self.state_path)],
            stdin=io.BytesIO(_encoded(_report(_violation(12)))),
            stdout=stdout,
            stderr=stderr,
        )

        self.assertEqual(rc, 1, stderr.getvalue())
        rendered = stdout.getvalue()
        self.assertNotIn("release-12", rendered)
        self.assertNotIn("request 12", rendered)
        state = load_world_audit_debt_state(self.state_path)
        for member in state.remaining:
            self.assertNotIn(member.identity_digest, rendered)
            self.assertNotIn(member.violation_digest, rendered)


if __name__ == "__main__":
    unittest.main()
