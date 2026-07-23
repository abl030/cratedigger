"""Typed audio-integrity validation reports.

The report is the wire boundary shared by preview content evidence and typed
measurement failures. Human decoder output is bounded explanation only; the
outcome/category pair is the machine-readable policy surface.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal

import msgspec


AUDIO_VALIDATION_POLICY_ID = "audio-integrity-v2"
AUDIO_VALIDATION_DIAGNOSTIC_LIMIT = 16
AUDIO_VALIDATION_STDERR_LIMIT_BYTES = 2048

AudioValidationOutcome = Literal[
    "passed",
    "audio_corrupt",
    "measurement_failed",
    "skipped",
    "legacy_failure",
    "legacy_unrecorded",
]

AudioToolDiagnosticCategory = Literal[
    "decode_error",
    "ffmpeg_failed_unclassified",
    "decode_timeout",
    "read_error",
    "source_changed",
    "process_unavailable",
    "process_interrupted",
    "legacy_failure",
]


class AudioToolDiagnostic(msgspec.Struct, frozen=True):
    """One bounded abnormal result from an audio tool or filesystem seam."""

    relative_path: str
    category: AudioToolDiagnosticCategory
    return_code: int | None = None
    stderr_excerpt: str = ""
    stderr_bytes: int = 0
    stderr_sha256: str = ""
    stderr_truncated: bool = False


class AudioValidationReport(msgspec.Struct, frozen=True):
    """Canonical persisted audit for one album-level validation attempt."""

    policy_id: str = AUDIO_VALIDATION_POLICY_ID
    tool: str = "ffmpeg"
    tool_version: str = ""
    outcome: AudioValidationOutcome = "skipped"
    files_checked: int = 0
    files_failed: int = 0
    diagnostics: list[AudioToolDiagnostic] = msgspec.field(
        default_factory=lambda: [],
    )
    omitted_diagnostics: int = 0


def bounded_audio_tool_diagnostic(
    *,
    relative_path: str,
    category: AudioToolDiagnosticCategory,
    return_code: int | None = None,
    stderr: bytes | str | None = None,
) -> AudioToolDiagnostic:
    """Build one normalized, bounded diagnostic without retaining raw stderr."""
    if stderr is None:
        raw = b""
    elif isinstance(stderr, bytes):
        raw = stderr
    else:
        raw = stderr.encode("utf-8", "replace")
    normalized = " ".join(raw.decode("utf-8", "replace").split())
    normalized_bytes = normalized.encode("utf-8")
    excerpt_bytes = normalized_bytes[:AUDIO_VALIDATION_STDERR_LIMIT_BYTES]
    return AudioToolDiagnostic(
        relative_path=relative_path,
        category=category,
        return_code=return_code,
        stderr_excerpt=excerpt_bytes.decode("utf-8", "ignore"),
        stderr_bytes=len(raw),
        stderr_sha256=hashlib.sha256(raw).hexdigest(),
        stderr_truncated=(
            len(raw) > AUDIO_VALIDATION_STDERR_LIMIT_BYTES
            or len(normalized_bytes) > AUDIO_VALIDATION_STDERR_LIMIT_BYTES
        ),
    )


def skipped_audio_validation_report() -> AudioValidationReport:
    """Return the explicit report for disabled audio validation."""
    return AudioValidationReport(outcome="skipped")


def legacy_unrecorded_audio_validation_report() -> AudioValidationReport:
    """Truthful default when no issue-#835 validation audit was recorded."""
    return AudioValidationReport(
        policy_id="pre-audio-integrity-v2",
        tool="legacy",
        outcome="legacy_unrecorded",
    )


def validate_audio_validation_report(report: AudioValidationReport) -> None:
    """Reject internally contradictory reports before persistence."""
    if report.files_checked < 0 or report.files_failed < 0:
        raise ValueError("audio validation counts cannot be negative")
    if (
        report.outcome != "legacy_failure"
        and report.files_failed > report.files_checked
    ):
        raise ValueError("audio validation failed count exceeds checked count")
    if len(report.diagnostics) > AUDIO_VALIDATION_DIAGNOSTIC_LIMIT:
        raise ValueError("audio validation diagnostic cap exceeded")
    if report.omitted_diagnostics < 0:
        raise ValueError("audio validation omitted count cannot be negative")
    for diagnostic in report.diagnostics:
        if len(diagnostic.stderr_excerpt.encode("utf-8")) > (
            AUDIO_VALIDATION_STDERR_LIMIT_BYTES
        ):
            raise ValueError("audio validation stderr excerpt cap exceeded")
        if diagnostic.stderr_bytes < 0:
            raise ValueError("audio validation stderr size cannot be negative")

    if report.outcome == "passed":
        if (
            report.files_failed
            or report.diagnostics
            or report.omitted_diagnostics
        ):
            raise ValueError("passed audio validation carries failure state")
    elif report.outcome == "skipped":
        if (
            report.files_checked
            or report.files_failed
            or report.diagnostics
            or report.omitted_diagnostics
        ):
            raise ValueError("skipped audio validation carries measured state")
    elif report.outcome == "audio_corrupt":
        if report.files_failed == 0:
            raise ValueError("audio_corrupt report has no failed files")
        if report.files_failed != (
            len(report.diagnostics) + report.omitted_diagnostics
        ):
            raise ValueError(
                "audio_corrupt report diagnostics do not account for failures",
            )
        if any(
            diagnostic.category not in {
                "decode_error",
                "ffmpeg_failed_unclassified",
                "decode_timeout",
            }
            for diagnostic in report.diagnostics
        ):
            raise ValueError(
                "audio_corrupt report carries a world-failure diagnostic",
            )
    elif report.outcome == "measurement_failed":
        if report.files_failed:
            raise ValueError(
                "measurement_failed report claims content failures",
            )
        if len(report.diagnostics) != 1 or report.omitted_diagnostics:
            raise ValueError(
                "measurement_failed report requires one world diagnostic",
            )
        if report.diagnostics[0].category not in {
            "read_error",
            "source_changed",
            "process_unavailable",
            "process_interrupted",
        }:
            raise ValueError(
                "measurement_failed report carries a content diagnostic",
            )
    elif report.outcome == "legacy_failure":
        if (
            report.files_checked != 0
            or report.files_failed != 1
            or len(report.diagnostics) != 1
            or report.omitted_diagnostics
            or report.diagnostics[0].category != "legacy_failure"
        ):
            raise ValueError("legacy_failure report is not the legacy sentinel")
    elif report.outcome == "legacy_unrecorded":
        if (
            report.files_checked
            or report.files_failed
            or report.diagnostics
            or report.omitted_diagnostics
        ):
            raise ValueError("legacy_unrecorded report carries measured state")


@dataclass(frozen=True)
class AudioValidationResult:
    """Immutable runtime result carrying the typed validation report."""

    report: AudioValidationReport
    failed_paths: tuple[str, ...] = ()

    @property
    def valid(self) -> bool:
        """Project the typed outcome; world failures are never a clean pass."""
        return self.report.outcome in {"passed", "skipped"}

    @property
    def measurement_failed(self) -> bool:
        return self.report.outcome == "measurement_failed"

    @property
    def failed_files(self) -> list[tuple[str, str]]:
        """Project every failed path for per-file evidence construction."""
        if self.report.outcome != "audio_corrupt":
            return []
        by_path = {
            diagnostic.relative_path: (
                diagnostic.stderr_excerpt or diagnostic.category
            )
            for diagnostic in self.report.diagnostics
        }
        paths = self.failed_paths or tuple(by_path)
        return [
            (path, by_path.get(path, "diagnostic omitted by album audit cap"))
            for path in paths
        ]

    @property
    def error(self) -> str | None:
        if self.report.outcome not in {"audio_corrupt", "measurement_failed"}:
            return None
        detail = "; ".join(
            (
                f"{diagnostic.relative_path}: "
                f"{diagnostic.stderr_excerpt or diagnostic.category}"
            ).strip()
            for diagnostic in self.report.diagnostics[:5]
        )
        detail = detail.encode("utf-8")[:1024].decode("utf-8", "ignore")
        if self.report.outcome == "measurement_failed":
            return detail or "audio validation measurement failed"
        return (
            f"{self.report.files_failed}/{self.report.files_checked} "
            f"files failed"
            + (f": {detail}" if detail else "")
        )


class AudioValidationMeasurementError(RuntimeError):
    """Raised when Cratedigger could not establish an audio integrity fact."""

    report: AudioValidationReport

    def __init__(self, report: AudioValidationReport):
        if report.outcome != "measurement_failed":
            raise ValueError(
                "AudioValidationMeasurementError requires "
                "outcome=measurement_failed",
            )
        self.report = report
        diagnostic = report.diagnostics[0] if report.diagnostics else None
        super().__init__(
            diagnostic.stderr_excerpt
            if diagnostic is not None and diagnostic.stderr_excerpt
            else (
                diagnostic.category
                if diagnostic is not None
                else "audio validation measurement failed"
            ),
        )
