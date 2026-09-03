"""Dispatch value types + module-level constants.

Extracted from ``lib/import_dispatch.py`` (issue #139). Holds the typed
results and the taxonomy/scenario constants shared across the dispatch
package. The import-attempt accumulator here owns result finalization; the
remaining types are value-only definitions shared above every use site.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import msgspec

from lib.import_execution import AutomationOwnerCheckpointDB
from lib.jellyfin_pin_service import _PinDBProto as _JellyfinPinDB
from lib.plex_pin_service import _PinDBProto as _PlexPinDB
from lib.sidecar_service import SidecarDB
from lib.transitions import TransitionsDB
from lib.wrong_match_policy import PREIMPORT_FACT_REJECTION_SCENARIOS

logger = logging.getLogger("cratedigger")

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from lib.config import CratediggerConfig
    from lib.grab_list import DownloadFile
    from lib.import_evidence import CandidateEvidenceActionResult
    from lib.import_execution import (
        CancellationToken,
        ExecutionLeaseSnapshot,
        OwnerSessionIdentity,
    )
    from lib.pipeline_db import DownloadLogOutcome
    from lib.pipeline_db._shared import MergeRekeyCollision
    from lib.pipeline_db.import_jobs import ImportJob
    from lib.pipeline_db.rows import DownloadLogWithEvidenceRow
    from lib.quality import (
        AlbumQualityEvidence,
        AudioQualityMeasurement,
        DownloadInfo,
        ImportResult,
        SpectralCodecContext,
        SpectralDetail,
        V0ProbeEvidence,
    )
    from lib.terminal_outcomes import (
        ImportTerminalOutcome,
        PendingImportTerminalOutcome,
        PreviewTerminalOutcome,
        RequestRejectionOutcome,
        RequestRejectionResult,
        TerminalOutcomeResult,
    )
    from lib.validation_envelope import ValidationProjectionUnset


# U2: when the importer claim arrives without valid candidate evidence
# (missing row, stale snapshot, incomplete), dispatch flips the row back to
# the preview lane via ``PipelineDB.requeue_import_job_for_preview`` and
# returns this code. The importer interprets it as "yield, do NOT
# write terminal failure, do NOT bump retry counters." Preview's next
# sweep recovers the row.
DISPATCH_CODE_REQUEUED_FOR_PREVIEW = "requeued_for_preview"
# U2: when the requeue UPDATE itself raised (DB transient, connection drop),
# dispatch swallows the exception and returns this code so the importer
# leaves the job in ``running`` for conservative startup recovery on next
# worker boot to recover. NEVER write terminal failure on this code.
DISPATCH_CODE_REQUEUE_FAILED = "requeue_failed"
# The action-time gate found another evidence miss after the bounded
# preview/import retry window. This is a world failure, not a candidate
# quality rejection; the automation owner must self-heal to ``wanted``.
DISPATCH_CODE_REQUEUE_EXHAUSTED = "requeue_exhausted"
# U4: programmer-error code returned by ``dispatch_import_from_db`` when
# neither ``import_job_id`` nor ``download_log_id`` is supplied. After U3
# the only production caller (``scripts/importer.py``) always supplies
# ``import_job_id``, so this code only surfaces from test seams or future
# misuse. The legacy direct-measurement branch that previously handled
# this case was deleted in U4 because no production path reaches it.
DISPATCH_CODE_BAD_REQUEST = "bad_request"
DISPATCH_CODE_PROCESSING_LOCKED = "processing_locked"
# Canonical terminal rejection from ``full_pipeline_decision_from_evidence``.
# Consumers may react to this outcome, but must not re-run a parallel import
# decision to prove it again.
DISPATCH_CODE_QUALITY_PIPELINE_REJECTED = "quality_pipeline_rejected"
DISPATCH_CODE_IMPORT_MANIFEST_REJECTED = "import_manifest_rejected"

# Scenarios whose ``path`` is the user's source data (``failed_imports/…``),
# NOT a disposable staging directory. Used to gate ``_cleanup_staged_dir``
# so a ``downgrade`` / ``transcode_downgrade`` decision from the harness
# can never delete the user's only copy of the source. Auto-import dispatches
# under ``bv_result.scenario``, which on that path is always ``strong_match``
# (``lib/beets.py`` sets ``valid`` and that name in one statement) and never
# appears here — its exact-owner processing source is always safe to remove
# (see issue #89).
FORCE_IMPORT_SCENARIOS: frozenset[str] = frozenset({"force_import"})


@runtime_checkable
class DispatchDB(
    SidecarDB,
    TransitionsDB,
    AutomationOwnerCheckpointDB,
    _PlexPinDB,
    _JellyfinPinDB,
    Protocol,
):
    """The exact pipeline-DB surface ``lib/dispatch/`` uses (#1277).

    ``dispatch_import_core`` used to take the concrete ``PipelineDB`` (200+
    public methods) while reaching for a couple of dozen of them. The
    concrete annotation is what forced every test to bridge its
    ``FakePipelineDB`` through ``Any``; a narrow port lets the fake satisfy
    the contract structurally instead, the same way ``ImportPreviewDB`` /
    ``ForceImportDB`` / ``QualityEvidenceDB`` already do elsewhere.

    The bases are the ports of the collaborators dispatch FORWARDS its
    handle into — the sidecar writer, the transition engine, the #898 owner
    checkpoint, and the two media-server pin services — so this one port
    stays honest without restating their members.

    ``lib/dispatch/`` calls 19 distinct DB methods. Five of them arrive
    through those bases and are NOT declared below: ``get_request``,
    ``get_import_job``, ``get_request_current_evidence_id`` and
    ``load_album_quality_evidence_by_id`` (``QualityEvidenceDB``, via
    ``SidecarDB``), and ``_probe_owner_session``
    (``AutomationOwnerCheckpointDB``, which is also where its
    ``deadline_seconds`` parameter is deliberately omitted — that
    narrowing is that port's, not this one's). The remaining fourteen are
    declared in the body, plus three that dispatch never calls at all,
    restated only to satisfy a different port's contract:
    ``merge_rekey_collision`` and ``update_request_release_for_merge``
    (``lib.download_validation.MergeRekeyDB``, which cannot be a base
    class because that module imports ``lib.dispatch``), and
    ``mark_import_job_failed`` (dead in production entirely as of issue
    #1355 item 3, which deleted its last real caller — see the
    ``tools/vulture/whitelist.py`` entries and the "Residuals from item 3"
    comment on that issue).

    Where a declaration below narrows what the real ``PipelineDB`` offers,
    that is deliberate — the port declares what dispatch needs, not
    everything an implementation may provide. ``check_and_apply_cooldown``
    omits ``config`` for exactly that reason.

    ``_probe_owner_session`` reaches this port underscore-and-all because
    cross-module private use is the house convention (PR #775); re-spelling
    it publicly anywhere in the chain would only hide which method
    production actually calls.
    """

    def advisory_lock(
        self, namespace: int, key: int,
    ) -> AbstractContextManager[bool]: ...

    def add_denylist(
        self, request_id: int, username: str, reason: str | None = None,
    ) -> None: ...

    def check_and_apply_cooldown(self, username: str) -> bool: ...

    def get_tracks(self, request_id: int) -> list[dict[str, object]]: ...

    def request_marked_incomplete(self, request_id: int) -> bool: ...

    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None: ...

    def authorize_import_job_launch(
        self,
        job_id: int,
        *,
        request_id: int,
        release_id: str,
        source_path: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None: ...

    def record_import_job_beets_child(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        beets_pid: int,
        beets_start_ticks: int,
    ) -> ImportJob | None: ...

    def mark_import_job_failed(
        self,
        job_id: int,
        *,
        error: str,
        result: dict[str, object] | None = None,
        message: str | None = None,
    ) -> ImportJob | None: ...

    def requeue_import_job_for_preview(
        self,
        job_id: int,
        *,
        reason: str,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> ImportJob | None: ...

    def capture_automation_import_completion(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
        receipt: object,
    ) -> ImportJob | None: ...

    # The force lane's merge-redirect seam
    # (``lib.download_validation.validate_release_with_merge_redirect``,
    # #1080/#1089). Its ``MergeRekeyDB`` port is not a base class here for
    # one mechanical reason: ``lib.download_validation`` imports
    # ``lib.dispatch``, so importing it back would be a cycle. Everything
    # else that port needs (``get_request``, ``get_import_job``,
    # ``advisory_lock``, ``log_download``) is already declared above or
    # inherited, so only these two are restated.
    def merge_rekey_collision(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
    ) -> MergeRekeyCollision: ...

    def update_request_release_for_merge(
        self,
        request_id: int,
        *,
        old_release_id: str,
        new_release_id: str,
        expected_import_job_id: int,
    ) -> bool: ...

    def persist_import_terminal_outcome(
        self, command: ImportTerminalOutcome,
    ) -> TerminalOutcomeResult: ...

    def persist_preview_terminal_outcome(
        self, command: PreviewTerminalOutcome,
    ) -> TerminalOutcomeResult: ...

    def persist_request_rejection_outcome(
        self, command: RequestRejectionOutcome,
    ) -> RequestRejectionResult: ...

    def log_download(
        self,
        request_id: int,
        soulseek_username: str | None = None,
        contributor_usernames: Sequence[str] | None = None,
        filetype: str | None = None,
        download_path: str | None = None,
        beets_distance: float | None | ValidationProjectionUnset = ...,
        beets_scenario: str | None | ValidationProjectionUnset = ...,
        beets_detail: str | None = None,
        valid: bool | None = None,
        outcome: DownloadLogOutcome | None = None,
        staged_path: str | None = None,
        error_message: str | None = None,
        bitrate: int | None = None,
        sample_rate: int | None = None,
        bit_depth: int | None = None,
        is_vbr: bool | None = None,
        was_converted: bool | None = None,
        original_filetype: str | None = None,
        slskd_filetype: str | None = None,
        actual_filetype: str | None = None,
        actual_min_bitrate: int | None = None,
        spectral_grade: str | None = None,
        spectral_bitrate: int | None = None,
        existing_min_bitrate: int | None = None,
        existing_spectral_bitrate: int | None = None,
        import_result: object = None,
        validation_result: object = None,
        final_format: str | None = None,
        v0_probe_kind: str | None = None,
        v0_probe_min_bitrate: int | None = None,
        v0_probe_avg_bitrate: int | None = None,
        v0_probe_median_bitrate: int | None = None,
        existing_v0_probe_kind: str | None = None,
        existing_v0_probe_min_bitrate: int | None = None,
        existing_v0_probe_avg_bitrate: int | None = None,
        existing_v0_probe_median_bitrate: int | None = None,
        transfer_detail: object = None,
        source_download_log_id: int | None = None,
        source: str = "slskd",
    ) -> int: ...


@dataclass(frozen=True)
class QualityGateState:
    """Resolved on-disk state for a quality-gate evaluation."""
    measurement: AudioQualityMeasurement
    verified_lossless_proof: bool = False
    source_v0_avg_bitrate_kbps: int | None = None
    # The installed album's codec-resolution context, captured ONCE by
    # ``load_quality_gate_state`` from the evidence row — including the two
    # album-level fields the measurement cannot carry (``storage_format``
    # and ``filetype_band``'s mixed-codec fail-closed). Carried here so
    # every consumer, the gate itself and the ``pipeline-cli quality``
    # simulator alike, resolves the codec with the SAME context instead of
    # re-deriving a weaker one from the measurement and showing a class
    # production withheld (issue #829 Phase 5 PR2b review S6). ``None`` on
    # a test-constructed state means "measurement-only", the old behaviour.
    spectral_context: SpectralCodecContext | None = None


@dataclass(frozen=True)
class PostCommitCleanup:
    """Destructive convergence that is safe only after terminal commit.

    This value is intentionally in-memory only. A crash after acknowledgement
    may leave harmless staging debris, but a crash before acknowledgement must
    leave every recovery source in place for operator inspection.
    """

    staged_path: str | None = None
    # Issue #1077, R3-3 (widened issue #1122, review round 2): every
    # deploy-provisioned root (``lib.processing_paths.protected_staging_
    # roots`` -- the canonical processing albums root AND the auto-import
    # staging root) that ``_cleanup_staged_dir``'s parent-prune step must
    # never remove, even though this deferred cleanup runs after
    # ``dispatch_import_core`` has already returned and lost direct access
    # to ``cfg``. Carried here so the eventual caller
    # (``scripts/importer.py::_run_post_commit_cleanup``) can pass it
    # through without needing its own config. Plural because a single lane
    # (force / automation / YouTube rescue) can pass through this SAME
    # producer, and its ``path`` may sit under either shared root -- a
    # guard scoped to only one of them silently falls through for the
    # other (issue #1122 F1/F2).
    staged_path_protected_parents: frozenset[str] | None = None
    duplicate_guard_source_path: str | None = None
    duplicate_guard_staging_dir: str | None = None
    duplicate_guard_request_id: int | None = None


@dataclass(frozen=True)
class DispatchOutcome:
    """Summary of an import outcome."""

    success: bool
    message: str
    deferred: bool = False
    code: str | None = None
    terminal_outcome: PendingImportTerminalOutcome | None = None
    post_commit_wrong_match_scenario: str | None = None
    post_commit_cleanup: PostCommitCleanup | None = None


@dataclass(frozen=True)
class DispatchRequest:
    """Everything one import dispatch is DESCRIBED by (#1277).

    Flat and frozen on purpose. Flat because these values are already the
    vocabulary every caller and every stage speaks — re-grouping them into
    sub-objects would only add a translation layer between the DB row and
    the decision. Frozen because a stage function must not be able to
    rewrite the description of the import it was handed; the three values
    dispatch legitimately re-derives (the normalized ``source_dirs``, the
    evidence-resolved ``override_min_bitrate``, and the recovered
    ``attempt_result``) are resolved once at the top of the core and passed
    forward explicitly.

    Deliberately NOT here: ``db`` and the six injected callables (the
    kwarg-DI seam — ``.claude/rules/code-quality.md`` § "Mocks"), ``cfg``
    (runtime configuration, not a description of this import), and
    ``cancellation_token`` (live control flow). Those stay keyword
    arguments on ``dispatch_import_core`` itself.

    A plain ``@dataclass``, not a ``msgspec.Struct``: this value never
    crosses JSON — it is built from typed Python by two production callers
    and consumed in-process.
    """

    # --- identity ---------------------------------------------------
    path: str
    mb_release_id: str
    request_id: int
    label: str
    beets_harness_path: str
    dl_info: DownloadInfo

    # --- mode / routing ---------------------------------------------
    force: bool = False
    scenario: str = "auto_import"
    outcome_label: DownloadLogOutcome = "success"
    requeue_on_failure: bool = True
    distance: float | None = None

    # --- quality contract -------------------------------------------
    override_min_bitrate: int | None = None
    target_format: str | None = None
    verified_lossless_target: str = ""

    # --- peer attribution -------------------------------------------
    #: The downloaded files this candidate came from. Dispatch reads exactly
    #: one thing off them — ``username``, for denylist attribution
    #: (``extract_usernames``). Both production callers pass real
    #: ``DownloadFile`` rows: the auto lane forwards ``album_data.files``,
    #: the force lane synthesises a single-element list from the operator's
    #: recorded source username when one was recorded — it passes an empty
    #: list when ``source_username`` is falsy, so a force reject can
    #: legitimately have no peer to attribute.
    files: Sequence[DownloadFile] | None = None
    cooled_down_users: set[str] | None = None
    source_dirs: list[str] | None = None

    # --- evidence / attempt -----------------------------------------
    candidate_import_job_id: int | None = None
    candidate_download_log_id: int | None = None
    attempt_spectral_audit: SpectralDetail | None = None
    attempt_result: ImportAttemptResult | None = None
    prevalidated_candidate_result: CandidateEvidenceActionResult | None = None

    # --- storage authority ------------------------------------------
    #: An inseparable pair for isolated real-Beets worlds. Production leaves
    #: both unset and derives the complete pair from runtime config.
    beets_library_db_path: str | None = None
    beets_library_root: str | None = None
    launch_authority_path: str | None = None

    # --- #898 ownership / fencing -----------------------------------
    execution_lease: ExecutionLeaseSnapshot | None = None
    owner_session_identity: OwnerSessionIdentity | None = None


class DispatchCoreFn(Protocol):
    """Exact callable contract for the test-injected core dispatch seam.

    Production calls ``dispatch_import_core`` directly. This protocol keeps
    the explicit test seam honest without allowing a ``dict[str, Any]`` splat
    to erase argument checking at the shared boundary.
    """

    def __call__(
        self,
        request: DispatchRequest,
        db: DispatchDB,
        *,
        cfg: CratediggerConfig | None = None,
        quality_gate_fn: QualityGateFn = ...,
        cancellation_token: CancellationToken | None = None,
    ) -> DispatchOutcome: ...


@dataclass(frozen=True)
class ImportOneRun:
    """Result of one import_one.py subprocess protocol invocation."""

    command: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    import_result: ImportResult | None


class ImportOneRunner(Protocol):
    """Explicit import-one invocation seam with complete Beets authority.

    Production supplies ``run_import_one``. Isolated tests and world models
    can inject a recorder or alternate runner, but receive the exact same
    snapshotted Beets config, Python, database, and library root as production.
    """

    def __call__(
        self,
        *,
        path: str,
        mb_release_id: str,
        request_id: int,
        force: bool,
        preserve_source: bool,
        override_min_bitrate: int | None,
        target_format: str | None,
        verified_lossless_target: str,
        beets_harness_path: str,
        quality_rank_config_json: str | None,
        existing_v0_probe: V0ProbeEvidence | None,
        quality_evidence_action_file: str | None,
        beets_config_dir: str | None,
        beets_python: str | None,
        beets_library_db_path: str | None,
        beets_library_root: str | None,
        cancellation_token: CancellationToken | None = None,
        on_spawn: Callable[[int], None] | None = None,
        owner_session_probe: Callable[[], bool] | None = None,
    ) -> ImportOneRun: ...


@dataclass
class ImportAttemptResult:
    """Own the richest result persisted for one dispatch attempt.

    The preview audit is display-only state. Harness results and later
    postflight mutations flow through this owner; serialization happens only
    when a terminal download-log writer calls :meth:`finalize_into`.
    """

    _audit: SpectralDetail | None
    _result: ImportResult | None = field(init=False, default=None, repr=False)

    @property
    def audit(self) -> SpectralDetail | None:
        return self._audit

    @property
    def result(self) -> ImportResult | None:
        return self._result

    @classmethod
    def from_import_job(
        cls,
        db: DispatchDB,
        import_job_id: int | None,
        audit: SpectralDetail | None = None,
    ) -> ImportAttemptResult:
        """Recover the preview audit once, before any terminal branch."""
        if audit is not None or import_job_id is None:
            return cls(audit)
        try:
            job = db.get_import_job(import_job_id)
            raw = (
                job.preview_result.get("import_result")
                if job is not None and job.preview_result is not None
                else None
            )
            if isinstance(raw, dict):
                from lib.quality import ImportResult
                # ``ImportJob.preview_result`` is ``dict[str, Any]``, so
                # ``.get(...)`` returns ``Any`` and the isinstance narrow
                # above leaves pyright with a partially unknown
                # ``dict[Unknown, Unknown]`` — same quirk documented on
                # ``lib.youtube_album_service._json_dict``. The stored
                # payload is always JSON-decoded (string-keyed);
                # ``msgspec.convert`` hands back a fully known
                # ``dict[str, object]`` matching ``from_dict``'s contract.
                raw_dict: dict[str, object] = msgspec.convert(
                    raw, type=dict[str, object],
                )
                audit = ImportResult.from_dict(raw_dict).spectral
        except Exception:
            logger.warning(
                "Unable to decode preview spectral audit for import job %s",
                import_job_id,
                exc_info=True,
            )
        return cls(audit)

    def merge(self, result: ImportResult) -> ImportResult:
        if self._audit is not None:
            result.spectral = self._audit
        self._result = result
        return result

    def apply(self, mutation: Callable[[ImportResult], None]) -> None:
        if self._result is None:
            raise RuntimeError("cannot mutate an import attempt before a result exists")
        mutation(self._result)

    def finalize_into(self, dl_info: DownloadInfo) -> None:
        result = self._result
        if result is None and self._audit is not None:
            from lib.quality import ImportResult
            result = ImportResult(spectral=self._audit)
            self._result = result
        dl_info.import_result = result.to_json() if result is not None else None


@dataclass(frozen=True)
class EvidenceImportGate:
    """Action-time quality evidence loaded for one mutating import."""

    current: AlbumQualityEvidence | None = None
    candidate: AlbumQualityEvidence | None = None
    candidate_status: str | None = None
    candidate_reason: str | None = None
    current_status: str | None = None
    current_reason: str | None = None
    current_path: str | None = None
    current_snapshot_guard: str | None = None
    snapshot_guard: str | None = None


# U11: ``_build_preimport_measurement_from_evidence``,
# ``_PREIMPORT_REJECT_DENYLIST_REASONS``, and
# ``_route_preimport_decision_reject`` have all been folded into the unified
# decider + reject helper. The five folder/audio-integrity facts are now
# early-exit branches inside ``full_pipeline_decision_from_evidence``; the
# unified ``_reject_import_from_evidence_decision`` below handles their
# denylist policy alongside the existing quality-side rejects. Lifecycle
# mutation remains caller-owned. See CLAUDE.md § "Quality decisions live in
# ONE place".


# Reject reasons that come from folder/audio-integrity facts persisted on
# ``AlbumQualityEvidence`` (formerly emitted by ``preimport_decide``). The
# set remains the shared taxonomy for generated routing/lifecycle coverage.
# Production routes each decision through ``dispatch_action`` and honours the
# caller's ``requeue_on_failure`` lifecycle authority.
_PREIMPORT_FACT_REJECT_DECISIONS = PREIMPORT_FACT_REJECTION_SCENARIOS



QualityGateFn = Callable[..., object | None]
"""Type of the post-import quality-gate callable injected into
``dispatch_import_core``. Production passes :func:`_check_quality_gate_core`;
tests can pass a stub or a recorder instead of patching the module
attribute. Signature matches ``_check_quality_gate_core`` (keyword-args
including ``mb_id``, ``label``, ``request_id``, ``files``, ``db``,
   ``quality_ranks``, ``expected_current_evidence_id``)."""
