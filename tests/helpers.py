"""Shared test helpers — canonical mock data builders.

Builders for structured data used across tests. Use these instead of
hand-rolling dicts or dataclass constructors with many fields.
"""

from __future__ import annotations

import configparser
import json
import os
import stat
import tempfile
import types
from collections.abc import Callable, Generator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol
from unittest.mock import MagicMock, patch

import msgspec
import requests

if TYPE_CHECKING:
    # Import-time cycle: ``tests.fakes`` does not import this module, but
    # keeping the reference type-only preserves that independence.
    from tests.fakes import FakePipelineDB

from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    AutomationHandoffResult,
    ImportJob,
)
from lib.pipeline_db._shared import ADVISORY_LOCK_NAMESPACE_IMPORT
from lib.quality import (
    CURRENT_EVIDENCE_LINEAGE_VERSION,
    DECISION_LOSSLESS_SOURCE_LOCKED,
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    EVIDENCE_PROVENANCE_CARRIED,
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_INSTALLED,
    EVIDENCE_SUBJECT_SOURCE,
    AacLatticeCapture,
    ActiveDownloadFileState,
    ActiveDownloadState,
    AlbumQualityEvidence,
    AlbumQualityEvidenceFile,
    AlbumQualityV0Metric,
    AudioQualityMeasurement,
    AudioToolDiagnostic,
    AudioValidationReport,
    CdRipBitVerification,
    CodecFamily,
    CodecRankBands,
    ConversionInfo,
    DisambiguationFailure,
    DownloadInfo,
    ImportResult,
    PostflightInfo,
    QualityRankConfig,
    RankBitrateMetric,
    SpectralMeasurement,
    TargetQualityContract,
    V0ProbeEvidence,
    ValidationResult,
    VerifiedLosslessProof,
    legacy_unrecorded_audio_validation_report,
)
from lib.quality_evidence import snapshot_fingerprint
from lib.slskd_client import DownloadDirectory, DownloadUser, TransferSnapshot

_DEPLOYED_BEETS_DB_PATHS = frozenset({
    "/mnt/virtio/Music/beets-library.db",
    "/var/lib/cratedigger-beets-db/beets-library.db",
})


# Every table ``TRUNCATE album_requests CASCADE`` reaches transitively on the
# live schema (verified against the real FK graph via ``pg_constraint``, not
# hand-traced): CASCADE follows every table with an FK pointing at
# ``album_requests`` -- directly or, like ``processing_cleanup_journal`` via
# ``import_jobs``, several hops away -- regardless of that FK's own ON DELETE
# action (CASCADE/SET NULL/RESTRICT all get pulled in by TRUNCATE's CASCADE,
# unlike a plain DELETE's cascade, which only follows ON DELETE CASCADE).
# Every test-reset call site that used to TRUNCATE some subset of
# {album_requests, import_jobs, processing_cleanup_journal, download_log,
# album_tracks, source_denylist, search_log, search_plans,
# search_plan_items} plus CASCADE landed on this identical 13-table closure
# regardless of which subset it spelled out, because ``album_requests`` was
# always one of the seeds and already dominates it.
#
# Table order within this list is only independently load-bearing for
# ``album_requests`` preceding ``import_jobs`` -- proven empirically, not
# merely read off the DDL (see ``delete_all_rows``'s docstring for the full
# mechanism). ``processing_cleanup_journal`` is kept before ``import_jobs``
# anyway, for the simplest correct mental model.
REQUEST_CASCADE_RESET_TABLES: tuple[str, ...] = (
    "album_requests",
    "processing_cleanup_journal",
    "import_jobs",
    "album_quality_evidence_files",
    "album_quality_evidence",
    "search_plan_items",
    "search_plans",
    "album_request_field_resolutions",
    "album_tracks",
    "download_log",
    "search_log",
    "source_denylist",
    "bad_audio_hashes",
)


class _ResettableConnection(Protocol):
    def commit(self) -> None: ...


class _ResettableDB(Protocol):
    conn: _ResettableConnection
    def _execute(self, sql: str) -> object: ...
    def _atomic(self) -> AbstractContextManager[object]: ...


def delete_all_rows(db: _ResettableDB, tables: Sequence[str]) -> None:
    """Empty ``tables``, replacing a per-test ``TRUNCATE ... CASCADE`` reset.

    Ephemeral test clusters run ``autovacuum=off`` (``lib/ephemeral_postgres.py``)
    because a disposable cluster is destroyed within minutes -- but TRUNCATE
    assigns each truncated table a new relfilenode, which is a catalog
    UPDATE, and with autovacuum off nothing ever reclaims those dead catalog
    tuples. Over thousands of per-test resets that bloats
    ``pg_class``/``pg_attribute`` for a benefit (immediate data-file space
    reclaim) this disposable workload never needed. The real win this buys,
    measured against a live ephemeral cluster running the actual
    ``make_db()`` reset call: ~2.2x faster than TRUNCATE (issue #1156 item
    7). A cruder synthetic loop of bare ``TRUNCATE``/``DELETE`` statements
    against no schema, no triggers, and no ``_atomic()`` overhead measured
    ~19x in the issue that motivated this change -- that 19x is the
    statement-level figure, not what this helper itself costs; quote 2.2x
    for this function.

    Runs every ``DELETE FROM`` inside ``db._atomic()`` -- the SAME explicit-
    transaction helper every other multi-row ``PipelineDB`` write already
    uses (``lib/pipeline_db/_core.py``) -- for TWO INDEPENDENT reasons, not
    merely atomicity for its own sake:

    1. Constraint triggers, not just FK actions. Migration 066 installs
       three constraint triggers
       (``album_requests_complete_processing_owner``,
       ``import_jobs_complete_processing_owner``,
       ``processing_cleanup_journal_exact_owner`` -- all ``AFTER INSERT OR
       DELETE OR UPDATE ... FOR EACH ROW DEFERRABLE INITIALLY DEFERRED``)
       that validate cross-table album_requests/import_jobs/
       processing_cleanup_journal consistency on every row change to any of
       those three tables. TRUNCATE never fires row-level triggers at all
       (a documented PostgreSQL behavior -- only TRUNCATE-specific triggers
       fire, and none are declared here), so this reset boundary was
       invisible to the old TRUNCATE-based implementation; DELETE fires
       them. Proven empirically, not merely inferred: with the shipped
       table order but autocommit left on (no transaction wrap), deleting a
       world with a live processing owner raises
       ``psycopg2.errors.CheckViolation`` ("cleanup journal must belong to
       the exact active processing owner") the moment ``album_requests`` is
       deleted -- the SET NULL cascade to ``import_jobs.request_id`` fires
       the trigger immediately, and ``processing_cleanup_journal`` has not
       been deleted yet. Only wrapping the whole reset in one transaction
       defers the (DEFERRABLE INITIALLY DEFERRED) trigger checks past the
       point the world is consistent again -- fully empty.

       PRECONDITION this puts on every caller: a table list that includes
       ``album_requests`` on a world with a live processing owner MUST also
       include ``import_jobs`` and ``processing_cleanup_journal`` in the
       SAME call. A caller-supplied list missing one of the three still
       raises the same ``CheckViolation``, now at THIS call's own commit.

    2. ``ON DELETE RESTRICT`` table order, independent of the transaction
       wrap. RESTRICT is the one action PostgreSQL documents as never
       deferrable, regardless of a ``DEFERRABLE INITIALLY DEFERRED``
       declaration on the constraint (that flag governs the referencing
       row's existence check, not the referenced row's delete-time
       RESTRICT action): it is checked at the end of the statement that
       changed the REFERENCED table, transaction or no. Proven
       empirically: even wrapped in ``_atomic()``, deleting ``import_jobs``
       before ``album_requests`` still raises
       ``psycopg2.errors.RestrictViolation`` (the
       ``album_requests_active_automation_owner_fk`` edge). A caller
       passing ``tables`` that include both ``import_jobs`` and
       ``album_requests`` MUST order the latter first (see
       ``REQUEST_CASCADE_RESET_TABLES`` above). ``processing_cleanup_journal``'s
       OWN position relative to ``import_jobs`` is NOT independently
       load-bearing once ``album_requests`` precedes ``import_jobs`` --
       proven empirically the other way too: reordering it after
       ``import_jobs`` survives, because its FK's ON UPDATE side (implicit
       "NO ACTION", genuinely deferrable) is satisfied by the same
       transaction-wide commit reason 1 already requires. It is kept first
       anyway, for the simplest correct mental model. ``ON DELETE
       CASCADE``/``SET NULL`` fire exactly as they always do for a DELETE,
       independent of order. The two NOT DEFERRABLE self-referencing
       foreign keys (``album_requests.replaces_request_id``,
       ``download_log.source_download_log_id``) need no special ordering
       either, because each table's entire content is cleared by a single
       DELETE statement rather than row by row, and that same
       end-of-statement check already sees every row in the same table
       deleted together.

    ``db`` is any ``_ResettableDB``-shaped object (``PipelineDB`` satisfies
    it structurally): ``._execute(sql)``, ``._atomic()``, and a ``.conn``
    exposing ``commit()``. Delegating to ``db._atomic()`` -- rather than a
    hand-rolled autocommit flip/restore -- also means a connection closed
    between tests reconnects correctly (``_ensure_conn()``, inside
    ``_atomic()``) and a mid-reset failure rolls back and restores
    autocommit without a second, untested copy of that logic living here.
    """
    with db._atomic():
        for table in tables:
            db._execute(f"DELETE FROM {table}")
        db.conn.commit()


def make_socket_file(path: str) -> None:
    """Plant one Unix-domain socket file at ``path``, at any depth.

    ``socket.socket(AF_UNIX).bind(path)`` is the obvious way to create the
    ``S_ISSOCK`` inode these worlds need, and it is how this repository kept
    creating it — but ``bind`` carries ``sun_path``'s ~107-byte ceiling,
    which has nothing to do with the filesystem's own limits.

    That ceiling is invisible locally and fatal on the daily unstable gate.
    Its scratch root is ``/run/cratedigger-daily-checks/scratch`` (23 bytes
    deeper than an interactive run's ``/run/user/<uid>``), so a socket
    planted inside a generated album folder overran ``sun_path`` THERE and
    nowhere else: 3 deterministic IDs and 24 fuzz shards failed with
    ``OSError: AF_UNIX path too long`` while every local run stayed green
    (2026-08-15 gate; third recurrence of this class, previously patched
    per-site by shortening one leaf name).

    ``mknod`` creates the same inode with no path ceiling and no descriptor
    to keep alive: an unprivileged caller may create a socket or a FIFO
    (only device nodes need ``CAP_MKNOD``), and ``open`` answers ENXIO on
    it exactly as it does for a bound one — the only property these worlds
    assert. Use this everywhere a test needs a socket FILE; a test that
    needs a real LISTENER still binds a real socket.
    """
    os.mknod(path, stat.S_IFSOCK | 0o600)


@contextmanager
def hermetic_beets_config_defaults() -> Generator[tuple[str, str]]:
    """Give one test module a disposable complete Beets authority pair.

    Omitted authority receives the pair. A test that supplies either half must
    supply both, so no test silently combines a disposable DB with a deployed
    root (or the reverse).
    """
    from beets import library as beets_library

    from lib.beets_db import validate_beets_storage_pair
    from lib.config import CratediggerConfig

    with tempfile.TemporaryDirectory(prefix="cratedigger-test-beets-") as root:
        library_root = f"{root}/library"
        library_db = f"{root}/beets-library.db"
        os.mkdir(library_root)
        library = beets_library.Library(library_db, library_root)
        library._close()
        validate_beets_storage_pair(
            db_path=library_db,
            library_root=library_root,
        )

        original_init: Callable[..., None] = CratediggerConfig.__init__
        original_from_ini = CratediggerConfig.from_ini.__func__
        constructed_configs: list[CratediggerConfig] = []

        def hermetic_init(
            self: CratediggerConfig,
            *args: object,
            **kwargs: object,
        ) -> None:
            if args:
                raise AssertionError(
                    "hermetic Beets test configs must use keyword arguments"
                )
            has_db = "beets_library_db" in kwargs
            has_root = "beets_directory" in kwargs
            if has_db != has_root:
                raise AssertionError(
                    "test Beets authority must specify both library DB and root"
                )
            if not has_db:
                kwargs["beets_library_db"] = library_db
                kwargs["beets_directory"] = library_root
            original_init(self, *args, **kwargs)
            constructed_configs.append(self)

        def hermetic_from_ini(
            cls: type[CratediggerConfig],
            config: configparser.RawConfigParser,
            config_dir: str = ".",
            var_dir: str = ".",
        ) -> CratediggerConfig:
            has_db = config.has_option("Beets", "library")
            has_root = config.has_option("Beets", "directory")
            if has_db != has_root:
                raise AssertionError(
                    "test Beets INI authority must specify both library and directory"
                )
            if not has_db:
                config = deepcopy(config)
                if not config.has_section("Beets"):
                    config.add_section("Beets")
                config.set("Beets", "library", library_db)
                config.set("Beets", "directory", library_root)
            return original_from_ini(cls, config, config_dir, var_dir)

        CratediggerConfig.__init__ = hermetic_init
        setattr(  # noqa: B010 - test temporarily replaces a classmethod
            CratediggerConfig, "from_ini", classmethod(hermetic_from_ini),
        )
        try:
            yield (library_db, library_root)
        finally:
            CratediggerConfig.__init__ = original_init
            setattr(  # noqa: B010 - restore the captured classmethod
                CratediggerConfig, "from_ini", classmethod(original_from_ini),
            )
            deployed = [
                config.beets_library_db
                for config in constructed_configs
                if config.beets_library_db in _DEPLOYED_BEETS_DB_PATHS
            ]
            if deployed:
                raise AssertionError(
                    f"test config retained deployed Beets DB authority: {deployed}"
                )


def make_request_row(**overrides: Any) -> dict[str, Any]:
    """Return a complete album_requests row dict with sensible defaults.

    Mirrors the shape of PipelineDB.get_request() (SELECT * FROM album_requests).
    Use keyword overrides to set specific fields for your test scenario.
    """
    row: dict[str, Any] = {
        "id": 1,
        "mb_release_id": "test-mbid-0001",
        "mb_release_group_id": None,
        "mb_artist_id": None,
        "discogs_release_id": None,
        "artist_name": "Test Artist",
        "album_title": "Test Album",
        "year": 2024,
        # Migration 026 — release-group's first-release year (U3 / R9).
        "release_group_year": None,
        # Migration 028 — VA detection flag (U4). NOT NULL DEFAULT FALSE.
        "is_va_compilation": False,
        # Migration 032 — label catalog number (PR1 U4). NULL when unresolved.
        "catalog_number": None,
        "country": "US",
        "format": None,
        "source": "request",
        "source_path": None,
        "reasoning": None,
        "status": "wanted",
        "search_attempts": 0,
        "download_attempts": 0,
        "validation_attempts": 0,
        "last_attempt_at": None,
        "next_retry_after": None,
        "beets_distance": None,
        "beets_scenario": None,
        "search_filetype_override": None,
        "target_format": None,
        "final_format": None,
        "min_bitrate": None,
        "prev_min_bitrate": None,
        "last_download_spectral_bitrate": None,
        "last_download_spectral_grade": None,
        "verified_lossless": False,
        "current_spectral_grade": None,
        "current_spectral_bitrate": None,
        "current_lossless_source_v0_probe_min_bitrate": None,
        "current_lossless_source_v0_probe_avg_bitrate": None,
        "current_lossless_source_v0_probe_median_bitrate": None,
        "active_download_state": None,
        # Migration 066 — exact active automation processor owner.
        "active_automation_import_job_id": None,
        # U1 persisted-search-plans cursor fields (migration 014).
        "active_plan_id": None,
        "next_plan_ordinal": 0,
        "plan_cycle_count": 0,
        # Migration 028 / U12 — failure_class materialised at plan-wrap.
        "failure_class": None,
        # Migration 028 / U13 — unfindable detection state. All nullable;
        # the four-category taxonomy is populated by the daily detection
        # job (lib/unfindable_detection_service.py).
        "unfindable_category": None,
        "unfindable_categorised_at": None,
        "last_artist_probe_at": None,
        "last_artist_probe_match_count": None,
        # Migration 028 / U14 — long-tail-rescue audit fields. Populated
        # when an unfindable-categorised request finally imports.
        "rescued_at": None,
        "prior_unfindable_category": None,
        # Migration 082 / issue #1241 — operator-set incomplete mark.
        "marked_incomplete_at": None,
        # Migration 021 addressing FK.
        "current_evidence_id": None,
        # Migration 023 — supersede lineage.
        "replaces_request_id": None,
        "created_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
        "priority_started_at": None,
        "updated_at": datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC),
    }
    row.update(overrides)
    if "mb_release_id" not in overrides:
        # Default derives from the row id (id=1 → "test-mbid-0001") so
        # multi-row fixtures get distinct mbids and don't collide with
        # the UNIQUE(mb_release_id) FakePipelineDB enforces (#445 item 4).
        rid = row["id"]
        suffix = f"{rid:04d}" if isinstance(rid, int) else str(rid)
        row["mb_release_id"] = f"test-mbid-{suffix}"
    return row


def make_album_quality_evidence(
    *,
    mb_release_id: str = "test-mbid-0001",
    source_path: str = "/tmp/test-staged",
    measured_at: datetime | None = None,
    files: list[AlbumQualityEvidenceFile] | None = None,
    measurement: AudioQualityMeasurement | None = None,
    v0_metric: AlbumQualityV0Metric | None = None,
    verified_lossless_proof: VerifiedLosslessProof | None = None,
    cd_rip_verification: CdRipBitVerification | None = None,
    codec: str | None = "mp3",
    container: str | None = "mp3",
    storage_format: str | None = "MP3",
    target_format: str | None = None,
    target_is_cbr: bool | None = None,
    lineage_version: int = CURRENT_EVIDENCE_LINEAGE_VERSION,
    on_disk_v0_research_attempted: bool = False,
    current_enrichment_required: bool = False,
    preserve_spectral_measurement_version: bool = False,
    audio_corrupt: bool = False,
    audio_error: str | None = None,
    audio_validation: AudioValidationReport | None = None,
    aac_lattice: AacLatticeCapture | None = None,
) -> AlbumQualityEvidence:
    """Build production-shaped active album-quality evidence.

    Migration 021: evidence is content-addressed by
    ``(mb_release_id, snapshot_fingerprint)``. The fingerprint is computed
    from ``files`` using the canonical helper, so the builder always
    produces a self-consistent row.
    """
    if measured_at is None:
        measured_at = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
    if files is None:
        files = [
            AlbumQualityEvidenceFile(
                relative_path="01 - Track.mp3",
                size_bytes=123456,
                mtime_ns=1_700_000_000_000_000_000,
                extension="mp3",
                container="mp3",
                codec="mp3",
            ),
        ]
    if measurement is None:
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=245,
            avg_bitrate_kbps=256,
            median_bitrate_kbps=252,
            format="MP3",
            spectral_grade="genuine",
            spectral_bitrate_kbps=None,
        )
    if (
        lineage_version >= 4
        and measurement.spectral_grade is not None
        and measurement.spectral_subject is None
    ):
        measurement = msgspec.structs.replace(
            measurement,
            spectral_subject=EVIDENCE_SUBJECT_INSTALLED,
            spectral_provenance=EVIDENCE_PROVENANCE_MEASURED,
        )
    if (
        lineage_version >= 4
        and measurement.spectral_grade is not None
        and measurement.spectral_measurement_version is None
        and not preserve_spectral_measurement_version
    ):
        from lib.spectral_check import SPECTRAL_MEASUREMENT_VERSION

        # This helper builds active production-shaped evidence by default.
        # Tests of legacy generations must opt in explicitly so an accidental
        # unstamped fixture cannot masquerade as reusable current evidence.
        measurement = msgspec.structs.replace(
            measurement,
            spectral_measurement_version=SPECTRAL_MEASUREMENT_VERSION,
        )
    if audio_validation is None and audio_corrupt:
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path if files else "",
            detail=audio_error or "synthetic decode failure",
            files_checked=len(files),
        )
        if files:
            files = [
                msgspec.structs.replace(file, decode_ok=index != 0)
                for index, file in enumerate(files)
            ]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint(files),
        source_path=source_path,
        measurement=measurement,
        measured_at=measured_at,
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        target_format=target_format,
        target_is_cbr=(
            target_is_cbr
            if target_is_cbr is not None
            else (
                TargetQualityContract.from_explicit_label(target_format).is_cbr
                if target_format is not None
                else None
            )
        ),
        lineage_version=lineage_version,
        v0_metric=v0_metric,
        on_disk_v0_research_attempted=on_disk_v0_research_attempted,
        current_enrichment_required=current_enrichment_required,
        verified_lossless_proof=verified_lossless_proof,
        cd_rip_verification=cd_rip_verification,
        audio_validation=(
            audio_validation
            if audio_validation is not None
            else legacy_unrecorded_audio_validation_report()
        ),
        audio_corrupt=audio_corrupt,
        audio_error=audio_error,
        aac_lattice=aac_lattice,
    )


def make_audio_corrupt_validation_report(
    relative_path: str,
    *,
    detail: str = "synthetic decode failure",
    return_code: int = 69,
    files_checked: int = 1,
) -> AudioValidationReport:
    """Build one production-shaped corrupt-audio report for tests."""
    return AudioValidationReport(
        outcome="audio_corrupt",
        files_checked=files_checked,
        files_failed=1,
        diagnostics=[
            AudioToolDiagnostic(
                relative_path=relative_path,
                category="decode_error",
                return_code=return_code,
                stderr_excerpt=detail,
            ),
        ],
    )


@contextmanager
def pinned_dispatch_authority(
    db: _PinnedDispatchDB,
    execution_lease: ExecutionLeaseSnapshot | None,
    *,
    cancellation_token: CancellationToken | None = None,
) -> Generator[
    tuple[CancellationToken | None, OwnerSessionIdentity | None],
]:
    """Pin the real fake-DB owner session for one automation dispatch scope."""
    if execution_lease is None:
        if cancellation_token is not None:
            raise AssertionError(
                "non-automation dispatch cannot carry a cancellation token"
            )
        yield None, None
        return

    existing_pin = getattr(db, "_owner_session_pin", None)
    if existing_pin is not None:
        identity, pinned_token = existing_pin
        if (
            cancellation_token is not None
            and cancellation_token is not pinned_token
        ):
            raise AssertionError(
                "nested dispatch authority must reuse the pinned token"
            )
        yield pinned_token, identity
        return

    token = cancellation_token or CancellationToken()
    with db._pin_owner_session(token) as identity:
        yield token, identity


def finalize_claimed_dispatch(db: Any, job: Any, outcome: Any) -> Any:
    """Apply a direct dispatch result through the production queue owner.

    ``outcome`` is ordinarily the ``DispatchOutcome`` (or equivalent) the
    caller already computed. Passing a ``BaseException`` INSTANCE instead
    lets a fixture drive ``process_claimed_job``'s own executor-crash
    handling without hand-rolling a raising ``execute_fn`` at the call
    site — no existing caller passes one, so this is purely additive and
    every existing caller's behavior is unchanged. This is the file's
    established, ``Any``-typed bridge from a ``FakePipelineDB`` fixture
    into the ``PipelineDB``-typed ``process_claimed_job``, so a crash-path
    caller reuses it instead of calling ``process_claimed_job`` directly
    (issue #1176 PR3 review round: keeps the tests typing ratchet frozen —
    no new escape hatch).
    """
    from lib.import_queue import IMPORT_JOB_AUTOMATION
    from scripts.importer import _execution_lease_from_job, process_claimed_job

    def _execute(*_args: object, **_kwargs: object):
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    if job.job_type == IMPORT_JOB_AUTOMATION:
        execution_lease = _execution_lease_from_job(job)
        assert execution_lease is not None, (
            "automation fixture must claim with an importer execution lease"
        )
        with pinned_dispatch_authority(
            db,
            execution_lease,
        ) as (cancellation_token, owner_session_identity):
            assert cancellation_token is not None
            assert owner_session_identity is not None
            return process_claimed_job(
                db,
                job,
                execute_fn=_execute,
                execution_lease=execution_lease,
                cancellation_token=cancellation_token,
                owner_session_identity=owner_session_identity,
            )
    return process_claimed_job(
        db,
        job,
        execute_fn=_execute,
    )


class ImportJobClaimDB(Protocol):
    def peek_import_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]: ...

    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]: ...

    def advisory_lock(
        self,
        namespace: int,
        key: int,
    ) -> AbstractContextManager[bool]: ...

    def claim_import_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None: ...

    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str | None = None,
    ) -> ImportJob | None: ...

    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ) -> ImportJob | None: ...

    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...

    def claim_local_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ) -> ImportJob | None: ...


def claim_next_import_job(
    db: ImportJobClaimDB,
    *,
    worker_id: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
) -> ImportJob | None:
    """Claim the first import candidate for direct test setup.

    Production workers scan bounded candidate pages and claim exact rows. Tests
    that need a claimed fixture retain the old one-shot convenience here
    without preserving a production API that no runtime caller uses.
    """
    candidates = db.peek_import_job_candidates(
        execution_lease=execution_lease,
        limit=1,
    )
    if not candidates:
        return None
    candidate = candidates[0]
    if candidate.job_type == IMPORT_JOB_AUTOMATION:
        if execution_lease is None or candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_automation_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
                execution_lease=execution_lease,
            )
    if candidate.job_type == IMPORT_JOB_FORCE:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_force_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    if candidate.job_type == IMPORT_JOB_LOCAL:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_local_import_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    return db.claim_import_job_candidate(
        candidate.id,
        worker_id=worker_id,
    )


def claim_next_import_preview_job(
    db: ImportJobClaimDB,
    *,
    worker_id: str | None = None,
    execution_lease: ExecutionLeaseSnapshot | None = None,
) -> ImportJob | None:
    """Claim the first preview candidate for direct test setup."""
    candidates = db.peek_import_preview_job_candidates(
        execution_lease=execution_lease,
        limit=1,
    )
    if not candidates:
        return None
    candidate = candidates[0]
    if candidate.job_type == IMPORT_JOB_AUTOMATION:
        if execution_lease is None or candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_automation_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
                execution_lease=execution_lease,
            )
    if candidate.job_type == IMPORT_JOB_FORCE:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_force_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    if candidate.job_type == IMPORT_JOB_LOCAL:
        if candidate.request_id is None:
            return None
        with db.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_IMPORT,
            candidate.request_id,
        ) as acquired:
            if not acquired:
                return None
            return db.claim_local_import_preview_job_under_lock(
                candidate.id,
                request_id=candidate.request_id,
                worker_id=worker_id,
            )
    return db.claim_import_preview_job_candidate(
        candidate.id,
        worker_id=worker_id,
    )


#: Every ``stage2_import`` decision produced by the provisional-lossless
#: lane (``lib/quality/decisions.py::provisional_lossless_decision``) — the
#: lane the V0 trust override routes an album AROUND. Membership is the
#: observable answer to "which lane decided this album", which the v3
#: ultrasonic leg must never change (issue #829 Phase 5 PR3): three of the
#: four are confident rejects that also denylist the offering peer, so a
#: leg that could re-route into this lane would turn a withheld proof into
#: a discarded album. Spelled from the production constants, once, for the
#: pins in ``tests/test_quality_classification.py`` and the property in
#: ``tests/test_quality_generated.py``.
PROVISIONAL_LANE_DECISIONS = frozenset({
    DECISION_PROVISIONAL_LOSSLESS_UPGRADE,
    DECISION_SUSPECT_LOSSLESS_DOWNGRADE,
    DECISION_SUSPECT_LOSSLESS_PROBE_MISSING,
    DECISION_LOSSLESS_SOURCE_LOCKED,
})


def make_aac_lattice_capture(
    tracks: Sequence[tuple[int | None, float | None]],
    *,
    proba: float = 0.12,
    error: str = "AacLatticeUnsupportedRateError: unsupported sample rate 96 kHz",
) -> AacLatticeCapture:
    """Build an AAC-lattice capture through the PRODUCTION derivation.

    ``tracks`` is one ``(offset, z)`` pair per track, in the deterministic
    filename order ``lib/aac_lattice.py`` scores them in; a pair whose
    offset is ``None`` records a per-track detector failure instead of a
    score, exactly as ``measure_album_aac_lattice`` does.

    The album statistics (``modal_offset``/``modal_count``/
    ``scored_tracks``/``max_z``) are ALWAYS derived by
    ``AacLatticeCapture.from_tracks`` — the one function production uses —
    never written by hand. A test that hand-set them could assert on an
    album statistic no measurement can produce (test-fidelity.md Rule C).
    """
    from lib.quality import AacLatticeTrackScore

    rows: list[AacLatticeTrackScore] = []
    for index, (offset, z) in enumerate(tracks):
        filename = f"{index + 1:02d}.flac"
        if offset is None:
            rows.append(AacLatticeTrackScore(filename=filename, error=error))
            continue
        rows.append(AacLatticeTrackScore(
            filename=filename, offset=offset, z=z, proba=proba,
        ))
    return AacLatticeCapture.from_tracks(rows)


def build_parity_candidate_evidence(
    *,
    is_flac: bool,
    min_bitrate: int,
    is_cbr: bool,
    avg_bitrate: int | None = None,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    post_conversion_min_bitrate: int | None = None,
    candidate_v0_probe_avg: int | None = None,
    candidate_v0_probe_min: int | None = None,
    native_codec: str = "mp3",
    native_format: str = "MP3",
    mb_release_id: str = "mbid-parity-candidate",
    audio_corrupt: bool = False,
    folder_layout: str = "flat",
    audio_file_count: int | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
    snapshot_fingerprint: str = "sha256:candidate-fingerprint",
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
    filetype_band: str | None = None,
    ultrasonic_deficit_db: float | None = None,
    spectral_measurement_version: int | None = 2,
    was_converted_from: str | None = None,
    lossless_container: str = "flac",
    lossless_codec: str = "flac",
    aac_lattice: AacLatticeCapture | None = None,
) -> AlbumQualityEvidence:
    """Build an ``AlbumQualityEvidence`` candidate row matching the
    simulator's flat-kwargs shape (post-U2/U3 schema).

    This is the canonical simulator-world → evidence-row mapping. The
    hand-written parity tests in ``tests/test_quality_classification.py``
    and the generated parity property in ``tests/test_quality_generated.py``
    both consume it, so a divergence between the decision twins can never
    hide behind two different world encodings.
    """
    # Candidate evidence always describes the downloaded source bytes.
    # Conversion policy/output stay on the target contract and decision facts;
    # a temporary V0 probe must never make a FLAC source wear an MP3 label.
    # The two lossless branches (converting and kept-on-disk) built the
    # identical row and were only ever spelled apart; PR3's
    # container/codec split made that literally true, so they are one.
    if is_flac:
        container = lossless_container
        codec = lossless_codec
        storage_format = lossless_codec
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate or 900,
            avg_bitrate_kbps=min_bitrate or 900,
            median_bitrate_kbps=min_bitrate or 900,
            format=lossless_codec.upper(),
            is_cbr=False,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )
    else:
        container = codec = native_codec
        storage_format = native_format.lower()
        _avg = avg_bitrate if avg_bitrate is not None else min_bitrate
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=_avg,
            median_bitrate_kbps=_avg,
            format=native_format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
        )

    # issue #829 Phase 5 PR1 capture. Stamped after the branch above so all
    # three candidate shapes carry it identically; a row with no spectral
    # grade may not carry these facts at all (evidence-row validation).
    if spectral_grade is not None and (
        cliff_hz is not None
        or codec_family is not None
        or ultrasonic_deficit_db is not None
        or was_converted_from is not None
    ):
        # ``spectral_measurement_version`` defaults to 2 (the PR1+ capture
        # code) because that is what any producer of these facts stamps.
        # A caller pins it to None to build the legacy world explicitly.
        measurement = msgspec.structs.replace(
            measurement,
            cliff_hz=cliff_hz,
            codec_family=codec_family,
            ultrasonic_deficit_db=ultrasonic_deficit_db,
            was_converted_from=was_converted_from,
            spectral_measurement_version=spectral_measurement_version,
        )

    v0_metric = None
    if candidate_v0_probe_avg is not None or candidate_v0_probe_min is not None:
        v0_metric = AlbumQualityV0Metric(
            min_bitrate_kbps=candidate_v0_probe_min,
            avg_bitrate_kbps=candidate_v0_probe_avg,
            median_bitrate_kbps=candidate_v0_probe_avg,
            subject=EVIDENCE_SUBJECT_SOURCE,
            provenance=EVIDENCE_PROVENANCE_MEASURED,
        )

    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=codec,
    )]
    audio_validation = legacy_unrecorded_audio_validation_report()
    if audio_corrupt:
        files = [msgspec.structs.replace(files[0], decode_ok=False)]
        audio_validation = make_audio_corrupt_validation_report(
            files[0].relative_path,
        )
    # ``audio_file_count`` defaults to len(files) for the standard
    # parity scenarios. Tests covering empty_fileset explicitly pass
    # ``audio_file_count=0`` and override ``files`` separately.
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint=snapshot_fingerprint,
        source_path="/Incoming/auto-import/candidate",
        measurement=measurement,
        measured_at=datetime(2026, 5, 16, tzinfo=UTC),
        files=files,
        codec=codec,
        container=container,
        storage_format=storage_format,
        v0_metric=v0_metric,
        audio_validation=audio_validation,
        audio_corrupt=audio_corrupt,
        folder_layout=folder_layout,
        audio_file_count=(
            audio_file_count if audio_file_count is not None else len(files)
        ),
        filetype_band=(
            filetype_band if filetype_band is not None else storage_format
        ),
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
        aac_lattice=aac_lattice,
    )


def build_parity_current_evidence(
    *,
    min_bitrate: int | None,
    avg_bitrate: int | None = None,
    format: str = "MP3",
    is_cbr: bool = False,
    spectral_grade: str | None = None,
    spectral_bitrate: int | None = None,
    mb_release_id: str = "mbid-parity-candidate",
    v0_metric: AlbumQualityV0Metric | None = None,
    matched_bad_audio_hash_id: int | None = None,
    matched_bad_audio_hash_path: str | None = None,
    cliff_hz: int | None = None,
    codec_family: CodecFamily | None = None,
    filetype_band: str | None = None,
    was_converted_from: str | None = None,
) -> AlbumQualityEvidence | None:
    """Build the existing-album evidence row for parity scenarios.

    Returns ``None`` when ``min_bitrate`` is ``None`` — the fresh-request
    shape where no current album exists.

    ``was_converted_from`` builds the R19 converted-lineage shape (issue
    #1204 defect 1's amended invariant): a row whose ``format`` names the
    on-disk DERIVATIVE (e.g. an MP3 converted from FLAC) but whose spectral
    facts describe the pre-conversion SOURCE. Matching production
    (``resolve_measured_codec_family``'s ``converted`` branch requires
    BOTH), setting it also switches ``spectral_subject`` from the ordinary
    installed-row default to ``EVIDENCE_SUBJECT_SOURCE`` and
    ``spectral_provenance`` to ``EVIDENCE_PROVENANCE_CARRIED`` — the
    overwhelming majority live shape for this R19 cohort specifically
    (15,333 of 15,368 rows; this is NOT the same population as the
    general verified-lossless-proof carried-provenance count elsewhere
    in this file).
    """
    if min_bitrate is None:
        return None

    container = format.lower().split()[0]
    files = [AlbumQualityEvidenceFile(
        relative_path=f"01.{container}",
        size_bytes=1, mtime_ns=1,
        extension=container, container=container, codec=container,
    )]
    return AlbumQualityEvidence(
        mb_release_id=mb_release_id,
        snapshot_fingerprint="sha256:current-fingerprint",
        source_path="/Beets/current",
        measurement=AudioQualityMeasurement(
            min_bitrate_kbps=min_bitrate,
            avg_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            median_bitrate_kbps=avg_bitrate if avg_bitrate is not None else min_bitrate,
            format=format,
            is_cbr=is_cbr,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                (
                    EVIDENCE_SUBJECT_SOURCE
                    if was_converted_from is not None
                    else EVIDENCE_SUBJECT_INSTALLED
                )
                if spectral_grade is not None
                else None
            ),
            spectral_provenance=(
                (
                    EVIDENCE_PROVENANCE_CARRIED
                    if was_converted_from is not None
                    else EVIDENCE_PROVENANCE_MEASURED
                )
                if spectral_grade is not None else None
            ),
            cliff_hz=cliff_hz if spectral_grade is not None else None,
            codec_family=codec_family if spectral_grade is not None else None,
            was_converted_from=(
                was_converted_from if spectral_grade is not None else None
            ),
            spectral_measurement_version=(
                2
                if spectral_grade is not None
                and (cliff_hz is not None or codec_family is not None)
                else None
            ),
        ),
        measured_at=datetime(2026, 5, 16, tzinfo=UTC),
        files=files,
        codec=container,
        container=container,
        storage_format=format.lower(),
        audio_file_count=len(files),
        filetype_band=(
            filetype_band if filetype_band is not None else format.lower()
        ),
        v0_metric=v0_metric,
        matched_bad_audio_hash_id=matched_bad_audio_hash_id,
        matched_bad_audio_hash_path=matched_bad_audio_hash_path,
    )


def make_file_complete_event_data(
    *,
    username: str,
    filename: str,
    local_filename: str,
    transfer_id: str = "t-1",
    size: int = 1000,
) -> str:
    """The JSON ``data`` string of a slskd DownloadFileComplete event,
    exactly as the live feed emits it (camelCase, nested transfer DTO)."""
    return json.dumps({
        "version": 0,
        "localFilename": local_filename,
        "remoteFilename": filename,
        "transfer": {
            "id": transfer_id,
            "username": username,
            "filename": filename,
            "size": size,
        },
    })


def make_active_download_file_state(
    username: str = "peer1",
    filename: str = "music\\Artist\\Album\\01 track.flac",
    size: int = 1000,
) -> ActiveDownloadFileState:
    return ActiveDownloadFileState(
        username=username,
        filename=filename,
        file_dir=filename.rsplit("\\", 1)[0] if "\\" in filename else "music",
        size=size,
    )


def make_active_download_state_json(
    files: list[ActiveDownloadFileState],
    filetype: str = "flac",
) -> str:
    return ActiveDownloadState(
        filetype=filetype,
        enqueued_at="2026-07-01T00:00:00+00:00",
        files=files,
    ).to_json()


def handoff_automation_owner(
    db: _AutomationHandoffDB,
    request_id: int,
    *,
    state: ActiveDownloadState | Mapping[str, object] | str | None = None,
    canonical_path: str | None = None,
    message: str = "test automation owner handoff",
) -> ImportJob:
    """Create a production-representable automation owner for tests.

    Tests must never bypass the sole lifecycle edge by inserting an
    ``automation_import`` job or assigning the owner pointer directly. This
    helper performs the real ``wanted -> downloading -> processing`` transcript
    through ``set_downloading`` and ``handoff_automation_import``.
    """
    active_state = (
        ActiveDownloadState(
            filetype="flac",
            enqueued_at="2026-07-01T00:00:00+00:00",
            files=[],
        )
        if state is None
        else ActiveDownloadState.from_raw(state)
    )
    path = (
        canonical_path
        or active_state.current_path
        or f"/processing/albums/request-{request_id}"
    )
    if not db.set_downloading(
        request_id,
        active_state.to_json(),
        expected_status="wanted",
    ):
        raise AssertionError(
            f"request {request_id} could not enter downloading for handoff"
        )
    result = db.handoff_automation_import(
        request_id=request_id,
        expected_enqueued_at=active_state.enqueued_at,
        canonical_path=path,
        message=message,
    )
    if not result.committed or result.job is None:
        raise AssertionError(
            f"request {request_id} handoff failed: {result.outcome}"
        )
    return result.job


def make_evidence(
    mb_release_id: str = "test-mbid-0001",
    files: list[AlbumQualityEvidenceFile] | None = None,
    **overrides: Any,
) -> AlbumQualityEvidence:
    """Concise builder for content-addressed evidence rows.

    Mirrors :func:`make_album_quality_evidence` with a positional-first
    signature optimised for the post-021 rekey: pass ``mb_release_id`` and
    ``files``, get back a fully-formed row with the snapshot fingerprint
    already computed.
    """
    return make_album_quality_evidence(
        mb_release_id=mb_release_id,
        files=files,
        **overrides,
    )


def make_import_result(
    decision: str = "import",
    new_min_bitrate: int = 245,
    prev_min_bitrate: int | None = None,
    was_converted: bool = False,
    original_filetype: str | None = None,
    target_filetype: str | None = None,
    spectral_grade: str = "genuine",
    spectral_bitrate: int | None = None,
    verified_lossless: bool | None = None,
    error: str | None = None,
    imported_path: str | None = None,
    disambiguated: bool = False,
    disambiguation_failure: DisambiguationFailure | None = None,
    final_format: str | None = None,
    v0_probe: V0ProbeEvidence | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
) -> ImportResult:
    """Build an ImportResult with sensible defaults."""
    if verified_lossless is None:
        verified_lossless = was_converted and spectral_grade == "genuine"
    return ImportResult(
        decision=decision,
        error=error,
        source_measurement=AudioQualityMeasurement(
            min_bitrate_kbps=new_min_bitrate,
            avg_bitrate_kbps=new_min_bitrate,
            median_bitrate_kbps=new_min_bitrate,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            spectral_subject=(
                EVIDENCE_SUBJECT_SOURCE if spectral_grade is not None else None
            ),
            spectral_provenance=(
                EVIDENCE_PROVENANCE_MEASURED
                if spectral_grade is not None else None
            ),
            format=(original_filetype or "FLAC").upper() if was_converted else None,
        ),
        verified_lossless_proof=(
            VerifiedLosslessProof(
                provenance=EVIDENCE_PROVENANCE_MEASURED,
                source=original_filetype or "lossless_source",
                classifier="test_helper",
                detail=spectral_grade,
            )
            if verified_lossless else None
        ),
        current_measurement=(AudioQualityMeasurement(
                                  min_bitrate_kbps=prev_min_bitrate,
                                  avg_bitrate_kbps=prev_min_bitrate,
                                  median_bitrate_kbps=prev_min_bitrate)
                              if prev_min_bitrate is not None else None),
        conversion=ConversionInfo(
            was_converted=was_converted,
            original_filetype=original_filetype or "",
            target_filetype=target_filetype or "",
        ),
        postflight=PostflightInfo(
            imported_path=imported_path,
            disambiguated=disambiguated,
            disambiguation_failure=disambiguation_failure,
        ),
        final_format=final_format,
        target_quality_contract=(
            TargetQualityContract.from_explicit_label(final_format)
            if was_converted and final_format
            else None
        ),
        v0_probe=v0_probe,
        existing_v0_probe=existing_v0_probe,
    )


def make_quality_rank_config(
    *,
    bitrate_metric: RankBitrateMetric | None = None,
    within_rank_tolerance_kbps: int | None = None,
    opus: CodecRankBands | None = None,
    mp3: CodecRankBands | None = None,
    aac: CodecRankBands | None = None,
) -> QualityRankConfig:
    """Build a QualityRankConfig with test-friendly overrides.

    Defaults match QualityRankConfig.defaults() — override individual fields
    to test metric swaps or custom codec bands. Use
    this instead of constructing QualityRankConfig directly so tests stay
    stable when the dataclass grows new fields.
    """
    base = QualityRankConfig.defaults()
    return QualityRankConfig(
        bitrate_metric=bitrate_metric if bitrate_metric is not None else base.bitrate_metric,
        within_rank_tolerance_kbps=(
            within_rank_tolerance_kbps
            if within_rank_tolerance_kbps is not None
            else base.within_rank_tolerance_kbps
        ),
        opus=opus if opus is not None else base.opus,
        mp3=mp3 if mp3 is not None else base.mp3,
        aac=aac if aac is not None else base.aac,
        mp3_vbr_levels=base.mp3_vbr_levels,
        lossless_codecs=base.lossless_codecs,
        mixed_format_precedence=base.mixed_format_precedence,
    )


def make_download_info(
    username: str | None = None,
    filetype: str | None = None,
    bitrate: int | None = None,
    download_spectral: SpectralMeasurement | None = None,
    current_spectral: SpectralMeasurement | None = None,
    existing_min_bitrate: int | None = None,
    **overrides: Any,
) -> DownloadInfo:
    """Build a DownloadInfo with sensible defaults."""
    di = DownloadInfo(
        username=username,
        filetype=filetype,
        bitrate=bitrate,
        download_spectral=download_spectral,
        current_spectral=current_spectral,
        existing_min_bitrate=existing_min_bitrate,
    )
    for k, v in overrides.items():
        setattr(di, k, v)
    return di


def make_download_file(
    filename: str = "01 - Track.mp3",
    id: str = "file-id-1",
    file_dir: str = "user1\\Music",
    username: str = "user1",
    size: int = 5_000_000,
    bitRate: int | None = 320,
    sampleRate: int | None = 44100,
    bitDepth: int | None = None,
    isVariableBitRate: bool | None = None,
    last_state: str | None = None,
    last_exception: str | None = None,
    bytes_transferred: int | None = None,
    retry: int | None = None,
) -> DownloadFile:
    """Build a real DownloadFile with sensible defaults.

    ``last_state``/``last_exception``/``bytes_transferred``/``retry`` are
    the persisted poll-state fields (issue #564) — default ``None`` like
    ``DownloadFile`` itself; pass overrides for scenarios that need
    pre-seeded failure evidence.
    """
    return DownloadFile(
        filename=filename,
        id=id,
        file_dir=file_dir,
        username=username,
        size=size,
        bitRate=bitRate,
        sampleRate=sampleRate,
        bitDepth=bitDepth,
        isVariableBitRate=isVariableBitRate,
        last_state=last_state,
        last_exception=last_exception,
        bytes_transferred=bytes_transferred,
        retry=retry,
    )


def make_transfer_snapshot(**overrides: Any) -> TransferSnapshot:
    """Build a TransferSnapshot (DownloadFile.status, issue #468) with a
    sensible default state. Every other field defaults per the Struct
    itself — pass overrides for the fields a scenario cares about."""
    defaults: dict[str, Any] = {"state": "Completed, Succeeded"}
    defaults.update(overrides)
    return TransferSnapshot(**defaults)


def make_download_directory(**overrides: Any) -> DownloadDirectory:
    """Build a DownloadDirectory — one directory row of the
    get_all_downloads() envelope (issue #507) — with an empty file list
    by default."""
    defaults: dict[str, Any] = {"directory": "user1\\Music", "files": []}
    defaults.update(overrides)
    return DownloadDirectory(**defaults)


def make_download_user(**overrides: Any) -> DownloadUser:
    """Build a DownloadUser — one user-group row of the
    get_all_downloads() envelope (issue #507) — with an empty directory
    list by default."""
    defaults: dict[str, Any] = {"username": "user1", "directories": []}
    defaults.update(overrides)
    return DownloadUser(**defaults)


def make_grab_list_entry(
    album_id: int = 1,
    files: list[DownloadFile] | None = None,
    filetype: str = "mp3",
    title: str = "Test Album",
    artist: str = "Test Artist",
    year: str = "2020",
    mb_release_id: str = "test-mbid",
    db_request_id: int | None = None,
    db_source: str | None = None,
    db_search_filetype_override: str | None = None,
    db_target_format: str | None = None,
    download_spectral: SpectralMeasurement | None = None,
    current_min_bitrate: int | None = None,
    current_spectral: SpectralMeasurement | None = None,
) -> GrabListEntry:
    """Build a real GrabListEntry with sensible defaults."""
    return GrabListEntry(
        album_id=album_id,
        files=files if files is not None else [make_download_file()],
        filetype=filetype,
        title=title,
        artist=artist,
        year=year,
        mb_release_id=mb_release_id,
        db_request_id=db_request_id,
        db_source=db_source,
        db_search_filetype_override=db_search_filetype_override,
        db_target_format=db_target_format,
        download_spectral=download_spectral,
        current_min_bitrate=current_min_bitrate,
        current_spectral=current_spectral,
    )


def make_validation_result(**overrides: Any) -> ValidationResult:
    """Build a ValidationResult with sensible defaults.

    Uses keyword overrides like make_request_row.
    """
    defaults: dict[str, Any] = {
        "valid": True,
        "distance": 0.05,
        "scenario": "strong_match",
    }
    defaults.update(overrides)
    return ValidationResult(**defaults)


# ---------------------------------------------------------------------------
# Shared context wiring
# ---------------------------------------------------------------------------

def make_ctx_with_fake_db(
    fake_db: Any,
    *,
    cfg: Any = None,
    slskd: Any = None,
) -> Any:
    """Build a CratediggerContext wired to a FakePipelineDB.

    The fake is wrapped in a ``FakePipelineDBSource`` so production code
    that calls ``ctx.pipeline_db_source._get_db()`` (or any of the source's
    higher-level methods) hits a typed surface, not a MagicMock that
    silently accepts arbitrary attribute access.
    """
    from lib.context import CratediggerContext
    from tests.fakes import FakePipelineDBSource
    source = FakePipelineDBSource(fake_db)
    return CratediggerContext(
        cfg=cfg if cfg is not None else MagicMock(),
        slskd=slskd if slskd is not None else MagicMock(),
        pipeline_db_source=source,
    )


def noop_quality_gate(**_kwargs: Any) -> None:
    """No-op quality-gate stub for ``dispatch_import_core(quality_gate_fn=...)``.

    Replaces the legacy module-attribute patch on
    ``_check_quality_gate_core`` for dispatch tests that don't care
    about the post-import quality gate's side effects — they want a
    no-op so the dispatch decision tree runs end-to-end without
    inspecting beets DB state."""
    return


def make_requests_http_error(
    body: str,
    *,
    status_code: int = 500,
) -> requests.HTTPError:
    """Build a real requests HTTPError with its immutable response supplied.

    ``requests.HTTPError.response`` is read-only in current stubs.  Passing a
    real ``Response`` to the constructor also keeps test doubles faithful to
    the slskd client's production exception contract.
    """
    response = requests.Response()
    response.status_code = status_code
    response._content = body.encode()
    return requests.HTTPError(f"{status_code} Server Error", response=response)


class RecordingQualityGate:
    """Recorder ``quality_gate_fn`` stub. Replaces the legacy
    module-attribute patch on ``_check_quality_gate_core`` (paired with
    ``as mock_gate``) for tests that assert
    ``mock_gate.assert_called_once()``.

    Records each invocation's kwargs (the gate is keyword-only) so tests
    can assert call counts and arguments."""

    def __init__(self, *, result: object | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.result = result

    def __call__(self, **kwargs: Any) -> object | None:
        self.calls.append(kwargs)
        return self.result

    @property
    def call_count(self) -> int:
        return len(self.calls)

    def assert_called_once(self) -> None:
        if len(self.calls) != 1:
            raise AssertionError(
                f"expected quality_gate_fn called exactly once, got {len(self.calls)}"
            )

    def assert_not_called(self) -> None:
        if self.calls:
            raise AssertionError(
                f"expected quality_gate_fn not called, got {len(self.calls)} call(s)"
            )


@contextmanager
def patch_dispatch_externals():
    """Patch external edges shared by all dispatch_import_core tests.

    Patches: sp.run, the evidence-rejection cleanup seam, trigger_plex_scan,
    and trigger_jellyfin_scan.

    Does NOT patch parse_import_result, _check_quality_gate_core,
    BeetsDB, read_runtime_config, or the vanished-replaced-album-path
    reconciler (issue #1203 item 2) — callers nest those as needed. The
    reconciler is a kwarg-DI seam on ``dispatch_import_core`` itself
    (``media_server_notify_fn``), not a module patch: it now contains real
    escalation-decision logic (``lib.library_delete_notifiers
    .notify_library_delete``), so it no longer qualifies as a thin leaf-seam
    wrapper for the mock-audit allowlist. Since ``sp.run`` below is always
    mocked, no test using this helper ever mutates the real Beets DB, so the
    reconciler's own before/after snapshot diff is empty by construction
    unless a test deliberately mutates Beets out of band (as
    ``tests.test_import_dispatch.TestVanishedPathReconciliation`` does) —
    ordinary dispatch tests never reach the reconciler at all and need no
    stand-in for it.

    Yields a SimpleNamespace with attributes: run, cleanup, plex, jellyfin.
    run is pre-configured with returncode=0, stdout="", stderr="".

    Importer post-commit cleanup is exercised through real inputs or its
    dedicated queue-owner seam; this helper does not patch that owned code.
    """
    cleanup = MagicMock()
    with patch("lib.dispatch.subprocess_runner.sp.run") as run, \
         patch("lib.dispatch.outcome_actions._cleanup_staged_dir", cleanup), \
         patch("lib.util.trigger_plex_scan") as plex, \
         patch("lib.util.trigger_jellyfin_scan") as jellyfin:
        run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        yield types.SimpleNamespace(
            run=run, cleanup=cleanup, plex=plex, jellyfin=jellyfin)
class _PinnedDispatchDB(Protocol):
    _owner_session_pin: tuple[OwnerSessionIdentity, CancellationToken] | None

    def _pin_owner_session(
        self,
        token: CancellationToken,
    ) -> AbstractContextManager[OwnerSessionIdentity]: ...


class _AutomationHandoffDB(Protocol):
    def set_downloading(
        self,
        request_id: int,
        state_json: str,
        *,
        expected_status: str = "wanted",
    ) -> bool: ...

    def handoff_automation_import(
        self,
        *,
        request_id: int,
        expected_enqueued_at: str,
        canonical_path: str,
        message: str,
    ) -> AutomationHandoffResult: ...


class SeededWrongMatch(msgspec.Struct, frozen=True):
    """One real Wrong Matches source folder plus its seeded DB rows."""

    request_id: int
    download_log_id: int
    path: str
    parent: str


def seed_visible_wrong_match(
    db: FakePipelineDB,
    root: str,
    *,
    request_id: int = 1,
    quarantine: str = "wrong_matches",
    name: str = "Artist - Album (2024) [abcd1234]",
) -> SeededWrongMatch:
    """Create a real quarantine folder and the rows that make it visible.

    Shared by every protected-path test (issue #1063) so the delete,
    triage and UI lanes all run against the same production-shaped
    world: a real directory holding a real file under a real
    ``wrong_matches`` ancestor, a request row, and a ``download_log`` row
    whose rejection scenario keeps it in the operator worklist.

    ``quarantine`` names the ancestor directory; passing something other
    than ``wrong_matches``/``failed_imports`` produces the unsafe-path
    world on purpose.
    """
    parent = os.path.join(root, quarantine)
    os.makedirs(parent, exist_ok=True)
    path = os.path.join(parent, name)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "01 Track.mp3"), "wb") as handle:
        handle.write(b"audio")
    if db.get_request(request_id) is None:
        db.seed_request(make_request_row(
            id=request_id,
            status="wanted",
            mb_release_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ))
    download_log_id = db.log_download(
        request_id,
        outcome="rejected",
        validation_result={
            "scenario": "wrong_match",
            "detail": "wrong album",
            "distance": 0.6,
            "failed_path": path,
            "soulseek_username": "peer",
            "candidates": [],
            "items": [],
        },
    )
    return SeededWrongMatch(
        request_id=request_id,
        download_log_id=download_log_id,
        path=path,
        parent=parent,
    )
