"""Shared test helpers — canonical mock data builders.

Builders for structured data used across tests. Use these instead of
hand-rolling dicts or dataclass constructors with many fields.

Two cohesive clusters live in sibling modules (issue #1278, "worth
exploring" item 5): ``tests/evidence_helpers.py`` owns the
``AlbumQualityEvidence``-family builders and parity worlds, and
``tests/dispatch_helpers.py`` owns the dispatch/import-lane bridges,
claim/handoff lifecycle helpers, and dispatch seam stubs.
"""

from __future__ import annotations

import configparser
import dataclasses
import json
import os
import stat
import tempfile
from collections.abc import Callable, Generator, Sequence
from contextlib import AbstractContextManager, contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Protocol
from unittest.mock import MagicMock

import msgspec
import requests

if TYPE_CHECKING:
    # Import-time cycle: ``tests.fakes`` does not import this module, but
    # keeping the reference type-only preserves that independence.
    from lib.beets_db import BeetsDB
    from lib.context import (
        CratediggerContext,
        CycleCollaborators,
        PipelineDBSource,
        WorkerCollaborators,
    )
    from lib.download_ownership import DownloadOwnershipWriter
    from lib.enqueue import ClaimedQueueKeysRegistry
    from lib.peer_cache import PeerCache
    from lib.pipeline_db import PipelineDB
    from tests.fakes import FakeBeetsDB, FakePipelineDB
    from web.runtime import WebRuntime

from lib.grab_list import DownloadFile, GrabListEntry
from lib.pipeline_db._shared import TransferLedgerRow
from lib.quality import (
    EVIDENCE_PROVENANCE_MEASURED,
    EVIDENCE_SUBJECT_SOURCE,
    ActiveDownloadFileState,
    ActiveDownloadState,
    AudioQualityMeasurement,
    CandidateSummary,
    ConversionInfo,
    DisambiguationFailure,
    DownloadInfo,
    HarnessItem,
    HarnessTrackInfo,
    ImportResult,
    PostflightInfo,
    SpectralMeasurement,
    TargetQualityContract,
    TrackMapping,
    V0ProbeEvidence,
    ValidationResult,
    VerifiedLosslessProof,
)
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


def make_request_row(**overrides: object) -> dict[str, Any]:
    """Return a complete album_requests row dict with sensible defaults.

    Mirrors the shape of PipelineDB.get_request() (SELECT * FROM album_requests).
    Use keyword overrides to set specific fields for your test scenario.
    """
    row: dict[str, object] = {
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


class _TransferOwnershipDB(Protocol):
    """The two ledger writes plus the read ``own_transfer_keys`` needs."""

    def record_transfer_enqueue(self, rows: list[TransferLedgerRow]) -> None: ...

    def confirm_transfer_enqueue(
        self, username: str, filename: str, *, request_id: int,
    ) -> int: ...

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]: ...


def own_transfer_keys(
    db: _TransferOwnershipDB,
    keys: Sequence[tuple[str, str]],
    *,
    request_id: int = 1,
) -> None:
    """Ensure each ``(username, filename)`` queue key is ledger-owned.

    This is the STEADY STATE a ``downloading`` request settles into, not an
    ordering guarantee. The real order runs the other way (issue #1278
    review F3): ``lib.enqueue._claim_initial_download_ownership`` persists
    ``active_download_state`` through ``writer.claim_downloading`` BEFORE
    ``_enqueue_with_claim_outcome`` reaches
    ``slskd_enqueue_with_outcome``, which writes the write-ahead row, POSTs,
    and only then confirms. So a ``downloading`` row whose keys hold no
    accepted row is produced routinely -- transiently between claim and
    confirm, and DURABLY whenever ``_leave_claim_for_poll_recovery`` leaves
    an ambiguous POST's claim in place. MEASURED 2026-08-27 on the live
    ledger: 64 distinct queue keys carry write-ahead rows and no acceptance
    at all, enqueued between 2026-07-09 and 2026-07-17 (none since, matching
    the per-key gate's own rationale). That world is real, and the stamping
    ownership gate (#1278 item 1) refuses to stamp in it; a test wanting it
    seeds the pending row itself rather than calling this helper. Composed
    end to end in
    ``tests/test_download.py::TestPollActiveDownloads::
    test_pending_only_ledger_world_refuses_the_stamp_and_hard_fails``.

    What this helper is for is every OTHER fixture: seeding the state alone
    and leaving the ledger empty silently models the recovery world by
    accident, and a test that meant to exercise stamping then proves
    nothing (Rule B, ``.claude/rules/test-fidelity.md``).

    This is a PRECONDITION helper, not a replay of ``enqueue``: a key that
    is already accepted is skipped, so re-seeding a request (an
    incarnation swap) does not multiply the ledger rows a test is
    counting. A test that needs a SECOND row on a key -- a later ambiguous
    attempt, say -- writes it itself through ``record_transfer_enqueue``.
    """
    already_owned = db.get_owned_transfer_keys()
    rows = [
        TransferLedgerRow(
            request_id=request_id,
            username=username,
            filename=filename,
        )
        for username, filename in keys
        if (username, filename) not in already_owned
    ]
    if not rows:
        return
    db.record_transfer_enqueue(rows)
    for row in rows:
        db.confirm_transfer_enqueue(
            row.username, row.filename, request_id=row.request_id)


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


def make_download_info(
    username: str | None = None,
    filetype: str | None = None,
    bitrate: int | None = None,
    download_spectral: SpectralMeasurement | None = None,
    current_spectral: SpectralMeasurement | None = None,
    existing_min_bitrate: int | None = None,
    **overrides: object,
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


def make_transfer_snapshot(**overrides: object) -> TransferSnapshot:
    """Build a TransferSnapshot (DownloadFile.status, issue #468) with a
    sensible default state. Every other field defaults per the Struct
    itself — pass overrides for the fields a scenario cares about."""
    defaults: dict[str, Any] = {"state": "Completed, Succeeded"}
    defaults.update(overrides)
    return TransferSnapshot(**defaults)


def make_download_directory(**overrides: object) -> DownloadDirectory:
    """Build a DownloadDirectory — one directory row of the
    get_all_downloads() envelope (issue #507) — with an empty file list
    by default."""
    defaults: dict[str, Any] = {"directory": "user1\\Music", "files": []}
    defaults.update(overrides)
    return DownloadDirectory(**defaults)


def make_download_user(**overrides: object) -> DownloadUser:
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


def make_candidate_summary(
    mbid: str = "",
    distance: float = 0.05,
    data_source: str = "MusicBrainz",
    tracks: list[HarnessTrackInfo] | None = None,
    mapping: list[TrackMapping] | None = None,
    extra_items: list[HarnessItem] | None = None,
    extra_tracks: list[HarnessTrackInfo] | None = None,
    **audit_overrides: object,
) -> CandidateSummary:
    """Build a wire-valid CandidateSummary with the required fields filled.

    The decision-consumed fields are required on the Struct (#1278 item
    8), so every test construction has to supply them; this builder gives
    them producible defaults. Audit metadata rides in ``audit_overrides``
    and is applied by ``setattr`` — msgspec Structs are slotted, so a
    mistyped field NAME raises ``AttributeError`` instead of vanishing —
    and the result is then round-tripped through ``msgspec.convert`` so a
    mistyped field VALUE (``year="2020"``) raises ``ValidationError``
    instead of building a shape the harness could never emit. Tests that
    need a deliberately wire-invalid candidate use a raw dict, never this
    builder.
    """
    summary = CandidateSummary(
        mbid=mbid,
        distance=distance,
        data_source=data_source,
        tracks=list(tracks or []),
        mapping=list(mapping or []),
        extra_items=list(extra_items or []),
        extra_tracks=list(extra_tracks or []),
    )
    for name, value in audit_overrides.items():
        setattr(summary, name, value)
    return msgspec.convert(
        msgspec.to_builtins(summary), type=CandidateSummary,
    )


def make_validation_result(**overrides: object) -> ValidationResult:
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

#: The builders' config seam. Tests pass either a real ``CratediggerConfig``
#: or a ``MagicMock`` stand-in, which is why it has always been ``Any`` here;
#: one alias so the reason is stated once instead of at every parameter.
_CfgLike = Any


def make_cycle_collaborators(
    *,
    cfg: _CfgLike = None,
    slskd: object = None,
    pipeline_db_source: PipelineDBSource | None = None,
    download_ownership: DownloadOwnershipWriter | None = None,
    claimed_queue_keys_registry: ClaimedQueueKeysRegistry | None = None,
    peer_cache: PeerCache | None = None,
) -> CycleCollaborators:
    """Build a ``CycleCollaborators`` with test defaults (issue #1313).

    ``lib.context.CycleCollaborators`` requires all six fields on purpose:
    a production site that forgets one does not type-check, which is what
    replaced the hand-registered construction audit. The defaults belong
    HERE rather than on the production type — a test that does not care
    about a collaborator says so by omission, while production must name
    every one. A seventh collaborator therefore breaks the five production
    sites (correct) and none of the tests (also correct).

    ``None`` is the honest default for four of them: it is exactly what
    ``CratediggerContext`` carried before this split, so the worlds these
    tests drive are unchanged. Passing ``download_ownership=None`` keeps
    ownership-gated destructive paths failing closed, which is what the
    tests that omit it have always exercised.
    """
    from lib.context import CycleCollaborators
    from tests.fakes import FakePipelineDBSource
    return CycleCollaborators(
        cfg=cfg if cfg is not None else MagicMock(),
        slskd=slskd,
        pipeline_db_source=(
            pipeline_db_source if pipeline_db_source is not None
            else FakePipelineDBSource()
        ),
        download_ownership=download_ownership,
        claimed_queue_keys_registry=claimed_queue_keys_registry,
        peer_cache=peer_cache,
    )


class _Keep:
    """Sentinel type for ``rebind_collaborators``.

    A distinct CLASS rather than a bare ``object()`` so each parameter keeps
    its real declared type in the union and ``isinstance`` narrows the
    sentinel away: "leave this one alone" stays distinguishable from an
    explicit ``None`` — which several tests pass on purpose, to exercise a
    fail-closed path — without an escape hatch.
    """


_KEEP = _Keep()


def rebind_collaborators(
    ctx: CratediggerContext,
    *,
    slskd: object = _KEEP,
    pipeline_db_source: PipelineDBSource | _Keep = _KEEP,
    download_ownership: DownloadOwnershipWriter | None | _Keep = _KEEP,
    claimed_queue_keys_registry: (
        ClaimedQueueKeysRegistry | None | _Keep) = _KEEP,
    peer_cache: PeerCache | None | _Keep = _KEEP,
) -> None:
    """Swap one collaborator on an existing context — TESTS ONLY (#1313).

    ``CycleCollaborators`` is frozen, so ``ctx.download_ownership = w`` no
    longer works and production has no way to bolt a collaborator on after
    the fact. Tests legitimately do vary exactly one collaborator against
    an otherwise-shared world — most often removing the ownership writer
    mid-test to exercise the destructive gate's fail-closed path — so this
    rebinds the whole frozen value rather than mutating a field.

    Every field is named explicitly here rather than going through
    ``dataclasses.replace``, for the same reason production does: measured
    against pyright 1.1.412, ``replace()`` kwargs are checked for neither
    name nor type. A misspelled NAME is still loud, at runtime, because
    ``__init__`` rejects the unknown kwarg; what ``replace()`` loses is the
    wrong-TYPE check, which a constructor call gets and which nothing else
    here would catch.

    ``cfg`` is deliberately not reboundable: no test varies it this way,
    and a context whose config changes mid-scenario is a different world,
    not a rebound collaborator.
    """
    from lib.context import CycleCollaborators
    current = ctx.collaborators
    if not isinstance(current, CycleCollaborators):
        raise TypeError(
            "rebind_collaborators expects a cycle-world context; "
            f"got {type(current).__name__}")
    ctx.collaborators = CycleCollaborators(
        cfg=current.cfg,
        slskd=current.slskd if isinstance(slskd, _Keep) else slskd,
        pipeline_db_source=(
            current.pipeline_db_source
            if isinstance(pipeline_db_source, _Keep)
            else pipeline_db_source),
        download_ownership=(
            current.download_ownership
            if isinstance(download_ownership, _Keep)
            else download_ownership),
        claimed_queue_keys_registry=(
            current.claimed_queue_keys_registry
            if isinstance(claimed_queue_keys_registry, _Keep)
            else claimed_queue_keys_registry),
        peer_cache=(
            current.peer_cache if isinstance(peer_cache, _Keep)
            else peer_cache),
    )


def make_worker_collaborators(
    *,
    cfg: _CfgLike = None,
    pipeline_db_source: PipelineDBSource | None = None,
) -> WorkerCollaborators:
    """Build a ``WorkerCollaborators`` — the slskd-less out-of-cycle world.

    Use this where the context under test stands in for the importer or
    the preview worker; ``make_cycle_collaborators`` everywhere else.
    """
    from lib.context import WorkerCollaborators
    from tests.fakes import FakePipelineDBSource
    return WorkerCollaborators(
        cfg=cfg if cfg is not None else MagicMock(),
        pipeline_db_source=(
            pipeline_db_source if pipeline_db_source is not None
            else FakePipelineDBSource()
        ),
    )


def make_ctx_with_fake_db(
    fake_db: FakePipelineDB,
    *,
    cfg: _CfgLike = None,
    slskd: _CfgLike = None,
) -> CratediggerContext:
    """Build a CratediggerContext wired to a FakePipelineDB.

    The fake is wrapped in a ``FakePipelineDBSource`` so production code
    that calls ``ctx.pipeline_db_source._get_db()`` (or any of the source's
    higher-level methods) hits a typed surface, not a MagicMock that
    silently accepts arbitrary attribute access. ``cfg``/``slskd`` stay
    ``Any`` on purpose: tests pass a real ``CratediggerConfig`` or
    ``FakeSlskdAPI`` when the scenario needs one and fall back to the
    ``MagicMock`` default otherwise.
    """
    from lib.context import CratediggerContext
    from tests.fakes import FakePipelineDBSource
    source = FakePipelineDBSource(fake_db)
    return CratediggerContext(
        collaborators=make_cycle_collaborators(
            cfg=cfg if cfg is not None else MagicMock(),
            slskd=slskd if slskd is not None else MagicMock(),
            pipeline_db_source=source,
        ),
    )


def make_web_runtime(
    base: WebRuntime | None = None,
    *,
    db: FakePipelineDB | None = None,
    beets: FakeBeetsDB | None = None,
) -> WebRuntime:
    """Build a :class:`WebRuntime` carrying the two DB fakes (#1313).

    The runtime's two handle fields are declared as the production types
    its routes hand onward — ``rt.db()`` reaches services annotated
    ``PipelineDB``, ``rt.beets_db()`` services annotated ``BeetsDB`` — so
    neither fake is nominally assignable to them. This is the one place
    that gap is crossed, and the two scoped ignores below are where it is
    visible; same shape as :func:`make_ctx_with_fake_db` above, fakes in,
    a real runtime out.

    Both ignores are load-bearing because the assignments are *annotated
    locals*, which Pyright does check. ``dataclasses.replace`` itself
    checks nothing — measured 2026-09-01 against pyright 1.1.412: it
    rejects neither an unknown field name nor a wrong-typed value nor a
    fake for a production-typed field. So passing the fakes straight to
    ``replace`` would have been silently accepted, which is precisely the
    kind of unchecked seam this whole change exists to remove; routing
    them through a typed local keeps the one real gap declared rather
    than laundered.

    ``base`` derives from an already-installed runtime; omit it for a
    bare one. A ``None`` argument means "leave that field as it is" — to
    clear a handle (the "no Beets configured" world) pass
    ``shared_beets=None`` to ``dataclasses.replace`` directly.
    """
    from web.runtime import WebRuntime

    runtime = base if base is not None else WebRuntime()
    shared_db: PipelineDB | None = (
        db  # pyright: ignore[reportAssignmentType]
        if db is not None else runtime.shared_db
    )
    shared_beets: BeetsDB | None = (
        beets  # pyright: ignore[reportAssignmentType]
        if beets is not None else runtime.shared_beets
    )
    return dataclasses.replace(
        runtime, shared_db=shared_db, shared_beets=shared_beets,
    )


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
    soulseek_username: str | None = None,
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
        soulseek_username=soulseek_username,
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
