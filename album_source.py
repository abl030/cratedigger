"""Album source — Pipeline DB as the source of wanted albums.

Provides the interface cratedigger.py uses to get wanted albums, fetch tracks,
and report completion. AlbumRecord is a typed dataclass returned by from_db_row().
"""

from __future__ import annotations

import json
import logging
import urllib.request
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, TypedDict
from urllib.parse import quote

if TYPE_CHECKING:
    from cratedigger import TrackRecord
    from lib.grab_list import GrabListEntry
    from lib.pipeline_db import PipelineDB
    from lib.quality import DownloadInfo, ValidationResult
    from lib.terminal_outcomes import PendingImportTerminalOutcome

logger = logging.getLogger("cratedigger")

from lib.discogs_positions import normalize_release_tracks
from lib.release_identity import detect_release_source


class _MBRecordingJSON(TypedDict, total=False):
    """Slice of a MusicBrainz ``recording`` object this module reads."""
    length: int


class _MBTrackJSON(TypedDict, total=False):
    """Slice of a MusicBrainz release-lookup ``track`` object."""
    position: int
    number: int
    title: str
    length: int
    recording: _MBRecordingJSON


class _MBMediumJSON(TypedDict, total=False):
    """Slice of a MusicBrainz release-lookup ``medium`` object."""
    position: int
    tracks: list[_MBTrackJSON]


class _MBReleaseJSON(TypedDict, total=False):
    """Slice of the MusicBrainz ``/release/<mbid>?inc=recordings`` response.

    Untyped (structural-only, no runtime validation) — mirrors the
    pre-existing ``.get(..., default)`` tolerance for an external API
    response, not a wire-boundary Struct. See ``_populate_tracks_mb``.
    """
    media: list[_MBMediumJSON]


class _DiscogsTrackJSON(TypedDict, total=False):
    """Slice of a Discogs release ``track`` object this module reads."""
    position: str
    duration: str
    title: str


class _DiscogsReleaseJSON(TypedDict, total=False):
    """Slice of the Discogs mirror's ``/api/releases/<id>`` response.

    Untyped (structural-only, no runtime validation) — same rationale as
    ``_MBReleaseJSON``. See ``_populate_tracks_discogs``.
    """
    tracks: list[_DiscogsTrackJSON]


@dataclass
class MediaRecord:
    """One disc/medium within a release."""
    medium_number: int
    medium_format: str
    track_count: int


@dataclass
class ReleaseRecord:
    """One release (pressing/edition) of an album."""
    id: int
    foreign_release_id: str
    title: str
    track_count: int
    medium_count: int
    format: str
    media: list[MediaRecord]
    monitored: bool
    country: list[str]
    status: str


@dataclass
class AlbumRecord:
    """Normalized album record from a pipeline DB row."""
    id: int
    title: str
    release_date: str
    artist_id: int
    artist_name: str
    foreign_artist_id: str
    releases: list[ReleaseRecord]
    db_request_id: int
    db_source: str
    db_mb_release_id: str
    db_search_filetype_override: str | None
    db_target_format: str | None
    # Release-group's first-release year, populated from the local MB
    # mirror at enqueue time or via the deploy-time backfill. NULL for
    # pre-backfill rows, Discogs-only rows, and rows missing
    # ``mb_release_group_id``. The generator emits a year-suffixed slot
    # when this differs from ``release_date``'s year.
    db_release_group_year: int | None = None

    @staticmethod
    def from_db_row(row: Mapping[str, object], tracks: list[dict[str, object]]) -> AlbumRecord:
        """Build a typed AlbumRecord from a pipeline DB row + tracks."""
        # Build media structure from tracks (grouped by disc)
        discs: dict[int, list[dict[str, object]]] = {}
        for t in tracks:
            d = t["disc_number"]
            assert isinstance(d, int)
            if d not in discs:
                discs[d] = []
            discs[d].append(t)

        media: list[MediaRecord] = []
        for disc_num in sorted(discs.keys()):
            disc_tracks = discs[disc_num]
            base_fmt = row.get("format") or "Digital Media"
            assert isinstance(base_fmt, str)
            media.append(MediaRecord(
                medium_number=disc_num,
                medium_format=base_fmt,
                track_count=len(disc_tracks),
            ))

        total_tracks = sum(len(dt) for dt in discs.values())
        num_discs = len(discs)

        # Build format string: "CD", "2xCD", "Digital Media"
        base_format = row.get("format") or "Digital Media"
        assert isinstance(base_format, str)
        format_str = f"{num_discs}x{base_format}" if num_discs > 1 else base_format

        row_id = row["id"]
        assert isinstance(row_id, int)
        mb_release_id = row["mb_release_id"]
        assert isinstance(mb_release_id, str) or mb_release_id is None
        album_title = row["album_title"]
        assert isinstance(album_title, str)
        artist_name = row["artist_name"]
        assert isinstance(artist_name, str)
        country_val = row.get("country") or "US"
        assert isinstance(country_val, str)
        source = row["source"]
        assert isinstance(source, str)

        release = ReleaseRecord(
            id=row_id * -1,
            foreign_release_id=mb_release_id or "",
            title=album_title,
            track_count=total_tracks,
            medium_count=num_discs,
            format=format_str,
            media=media,
            monitored=True,
            country=[country_val],
            status="Official",
        )

        year = row.get("year") or "0000"
        mb_artist_id = row.get("mb_artist_id") or ""
        assert isinstance(mb_artist_id, str)
        search_filetype_override = row.get("search_filetype_override")
        assert isinstance(search_filetype_override, (str, type(None)))
        target_format = row.get("target_format")
        assert isinstance(target_format, (str, type(None)))
        release_group_year = row.get("release_group_year")
        assert isinstance(release_group_year, (int, type(None)))

        return AlbumRecord(
            id=row_id * -1,
            title=album_title,
            release_date=f"{year}-01-01T00:00:00Z",
            artist_id=0,
            artist_name=artist_name,
            foreign_artist_id=mb_artist_id,
            releases=[release],
            db_request_id=row_id,
            db_source=source,
            db_mb_release_id=mb_release_id or "",
            db_search_filetype_override=search_filetype_override,
            db_target_format=target_format,
            db_release_group_year=release_group_year,
        )


class DatabaseSource:
    """Fetch wanted albums from pipeline.db."""

    def __init__(
        self,
        dsn: str,
        *,
        musicbrainz_ws2_base: str,
        discogs_api_base: str,
        borrowed_db: PipelineDB | None = None,
    ) -> None:
        self.dsn = dsn
        self.musicbrainz_ws2_base = musicbrainz_ws2_base.rstrip("/")
        self.discogs_api_base = discogs_api_base.rstrip("/")
        self._db: PipelineDB | None = borrowed_db
        self._owns_db = borrowed_db is None
        self._closed = False

    def _get_db(self) -> PipelineDB:
        if getattr(self, "_closed", False):
            raise RuntimeError("DatabaseSource is closed")
        if self._db is None:
            from lib.pipeline_db import PipelineDB
            self._db = PipelineDB(self.dsn)
            self._owns_db = True
        return self._db

    def get_wanted(self, limit: int | None = None) -> list[AlbumRecord]:
        """Get wanted albums as normalized records.

        Forensic / dashboard / inspection callers should use this method
        -- it returns every wanted row regardless of plan readiness.

        Phase 2 search execution should use ``get_wanted_searchable``
        (introduced in U4) to filter to rows whose active plan matches
        the current generator id.
        """
        db = self._get_db()
        wanted = db.get_wanted(limit=limit)
        records: list[AlbumRecord] = []
        for row in wanted:
            tracks = db.get_tracks(row["id"])
            if not tracks:
                # Try to populate tracks from MB API
                tracks = self._populate_tracks(row)
            record = AlbumRecord.from_db_row(row, tracks)
            records.append(record)
        return records

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
    ) -> list[AlbumRecord]:
        """Get wanted albums whose active plan matches ``generator_id``.

        This is the **execution-eligibility** filter for Phase 2 (U4).
        Rows without a current-generator active plan are excluded --
        startup reconciliation owns repairing those before the next
        cycle. Mirrors the track-population behavior of ``get_wanted``.
        """
        db = self._get_db()
        wanted = db.get_wanted_searchable(
            generator_id,
            limit=limit,
            title_blacklist=title_blacklist,
        )
        records: list[AlbumRecord] = []
        for row in wanted:
            tracks = db.get_tracks(row["id"])
            if not tracks:
                tracks = self._populate_tracks(row)
            record = AlbumRecord.from_db_row(row, tracks)
            records.append(record)
        return records

    def get_tracks(self, album_record: AlbumRecord | object) -> list[TrackRecord]:
        """Get tracks for an album in normalized track format.

        Returns list of dicts with keys: title, trackNumber, mediumNumber, duration.
        """
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return []

        db = self._get_db()
        tracks = db.get_tracks(request_id)
        album_id = request_id * -1  # Negative ID space
        return [
            {
                "title": t["title"],
                "trackNumber": str(t["track_number"]),
                "mediumNumber": t["disc_number"],
                "duration": int((t.get("length_seconds") or 0) * 10000000),  # ticks (100ns units)
                "id": 0,
                "albumId": album_id,
            }
            for t in tracks
        ]

    def mark_done(
        self,
        album_record: GrabListEntry,
        bv_result: ValidationResult,
        dest_path: str | None = None,
        download_info: DownloadInfo | None = None,
        import_job_id: int | None = None,
    ) -> int | None | PendingImportTerminalOutcome:
        """Mark album as imported."""
        from lib.dispatch import _do_mark_done
        from lib.quality import DownloadInfo
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return None

        db = self._get_db()
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()
        return _do_mark_done(
            db=db,
            request_id=request_id,
            dl_info=dl,
            distance=bv_result.distance,
            scenario=bv_result.scenario,
            dest_path=dest_path,
            detail=bv_result.detail,
            import_job_id=import_job_id,
        )

    def reject_and_requeue(
        self,
        album_record: GrabListEntry,
        bv_result: ValidationResult,
        usernames: Collection[str] | None = None,
        download_info: DownloadInfo | None = None,
        search_filetype_override: str | None = None,
        cooled_down_users: set[str] | None = None,
        import_job_id: int | None = None,
    ) -> int | None | PendingImportTerminalOutcome:
        """Record a rejected validation and keep the album wanted for retry.

        Delegates to ``_record_rejection_and_maybe_requeue`` for BOTH the
        job-backed and job-less cases, differing only in whether
        ``import_job_id`` is attached (issue #1355 item 3): the job-backed
        branch returns an uncommitted ``PendingImportTerminalOutcome`` for
        its owning import job to finish atomically later; the job-less
        branch commits the request transition, the ``download_log`` audit
        row, and every denylist/cooldown entry together in one PostgreSQL
        transaction (``PipelineDB.persist_request_rejection_outcome``),
        instead of three separate autocommitted writes — never a
        transitioned-but-unaudited or denylisted-but-still-searchable
        request left behind by a mid-sequence crash. Since issue #1355
        item A4, the job-less branch also routes its transition through
        ``_apply_terminal_request_transition`` — the same arbitration the
        job-backed path already had — so a job-less rejection against an
        ``unsearchable`` request stays ``unsearchable`` instead of
        clearing the operator's stop.
        """
        from lib.dispatch import _record_rejection_and_maybe_requeue
        from lib.quality import DownloadInfo
        from lib.terminal_outcomes import TerminalDenylist

        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return None

        db = self._get_db()
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()
        denylists = tuple(
            TerminalDenylist(username, "beets validation rejected", apply_cooldown=True)
            for username in sorted(usernames or ())
        )
        return _record_rejection_and_maybe_requeue(
            db,
            request_id,
            dl,
            detail=bv_result.detail,
            error=bv_result.error,
            validation_result=(dl.validation_result or bv_result.to_json()),
            requeue=True,
            search_filetype_override=search_filetype_override,
            import_job_id=import_job_id,
            denylists=denylists,
            cooled_down_users=cooled_down_users,
        )

    def get_denylisted_users(self, album_record: GrabListEntry) -> set[str]:
        """Get denylisted usernames for an album."""
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return set()
        db = self._get_db()
        entries = db.get_denylisted_users(request_id)
        return {e["username"] for e in entries}

    def _populate_tracks(self, row: Mapping[str, object]) -> list[dict[str, object]]:
        """Fetch tracks from MB or Discogs API and store in DB."""
        release_id = row.get("mb_release_id")
        if not release_id:
            return []
        assert isinstance(release_id, str)

        source = detect_release_source(release_id)
        if source == "discogs":
            return self._populate_tracks_discogs(row, release_id)
        return self._populate_tracks_mb(row, release_id)

    def _populate_tracks_mb(
        self,
        row: Mapping[str, object],
        mb_id: str,
    ) -> list[dict[str, object]]:
        """Fetch tracks from the MusicBrainz API."""
        try:
            url = (
                f"{self.musicbrainz_ws2_base}/release/{quote(mb_id, safe='')}"
                "?inc=recordings&fmt=json"
            )
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "cratedigger-db/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                data: _MBReleaseJSON = json.loads(resp.read())
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            logger.warning(f"Failed to fetch tracks from MB API for {mb_id}")
            return []

        tracks: list[dict[str, object]] = []
        for medium in data.get("media", []):
            disc = medium.get("position", 1)
            for track in medium.get("tracks", []):
                recording: _MBRecordingJSON = track.get("recording") or {}
                length_ms = track.get("length") or recording.get("length")
                tracks.append({
                    "disc_number": disc,
                    "track_number": track.get("position", track.get("number", 0)),
                    "title": track.get("title", ""),
                    "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
                })

        if tracks:
            row_id = row["id"]
            assert isinstance(row_id, int)
            db = self._get_db()
            db.set_tracks(row_id, tracks)

        return tracks

    def _populate_tracks_discogs(
        self,
        row: Mapping[str, object],
        discogs_id: str,
    ) -> list[dict[str, object]]:
        """Fetch tracks from the Discogs mirror API.

        Parses through the one canonical normalizer
        (`lib.discogs_positions.normalize_release_tracks`) so this
        fallback writer produces the same rip-shaped manifest as the
        add/Replace flows — a divergent inline parser here shipped
        per-sub-entry manifests the matcher count gate can never pass
        (issue #1261).
        """
        try:
            url = f"{self.discogs_api_base}/api/releases/{discogs_id}"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "cratedigger-db/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                data: _DiscogsReleaseJSON = json.loads(resp.read())
        except Exception:  # noqa: BLE001 - boundary converts or isolates collaborator failures
            logger.warning(f"Failed to fetch tracks from Discogs API for {discogs_id}")
            return []

        tracks = normalize_release_tracks(data.get("tracks", []))

        if tracks:
            row_id = row["id"]
            assert isinstance(row_id, int)
            db = self._get_db()
            db.set_tracks(row_id, tracks)

        return tracks

    def close(self) -> None:
        if self._db and getattr(self, "_owns_db", True):
            self._db.close()
        self._db = None
        self._closed = True
