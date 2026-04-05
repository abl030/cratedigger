"""Album source — Pipeline DB as the source of wanted albums.

Provides the interface soularr.py uses to get wanted albums, fetch tracks,
and report completion. AlbumRecord is a typed dataclass returned by from_db_row().
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from dataclasses import dataclass

logger = logging.getLogger("soularr")


MB_API_BASE = "http://192.168.1.35:5200/ws/2"


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
    db_quality_override: str | None

    @staticmethod
    def from_db_row(row: dict[str, object], tracks: list[dict[str, object]]) -> AlbumRecord:
        """Build a typed AlbumRecord from a pipeline DB row + tracks."""
        # Build media structure from tracks (grouped by disc)
        discs: dict[int, list[dict[str, object]]] = {}
        for t in tracks:
            d = t["disc_number"]
            assert isinstance(d, int)
            if d not in discs:
                discs[d] = []
            discs[d].append(t)

        media = []
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
        quality_override = row.get("quality_override")
        assert isinstance(quality_override, (str, type(None)))

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
            db_quality_override=quality_override,
        )


class DatabaseSource:
    """Fetch wanted albums from pipeline.db."""

    def __init__(self, dsn):
        self.dsn = dsn
        self._db = None

    def _get_db(self):
        if self._db is None:
            import sys
            lib_dir = os.path.join(os.path.dirname(__file__), "lib")
            if lib_dir not in sys.path:
                sys.path.insert(0, lib_dir)
            from pipeline_db import PipelineDB
            self._db = PipelineDB(self.dsn)
        return self._db

    def get_wanted(self, limit=None):
        """Get wanted albums as normalized records."""
        db = self._get_db()
        wanted = db.get_wanted(limit=limit)
        records = []
        for row in wanted:
            tracks = db.get_tracks(row["id"])
            if not tracks:
                # Try to populate tracks from MB API
                tracks = self._populate_tracks(row)
            record = AlbumRecord.from_db_row(row, tracks)
            records.append(record)
        return records

    def get_tracks(self, album_record: AlbumRecord | object) -> list[dict[str, object]]:
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

    def update_status(self, album_record, status, **extra):
        """Update album status in the pipeline DB."""
        from lib.transitions import apply_transition
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return
        db = self._get_db()
        apply_transition(db, request_id, status, **extra)

    def mark_done(self, album_record, bv_result, dest_path=None,
                  download_info=None):
        """Mark album as imported."""
        from lib.quality import DownloadInfo, SpectralMeasurement, is_verified_lossless
        from lib.pipeline_db import RequestSpectralStateUpdate
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return

        db = self._get_db()
        distance = bv_result.distance
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()

        update_fields = dict(
            beets_distance=distance,
            beets_scenario=bv_result.scenario,
            imported_path=dest_path,
        )
        if dl.verified_lossless_override is not None:
            # import_one.py computed this — trust it over re-derivation
            if dl.verified_lossless_override:
                update_fields["verified_lossless"] = True
        elif is_verified_lossless(
            dl.was_converted,
            dl.original_filetype,
            dl.download_spectral.grade if dl.download_spectral else None,
        ):
            update_fields["verified_lossless"] = True
        if dl.download_spectral is not None:
            current_spectral = dl.download_spectral
            if update_fields.get("verified_lossless") and dl.bitrate:
                # Verified lossless: V0 bitrate is the real quality fingerprint,
                # not the spectral cliff estimate (which can miscalibrate)
                current_spectral = SpectralMeasurement(
                    grade=dl.download_spectral.grade,
                    bitrate_kbps=dl.bitrate // 1000,
                )
            update_fields.update(
                RequestSpectralStateUpdate(
                    last_download=dl.download_spectral,
                    current=current_spectral,
                ).as_update_fields()
            )
        if dl.final_format:
            update_fields["final_format"] = dl.final_format
        from lib.transitions import apply_transition
        apply_transition(db, request_id, "imported", **update_fields)

        db.log_download(
            request_id=request_id,
            soulseek_username=dl.username,
            filetype=dl.filetype,
            beets_distance=distance,
            beets_scenario=bv_result.scenario,
            beets_detail=bv_result.detail,
            outcome="success",
            staged_path=dest_path,
            bitrate=dl.bitrate,
            sample_rate=dl.sample_rate,
            bit_depth=dl.bit_depth,
            is_vbr=dl.is_vbr,
            was_converted=dl.was_converted,
            original_filetype=dl.original_filetype,
            slskd_filetype=dl.slskd_filetype,
            slskd_bitrate=dl.slskd_bitrate,
            actual_filetype=dl.actual_filetype,
            actual_min_bitrate=dl.actual_min_bitrate,
            spectral_grade=dl.download_spectral.grade if dl.download_spectral else None,
            spectral_bitrate=(
                dl.download_spectral.bitrate_kbps if dl.download_spectral else None
            ),
            existing_min_bitrate=dl.existing_min_bitrate,
            existing_spectral_bitrate=(
                dl.current_spectral.bitrate_kbps if dl.current_spectral else None
            ),
            import_result=dl.import_result,
            validation_result=dl.validation_result,
            final_format=dl.final_format,
        )

    def mark_failed(self, album_record, bv_result, usernames=None,
                    download_info=None):
        """Log the failure and denylist users, but keep album wanted for retry."""
        from lib.quality import DownloadInfo
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return

        db = self._get_db()
        dl = download_info if isinstance(download_info, DownloadInfo) else DownloadInfo()
        from lib.transitions import apply_transition
        apply_transition(db, request_id, "wanted",
                         beets_distance=bv_result.distance,
                         beets_scenario=bv_result.scenario)
        db.record_attempt(request_id, "validation")

        db.log_download(
            request_id=request_id,
            soulseek_username=dl.username,
            filetype=dl.filetype,
            beets_distance=bv_result.distance,
            beets_scenario=bv_result.scenario,
            beets_detail=bv_result.detail,
            outcome="rejected",
            error_message=bv_result.error,
            bitrate=dl.bitrate,
            sample_rate=dl.sample_rate,
            bit_depth=dl.bit_depth,
            is_vbr=dl.is_vbr,
            was_converted=dl.was_converted,
            original_filetype=dl.original_filetype,
            slskd_filetype=dl.slskd_filetype,
            slskd_bitrate=dl.slskd_bitrate,
            actual_filetype=dl.actual_filetype,
            actual_min_bitrate=dl.actual_min_bitrate,
            spectral_grade=dl.download_spectral.grade if dl.download_spectral else None,
            spectral_bitrate=(
                dl.download_spectral.bitrate_kbps if dl.download_spectral else None
            ),
            existing_min_bitrate=dl.existing_min_bitrate,
            existing_spectral_bitrate=(
                dl.current_spectral.bitrate_kbps if dl.current_spectral else None
            ),
            import_result=dl.import_result,
            validation_result=dl.validation_result,
        )

        # Denylist source users
        if usernames:
            for username in usernames:
                db.add_denylist(request_id, username, "beets validation rejected")

    def get_denylisted_users(self, album_record):
        """Get denylisted usernames for an album."""
        request_id = getattr(album_record, "db_request_id", None)
        if not request_id:
            return set()
        db = self._get_db()
        entries = db.get_denylisted_users(request_id)
        return {e["username"] for e in entries}

    def _populate_tracks(self, row):
        """Fetch tracks from MB API and store in DB."""
        mb_id = row.get("mb_release_id")
        if not mb_id:
            return []

        try:
            url = f"{MB_API_BASE}/release/{mb_id}?inc=recordings&fmt=json"
            req = urllib.request.Request(url)
            req.add_header("User-Agent", "soularr-db/1.0")
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read())
        except Exception:
            logger.warning(f"Failed to fetch tracks from MB API for {mb_id}")
            return []

        tracks = []
        for medium in data.get("media", []):
            disc = medium.get("position", 1)
            for track in medium.get("tracks", []):
                length_ms = track.get("length") or (track.get("recording") or {}).get("length")
                tracks.append({
                    "disc_number": disc,
                    "track_number": track.get("position", track.get("number", 0)),
                    "title": track.get("title", ""),
                    "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
                })

        if tracks:
            db = self._get_db()
            db.set_tracks(row["id"], tracks)

        return tracks

    def close(self):
        if self._db:
            self._db.close()
            self._db = None
