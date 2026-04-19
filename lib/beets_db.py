"""Beets library database queries.

Read-only access to the beets SQLite DB. Centralizes all scattered
sqlite3.connect() calls from soularr.py and import_one.py.

Usage:
    with BeetsDB() as db:
        info = db.get_album_info("mbid-here", cfg.quality_ranks)
        if info:
            print(info.format, info.min_bitrate_kbps, info.avg_bitrate_kbps, info.is_cbr)
"""

import os
import sqlite3
import statistics
from dataclasses import dataclass
from typing import Literal, Optional, TYPE_CHECKING

from lib.quality import detect_release_source

if TYPE_CHECKING:
    from lib.quality import QualityRankConfig


@dataclass(frozen=True)
class ReleaseLocation:
    """Single seam for 'is this release on disk?' — see issue #121.

    The pipeline DB packs two ID kinds into one column (``mb_release_id``):
    MusicBrainz UUIDs and Discogs numeric strings. Beets stores them in
    up to two columns (``mb_albumid`` for UUIDs; ``discogs_albumid`` for
    new-layout Discogs; ``mb_albumid`` again for legacy Discogs imports
    predating the plugin patch). Every caller used to re-invent the
    dispatch — this type is the one place we answer the presence
    question, and callers pattern-match on ``.kind``.

    - ``kind="exact"``: beets holds the specific pressing keyed by
      ``release_id``. Quality / cleanup decisions may rely on this.
    - ``kind="fuzzy"``: no ID hit, but artist+album name fuzzy-matched
      something. Used ONLY for the user-facing 'in library' badge.
      Never attribute quality or trigger removes off a fuzzy hit —
      multiple pressings share titles, so we'd act on the wrong one.
    - ``kind="absent"``: nothing matches. ``album_id is None`` and
      ``selectors == ()``.

    ``selectors`` is the set of ``beet remove -d`` queries the ID
    could live under. Iterating every selector turns a selector-
    skipped remove into a harmless no-op instead of silently leaving
    the banned copy on disk — see ``BeetsDB.remove_selectors`` /
    ``web/routes/pipeline.py::post_pipeline_ban_source``.
    """
    kind: Literal["exact", "fuzzy", "absent"]
    album_id: int | None
    selectors: tuple[str, ...]

DEFAULT_BEETS_DB = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")


def _reduce_album_format(
    formats_on_disk: set[str],
    cfg: "QualityRankConfig",
) -> str:
    """Reduce a set of beets format strings to a single canonical one.

    Uses cfg.mixed_format_precedence (worst-first). If the album contains
    any codec listed in the precedence tuple, the first match wins. Otherwise
    returns the first format alphabetically (stable but not meaningful) or
    an empty string if the set is empty.
    """
    if not formats_on_disk:
        return ""
    # Normalized lookup: lowercase -> original.
    normalized: dict[str, str] = {f.lower(): f for f in formats_on_disk if f}
    for preferred in cfg.mixed_format_precedence:
        if preferred in normalized:
            return normalized[preferred]
    # No precedence match — pick a deterministic fallback.
    return sorted(formats_on_disk)[0]


@dataclass
class AlbumInfo:
    """Query result from beets DB for a single album.

    format:
        The canonical codec family for the album, derived from
        beets.items.format (e.g. "MP3", "FLAC", "Opus", "AAC"). When an album
        has multiple codecs on disk (rare — manually merged album), the
        worst-ranked codec wins per QualityRankConfig.mixed_format_precedence.
        This is the bare codec string for quality_rank() — the pipeline
        carries the richer "opus 128" / "mp3 v0" labels via ImportResult /
        album_requests.final_format when available. Defaults to empty string
        so tests constructing AlbumInfo directly (e.g. integration slices)
        don't have to pass every field. Production always sets it via
        get_album_info() → _reduce_album_format().
    min_bitrate_kbps / avg_bitrate_kbps / median_bitrate_kbps:
        Minimum, mean, and median per-track bitrate (kbps). The rank model's
        measurement_rank() picks between these based on
        QualityRankConfig.bitrate_metric. ``avg_bitrate_kbps`` and
        ``median_bitrate_kbps`` default to None for test-ergonomics —
        measurement_rank() falls back to min when the configured metric's
        field is None.
    """
    album_id: int
    track_count: int
    min_bitrate_kbps: int
    is_cbr: bool
    album_path: str
    avg_bitrate_kbps: Optional[int] = None
    median_bitrate_kbps: Optional[int] = None
    format: str = ""


class BeetsDB:
    """Read-only connection to the beets SQLite library database."""

    def __init__(self, db_path: str = DEFAULT_BEETS_DB) -> None:
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Beets DB not found: {db_path}")
        self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "BeetsDB":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @staticmethod
    def _decode_path(raw: object) -> str:
        """Decode a beets path (stored as bytes or str) to a string."""
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw)

    def locate(
        self,
        release_id: str,
        artist: Optional[str] = None,
        album: Optional[str] = None,
    ) -> ReleaseLocation:
        """Resolve a pipeline ``mb_release_id`` to a ``ReleaseLocation``.

        Single seam for 'is this release on disk?' (issue #121).
        See ``ReleaseLocation`` for the contract.

        Dispatch:
        - Numeric ID (Discogs shape): check both ``discogs_albumid``
          (new-layout) and ``mb_albumid`` (legacy) so pre-plugin-patch
          libraries still resolve. Selectors include BOTH columns so
          ``beet remove -d`` tries every layout.
        - UUID shape: check ``mb_albumid`` only. Selector is the
          single ``mb_albumid:<uuid>`` query.
        - When the ID misses AND the caller supplied artist+album,
          fall back to a fuzzy ``LIKE`` match. Fuzzy hits expose
          ``kind="fuzzy"`` and EMPTY selectors — callers must not
          turn a fuzzy hit into a ``beet remove -d``.
        """
        source = detect_release_source(release_id)
        numeric: int | None = None
        if source == "discogs":
            try:
                numeric = int(release_id)
            except ValueError:
                numeric = None

        album_id: Optional[int] = None
        if numeric is not None:
            row = self._conn.execute(
                "SELECT id FROM albums "
                "WHERE discogs_albumid = ? OR mb_albumid = ? "
                "LIMIT 1",
                (numeric, release_id),
            ).fetchone()
            if row:
                album_id = row[0]
        elif release_id:
            row = self._conn.execute(
                "SELECT id FROM albums WHERE mb_albumid = ?",
                (release_id,),
            ).fetchone()
            if row:
                album_id = row[0]

        if album_id is not None:
            if numeric is not None:
                selectors: tuple[str, ...] = (
                    f"discogs_albumid:{release_id}",
                    f"mb_albumid:{release_id}",
                )
            else:
                selectors = (f"mb_albumid:{release_id}",)
            return ReleaseLocation(
                kind="exact", album_id=album_id, selectors=selectors)

        # Fuzzy fallback — only when the caller supplies artist+album.
        # Returns the first match so the UI can still show 'in library',
        # but no selectors (we can't identify a specific pressing).
        if artist and album:
            fuzzy_id = self._fuzzy_album_id(artist, album)
            if fuzzy_id is not None:
                return ReleaseLocation(
                    kind="fuzzy", album_id=fuzzy_id, selectors=())

        return ReleaseLocation(kind="absent", album_id=None, selectors=())

    def _fuzzy_album_id(self, artist: str, album: str) -> Optional[int]:
        """Internal: first album whose artist+album fuzzily matches."""
        row = self._conn.execute(
            "SELECT id FROM albums "
            "WHERE albumartist LIKE ? COLLATE NOCASE "
            "  AND album LIKE ? COLLATE NOCASE "
            "LIMIT 1",
            (f"%{artist}%", f"%{album}%"),
        ).fetchone()
        return row[0] if row else None

    def _lookup_album_id(self, release_id: str) -> Optional[int]:
        """Legacy thin wrapper — kept for internal callers only.

        New code should call ``locate()`` and inspect ``.album_id``.
        Returns the exact-match album id (no fuzzy fallback) or None.
        """
        loc = self.locate(release_id)
        return loc.album_id if loc.kind == "exact" else None

    def album_exists(self, release_id: str) -> bool:
        """Check if a release is already in the beets library.

        Exact ID match only — no fuzzy fallback. For the 'in library'
        badge with fuzzy fallback, use ``locate(id, artist, album)``
        and check ``loc.kind in ("exact", "fuzzy")``.
        """
        return self.locate(release_id).kind == "exact"

    def get_album_info(
        self,
        mb_release_id: str,
        cfg: "QualityRankConfig",
    ) -> Optional[AlbumInfo]:
        """Get full album info for quality gate / postflight verification.

        Returns None if the release isn't in beets or has no tracks.

        Mixed-format albums (rare: manually merged albums with tracks in
        multiple codecs) are reduced to a single canonical format using
        ``cfg.mixed_format_precedence`` — the worst codec in that tuple wins
        so the rank stays conservative.
        """
        album_id = self._lookup_album_id(mb_release_id)
        if album_id is None:
            return None

        # Get bitrate + format stats (exclude 0-bitrate tracks)
        rows = self._conn.execute(
            "SELECT bitrate, path, format FROM items "
            "WHERE album_id = ? AND bitrate > 0",
            (album_id,)
        ).fetchall()
        if not rows:
            return None

        bitrates = [r[0] for r in rows]
        min_br = min(bitrates)
        avg_br = sum(bitrates) / len(bitrates)
        # statistics.median() returns the middle value (or the mean of the two
        # middle values for even counts) — robust to per-track outliers like
        # short interludes or hidden tracks at the album boundary. Computed in
        # Python because the beets DB is SQLite, which has no native median.
        median_br = statistics.median(bitrates)
        is_cbr = len(set(bitrates)) == 1
        track_count = len(rows)

        # Album path = directory of first track
        first_path = self._decode_path(rows[0][1])
        album_path = os.path.dirname(first_path)

        # Reduce multi-format albums via cfg.mixed_format_precedence.
        formats_on_disk = {r[2] for r in rows if r[2]}
        album_format = _reduce_album_format(formats_on_disk, cfg)

        return AlbumInfo(
            album_id=album_id,
            track_count=track_count,
            min_bitrate_kbps=int(min_br / 1000),
            avg_bitrate_kbps=int(avg_br / 1000),
            median_bitrate_kbps=int(median_br / 1000),
            is_cbr=is_cbr,
            album_path=album_path,
            format=album_format,
        )

    def get_min_bitrate(self, mb_release_id: str) -> Optional[int]:
        """Get min track bitrate (kbps) for a release. Returns None if not found."""
        album_id = self._lookup_album_id(mb_release_id)
        if album_id is None:
            return None
        br_row = self._conn.execute(
            "SELECT MIN(bitrate) FROM items WHERE album_id = ? AND bitrate > 0",
            (album_id,)
        ).fetchone()
        if not br_row or not br_row[0]:
            return None
        return int(br_row[0] / 1000)

    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]:
        """Get all (item_id, path) pairs for an album. Returns empty list if not found."""
        album_id = self._lookup_album_id(mb_release_id)
        if album_id is None:
            return []
        rows = self._conn.execute(
            "SELECT id, path FROM items WHERE album_id = ?", (album_id,)
        ).fetchall()
        return [(r[0], self._decode_path(r[1])) for r in rows]

    def get_album_path(self, mb_release_id: str) -> Optional[str]:
        """Get the directory path for an album's tracks. Returns None if not found."""
        album_id = self._lookup_album_id(mb_release_id)
        if album_id is None:
            return None
        row = self._conn.execute(
            "SELECT path FROM items WHERE album_id = ? LIMIT 1",
            (album_id,)
        ).fetchone()
        if not row or not row[0]:
            return None
        return os.path.dirname(self._decode_path(row[0]))

    # ── Web UI query methods ────────────────────────────────────────

    def check_mbids(self, mbids: list[str]) -> set[str]:
        """Return the subset of release IDs that exist in the beets library.

        Routes through ``locate`` per-ID (issue #121) so Discogs numerics
        resolve against ``discogs_albumid`` AND legacy ``mb_albumid``,
        matching the single-lookup contract. Before the seam, this
        method only queried ``mb_albumid`` — Discogs releases imported
        under ``discogs_albumid`` silently disappeared from every
        'already in library' check the browse routes make.
        """
        if not mbids:
            return set()
        return {m for m in mbids if self.locate(m).kind == "exact"}

    def check_mbids_detail(self, mbids: list[str]) -> dict[str, dict[str, object]]:
        """Batch lookup: release ID → {beets_tracks, beets_format, beets_bitrate, beets_samplerate, beets_bitdepth}.

        Accepts both MusicBrainz UUIDs (matched against ``albums.mb_albumid``)
        and Discogs numeric IDs (matched against ``albums.discogs_albumid``,
        which beets stores as an INTEGER). The pipeline DB packs both kinds
        of identifier into the ``mb_release_id`` column for compatibility,
        so consumers must be able to round-trip either one back to the
        right beets column.
        """
        if not mbids:
            return {}

        # Split by ID shape so each id queries the columns it could possibly
        # live in:
        # - UUIDs → ``mb_albumid`` only (UUID format can't land in
        #   ``discogs_albumid``, which is INTEGER).
        # - Numerics → both ``discogs_albumid`` (newer imports) and
        #   ``mb_albumid`` (legacy Discogs imports that predate
        #   ``discogs_albumid`` being populated). Skipping ``mb_albumid``
        #   for numerics would silently drop real on-disk matches for
        #   older libraries — see lib/artist_compare.py and
        #   docs/webui-primer.md for the duality contract.
        # - Anything else falls through to ``mb_albumid`` (synthetic
        #   fixture strings, manual edits).
        from lib.quality import detect_release_source
        mb_ids: list[str] = []
        discogs_ids: list[int] = []
        for raw in mbids:
            source = detect_release_source(raw)
            if source == "discogs":
                try:
                    discogs_ids.append(int(raw))
                except ValueError:
                    continue
                # Also check mb_albumid as the TEXT value; covers legacy
                # Discogs imports that stored the numeric ID there.
                mb_ids.append(raw)
            else:
                mb_ids.append(raw)

        result: dict[str, dict[str, object]] = {}

        def _add_rows(rows: list[tuple[object, ...]]) -> None:
            for r in rows:
                if r[0] is None:
                    continue
                bitrate = r[3]
                kbps = int(bitrate / 1000) if isinstance(bitrate, (int, float)) else None
                result[str(r[0])] = {
                    "beets_tracks": r[1],
                    "beets_format": r[2],
                    "beets_bitrate": kbps,
                    "beets_samplerate": r[4],
                    "beets_bitdepth": r[5],
                }

        detail_cols = (
            "  (SELECT COUNT(*) FROM items WHERE album_id = a.id) AS track_count, "
            "  (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) AS formats, "
            "  (SELECT MIN(i.bitrate) FROM items i WHERE i.album_id = a.id) AS min_bitrate, "
            "  (SELECT MIN(i.samplerate) FROM items i WHERE i.album_id = a.id) AS samplerate, "
            "  (SELECT MAX(i.bitdepth) FROM items i WHERE i.album_id = a.id) AS bitdepth "
        )

        if mb_ids:
            ph = ",".join("?" for _ in mb_ids)
            _add_rows(self._conn.execute(
                f"SELECT a.mb_albumid, {detail_cols}"
                f"FROM albums a WHERE a.mb_albumid IN ({ph})",
                mb_ids,
            ).fetchall())

        if discogs_ids:
            ph = ",".join("?" for _ in discogs_ids)
            _add_rows(self._conn.execute(
                f"SELECT a.discogs_albumid, {detail_cols}"
                f"FROM albums a WHERE a.discogs_albumid IN ({ph})",
                discogs_ids,
            ).fetchall())

        return result

    def search_albums(self, query: str, limit: int = 100) -> list[dict[str, object]]:
        """Search albums by artist or album name (LIKE, case-insensitive)."""
        rows = self._conn.execute(
            self._ALBUM_SELECT +
            "WHERE a.albumartist LIKE ? COLLATE NOCASE OR a.album LIKE ? COLLATE NOCASE "
            "ORDER BY a.albumartist, a.year, a.album LIMIT ?",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()
        return [self._album_row_to_dict(r) for r in rows]

    def get_recent(self, limit: int = 50) -> list[dict[str, object]]:
        """Get most recently added albums."""
        rows = self._conn.execute(
            self._ALBUM_SELECT + "ORDER BY a.added DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._album_row_to_dict(r) for r in rows]

    def get_album_detail(self, album_id: int) -> Optional[dict[str, object]]:
        """Get full album metadata + track list. Returns None if not found."""
        album = self._conn.execute(
            "SELECT id, album, albumartist, year, mb_albumid, albumtype, "
            "       label, country, artpath, added "
            "FROM albums WHERE id = ?", (album_id,)
        ).fetchone()
        if not album:
            return None
        items = self._conn.execute(
            "SELECT id, title, artist, track, disc, length, format, "
            "       bitrate, samplerate, bitdepth, path "
            "FROM items WHERE album_id = ? ORDER BY disc, track", (album_id,)
        ).fetchall()
        tracks = [{
            "id": i[0], "title": i[1], "artist": i[2], "track": i[3],
            "disc": i[4], "length": i[5], "format": i[6],
            "bitrate": i[7], "samplerate": i[8], "bitdepth": i[9],
            "path": self._decode_path(i[10]) if i[10] else None,
        } for i in items]
        album_path = os.path.dirname(tracks[0]["path"]) if tracks and tracks[0]["path"] else None
        return {
            "id": album[0], "album": album[1], "artist": album[2],
            "year": album[3], "mb_albumid": album[4], "type": album[5],
            "label": album[6], "country": album[7],
            "artpath": self._decode_path(album[8]) if album[8] else None,
            "added": album[9], "tracks": tracks, "path": album_path,
        }

    _ALBUM_SELECT = (
        "SELECT a.id, a.album, a.albumartist, a.year, a.mb_albumid, "
        "       a.albumtype, a.label, a.country, "
        "       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count, "
        "       (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) as formats, "
        "       a.added, a.mb_releasegroupid, a.release_group_title, "
        "       (SELECT MIN(i.bitrate) FROM items i WHERE i.album_id = a.id) as min_bitrate, "
        "       a.discogs_albumid "
        "FROM albums a "
    )

    def get_albums_by_artist(self, name: str, mbid: str = "") -> list[dict[str, object]]:
        """Get all albums by an artist. Matches by MB artist ID (if given) or name.

        When mbid is provided, matches on mb_albumartistid exact or mb_albumartistids LIKE,
        plus a name fallback for Discogs-only albums (no MB UUID in mb_albumartistid).
        """
        if mbid:
            rows = self._conn.execute(
                self._ALBUM_SELECT +
                "WHERE a.mb_albumartistid = ? OR a.mb_albumartistids LIKE ? "
                "  OR (a.albumartist LIKE ? COLLATE NOCASE "
                "      AND (a.mb_albumartistid IS NULL OR a.mb_albumartistid = '' "
                "           OR a.mb_albumartistid NOT LIKE '%-%')) "
                "ORDER BY a.year, a.album",
                (mbid, f"%{mbid}%", f"%{name}%"),
            ).fetchall()
        else:
            rows = self._conn.execute(
                self._ALBUM_SELECT +
                "WHERE a.albumartist LIKE ? COLLATE NOCASE "
                "ORDER BY a.year, a.album",
                (f"%{name}%",),
            ).fetchall()
        return [self._album_row_to_dict(r) for r in rows]

    def get_tracks_by_mb_release_id(self, mbid: str) -> Optional[list[dict[str, object]]]:
        """Get all tracks for an album by release ID.

        Routes through ``locate`` (issue #121) so Discogs numerics in
        ``discogs_albumid`` resolve the same way ``album_exists`` does —
        otherwise the browse-tab 'view release' endpoint would render a
        release as in-library but fail to show its track list.
        """
        album_id = self._lookup_album_id(mbid)
        if album_id is None:
            return None
        items = self._conn.execute(
            "SELECT title, track, disc, length, format, bitrate, "
            "       samplerate, bitdepth "
            "FROM items WHERE album_id = ? ORDER BY disc, track",
            (album_id,),
        ).fetchall()
        return [{
            "title": i[0], "track": i[1], "disc": i[2],
            "length": i[3], "format": i[4], "bitrate": i[5],
            "samplerate": i[6], "bitdepth": i[7],
        } for i in items]

    def get_album_ids_by_mbids(self, mbids: list[str]) -> dict[str, int]:
        """Map MBIDs to beets album IDs. Returns {mbid: album_id}."""
        if not mbids:
            return {}
        ph = ",".join("?" for _ in mbids)
        rows = self._conn.execute(
            f"SELECT mb_albumid, id FROM albums WHERE mb_albumid IN ({ph})",
            mbids,
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    @staticmethod
    def delete_album(db_path: str, album_id: int) -> tuple[str, str, list[str]]:
        """Delete an album from beets DB (read-write). Returns (album, artist, file_paths).

        Opens a separate writable connection — does not use the read-only instance conn.
        Raises ValueError if album not found.
        """
        conn = sqlite3.connect(db_path)
        try:
            album_row = conn.execute(
                "SELECT album, albumartist FROM albums WHERE id = ?", (album_id,)
            ).fetchone()
            if not album_row:
                raise ValueError(f"Album {album_id} not found")
            items = conn.execute(
                "SELECT path FROM items WHERE album_id = ?", (album_id,)
            ).fetchall()
            file_paths = [
                r[0].decode("utf-8", errors="replace") if isinstance(r[0], bytes) else r[0]
                for r in items
            ]
            conn.execute("DELETE FROM items WHERE album_id = ?", (album_id,))
            conn.execute("DELETE FROM albums WHERE id = ?", (album_id,))
            conn.commit()
            return album_row[0], album_row[1], file_paths
        finally:
            conn.close()

    def find_by_artist_album(self, artist: str, album: str) -> Optional[int]:
        """Find track count by artist+album name. Returns None if not found."""
        row = self._conn.execute(
            "SELECT a.id FROM albums a "
            "WHERE a.albumartist LIKE ? COLLATE NOCASE AND a.album LIKE ? COLLATE NOCASE "
            "LIMIT 1",
            (f"%{artist}%", f"%{album}%"),
        ).fetchone()
        if not row:
            return None
        count = self._conn.execute(
            "SELECT COUNT(*) FROM items WHERE album_id = ?", (row[0],)
        ).fetchone()
        return count[0] if count else None

    def get_avg_bitrate_kbps(self, mb_release_id: str) -> Optional[int]:
        """Get average track bitrate (kbps) for a release. None if not found.

        Routes through ``locate`` (issue #121) so Discogs numerics
        resolve the same way every other postflight lookup does.
        """
        album_id = self._lookup_album_id(mb_release_id)
        if album_id is None:
            return None
        avg_row = self._conn.execute(
            "SELECT CAST(AVG(bitrate) AS INTEGER) FROM items "
            "WHERE album_id = ? AND bitrate > 0",
            (album_id,),
        ).fetchone()
        if not avg_row or not avg_row[0]:
            return None
        return int(avg_row[0] / 1000)

    @staticmethod
    def _album_row_to_dict(r: tuple[object, ...]) -> dict[str, object]:
        """Convert a standard album query row to dict.

        Column order must match _ALBUM_SELECT (indices 0-14).
        Field names here are the API contract — the frontend depends on them.
        """
        mb_id = r[4] or ""
        source = detect_release_source(str(mb_id))
        if source == "unknown" and bool(r[14]):
            source = "discogs"
        return {
            "id": r[0], "album": r[1], "artist": r[2], "year": r[3],
            "mb_albumid": r[4], "type": r[5], "label": r[6],
            "country": r[7], "track_count": r[8], "formats": r[9],
            "added": r[10], "mb_releasegroupid": r[11],
            "release_group_title": r[12], "min_bitrate": r[13],
            "source": source,
        }
