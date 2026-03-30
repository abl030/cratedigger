"""Beets library database queries.

Read-only access to the beets SQLite DB. Centralizes all scattered
sqlite3.connect() calls from soularr.py and import_one.py.

Usage:
    with BeetsDB() as db:
        info = db.get_album_info("mbid-here")
        if info:
            print(info.min_bitrate_kbps, info.is_cbr)
"""

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional

DEFAULT_BEETS_DB = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")


@dataclass
class AlbumInfo:
    """Query result from beets DB for a single album."""
    album_id: int
    track_count: int
    min_bitrate_kbps: int
    is_cbr: bool
    album_path: str  # directory containing the tracks


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

    def album_exists(self, mb_release_id: str) -> bool:
        """Check if an MBID is already in the beets library."""
        row = self._conn.execute(
            "SELECT 1 FROM albums WHERE mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        return row is not None

    def get_album_info(self, mb_release_id: str) -> Optional[AlbumInfo]:
        """Get full album info for quality gate / postflight verification.

        Returns None if the MBID isn't in beets or has no tracks.
        """
        album_row = self._conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        if not album_row:
            return None
        album_id: int = album_row[0]

        # Get bitrate stats (exclude 0-bitrate tracks)
        rows = self._conn.execute(
            "SELECT bitrate, path FROM items WHERE album_id = ? AND bitrate > 0",
            (album_id,)
        ).fetchall()
        if not rows:
            return None

        bitrates = [r[0] for r in rows]
        min_br = min(bitrates)
        is_cbr = len(set(bitrates)) == 1
        track_count = len(rows)

        # Album path = directory of first track
        first_path = self._decode_path(rows[0][1])
        album_path = os.path.dirname(first_path)

        return AlbumInfo(
            album_id=album_id,
            track_count=track_count,
            min_bitrate_kbps=int(min_br / 1000),
            is_cbr=is_cbr,
            album_path=album_path,
        )

    def get_min_bitrate(self, mb_release_id: str) -> Optional[int]:
        """Get min track bitrate (kbps) for an MBID. Returns None if not found."""
        album_row = self._conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        if not album_row:
            return None
        br_row = self._conn.execute(
            "SELECT MIN(bitrate) FROM items WHERE album_id = ? AND bitrate > 0",
            (album_row[0],)
        ).fetchone()
        if not br_row or not br_row[0]:
            return None
        return int(br_row[0] / 1000)

    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]:
        """Get all (item_id, path) pairs for an album. Returns empty list if not found."""
        album_row = self._conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        if not album_row:
            return []
        rows = self._conn.execute(
            "SELECT id, path FROM items WHERE album_id = ?", (album_row[0],)
        ).fetchall()
        return [(r[0], self._decode_path(r[1])) for r in rows]

    def get_album_path(self, mb_release_id: str) -> Optional[str]:
        """Get the directory path for an album's tracks. Returns None if not found."""
        row = self._conn.execute(
            "SELECT (SELECT path FROM items WHERE album_id = a.id LIMIT 1) "
            "FROM albums a WHERE a.mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        if not row or not row[0]:
            return None
        return os.path.dirname(self._decode_path(row[0]))
