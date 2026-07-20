"""Beets library database queries.

Read-only access to the beets SQLite DB. Centralizes all scattered
sqlite3.connect() calls from cratedigger.py and import_one.py.

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
from typing import Literal, Optional, TYPE_CHECKING, TypeAlias

from lib.release_identity import (
    ReleaseIdentity,
    detect_release_source,
    frontend_release_id,
    normalize_release_id,
)

if TYPE_CHECKING:
    from lib.config import CratediggerConfig
    from lib.quality import QualityRankConfig


@dataclass(frozen=True)
class ReleaseLocation:
    """Single seam for 'is this release on disk?' — see issues #121 / #123.

    The pipeline DB packs two ID kinds into one column (``mb_release_id``):
    MusicBrainz UUIDs and Discogs numeric strings. Beets stores them in
    up to two columns (``mb_albumid`` for UUIDs; ``discogs_albumid`` for
    new-layout Discogs; ``mb_albumid`` again for legacy Discogs imports
    predating the plugin patch). Every caller used to re-invent the
    dispatch — this type is the one place we answer the presence
    question, and callers pattern-match on ``.kind``.

    - ``kind="exact"``: beets holds the specific pressing keyed by
      ``release_id``. Quality / cleanup decisions may rely on this.
    - ``kind="absent"``: nothing matches. ``album_id is None`` and
      ``selectors == ()``.
    - ``kind="ambiguous"``: multiple exact rows or an unusable item topology.
      ``album_id is None`` so legacy callers fail closed.

    Issue #123 removed the old ``fuzzy`` state. The artist+album fallback conflated
    identity with presence — a sibling pressing title-match would
    silently attribute another release's quality fields to the badge.
    The later ``ambiguous`` state represents exact-ID rows whose cardinality
    or item topology cannot authorize one current album; it never reintroduces
    metadata matching.

    ``selectors`` is the set of ``beet remove -d`` queries the ID
    could live under. Iterating every selector turns a selector-
    skipped remove into a harmless no-op instead of silently leaving
    the banned copy on disk — see
    ``web/routes/pipeline.py::post_pipeline_ban_source``.
    """
    kind: Literal["exact", "absent", "ambiguous"]
    album_id: int | None
    selectors: tuple[str, ...]


@dataclass(frozen=True)
class CurrentBeetsItem:
    """One item from the resolver's coherent current-library snapshot."""

    id: int
    path: str
    title: str | None = None
    track: int | None = None
    disc: int | None = None
    length: float | None = None
    format: str | None = None
    bitrate: int | None = None
    samplerate: int | None = None
    bitdepth: int | None = None


@dataclass(frozen=True)
class CurrentBeetsUnique:
    """Exactly one usable current Beets album for an exact release."""

    identity: ReleaseIdentity
    album_id: int
    album_path: str
    items: tuple[CurrentBeetsItem, ...]
    selectors: tuple[str, ...]


@dataclass(frozen=True)
class CurrentBeetsMissing:
    """No current Beets album has the exact release identity."""

    identity: ReleaseIdentity


CurrentBeetsAmbiguityReason: TypeAlias = Literal[
    "multiple_matches",
    "conflicting_identity",
    "empty_topology",
    "split_topology",
    "invalid_path",
    "unresolved_relative_path",
]


@dataclass(frozen=True)
class CurrentBeetsAmbiguous:
    """Exact membership exists but cannot authorize one current album path."""

    identity: ReleaseIdentity
    album_ids: tuple[int, ...]
    reason: CurrentBeetsAmbiguityReason


CurrentBeetsResolution: TypeAlias = (
    CurrentBeetsUnique | CurrentBeetsMissing | CurrentBeetsAmbiguous
)
_RawCurrentItem: TypeAlias = tuple[
    int,
    object,
    object,
    object,
    object,
    object,
    object,
    object,
    object,
    object,
]

def _resolve_library_path(path: str, library_root: str) -> str:
    """Anchor a Beets-relative path to its configured library root."""
    if library_root and not os.path.isabs(path):
        return os.path.join(library_root, path)
    return path


def _lookup_identity(raw: object | None) -> ReleaseIdentity | None:
    """Translate a legacy string lookup into an exact typed identity.

    Runtime requests are UUIDs or Discogs numerics. Keeping nonempty malformed
    values on the MusicBrainz column path makes audit/dev callers able to
    inspect bad historical rows without introducing any metadata fallback.
    """

    identity = ReleaseIdentity.from_id(raw)
    if identity is not None:
        return identity
    normalized = normalize_release_id(raw)
    if not normalized:
        return None
    return ReleaseIdentity(source="musicbrainz", release_id=normalized)


def _lookup_identities(
    release_ids: list[str],
) -> dict[str, ReleaseIdentity]:
    """Canonical input-key map shared by all batch resolver adapters."""

    identities: dict[str, ReleaseIdentity] = {}
    for release_id in release_ids:
        identity = _lookup_identity(release_id)
        if identity is not None:
            identities.setdefault(identity.release_id, identity)
    return identities


def open_beets_db(
    config: "CratediggerConfig | None" = None,
    *,
    db_path: str | None = None,
    library_root: str | None = None,
) -> "BeetsDB":
    """Open one inseparable Beets database/root pair.

    Production omits explicit paths and reads the module-rendered runtime pair.
    Development/operator overrides must supply both values together.
    """

    if config is not None and (db_path is not None or library_root is not None):
        raise ValueError("config and explicit Beets paths are mutually exclusive")
    if (db_path is None) != (library_root is None):
        raise ValueError(
            "Beets DB and library root overrides must be supplied together"
        )
    if db_path is not None and library_root is not None:
        return BeetsDB(db_path, library_root=library_root)
    if config is None:
        from lib.config import read_runtime_config

        config = read_runtime_config()
    return BeetsDB(
        config.beets_library_db,
        library_root=config.beets_directory,
    )


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

    def __init__(
        self,
        db_path: str | None = None,
        *,
        library_root: str | None = None,
    ) -> None:
        """Open the library DB.

        ``library_root`` is the absolute filesystem path that beets'
        ``items.path`` values are stored relative to (matches the
        ``directory:`` setting in the beets config). A unique current release
        always exposes absolute paths. A relative stored path with no root is
        an explicit ambiguous result and every legacy lookup fails closed.
        """
        if db_path is None:
            from lib.config import read_runtime_config

            runtime_config = read_runtime_config()
            db_path = runtime_config.beets_library_db
            if library_root is None:
                library_root = runtime_config.beets_directory
        if library_root is None:
            library_root = ""
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Beets DB not found: {db_path}")
        self._db_path = db_path
        self._conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        self._library_root = library_root

    def close(self) -> None:
        self._conn.close()

    @property
    def library_db_path(self) -> str:
        """Exact SQLite path represented by this preflight handle."""
        return self._db_path

    @property
    def library_root(self) -> str:
        """Exact filesystem root used to resolve paths from this handle."""
        return self._library_root

    def __enter__(self) -> "BeetsDB":
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @staticmethod
    def _decode_path(raw: object) -> str:
        """Decode a beets path (stored as bytes or str) to a string."""
        if isinstance(raw, bytes):
            return os.fsdecode(raw)
        return str(raw)

    def _resolve_path(self, raw: object) -> str:
        """Decode a stored path and apply the one library-root policy."""
        return _resolve_library_path(
            self._decode_path(raw),
            self._library_root,
        )

    @staticmethod
    def _selectors_for(identity: ReleaseIdentity) -> tuple[str, ...]:
        if identity.source == "discogs":
            return (
                f"discogs_albumid:{identity.release_id}",
                f"mb_albumid:{identity.release_id}",
            )
        return (f"mb_albumid:{identity.release_id}",)

    def _matching_album_ids(self, identity: ReleaseIdentity) -> tuple[int, ...]:
        """Enumerate every album primary key for one exact identity."""

        if identity.source == "discogs":
            rows = self._conn.execute(
                "SELECT id FROM albums "
                "WHERE discogs_albumid = ? OR mb_albumid = ? "
                "ORDER BY id",
                (int(identity.release_id), identity.release_id),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id FROM albums WHERE mb_albumid = ? ORDER BY id",
                (identity.release_id,),
            ).fetchall()
        return tuple(int(row[0]) for row in rows)

    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution:
        """Resolve one exact identity to unique, missing, or ambiguous.

        This is the sole current-library membership/path authority. It reads
        every matching album row, accepts both Discogs storage generations,
        and only returns ``unique`` when every item has an absolute path in
        one directory.
        """

        return self.resolve_current_releases([identity])[identity]

    def resolve_current_releases(
        self,
        identities: list[ReleaseIdentity],
    ) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
        """Batch the same exact resolver contract without cardinality loss."""

        unique_identities = tuple(dict.fromkeys(identities))
        album_ids_by_identity: dict[ReleaseIdentity, set[int]] = {
            identity: set() for identity in unique_identities
        }
        mb_by_release_id = {
            identity.release_id: identity
            for identity in unique_identities
        }
        discogs_by_release_id = {
            identity.release_id: identity
            for identity in unique_identities
            if identity.source == "discogs"
        }

        conditions: list[str] = []
        parameters: list[object] = []
        if mb_by_release_id:
            mb_values = tuple(mb_by_release_id)
            conditions.append(
                f"a.mb_albumid IN ({','.join('?' for _ in mb_values)})"
            )
            parameters.extend(mb_values)
        if discogs_by_release_id:
            discogs_values = tuple(
                int(release_id) for release_id in discogs_by_release_id
            )
            conditions.append(
                f"a.discogs_albumid IN "
                f"({','.join('?' for _ in discogs_values)})"
            )
            parameters.extend(discogs_values)

        # One joined SELECT is the snapshot boundary. Beets can move an album
        # concurrently; separate album and item queries could otherwise return
        # a cardinality from before the move and paths from after it.
        item_rows_by_album_id: dict[int, list[_RawCurrentItem]] = {}
        conflicting_album_ids: set[int] = set()
        if conditions:
            rows = self._conn.execute(
                "SELECT a.id, a.mb_albumid, a.discogs_albumid, "
                "i.id, i.path, i.title, i.track, i.disc, i.length, i.format, "
                "i.bitrate, i.samplerate, i.bitdepth "
                "FROM albums a LEFT JOIN items i ON i.album_id = a.id "
                f"WHERE {' OR '.join(conditions)} ORDER BY a.id, i.id",
                parameters,
            ).fetchall()
            for (
                raw_album_id,
                raw_mb_release_id,
                raw_discogs_release_id,
                raw_item_id,
                raw_path,
                raw_title,
                raw_track,
                raw_disc,
                raw_length,
                raw_format,
                raw_bitrate,
                raw_samplerate,
                raw_bitdepth,
            ) in rows:
                album_id = int(raw_album_id)
                item_rows_by_album_id.setdefault(album_id, [])

                mb_release_id = normalize_release_id(raw_mb_release_id)
                mb_identity = mb_by_release_id.get(mb_release_id)
                if mb_identity is not None:
                    album_ids_by_identity[mb_identity].add(album_id)

                discogs_release_id = normalize_release_id(
                    raw_discogs_release_id,
                )
                discogs_identity = discogs_by_release_id.get(
                    discogs_release_id,
                )
                if discogs_identity is not None:
                    album_ids_by_identity[discogs_identity].add(album_id)

                # UUID + numeric Discogs is a valid cross-source identity pair.
                # Two different numeric values claim two Discogs pressings for
                # one album row and cannot authorize either destructive path.
                if (
                    detect_release_source(mb_release_id) == "discogs"
                    and discogs_release_id
                    and mb_release_id != discogs_release_id
                ):
                    conflicting_album_ids.add(album_id)

                if raw_item_id is not None:
                    item_rows_by_album_id[album_id].append(
                        (
                            int(raw_item_id), raw_path, raw_title, raw_track,
                            raw_disc, raw_length, raw_format, raw_bitrate,
                            raw_samplerate, raw_bitdepth,
                        ),
                    )

        resolutions: dict[ReleaseIdentity, CurrentBeetsResolution] = {}
        for identity in unique_identities:
            album_ids = tuple(sorted(album_ids_by_identity[identity]))
            if not album_ids:
                resolutions[identity] = CurrentBeetsMissing(identity=identity)
                continue
            if any(
                album_id in conflicting_album_ids for album_id in album_ids
            ):
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="conflicting_identity",
                )
                continue
            if len(album_ids) != 1:
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="multiple_matches",
                )
                continue

            album_id = album_ids[0]
            rows = item_rows_by_album_id[album_id]
            if not rows:
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="empty_topology",
                )
                continue

            items: list[CurrentBeetsItem] = []
            directories: set[str] = set()
            unresolved_relative_path = False
            invalid_path = False
            for (
                item_id,
                raw_path,
                raw_title,
                raw_track,
                raw_disc,
                raw_length,
                raw_format,
                raw_bitrate,
                raw_samplerate,
                raw_bitdepth,
            ) in rows:
                if raw_path is None:
                    invalid_path = True
                    break
                decoded = self._decode_path(raw_path)
                if not decoded or "\x00" in decoded:
                    invalid_path = True
                    break
                if not os.path.isabs(decoded):
                    if not self._library_root:
                        unresolved_relative_path = True
                        break
                    root = os.path.abspath(self._library_root)
                    decoded = os.path.abspath(os.path.join(root, decoded))
                    if os.path.commonpath((root, decoded)) != root:
                        invalid_path = True
                        break
                absolute = os.path.abspath(decoded)
                items.append(CurrentBeetsItem(
                    id=int(item_id),
                    path=absolute,
                    title=(str(raw_title) if raw_title is not None else None),
                    track=(int(raw_track) if isinstance(
                        raw_track, (int, float)) else None),
                    disc=(int(raw_disc) if isinstance(
                        raw_disc, (int, float)) else None),
                    length=(float(raw_length) if isinstance(
                        raw_length, (int, float)) else None),
                    format=(str(raw_format) if raw_format is not None else None),
                    bitrate=(int(raw_bitrate) if isinstance(
                        raw_bitrate, (int, float)) else None),
                    samplerate=(int(raw_samplerate) if isinstance(
                        raw_samplerate, (int, float)) else None),
                    bitdepth=(int(raw_bitdepth) if isinstance(
                        raw_bitdepth, (int, float)) else None),
                ))
                directories.add(os.path.dirname(absolute))
            if invalid_path:
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="invalid_path",
                )
            elif unresolved_relative_path:
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="unresolved_relative_path",
                )
            elif len(directories) != 1:
                resolutions[identity] = CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=album_ids,
                    reason="split_topology",
                )
            else:
                resolutions[identity] = CurrentBeetsUnique(
                    identity=identity,
                    album_id=album_id,
                    album_path=next(iter(directories)),
                    items=tuple(items),
                    selectors=self._selectors_for(identity),
                )
        return resolutions

    def locate(self, release_id: str) -> ReleaseLocation:
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
        - Miss on both paths → ``kind="absent"``.

        Issue #123: the artist/album fuzzy fallback was removed. It
        conflated identity with presence and silently attributed stale
        quality fields from sibling pressings to the badge. The honest
        UI for a legacy untagged album is 'not in library' — re-tag it
        or add the release to the pipeline.
        """
        identity = _lookup_identity(release_id)
        if identity is None:
            return ReleaseLocation(kind="absent", album_id=None, selectors=())
        result = self.resolve_current_release(identity)
        if isinstance(result, CurrentBeetsUnique):
            return ReleaseLocation(
                kind="exact",
                album_id=result.album_id,
                selectors=result.selectors,
            )
        if isinstance(result, CurrentBeetsAmbiguous):
            return ReleaseLocation(
                kind="ambiguous",
                album_id=None,
                selectors=self._selectors_for(identity),
            )
        return ReleaseLocation(kind="absent", album_id=None, selectors=())

    def get_all_album_ids_for_release(self, release_id: str) -> list[int]:
        """Return every album id whose mb/discogs id matches ``release_id``.

        This raw cardinality helper enumerates *every* row — needed so
        post-import stale
        cleanup can detect the split-brain "multiple same-MBID rows
        already exist" state and fail-fast rather than delete just
        one while the others survive (Codex PR #131 round 3 P2).

        Same dispatch as ``locate()``:
        - Numeric ID → match on ``discogs_albumid`` OR ``mb_albumid``
          (dual Discogs layout).
        - UUID → match on ``mb_albumid`` only.
        - Empty → ``[]``.

        Returns an empty list if the release is absent.
        """
        identity = _lookup_identity(release_id)
        if identity is None:
            return []
        return list(self._matching_album_ids(identity))

    def _batch_lookup_album_ids(
        self, release_ids: list[str]
    ) -> dict[str, int]:
        """Batched version of ``locate(id).album_id`` for exact hits only.

        Single source of truth for 'which of these release IDs has an
        usable unique beets row, and what's its album_id?'. Two identity
        ``IN (...)`` queries enumerate both storage generations and one item
        query validates every candidate topology. Returns a dict keyed by the
        canonical normalized release ID; missing and ambiguous identities are
        omitted.

        Used by ``check_mbids`` and ``get_album_ids_by_mbids`` so they
        stay in sync without either falling into an N+1 pattern — the
        paired-consistency concern Codex round 1 + round 2 kept circling
        (issue #121).
        """
        identities_by_release_id = _lookup_identities(release_ids)
        resolutions = self.resolve_current_releases(
            list(identities_by_release_id.values()),
        )
        result: dict[str, int] = {}
        for release_id, identity in identities_by_release_id.items():
            resolution = resolutions[identity]
            if isinstance(resolution, CurrentBeetsUnique):
                result[release_id] = resolution.album_id

        return result

    def _resolve_unique(self, release_id: str) -> CurrentBeetsUnique | None:
        """Resolve one legacy string without discarding its snapshot."""

        identity = _lookup_identity(release_id)
        if identity is None:
            return None
        result = self.resolve_current_release(identity)
        return result if isinstance(result, CurrentBeetsUnique) else None

    def album_exists(self, release_id: str) -> bool:
        """Check if a release is already in the beets library.

        Exact ID match. Issue #123 collapsed the 'in library' signal
        to this single predicate — the fuzzy artist+album fallback was
        deleted because it attributed sibling pressings' quality to
        the badge.
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
        current = self._resolve_unique(mb_release_id)
        if current is None:
            return None

        measured = [
            (item, bitrate)
            for item in current.items
            if (bitrate := item.bitrate) is not None and bitrate > 0
        ]
        if not measured:
            return None

        numeric_bitrates = [bitrate for _item, bitrate in measured]
        min_br = min(numeric_bitrates)
        avg_br = sum(numeric_bitrates) / len(numeric_bitrates)
        # statistics.median() returns the middle value (or the mean of the two
        # middle values for even counts) — robust to per-track outliers like
        # short interludes or hidden tracks at the album boundary. Computed in
        # Python because the beets DB is SQLite, which has no native median.
        median_br = statistics.median(numeric_bitrates)
        is_cbr = len(set(numeric_bitrates)) == 1
        track_count = len(measured)

        # Reduce multi-format albums via cfg.mixed_format_precedence.
        formats_on_disk = {
            item.format for item, _bitrate in measured if item.format
        }
        album_format = _reduce_album_format(formats_on_disk, cfg)

        return AlbumInfo(
            album_id=current.album_id,
            track_count=track_count,
            min_bitrate_kbps=int(min_br / 1000),
            avg_bitrate_kbps=int(avg_br / 1000),
            median_bitrate_kbps=int(median_br / 1000),
            is_cbr=is_cbr,
            album_path=current.album_path,
            format=album_format,
        )

    def get_min_bitrate(self, mb_release_id: str) -> Optional[int]:
        """Get min track bitrate (kbps) for a release. Returns None if not found."""
        current = self._resolve_unique(mb_release_id)
        if current is None:
            return None
        bitrates = [
            item.bitrate for item in current.items
            if item.bitrate is not None and item.bitrate > 0
        ]
        if not bitrates:
            return None
        return int(min(bitrates) / 1000)

    def get_item_paths(self, mb_release_id: str) -> list[tuple[int, str]]:
        """Get all (item_id, path) pairs for an album. Returns empty list if not found."""
        current = self._resolve_unique(mb_release_id)
        if current is None:
            return []
        return [(item.id, item.path) for item in current.items]

    # ── Web UI query methods ────────────────────────────────────────

    def check_mbids(self, mbids: list[str]) -> set[str]:
        """Return the subset of release IDs that exist in the beets library.

        Routes through ``_batch_lookup_album_ids`` (issue #121) so
        Discogs numerics resolve against ``discogs_albumid`` AND
        legacy ``mb_albumid``, matching the single-lookup contract.
        Two batched ``IN (...)`` queries — no N+1 per call site.
        Before the seam, this method only queried ``mb_albumid`` —
        Discogs releases imported under ``discogs_albumid`` silently
        disappeared from every 'already in library' check the browse
        routes make.
        """
        return set(self._batch_lookup_album_ids(mbids).keys())

    def list_release_identities(self) -> list[dict[str, object]]:
        """Return beets album identity columns for inverse coverage views."""
        rows = self._conn.execute(
            "SELECT id, album, albumartist, mb_albumid, discogs_albumid "
            "FROM albums "
            "WHERE NULLIF(mb_albumid, '') IS NOT NULL "
            "OR (discogs_albumid IS NOT NULL AND discogs_albumid != 0) "
            "ORDER BY id ASC"
        ).fetchall()
        return [
            {
                "id": r[0],
                "album": r[1],
                "albumartist": r[2],
                "mb_albumid": r[3],
                "discogs_albumid": r[4],
            }
            for r in rows
        ]

    def check_mbids_detail(self, mbids: list[str]) -> dict[str, dict[str, object]]:
        """Batch lookup of release IDs and current beets audio aggregates.

        ``beets_bitrate`` remains the minimum-track floor in kbps.
        ``beets_avg_bitrate`` is the average across positive track bitrates
        and is the current-state label/rank signal.

        Accepts both MusicBrainz UUIDs (matched against ``albums.mb_albumid``)
        and Discogs numeric IDs (matched against ``albums.discogs_albumid``,
        which beets stores as an INTEGER). The pipeline DB packs both kinds
        of identifier into the ``mb_release_id`` column for compatibility,
        so consumers must be able to round-trip either one back to the
        right beets column.
        """
        if not mbids:
            return {}

        identities_by_release_id = _lookup_identities(mbids)
        resolutions = self.resolve_current_releases(
            list(identities_by_release_id.values()),
        )
        result: dict[str, dict[str, object]] = {}
        for release_id, identity in identities_by_release_id.items():
            current = resolutions[identity]
            if not isinstance(current, CurrentBeetsUnique):
                continue
            formats = tuple(dict.fromkeys(
                item.format for item in current.items if item.format
            ))
            bitrates = [
                item.bitrate for item in current.items
                if item.bitrate is not None and item.bitrate > 0
            ]
            samplerates = [
                item.samplerate for item in current.items
                if item.samplerate is not None
            ]
            bitdepths = [
                item.bitdepth for item in current.items
                if item.bitdepth is not None
            ]
            result[release_id] = {
                "beets_tracks": len(current.items),
                "beets_format": ",".join(formats) if formats else None,
                "beets_bitrate": (
                    int(min(bitrates) / 1000) if bitrates else None
                ),
                "beets_avg_bitrate": (
                    int(sum(bitrates) / len(bitrates) / 1000)
                    if bitrates else None
                ),
                "beets_samplerate": min(samplerates) if samplerates else None,
                "beets_bitdepth": max(bitdepths) if bitdepths else None,
            }
        return result

    def get_album_detail(self, album_id: int) -> Optional[dict[str, object]]:
        """Get full album metadata + track list. Returns None if not found."""
        album = self._conn.execute(
            "SELECT id, album, albumartist, year, mb_albumid, albumtype, "
            "       label, country, artpath, added, discogs_albumid "
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
            "path": self._resolve_path(i[10]) if i[10] else None,
        } for i in items]
        album_path = os.path.dirname(tracks[0]["path"]) if tracks and tracks[0]["path"] else None
        identity = ReleaseIdentity.from_fields(album[4], album[10])
        mb_release_id = normalize_release_id(album[4]) or None
        discogs_release_id = normalize_release_id(album[10]) or None
        return {
            "id": album[0], "album": album[1], "artist": album[2],
            "year": album[3],
            # Preserve both server-owned columns, including malformed
            # nonempty values. Collapsing or dropping either field hides an
            # authority ambiguity from destructive callers.
            "mb_albumid": mb_release_id,
            "discogs_albumid": discogs_release_id,
            "type": album[5],
            "label": album[6], "country": album[7],
            "artpath": self._resolve_path(album[8]) if album[8] else None,
            "added": album[9], "tracks": tracks, "path": album_path,
            "source": identity.source if identity else "unknown",
        }

    def album_and_items_absent(self, album_id: int) -> bool:
        """Prove the exact album PK and every item row for it are absent.

        This is a defense-in-depth postcondition for a confirmed pinned-Beets
        delete result. It is never used to promote a lost child acknowledgement
        into success. Items are addressed by the original album primary key so
        an orphan row cannot be mistaken for a completed metadata transaction.
        """
        row = self._conn.execute(
            "SELECT "
            "NOT EXISTS (SELECT 1 FROM albums WHERE id = ?), "
            "NOT EXISTS (SELECT 1 FROM items WHERE album_id = ?)",
            (album_id, album_id),
        ).fetchone()
        return bool(row and row[0] and row[1])

    _ALBUM_SELECT = (
        "SELECT a.id, a.album, a.albumartist, a.year, a.mb_albumid, "
        "       a.albumtype, a.label, a.country, "
        "       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count, "
        "       (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) as formats, "
        "       a.added, a.mb_releasegroupid, a.release_group_title, "
        "       (SELECT MIN(i.bitrate) FROM items i "
        "        WHERE i.album_id = a.id AND i.bitrate > 0) as min_bitrate, "
        "       (SELECT CAST(AVG(i.bitrate) AS INTEGER) FROM items i "
        "        WHERE i.album_id = a.id AND i.bitrate > 0) as avg_bitrate, "
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
        current = self._resolve_unique(mbid)
        if current is None:
            return None
        items = sorted(
            current.items,
            key=lambda item: (item.disc or 0, item.track or 0, item.id),
        )
        return [{
            "title": item.title, "track": item.track, "disc": item.disc,
            "length": item.length, "format": item.format,
            "bitrate": item.bitrate, "samplerate": item.samplerate,
            "bitdepth": item.bitdepth,
        } for item in items]

    def get_album_ids_by_mbids(self, mbids: list[str]) -> dict[str, int]:
        """Map release IDs to beets album IDs. Returns {id: album_id}.

        Shares the ``_batch_lookup_album_ids`` seam with
        ``check_mbids`` so presence and album-id mapping stay in
        sync — the paired-consistency concern Codex round 1 + 2
        kept circling (issue #121). Without shared batching, the
        browse routes would either diverge on Discogs visibility
        or regress to an N+1 query pattern for large artist pages.
        """
        return self._batch_lookup_album_ids(mbids)

    def get_avg_bitrate_kbps(self, mb_release_id: str) -> Optional[int]:
        """Get average track bitrate (kbps) for a release. None if not found.

        Routes through ``locate`` (issue #121) so Discogs numerics
        resolve the same way every other postflight lookup does.
        """
        current = self._resolve_unique(mb_release_id)
        if current is None:
            return None
        bitrates = [
            item.bitrate for item in current.items
            if item.bitrate is not None and item.bitrate > 0
        ]
        if not bitrates:
            return None
        return int(sum(bitrates) / len(bitrates) / 1000)

    def list_world_albums(self) -> list["BeetsWorldAlbum"]:
        """Return every Beets album with exact identities and resolved paths.

        Unlike :meth:`list_release_identities`, albums with no usable release
        identity are retained so the world audit can report them instead of
        silently treating them as out of scope.
        """
        rows = self._conn.execute(
            "SELECT a.id, a.mb_albumid, a.discogs_albumid, i.path "
            "FROM albums a "
            "LEFT JOIN items i ON i.album_id = a.id "
            "ORDER BY a.id ASC, i.id ASC"
        ).fetchall()
        grouped: dict[int, dict[str, object]] = {}
        for album_id, mb_albumid, discogs_albumid, raw_path in rows:
            entry = grouped.setdefault(int(album_id), {
                "mb_albumid": mb_albumid,
                "discogs_albumid": discogs_albumid,
                "paths": [],
            })
            if raw_path is not None:
                paths = entry["paths"]
                assert isinstance(paths, list)
                paths.append(self._resolve_path(raw_path))

        albums: list[BeetsWorldAlbum] = []
        for album_id, entry in grouped.items():
            identities = {
                identity.release_id
                for raw in (
                    entry["mb_albumid"],
                    entry["discogs_albumid"],
                )
                if (identity := ReleaseIdentity.from_id(raw)) is not None
            }
            raw_paths = entry["paths"]
            assert isinstance(raw_paths, list)
            item_paths = tuple(str(path) for path in raw_paths)
            album_path = os.path.dirname(item_paths[0]) if item_paths else ""
            albums.append(BeetsWorldAlbum(
                album_id=album_id,
                release_ids=tuple(sorted(identities)),
                album_path=album_path,
                item_paths=item_paths,
            ))
        return albums

    @staticmethod
    def _album_row_to_dict(r: tuple[object, ...]) -> dict[str, object]:
        """Convert a standard album query row to dict.

        Column order must match _ALBUM_SELECT (indices 0-15).
        Field names here are the API contract — the frontend depends on them.
        """
        frontend_id = frontend_release_id(r[4], r[15])
        source = detect_release_source(frontend_id)
        discogs_identity = ReleaseIdentity.from_id(r[15])
        return {
            "id": r[0], "album": r[1], "artist": r[2], "year": r[3],
            "mb_albumid": frontend_id, "type": r[5], "label": r[6],
            "country": r[7], "track_count": r[8], "formats": r[9],
            "added": r[10], "mb_releasegroupid": r[11],
            "release_group_title": r[12], "min_bitrate": r[13],
            "avg_bitrate": r[14],
            "source": source,
            "discogs_albumid": (
                discogs_identity.release_id
                if discogs_identity and discogs_identity.source == "discogs"
                else None
            ),
        }


@dataclass(frozen=True)
class BeetsWorldAlbum:
    """One physical Beets album for cross-engine world auditing.

    ``release_ids`` contains every exact identity Beets stores for the album.
    Keeping both columns is important: MusicBrainz and Discogs are independent
    source identities, and the audit must not silently discard either one.
    """

    album_id: int
    release_ids: tuple[str, ...]
    album_path: str
    item_paths: tuple[str, ...]
