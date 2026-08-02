"""Generated authority laws for current-library request displays."""

from __future__ import annotations

import os
import unittest

import msgspec
from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.current_library_display import (
    CurrentLibraryDisplay,
    CurrentLibraryUnavailableDisplay,
    CurrentLibraryUniqueDisplay,
    current_library_display,
    resolve_request_current_library,
)
from lib.release_identity import ConflictingReleaseIdentityError, ReleaseIdentity
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_request_row
from web.library_album_detail_service import (
    LibraryAlbumDetail,
    load_library_album_detail,
)
from web.library_album_row import (
    AmbiguousLibraryRequestAttachmentError,
    LibraryAlbumRow,
)
from web.library_artist_service import build_library_artist_rows

MB_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DISCOGS_ID = "12856590"


def assert_display_authority(
    display: CurrentLibraryDisplay,
    *,
    expected_state: str,
    expected_path: str | None,
) -> None:
    """Executable law: only a fresh unique resolver result exposes a path."""

    state = msgspec.to_builtins(display)["state"]
    if state != expected_state:
        raise AssertionError(f"display state drifted: {state!r}")
    actual_path = getattr(display, "path", None)
    if actual_path != expected_path:
        raise AssertionError("display path did not come from fresh Beets authority")


def assert_independent_library_facts(
    row: LibraryAlbumRow,
    *,
    held: bool,
    tracked: bool,
    captured: bool,
    verified: bool,
    provisional: bool,
) -> None:
    """Executable law: presence, tracking, acquisition, and proof do not infer peers."""
    expected = {
        "held": (row.in_library, held),
        "tracked": (row.pipeline_id is not None, tracked),
        "captured": (row.has_captured_history, captured if tracked else False),
        "verified": (row.pipeline_verified_lossless, verified if tracked else False),
        "provisional": (row.pipeline_provisional, provisional if tracked else False),
        "installed_quality": (row.library_rank is not None, held),
    }
    for fact, (actual, wanted) in expected.items():
        if actual != wanted:
            raise AssertionError(
                f"{fact} fact drifted: actual={actual!r}, expected={wanted!r}"
            )


def assert_action_identity(
    row: LibraryAlbumRow | LibraryAlbumDetail,
    *,
    expected: ReleaseIdentity,
    expected_pipeline_id: int | None,
) -> None:
    """Executable law: the serialized action key names the attached request."""
    actual = row.mb_albumid
    if actual != expected.release_id:
        raise AssertionError(
            "action identity drifted: "
            f"actual={actual!r}, expected={expected.release_id!r}"
        )
    if row.source != expected.source:
        raise AssertionError(
            "action source drifted: "
            f"actual={row.source!r}, expected={expected.source!r}"
        )
    if row.pipeline_id != expected_pipeline_id:
        raise AssertionError(
            "pipeline attachment drifted: "
            f"actual={row.pipeline_id!r}, expected={expected_pipeline_id!r}"
        )
    if isinstance(row, LibraryAlbumDetail):
        history_request_ids = {item.request_id for item in row.download_history}
        expected_history_ids = (
            {expected_pipeline_id} if expected_pipeline_id is not None else set()
        )
        if history_request_ids != expected_history_ids:
            raise AssertionError(
                "detail history attachment drifted: "
                f"actual={history_request_ids!r}, expected={expected_history_ids!r}"
            )


def _fact_beets_album(source: str) -> dict[str, object]:
    dual = source.startswith("dual_")
    return {
        "id": 7,
        "album": "Independent Facts",
        "artist": "Boundary Archivist",
        "year": 2001,
        "mb_albumid": MB_ID if source == "mb" or dual else None,
        "discogs_albumid": DISCOGS_ID if source == "discogs" or dual else None,
        "track_count": 2,
        "mb_releasegroupid": "11111111-1111-1111-1111-111111111111",
        "release_group_title": "Independent Facts",
        "added": 1773651901.0,
        "formats": "FLAC",
        "min_bitrate": 811000,
        "avg_bitrate": 922000,
        "type": "album",
        "label": "Archive",
        "country": "AU",
    }


class _GeneratedDetailLookup:
    def __init__(self, album: dict[str, object]) -> None:
        self._album = album

    def get_album_detail(self, album_id: int) -> dict[str, object] | None:
        if album_id != self._album["id"]:
            return None
        return dict(self._album)


class TestCurrentLibraryDisplayGenerated(unittest.TestCase):
    @given(
        source=st.sampled_from(("mb", "discogs_modern", "discogs_legacy")),
        cardinality=st.integers(min_value=0, max_value=2),
        moved_segment=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"),
                blacklist_characters=("/", "\\", "\x00"),
            ),
            min_size=1,
            max_size=20,
        ),
    )
    @example(
        source="discogs_legacy",
        cardinality=1,
        moved_segment="Beyonce-current",
    )
    def test_exact_typed_display_follows_current_beets_cardinality(
        self,
        source: str,
        cardinality: int,
        moved_segment: str,
    ) -> None:
        release_id = MB_ID if source == "mb" else DISCOGS_ID
        row: dict[str, object] = {
            "mb_release_id": release_id,
            "discogs_release_id": (
                DISCOGS_ID if source == "discogs_modern" else None
            ),
        }
        beets = FakeBeetsDB(library_root="/library")
        album_ids = list(range(100, 100 + cardinality))
        beets.set_album_ids_for_release(release_id, album_ids)
        expected_path = None
        if cardinality == 1:
            beets.set_item_paths(
                release_id,
                [(1001, f"/library/{moved_segment}/01.flac")],
            )
            expected_path = os.path.join("/library", moved_segment)

        display = current_library_display(
            resolve_request_current_library(row, beets),
        )

        expected_state = (
            "missing" if cardinality == 0
            else "unique" if cardinality == 1
            else "ambiguous"
        )
        assert_display_authority(
            display,
            expected_state=expected_state,
            expected_path=expected_path,
        )

    def test_conflicting_request_ids_never_reach_beets(self) -> None:
        beets = FakeBeetsDB()
        display = current_library_display(resolve_request_current_library({
            "mb_release_id": MB_ID,
            "discogs_release_id": DISCOGS_ID,
        }, beets))
        self.assertIsInstance(display, CurrentLibraryUnavailableDisplay)
        assert isinstance(display, CurrentLibraryUnavailableDisplay)
        self.assertEqual(msgspec.to_builtins(display)["state"], "unavailable")
        self.assertEqual(display.reason, "conflicting_request_identity")
        self.assertEqual(beets.resolve_current_release_calls, [])

    def test_checker_rejects_a_non_authoritative_path_mutant(self) -> None:
        mutant = CurrentLibraryUniqueDisplay(
            release_source="musicbrainz",
            release_id=MB_ID,
            album_id=1,
            path="/poisoned/cache",
        )
        with self.assertRaisesRegex(AssertionError, "fresh Beets authority"):
            assert_display_authority(
                mutant,
                expected_state="unique",
                expected_path="/library/current",
            )


class TestIndependentLibraryFactsGenerated(unittest.TestCase):
    @given(
        primary=st.integers(min_value=1, max_value=100_000_000),
        offset=st.integers(min_value=1, max_value=100_000_000),
    )
    @example(primary=12856590, offset=1)
    def test_conflicting_numeric_beets_identity_never_renders(
        self,
        primary: int,
        offset: int,
    ) -> None:
        album = _fact_beets_album("discogs")
        album["mb_albumid"] = str(primary)
        album["discogs_albumid"] = str(primary + offset)

        with self.assertRaises(ConflictingReleaseIdentityError):
            build_library_artist_rows(
                library_albums=[album],
                pipeline_rows=[],
                track_counts={},
                rank_fn=lambda _format, _bitrate: "lossless",
            )

    @given(
        mb_release_id=st.uuids().map(str),
        discogs_release_id=st.integers(
            min_value=1,
            max_value=100_000_000,
        ).map(str),
        attachment=st.sampled_from((
            "untracked",
            "mb",
            "discogs",
            "cross_source",
            "duplicate_discogs",
            "modern_legacy_discogs",
        )),
        status=st.sampled_from(("wanted", "imported", "replaced")),
    )
    @example(
        mb_release_id=MB_ID,
        discogs_release_id=DISCOGS_ID,
        attachment="discogs",
        status="wanted",
    )
    def test_dual_tagged_action_identity_follows_exact_attachment(
        self,
        mb_release_id: str,
        discogs_release_id: str,
        attachment: str,
        status: str,
    ) -> None:
        album = _fact_beets_album("dual_mb")
        album["mb_albumid"] = mb_release_id
        album["discogs_albumid"] = discogs_release_id
        pipeline_rows: list[dict[str, object]] = []
        expected = ReleaseIdentity(
            source="musicbrainz",
            release_id=mb_release_id,
        )
        expected_pipeline_id: int | None = None
        if attachment == "mb" or attachment == "cross_source":
            pipeline_rows.append(make_request_row(
                id=42,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=mb_release_id,
                discogs_release_id=None,
                status=status,
                has_captured_history=False,
                verified_lossless=False,
                provisional_lossless=False,
            ))
            expected_pipeline_id = 42
        if attachment in {
            "discogs",
            "cross_source",
            "duplicate_discogs",
            "modern_legacy_discogs",
        }:
            pipeline_rows.append(make_request_row(
                id=43,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=None,
                discogs_release_id=discogs_release_id,
                status=status,
                has_captured_history=False,
                verified_lossless=False,
                provisional_lossless=False,
            ))
            expected = ReleaseIdentity(
                source="discogs",
                release_id=discogs_release_id,
            )
            expected_pipeline_id = 43

        if attachment == "duplicate_discogs":
            pipeline_rows.append(make_request_row(
                id=44,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=None,
                discogs_release_id=discogs_release_id,
                status=status,
                has_captured_history=False,
                verified_lossless=False,
                provisional_lossless=False,
            ))
        elif attachment == "modern_legacy_discogs":
            pipeline_rows.append(make_request_row(
                id=44,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=discogs_release_id,
                discogs_release_id=None,
                status=status,
                has_captured_history=False,
                verified_lossless=False,
                provisional_lossless=False,
            ))

        pipeline_db = FakePipelineDB()
        request_ids: list[int] = []
        for request_row in pipeline_rows:
            pipeline_db.seed_request(request_row)
            request_id = request_row["id"]
            if not isinstance(request_id, int):
                raise TypeError("generated request id must be int")
            request_ids.append(request_id)
            pipeline_db.log_download(
                request_id,
                outcome="success",
                soulseek_username=f"request-{request_id}",
            )

        if attachment in {
            "cross_source",
            "duplicate_discogs",
            "modern_legacy_discogs",
        }:
            with self.assertRaises(AmbiguousLibraryRequestAttachmentError):
                build_library_artist_rows(
                    library_albums=[album],
                    pipeline_rows=pipeline_rows,
                    track_counts={request_id: 2 for request_id in request_ids},
                    rank_fn=lambda _format, _bitrate: "lossless",
                )
            with self.assertRaises(AmbiguousLibraryRequestAttachmentError):
                load_library_album_detail(
                    library_lookup=_GeneratedDetailLookup(album),
                    pipeline_db=pipeline_db,
                    album_id=7,
                )
            return

        rows = build_library_artist_rows(
            library_albums=[album],
            pipeline_rows=pipeline_rows,
            track_counts=(
                {expected_pipeline_id: 2}
                if expected_pipeline_id is not None
                else {}
            ),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        detail = load_library_album_detail(
            library_lookup=_GeneratedDetailLookup(album),
            pipeline_db=pipeline_db,
            album_id=7,
        )

        self.assertEqual(len(rows), 1)
        self.assertIsNotNone(detail)
        assert detail is not None
        assert_action_identity(
            rows[0],
            expected=expected,
            expected_pipeline_id=expected_pipeline_id,
        )
        assert_action_identity(
            detail,
            expected=expected,
            expected_pipeline_id=expected_pipeline_id,
        )

    @given(
        invalid_shape=st.sampled_from((
            "malformed",
            "conflicting",
            "identityless",
        )),
        status=st.sampled_from(("wanted", "imported", "replaced")),
    )
    @example(invalid_shape="conflicting", status="wanted")
    def test_invalid_pipeline_identity_stays_visible_and_unattached(
        self,
        invalid_shape: str,
        status: str,
    ) -> None:
        identity_fields: dict[str, object] = {
            "mb_release_id": None,
            "discogs_release_id": None,
        }
        if invalid_shape == "malformed":
            identity_fields["mb_release_id"] = "not-a-release-id"
        elif invalid_shape == "conflicting":
            identity_fields = {
                "mb_release_id": MB_ID,
                "discogs_release_id": DISCOGS_ID,
            }
        pipeline_row = make_request_row(
            id=42,
            artist_name="Boundary Archivist",
            album_title="Invalid Authority",
            status=status,
            has_captured_history=True,
            verified_lossless=True,
            provisional_lossless=False,
            **identity_fields,
        )
        album = _fact_beets_album("dual_mb")

        rows = build_library_artist_rows(
            library_albums=[album],
            pipeline_rows=[pipeline_row],
            track_counts={42: 2},
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        detail_db = FakePipelineDB()
        detail_db.seed_request(pipeline_row)
        detail = load_library_album_detail(
            library_lookup=_GeneratedDetailLookup(album),
            pipeline_db=detail_db,
            album_id=7,
        )

        self.assertEqual(len(rows), 2)
        held = next(row for row in rows if row.in_library)
        invalid = next(row for row in rows if not row.in_library)
        self.assertIsNone(held.pipeline_id)
        self.assertIsNone(invalid.mb_albumid)
        self.assertEqual(invalid.source, "unknown")
        self.assertEqual(invalid.pipeline_id, 42)
        self.assertIsNotNone(detail)
        assert detail is not None
        self.assertIsNone(detail.pipeline_id)

    @given(
        source=st.sampled_from(("mb", "discogs", "dual_mb", "dual_discogs")),
        shape=st.sampled_from(("held_untracked", "missing_tracked", "held_tracked")),
        status=st.sampled_from(("wanted", "imported", "replaced")),
        captured=st.booleans(),
        proof=st.sampled_from(("none", "verified", "provisional")),
    )
    @example(
        source="mb",
        shape="missing_tracked",
        status="wanted",
        captured=True,
        proof="verified",
    )
    @example(
        source="discogs",
        shape="held_untracked",
        status="replaced",
        captured=True,
        proof="provisional",
    )
    def test_exact_identity_merge_preserves_each_fact_independently(
        self,
        source: str,
        shape: str,
        status: str,
        captured: bool,
        proof: str,
    ) -> None:
        held = shape != "missing_tracked"
        tracked = shape != "held_untracked"
        request_source = "discogs" if source.endswith("discogs") else "mb"
        verified = proof == "verified"
        provisional = proof == "provisional"
        library_albums = [_fact_beets_album(source)] if held else []
        pipeline_rows = []
        if tracked:
            pipeline_rows.append(make_request_row(
                id=42,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=MB_ID if request_source == "mb" else None,
                discogs_release_id=(
                    DISCOGS_ID if request_source == "discogs" else None
                ),
                status=status,
                has_captured_history=captured,
                verified_lossless=verified,
                provisional_lossless=provisional,
            ))

        rows = build_library_artist_rows(
            library_albums=library_albums,
            pipeline_rows=pipeline_rows,
            track_counts={42: 2} if tracked else {},
            rank_fn=lambda _format, _bitrate: "lossless",
        )

        self.assertEqual(len(rows), 1)
        assert_independent_library_facts(
            rows[0],
            held=held,
            tracked=tracked,
            captured=captured,
            verified=verified,
            provisional=provisional,
        )

    def test_checker_rejects_presence_to_capture_inference_mutant(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            _fact_beets_album("mb"),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        mutant = msgspec.structs.replace(row, has_captured_history=True)

        with self.assertRaisesRegex(AssertionError, "captured fact drifted"):
            assert_independent_library_facts(
                mutant,
                held=True,
                tracked=False,
                captured=False,
                verified=False,
                provisional=False,
            )

    def test_action_identity_checker_rejects_first_observation_mutant(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            _fact_beets_album("dual_mb"),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        mutant = msgspec.structs.replace(row, pipeline_id=42)

        with self.assertRaisesRegex(AssertionError, "action identity drifted"):
            assert_action_identity(
                mutant,
                expected=ReleaseIdentity(
                    source="discogs",
                    release_id=DISCOGS_ID,
                ),
                expected_pipeline_id=42,
            )

    def test_action_identity_checker_rejects_detached_source_mutant(self) -> None:
        row = LibraryAlbumRow.from_beets_album(
            _fact_beets_album("dual_mb"),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        mutant = msgspec.structs.replace(
            row,
            mb_albumid=DISCOGS_ID,
            pipeline_id=42,
        )

        with self.assertRaisesRegex(AssertionError, "action source drifted"):
            assert_action_identity(
                mutant,
                expected=ReleaseIdentity(
                    source="discogs",
                    release_id=DISCOGS_ID,
                ),
                expected_pipeline_id=42,
            )

    def test_action_identity_checker_rejects_detail_history_mutant(self) -> None:
        detail = load_library_album_detail(
            library_lookup=_GeneratedDetailLookup(_fact_beets_album("dual_mb")),
            pipeline_db=FakePipelineDB(),
            album_id=7,
        )
        self.assertIsNotNone(detail)
        assert detail is not None
        mutant = msgspec.structs.replace(detail, pipeline_id=42)

        with self.assertRaisesRegex(AssertionError, "detail history attachment drifted"):
            assert_action_identity(
                mutant,
                expected=ReleaseIdentity(
                    source="musicbrainz",
                    release_id=MB_ID,
                ),
                expected_pipeline_id=42,
            )

    def test_cardinality_checker_rejects_first_wins_overwrite_mutant(self) -> None:
        album = _fact_beets_album("discogs")
        candidates = [
            make_request_row(
                id=request_id,
                artist_name="Boundary Archivist",
                album_title="Independent Facts",
                mb_release_id=None,
                discogs_release_id=DISCOGS_ID,
                has_captured_history=False,
                verified_lossless=False,
                provisional_lossless=False,
            )
            for request_id in (42, 43)
        ]
        overwrite_mutant = {DISCOGS_ID: candidates[-1]}

        with self.assertRaisesRegex(AssertionError, "candidate cardinality"):
            actual_ids = {
                int(row["id"]) for row in overwrite_mutant.values()
            }
            expected_ids = {42, 43}
            if actual_ids != expected_ids:
                raise AssertionError(
                    "candidate cardinality drifted under first-wins overwrite"
                )

        with self.assertRaises(AmbiguousLibraryRequestAttachmentError):
            build_library_artist_rows(
                library_albums=[album],
                pipeline_rows=candidates,
                track_counts={42: 2, 43: 2},
                rank_fn=lambda _format, _bitrate: "lossless",
            )

    def test_unactionable_checker_rejects_permissive_identity_mutant(self) -> None:
        valid = LibraryAlbumRow.from_beets_album(
            _fact_beets_album("mb"),
            rank_fn=lambda _format, _bitrate: "lossless",
        )
        permissive_mutant = msgspec.structs.replace(
            valid,
            in_library=False,
            beets_album_id=None,
            pipeline_id=42,
            source="request",
        )

        with self.assertRaisesRegex(AssertionError, "actionable identity"):
            if (
                permissive_mutant.mb_albumid is not None
                or permissive_mutant.source != "unknown"
            ):
                raise AssertionError(
                    "invalid pipeline row retained an actionable identity"
                )


if __name__ == "__main__":
    unittest.main()
