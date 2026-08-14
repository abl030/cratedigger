"""Generated exact-identity laws for the long-tail worklist."""

from __future__ import annotations

import errno
import os
import sqlite3
import tempfile
import unittest
import uuid
from collections.abc import Mapping, Sequence
from typing import get_args

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.banding import (
    BAND_MISSING,
    CurrentBeetsBandingAmbiguityError,
    band_current_resolutions,
    band_from_detail,
    compute_library_rank,
)
from lib.beets_db import (
    BeetsDB,
    CurrentBeetsAmbiguityReason,
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
    album_info_from_current,
    rank_format_for_current,
)
from lib.long_tail_service import list_long_tail
from lib.media_readiness import kbps_from_bps
from lib.quality import QualityRankConfig
from lib.release_identity import ReleaseIdentity
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

MB_RELEASE = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
DISCOGS_RELEASE = "12856590"
AMBIGUITY_REASONS = tuple(get_args(CurrentBeetsAmbiguityReason.__value__))
RESOLUTION_STATES = ("missing", "unique", *AMBIGUITY_REASONS)


def _authority_failure(kind: str) -> OSError | sqlite3.OperationalError:
    if kind == "missing_file":
        return FileNotFoundError(
            errno.ENOENT,
            "No such file or directory",
            "/beets/library.db",
        )
    failure = sqlite3.OperationalError("database is locked")
    failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
    failure.sqlite_errorname = "SQLITE_BUSY"
    return failure


@st.composite
def _valid_request_rows(draw: st.DrawFn) -> list[dict[str, object]]:
    sources = draw(st.lists(
        st.sampled_from(("musicbrainz", "discogs_modern", "discogs_legacy")),
        min_size=1,
        max_size=12,
    ))
    mb_seed = draw(st.integers(min_value=0, max_value=(1 << 120) - 1))
    discogs_seed = draw(st.integers(min_value=1, max_value=99_000_000))
    rows: list[dict[str, object]] = []
    for request_id, source in enumerate(sources, start=1):
        if source == "musicbrainz":
            mb_release_id = str(uuid.UUID(int=(mb_seed << 8) + request_id))
            discogs_release_id = None
        else:
            discogs_release_id = str(discogs_seed + request_id)
            mb_release_id = (
                discogs_release_id if source == "discogs_legacy" else None
            )
        rows.append(make_request_row(
            id=request_id,
            status="wanted",
            mb_release_id=mb_release_id,
            discogs_release_id=discogs_release_id,
        ))
    return rows


def assert_exact_identity_selection(
    rows: Sequence[Mapping[str, object]],
    selected_release_ids: Sequence[str],
) -> None:
    """Every strict request identity appears in the one Beets batch."""
    expected: list[str] = []
    for row in rows:
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"),
            row.get("discogs_release_id"),
        )
        if identity is None:
            raise AssertionError(f"generated row has no strict identity: {row!r}")
        expected.append(identity.release_id)
    if list(selected_release_ids) != expected:
        raise AssertionError(
            "long-tail exact identity selection drifted: "
            f"actual={list(selected_release_ids)!r}, expected={expected!r}"
        )


def _resolution_world(
    states: Sequence[str],
) -> dict[ReleaseIdentity, CurrentBeetsResolution]:
    resolutions: dict[ReleaseIdentity, CurrentBeetsResolution] = {}
    for index, state in enumerate(states, start=1):
        identity = ReleaseIdentity(
            source="musicbrainz",
            release_id=str(uuid.UUID(int=index)),
        )
        if state == "missing":
            resolution: CurrentBeetsResolution = CurrentBeetsMissing(
                identity=identity,
            )
        elif state == "unique":
            resolution = CurrentBeetsUnique(
                identity=identity,
                album_id=index,
                album_path=f"/music/album-{index}",
                items=(CurrentBeetsItem(
                    id=index * 100,
                    path=f"/music/album-{index}/01.mp3",
                    format="MP3",
                    bitrate=320_000,
                ),),
                selectors=(f"mb_albumid:{identity.release_id}",),
            )
        else:
            if state not in AMBIGUITY_REASONS:
                raise AssertionError(f"unknown generated resolution: {state}")
            resolution = CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=(index, index + 10_000),
                # Runtime membership is checked against values derived from
                # the Literal alias above; Pyright cannot narrow through it.
                reason=state,  # pyright: ignore[reportArgumentType]
            )
        resolutions[identity] = resolution
    return resolutions


def assert_current_resolution_banding(
    resolutions: Mapping[ReleaseIdentity, CurrentBeetsResolution],
    outcome: dict[str, str] | CurrentBeetsBandingAmbiguityError,
) -> None:
    """Missing/unique resolve; any ambiguity aborts without a band map."""
    ambiguities = tuple(
        resolution
        for resolution in resolutions.values()
        if isinstance(resolution, CurrentBeetsAmbiguous)
    )
    if ambiguities:
        if not isinstance(outcome, CurrentBeetsBandingAmbiguityError):
            raise AssertionError(
                "ambiguous CurrentBeets resolution produced a band payload"
            )
        if outcome.ambiguities != ambiguities:
            raise AssertionError(
                "banding exception did not retain every ambiguity: "
                f"actual={outcome.ambiguities!r}, expected={ambiguities!r}"
            )
        return

    assert not isinstance(outcome, CurrentBeetsBandingAmbiguityError), (
        "unambiguous resolutions raised ambiguity"
    )
    for identity, resolution in resolutions.items():
        expected = (
            BAND_MISSING
            if isinstance(resolution, CurrentBeetsMissing)
            # ``_resolution_world`` seeds every unique release at the MP3
            # transparent floor (320 kbps under #1145's one ladder), so this
            # property stays about resolution STATE, not about bitrate.
            else "transparent"
        )
        if outcome.get(identity.release_id) != expected:
            raise AssertionError(
                "current-resolution band drifted: "
                f"identity={identity!r}, actual={outcome!r}, "
                f"expected={expected!r}"
            )


def _mixed_format_current(
    *,
    flac_first: bool,
    mp3_bitrate: int,
    flac_bitrate: int,
) -> tuple[ReleaseIdentity, CurrentBeetsUnique]:
    identity = ReleaseIdentity(source="musicbrainz", release_id=MB_RELEASE)
    flac = CurrentBeetsItem(
        id=1,
        path="/music/mixed/01.flac",
        format="FLAC",
        bitrate=flac_bitrate,
    )
    mp3 = CurrentBeetsItem(
        id=2,
        path="/music/mixed/02.mp3",
        format="MP3",
        bitrate=mp3_bitrate,
    )
    return identity, CurrentBeetsUnique(
        identity=identity,
        album_id=1,
        album_path="/music/mixed",
        items=(flac, mp3) if flac_first else (mp3, flac),
        selectors=(f"mb_albumid:{MB_RELEASE}",),
    )


def assert_mixed_format_order_invariant(
    bands: Sequence[str],
    *,
    expected_band: str,
) -> None:
    """Canonical mixed-format precedence cannot depend on item row order."""
    if list(bands) != [expected_band, expected_band]:
        raise AssertionError(
            "mixed-format band changed with item order or ignored precedence: "
            f"actual={list(bands)!r}, expected={expected_band!r}"
        )


class TestLongTailIdentitySelectionGenerated(unittest.TestCase):
    @given(rows=_valid_request_rows())
    @example(rows=[
        make_request_row(
            id=1,
            status="wanted",
            mb_release_id=MB_RELEASE,
            discogs_release_id=None,
        ),
        make_request_row(
            id=2,
            status="wanted",
            mb_release_id=None,
            discogs_release_id=DISCOGS_RELEASE,
        ),
    ])
    def test_every_valid_mb_or_discogs_identity_is_batched_exactly(
        self,
        rows: list[dict[str, object]],
    ) -> None:
        db = FakePipelineDB()
        for row in rows:
            db.seed_request(row)
        batches: list[list[str]] = []

        def band_fn(release_ids: list[str]) -> dict[str, str]:
            batches.append(list(release_ids))
            return {release_id: "good" for release_id in release_ids}

        result = list_long_tail(db, band_fn)

        self.assertEqual(len(batches), 1)
        assert_exact_identity_selection(rows, batches[0])
        self.assertEqual([row.band for row in result.rows], ["good"] * len(rows))

    @given(
        rows=_valid_request_rows(),
        failure_kind=st.sampled_from(("missing_file", "locked_query")),
    )
    @example(
        rows=[make_request_row(
            id=1,
            status="wanted",
            mb_release_id=None,
            discogs_release_id=DISCOGS_RELEASE,
        )],
        failure_kind="locked_query",
    )
    def test_real_authority_failures_escape_the_public_service_seam(
        self,
        rows: list[dict[str, object]],
        failure_kind: str,
    ) -> None:
        db = FakePipelineDB()
        for row in rows:
            db.seed_request(row)
        batches: list[list[str]] = []
        failure = _authority_failure(failure_kind)

        def failed_band_fn(release_ids: list[str]) -> dict[str, str]:
            batches.append(list(release_ids))
            raise failure

        with self.assertRaises(type(failure)) as raised:
            list_long_tail(db, failed_band_fn)

        self.assertIs(raised.exception, failure)
        self.assertEqual(len(batches), 1)
        assert_exact_identity_selection(rows, batches[0])

    def test_checker_rejects_the_mb_only_selection_mutant(self) -> None:
        rows = [
            make_request_row(
                id=1,
                status="wanted",
                mb_release_id=MB_RELEASE,
                discogs_release_id=None,
            ),
            make_request_row(
                id=2,
                status="wanted",
                mb_release_id=None,
                discogs_release_id=DISCOGS_RELEASE,
            ),
        ]
        mb_only = [
            str(row["mb_release_id"])
            for row in rows
            if row.get("mb_release_id")
        ]

        with self.assertRaisesRegex(AssertionError, "identity selection drifted"):
            assert_exact_identity_selection(rows, mb_only)


#: LAME settings strings drawn from the live ``items.encoder_settings``
#: census — the fact ``album_info_from_current`` mints a contract from.
_BANDING_SETTINGS = (None, "", "-V 0", "-V 2", "-V 5", "-b 320",
                     "--alt-preset standard")


def banding_projection_violations(
    *,
    band: str,
    projected_band: str,
    context: str,
) -> list[str]:
    """The badge must be the decision projection's own answer.

    ``projected_band`` is ``compute_library_rank`` applied to
    ``album_info_from_current``'s output — the aggregates the importer ranks.
    Any other derivation is the divergence issue #1145 F5 closed.
    """
    violations: list[str] = []
    if band != projected_band:
        violations.append(
            f"banded {band!r} where the decision projection says "
            f"{projected_band!r}: {context}"
        )
    return violations


def _browse_surface_violations(
    current: CurrentBeetsUnique,
    projected_band: str,
    cfg: QualityRankConfig,
) -> list[str]:
    """Band the same album through the real browse adapter.

    Materialises the items as an actual Beets-shaped SQLite library so
    ``BeetsDB.check_mbids_detail`` — not a fake — produces the projection
    ``band_from_detail`` reads. Anything less would leave the browse surface
    asserted only by construction.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "beets.db")
        library_root = os.path.join(tmp, "Music")
        os.makedirs(library_root, exist_ok=True)
        conn = sqlite3.connect(db_path)
        conn.executescript(
            "CREATE TABLE albums (id INTEGER PRIMARY KEY, mb_albumid TEXT, "
            "discogs_albumid INTEGER);"
            "CREATE TABLE items (id INTEGER PRIMARY KEY, album_id INTEGER, "
            "path BLOB, title TEXT, track INTEGER, disc INTEGER, "
            "length REAL, format TEXT, bitrate INTEGER, samplerate INTEGER, "
            "bitdepth INTEGER, encoder_settings TEXT);"
        )
        conn.execute(
            "INSERT INTO albums (id, mb_albumid, discogs_albumid) "
            "VALUES (1, ?, NULL)",
            (current.identity.release_id,),
        )
        for index, item in enumerate(current.items, start=1):
            path = os.path.join(library_root, "Artist", "Album", f"{index}.mp3")
            conn.execute(
                "INSERT INTO items (id, album_id, path, title, track, disc, "
                "length, format, bitrate, samplerate, bitdepth, "
                "encoder_settings) VALUES (?, 1, ?, ?, ?, 1, 100.0, ?, ?, "
                "44100, NULL, ?)",
                (index, path.encode(), f"Track {index}", index, item.format,
                 item.bitrate, item.encoder_settings),
            )
        conn.commit()
        conn.close()

        release_id = current.identity.release_id
        with BeetsDB(db_path, library_root=library_root) as db:
            detail = db.check_mbids_detail([release_id], cfg)
        return banding_projection_violations(
            band=band_from_detail(release_id, {release_id}, detail, cfg),
            projected_band=projected_band,
            context=f"browse surface: {current.items!r}",
        )


class TestBandingReadsTheDecisionProjectionGenerated(unittest.TestCase):
    """One projection, both surfaces — patrolled over generated item worlds.

    The deterministic pins live in ``tests/test_banding_projection.py`` and
    drive real Beets SQLite rows; this drives the in-memory resolution
    directly so the world space can include contract-bearing, mixed-codec and
    band-edge albums the fixtures cannot enumerate.

    ``bitrates`` deliberately includes 0, which is what Beets stores for an
    item it could not measure. Those albums do not project at all, and the
    first version of this property could not draw one — so the codec-only
    fallback both surfaces depend on went unpatrolled, and a real divergence
    (``lossless`` on the worklist, ``unknown`` on browse) survived it. The
    remedy is a wider world set, not a narrower invariant.
    """

    @given(
        settings=st.lists(
            st.sampled_from(_BANDING_SETTINGS), min_size=1, max_size=4),
        bitrates=st.lists(
            st.one_of(
                st.just(0),
                st.integers(min_value=32_000, max_value=1_400_000),
            ),
            min_size=1, max_size=4),
        formats=st.lists(
            st.sampled_from(("MP3", "FLAC", "Opus", "AAC")),
            min_size=1, max_size=4),
    )
    # A proven V0 whose measured average alone would band two tiers lower.
    @example(settings=["-V 0", "-V 0"], bitrates=[245_000, 245_000],
             formats=["MP3", "MP3"])
    # The #1144 band edge: 255.6 kbps floors to good, rounds to excellent.
    @example(settings=[None, None], bitrates=[255_600, 255_600],
             formats=["MP3", "MP3"])
    # Mixed codec: the precedence reduction must reach the badge.
    @example(settings=[None, None], bitrates=[900_000, 200_000],
             formats=["FLAC", "MP3"])
    # No usable bitrate anywhere: no projection, codec-only band on both.
    @example(settings=[None], bitrates=[0], formats=["FLAC"])
    @example(settings=["-V 0"], bitrates=[0], formats=["MP3"])
    # Partly measured: the projection exists but is built from the measured
    # items only, so the unmeasured one must not drag the label around.
    @example(settings=[None, None], bitrates=[0, 245_000],
             formats=["FLAC", "MP3"])
    def test_the_badge_is_the_projections_own_answer(
        self,
        settings: list[str | None],
        bitrates: list[int],
        formats: list[str],
    ) -> None:
        count = min(len(settings), len(bitrates), len(formats))
        identity = ReleaseIdentity(
            source="musicbrainz", release_id=MB_RELEASE)
        current = CurrentBeetsUnique(
            identity=identity,
            album_id=1,
            album_path="/music/projection",
            items=tuple(
                CurrentBeetsItem(
                    id=index,
                    path=f"/music/projection/{index:02d}.mp3",
                    format=formats[index],
                    bitrate=bitrates[index],
                    encoder_settings=settings[index],
                )
                for index in range(count)
            ),
            selectors=(f"mb_albumid:{MB_RELEASE}",),
        )
        cfg = QualityRankConfig.defaults()
        info = album_info_from_current(current, cfg)
        # ``info`` is None exactly when no item carries a usable bitrate.
        # The band is then codec-only, through the same shared helper both
        # production surfaces use — asserting the two AGREE, which is the
        # invariant, rather than asserting a value derived here.
        projected = compute_library_rank(
            rank_format_for_current(current, info, cfg),
            info.avg_bitrate_kbps if info is not None else 0,
            cfg,
        )

        violations = banding_projection_violations(
            band=band_current_resolutions({identity: current}, cfg)[
                identity.release_id],
            projected_band=projected,
            context=f"long-tail surface: {current.items!r}",
        )
        # The OTHER badge surface, through the real BeetsDB adapter — the
        # rule that "agree by construction stops at the outermost real
        # adapter" (.claude/rules/code-quality.md). The long-tail path above
        # never touches ``check_mbids_detail``, so a divergence reintroduced
        # there would be invisible to it.
        violations += _browse_surface_violations(current, projected, cfg)
        self.assertEqual(violations, [])

    def test_projection_checker_trips_on_a_divergent_band(self) -> None:
        """Known-bad self-test for the checker's one clause."""
        violations = banding_projection_violations(
            band="good", projected_band="transparent", context="planted",
        )
        self.assertEqual(len(violations), 1, violations)
        self.assertIn("where the decision projection says", violations[0])

    def test_projection_checker_is_silent_when_they_agree(self) -> None:
        self.assertEqual(
            banding_projection_violations(
                band="excellent", projected_band="excellent",
                context="agreeing",
            ),
            [],
        )


class TestCurrentBeetsBandingGenerated(unittest.TestCase):
    @given(
        mp3_bitrate=st.integers(min_value=64_000, max_value=320_000),
        flac_bitrate=st.integers(min_value=500_000, max_value=2_000_000),
    )
    @example(mp3_bitrate=256_000, flac_bitrate=1_100_000)
    def test_mixed_format_precedence_is_item_order_invariant(
        self,
        mp3_bitrate: int,
        flac_bitrate: int,
    ) -> None:
        cfg = QualityRankConfig.defaults()
        # Production's reduction, so the expectation cannot drift from the
        # projection it is compared against. This warning came due: moving
        # the MP3 band edges (#1145) put a rank boundary exactly where floor
        # and round disagree, and ``_band_current_unique``'s own local
        # float-truncate — the last unconverted copy — went red here. It now
        # calls the same ``kbps_from_bps``.
        average_kbps = kbps_from_bps((mp3_bitrate + flac_bitrate) // 2)
        expected = compute_library_rank("MP3", average_kbps, cfg)
        bands: list[str] = []
        for flac_first in (True, False):
            identity, current = _mixed_format_current(
                flac_first=flac_first,
                mp3_bitrate=mp3_bitrate,
                flac_bitrate=flac_bitrate,
            )
            bands.append(band_current_resolutions(
                {identity: current},
                cfg,
            )[identity.release_id])

        assert_mixed_format_order_invariant(bands, expected_band=expected)

    @given(states=st.lists(
        st.sampled_from(RESOLUTION_STATES),
        min_size=1,
        max_size=12,
    ))
    @example(states=["missing", "unique", *AMBIGUITY_REASONS])
    def test_missing_unique_and_every_ambiguity_reason_are_distinct(
        self,
        states: list[str],
    ) -> None:
        resolutions = _resolution_world(states)
        try:
            outcome: dict[str, str] | CurrentBeetsBandingAmbiguityError = (
                band_current_resolutions(
                    resolutions,
                    QualityRankConfig.defaults(),
                )
            )
        except CurrentBeetsBandingAmbiguityError as exc:
            outcome = exc

        assert_current_resolution_banding(resolutions, outcome)

    def test_every_ambiguity_reason_fails_loudly_without_a_payload(self) -> None:
        for reason in AMBIGUITY_REASONS:
            with self.subTest(reason=reason):
                resolutions = _resolution_world([reason])
                with self.assertRaises(
                    CurrentBeetsBandingAmbiguityError,
                ) as raised:
                    band_current_resolutions(
                        resolutions,
                        QualityRankConfig.defaults(),
                    )
                ambiguity = next(iter(resolutions.values()))
                self.assertEqual(raised.exception.ambiguities, (ambiguity,))

    def test_checker_rejects_ambiguity_as_missing_mutant(self) -> None:
        resolutions = _resolution_world(["multiple_matches"])
        identity = next(iter(resolutions))
        mutant_payload = {identity.release_id: BAND_MISSING}

        with self.assertRaisesRegex(AssertionError, "produced a band payload"):
            assert_current_resolution_banding(resolutions, mutant_payload)

    def test_checker_rejects_first_item_format_mutant(self) -> None:
        cfg = QualityRankConfig.defaults()
        expected = compute_library_rank("MP3", 678, cfg)
        first_item_mutant = [
            compute_library_rank("FLAC", 678, cfg),
            expected,
        ]

        with self.assertRaisesRegex(AssertionError, "changed with item order"):
            assert_mixed_format_order_invariant(
                first_item_mutant,
                expected_band=expected,
            )


if __name__ == "__main__":
    unittest.main()
