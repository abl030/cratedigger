"""Generated exact-identity laws for the long-tail worklist."""

from __future__ import annotations

import errno
import sqlite3
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
    compute_library_rank,
)
from lib.beets_db import (
    CurrentBeetsAmbiguityReason,
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.long_tail_service import list_long_tail
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


def _selected_release_ids(rows: list[dict[str, object]]) -> list[str]:
    """The acquisition release id each banded row is keyed by (#1059).

    band_fn receives request rows so each can resolve over its own identity
    union; the selection this property asserts on is still the exact
    acquisition identity per row.
    """
    keys: list[str] = []
    for row in rows:
        identity = ReleaseIdentity.from_strict_fields(
            row.get("mb_release_id"), row.get("discogs_release_id"),
        )
        assert identity is not None
        keys.append(identity.release_id)
    return keys


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
                    bitrate=256_000,
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

        def band_fn(banded_rows: list[dict[str, object]]) -> dict[str, str]:
            ids = _selected_release_ids(banded_rows)
            batches.append(ids)
            return {release_id: "good" for release_id in ids}

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

        def failed_band_fn(banded_rows: list[dict[str, object]]) -> dict[str, str]:
            batches.append(_selected_release_ids(banded_rows))
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
        average_kbps = int((mp3_bitrate + flac_bitrate) / 2 / 1000)
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
