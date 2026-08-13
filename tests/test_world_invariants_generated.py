"""Generated properties for the cross-engine world invariant bank (#743)."""

from __future__ import annotations

import os
import tempfile
import unittest

import msgspec
from hypothesis import assume, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads active profile)
from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsUnique,
)
from lib.quality import dispatch_action
from lib.quality.decisions import post_import_search_action_if_known
from lib.release_identity import ReleaseIdentity
from lib.world_audit_service import WorldAuditCounts, build_world_audit_report
from lib.world_invariants import (
    WORLD_VIOLATION_BUCKETS,
    DenylistAuthoritySnapshot,
    EvidenceDiskSnapshot,
    LibraryAlbumSnapshot,
    LifecycleTransitionSnapshot,
    RequestMembershipSnapshot,
    WorldViolation,
    check_denylist_authority,
    check_evidence_disk_coherence,
    check_folder_exclusivity,
    check_library_filesystem,
    check_library_root_containment,
    check_no_lossy_tier_widening,
    check_proof_lock_terminality,
    check_status_membership,
    derive_denylist_authorities,
    world_violation_bucket,
)

_SEGMENT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters=("/", "\x00"),
    ),
    min_size=1,
    max_size=20,
)


def _decision_denylists_for_test(decision: str) -> bool:
    search_action = post_import_search_action_if_known(decision)
    return bool(
        (search_action is not None and search_action.denylist)
        or dispatch_action(decision).denylist
    )


def _identity(release_id: str) -> ReleaseIdentity:
    identity = ReleaseIdentity.from_id(release_id)
    if identity is not None:
        return identity
    return ReleaseIdentity(source="musicbrainz", release_id=release_id)


def _unique(release_id: str, album_id: int, folder: str) -> CurrentBeetsUnique:
    identity = _identity(release_id)
    return CurrentBeetsUnique(
        identity=identity,
        album_id=album_id,
        album_path=folder,
        items=(CurrentBeetsItem(
            id=album_id * 100,
            path=os.path.join(folder, "01 Track.flac"),
        ),),
        selectors=(f"mb_albumid:{release_id}",),
    )


class TestWorldInvariantGenerated(unittest.TestCase):
    @given(
        codes=st.lists(
            st.sampled_from(tuple(WORLD_VIOLATION_BUCKETS)),
            min_size=0,
            max_size=40,
        ),
        reverse=st.booleans(),
    )
    def test_every_generated_finding_is_grouped_once_by_owner(
        self,
        codes: list[str],
        reverse: bool,
    ) -> None:
        violations = [
            WorldViolation(code=code, detail=f"{index:03d}:{code}")
            for index, code in enumerate(codes)
        ]
        if reverse:
            violations.reverse()

        report = build_world_audit_report(
            counts=WorldAuditCounts(0, 0, 0, 0),
            violations=tuple(violations),
        )
        grouped = tuple(
            member
            for group in (report.groups.a, report.groups.b, report.groups.c)
            for member in group.members
        )

        self.assertCountEqual(grouped, violations)
        for bucket, group in (
            ("A", report.groups.a),
            ("B", report.groups.b),
            ("C", report.groups.c),
        ):
            expected = tuple(sorted(
                (
                    item for item in violations
                    if world_violation_bucket(item.code) == bucket
                ),
                key=lambda item: (
                    item.code,
                    item.request_id or -1,
                    item.release_id or "",
                    item.album_ids,
                    item.detail,
                ),
            ))
            self.assertEqual(group.members, expected)
            self.assertEqual(group.count, len(group.members))

    @given(release_ids=st.lists(_SEGMENT, min_size=1, max_size=8, unique=True))
    def test_unique_release_folders_are_coherent(self, release_ids: list[str]) -> None:
        albums: list[LibraryAlbumSnapshot] = []
        requests: list[RequestMembershipSnapshot] = []
        for index, release_id in enumerate(release_ids, start=1):
            folder = os.path.join("/library", f"album-{index}")
            albums.append(LibraryAlbumSnapshot(
                album_id=index,
                release_id=release_id,
                album_path=folder,
                item_paths=(os.path.join(folder, "01 Track.flac"),),
            ))
            requests.append(RequestMembershipSnapshot(
                request_id=index,
                release_id=release_id,
                status="imported",
            ))

        self.assertEqual(check_folder_exclusivity(tuple(albums)), ())
        self.assertEqual(
            check_status_membership(
                tuple(requests),
                {
                    release_id: _unique(release_id, index, os.path.join(
                        "/library", f"album-{index}",
                    ))
                    for index, release_id in enumerate(release_ids, start=1)
                },
            ),
            (),
        )

    @given(
        release_a=_SEGMENT,
        release_b=_SEGMENT.filter(lambda value: bool(value)),
        folder=_SEGMENT,
    )
    def test_any_shared_folder_is_rejected(
        self,
        release_a: str,
        release_b: str,
        folder: str,
    ) -> None:
        shared = os.path.join("/library", folder)
        violations = check_folder_exclusivity((
            LibraryAlbumSnapshot(1, release_a, shared, (os.path.join(shared, "1.flac"),)),
            LibraryAlbumSnapshot(2, release_b, shared, (os.path.join(shared, "2.flac"),)),
        ))

        self.assertIn("folder_shared", {v.code for v in violations})

    @given(
        album_id=st.integers(min_value=1),
        release_id=_SEGMENT,
        folder=_SEGMENT,
    )
    def test_any_empty_album_is_rejected(
        self,
        album_id: int,
        release_id: str,
        folder: str,
    ) -> None:
        violations = check_folder_exclusivity((LibraryAlbumSnapshot(
            album_id,
            release_id,
            os.path.join("/library", folder),
            (),
        ),))

        self.assertIn("album_empty", {v.code for v in violations})

    @given(release_id=_SEGMENT, folder=_SEGMENT)
    def test_any_missing_physical_album_is_rejected(
        self,
        release_id: str,
        folder: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            missing = os.path.join(tmpdir, f"missing-{folder}")
            violations = check_library_filesystem((LibraryAlbumSnapshot(
                1,
                release_id,
                missing,
                (os.path.join(missing, "01 Track.flac"),),
            ),))

        self.assertIn("album_folder_missing", {v.code for v in violations})
        self.assertIn("album_item_missing", {v.code for v in violations})

    @given(release_id=_SEGMENT, outside_segment=_SEGMENT)
    def test_any_album_wholly_outside_the_library_root_is_rejected(
        self,
        release_id: str,
        outside_segment: str,
    ) -> None:
        """Issue #1089: a beets album whose folder AND items never reached
        the library root (killed automation import crash debris) must
        always be reported — a disjoint top-level tree can never overlap
        the root regardless of the generated segment's content."""
        root = "/library/Beets"
        outside = os.path.join("/processing/albums", outside_segment)
        violations = check_library_root_containment((LibraryAlbumSnapshot(
            1,
            release_id,
            outside,
            (os.path.join(outside, "01 Track.flac"),),
        ),), library_root=root)

        codes = {v.code for v in violations}
        self.assertIn("album_folder_outside_library_root", codes)
        self.assertIn("album_item_outside_library_root", codes)

    @given(release_id=_SEGMENT, inside_segment=_SEGMENT)
    def test_any_album_wholly_inside_the_library_root_is_accepted(
        self,
        release_id: str,
        inside_segment: str,
    ) -> None:
        """Must-still-work: the false-positive direction. An ordinary
        installed album, wherever it lives under the root, never trips."""
        root = "/library/Beets"
        inside = os.path.join(root, inside_segment)
        violations = check_library_root_containment((LibraryAlbumSnapshot(
            1,
            release_id,
            inside,
            (os.path.join(inside, "01 Track.flac"),),
        ),), library_root=root)

        self.assertEqual(violations, ())

    @given(
        release_id=_SEGMENT,
        inside_segment=_SEGMENT,
        outside_segment=_SEGMENT,
    )
    def test_partially_moved_item_alone_is_rejected(
        self,
        release_id: str,
        inside_segment: str,
        outside_segment: str,
    ) -> None:
        """The partially-moved world: the album folder itself is under the
        root, but one item already escaped it. Item-level clause fires
        alone — the folder-level clause must not (Q1, isolated)."""
        root = "/library/Beets"
        inside = os.path.join(root, inside_segment)
        outside = os.path.join("/processing/albums", outside_segment)
        violations = check_library_root_containment((LibraryAlbumSnapshot(
            1,
            release_id,
            inside,
            (
                os.path.join(inside, "01 Track.flac"),
                os.path.join(outside, "02 Track.flac"),
            ),
        ),), library_root=root)

        self.assertEqual(
            {v.code for v in violations},
            {"album_item_outside_library_root"},
        )

    @given(
        release_id=_SEGMENT,
        folder=_SEGMENT,
    )
    def test_library_root_containment_is_silent_without_a_configured_root(
        self,
        release_id: str,
        folder: str,
    ) -> None:
        """Must-still-work: an unconfigured root can prove nothing, so it
        reports nothing — never a false accusation on every album."""
        outside = os.path.join("/processing/albums", folder)
        violations = check_library_root_containment((LibraryAlbumSnapshot(
            1,
            release_id,
            outside,
            (os.path.join(outside, "01 Track.flac"),),
        ),), library_root="")

        self.assertEqual(violations, ())

    @given(
        release_id=_SEGMENT,
    )
    def test_imported_without_exact_release_is_always_rejected(
        self,
        release_id: str,
    ) -> None:
        identity = _identity(release_id)
        violations = check_status_membership((
            RequestMembershipSnapshot(
                1,
                release_id,
                "imported",
            ),
        ), {release_id: CurrentBeetsMissing(identity=identity)})

        self.assertIn("current_beets_missing", {v.code for v in violations})

    @given(
        request_id=st.integers(min_value=1),
        release_id=_SEGMENT,
        album_ids=st.lists(
            st.integers(min_value=1), min_size=2, max_size=6, unique=True,
        ),
        status=st.sampled_from(("wanted", "unsearchable", "imported")),
    )
    def test_any_typed_ambiguity_is_rejected(
        self,
        request_id: int,
        release_id: str,
        album_ids: list[int],
        status: str,
    ) -> None:
        identity = _identity(release_id)
        violations = check_status_membership((RequestMembershipSnapshot(
            request_id=request_id,
            release_id=release_id,
            status=status,
        ),), {
            release_id: CurrentBeetsAmbiguous(
                identity=identity,
                album_ids=tuple(sorted(album_ids)),
                reason="multiple_matches",
            ),
        })

        self.assertIn("current_beets_ambiguous", {v.code for v in violations})

    @given(
        request_id=st.integers(min_value=1),
        release_id=_SEGMENT,
        historical_path=_SEGMENT,
        current_path=_SEGMENT,
        fingerprint=_SEGMENT,
    )
    def test_historical_evidence_path_never_invalidates_current_fingerprint(
        self,
        request_id: int,
        release_id: str,
        historical_path: str,
        current_path: str,
        fingerprint: str,
    ) -> None:
        violations = check_evidence_disk_coherence((EvidenceDiskSnapshot(
            request_id=request_id,
            release_id=release_id,
            status="imported",
            album_path=os.path.join("/library", current_path),
            current_evidence_id=1,
            evidence_id=1,
            evidence_release_id=release_id,
            evidence_source_path=os.path.join("/historical", historical_path),
            evidence_fingerprint=fingerprint,
            actual_fingerprint=fingerprint,
        ),))

        self.assertEqual(violations, ())

    @given(
        request_id=st.integers(min_value=1),
        release_id=_SEGMENT,
        evidence_fingerprint=_SEGMENT,
        actual_fingerprint=_SEGMENT,
    )
    def test_any_evidence_fingerprint_drift_is_rejected(
        self,
        request_id: int,
        release_id: str,
        evidence_fingerprint: str,
        actual_fingerprint: str,
    ) -> None:
        assume(evidence_fingerprint != actual_fingerprint)
        violations = check_evidence_disk_coherence((EvidenceDiskSnapshot(
            request_id=request_id,
            release_id=release_id,
            status="imported",
            album_path="/library/A",
            current_evidence_id=1,
            evidence_id=1,
            evidence_release_id=release_id,
            evidence_source_path="/library/A",
            evidence_fingerprint=evidence_fingerprint,
            actual_fingerprint=actual_fingerprint,
        ),))

        self.assertIn("evidence_fingerprint_mismatch", {v.code for v in violations})

    @given(
        request_id=st.integers(min_value=1),
        operation=st.sampled_from(("upgrade_import", "force_import")),
        after_status=st.sampled_from(("wanted", "unsearchable", "replaced")),
    )
    def test_any_automated_proof_lock_status_change_is_rejected(
        self,
        request_id: int,
        operation: str,
        after_status: str,
    ) -> None:
        transition = LifecycleTransitionSnapshot(
            request_id=request_id,
            operation=operation,
            before_status="imported",
            after_status=after_status,
            before_release_id="release-a",
            after_release_id="release-a",
            before_override=None,
            after_override=None,
            before_album_fingerprint="sha256:a",
            after_album_fingerprint="sha256:a",
            before_verified_lossless=True,
        )

        self.assertIn(
            "proof_lock_broken",
            {v.code for v in check_proof_lock_terminality((transition,))},
        )

    @given(
        request_id=st.integers(min_value=1),
        after_status=st.sampled_from(("wanted", "unsearchable")),
        after_override=st.one_of(st.none(), _SEGMENT.filter(lambda v: v != "lossless")),
    )
    def test_any_searchable_lossless_widening_is_rejected(
        self,
        request_id: int,
        after_status: str,
        after_override: str | None,
    ) -> None:
        transition = LifecycleTransitionSnapshot(
            request_id=request_id,
            operation="reset_to_wanted",
            before_status="wanted",
            after_status=after_status,
            before_release_id="release-a",
            after_release_id="release-a",
            before_override="lossless",
            after_override=after_override,
            before_album_fingerprint="sha256:a",
            after_album_fingerprint="sha256:a",
        )

        self.assertIn(
            "lossy_tier_widened",
            {v.code for v in check_no_lossy_tier_widening((transition,))},
        )

    @given(request_id=st.integers(min_value=1), username=_SEGMENT)
    def test_any_unauthorized_denylist_row_is_rejected(
        self,
        request_id: int,
        username: str,
    ) -> None:
        violations = check_denylist_authority((DenylistAuthoritySnapshot(
            request_id=request_id,
            username=username,
        ),))

        self.assertIn("denylist_without_authority", {v.code for v in violations})

    @given(
        denied_username=_SEGMENT,
        history_username=_SEGMENT,
        scenario=_SEGMENT,
        as_jsonb=st.booleans(),
        valid=st.one_of(st.none(), st.booleans()),
        canonical_reason=st.booleans(),
    )
    def test_multi_peer_validation_authority_requires_rejection_provenance(
        self,
        denied_username: str,
        history_username: str,
        scenario: str,
        as_jsonb: bool,
        valid: bool | None,
        canonical_reason: bool,
    ) -> None:
        payload = {"valid": valid, "scenario": scenario}
        validation_result: object = (
            msgspec.json.encode(payload).decode()
            if not as_jsonb
            else payload
        )

        authorities = derive_denylist_authorities(
            username=denied_username,
            reason=(
                "beets validation rejected"
                if canonical_reason
                else "manual note"
            ),
            history=[{
                "outcome": "rejected",
                "soulseek_username": history_username,
                "validation_result": validation_result,
            }],
        )

        expected = (
            valid is False and denied_username == history_username
        ) or (
            canonical_reason and valid is not True
        )
        self.assertEqual("validation_reject" in authorities, expected)

    @given(
        username=_SEGMENT,
        decision=st.sampled_from((
            "downgrade",
            "audio_corrupt",
            "bad_audio_hash",
            "spectral_reject",
            "mixed_source",
            "nested_layout",
            "empty_fileset",
            "requeue_lossless",
            "requeue_upgrade",
            "transcode_upgrade",
        )),
    )
    def test_preview_reason_authority_follows_current_denylist_policy(
        self,
        username: str,
        decision: str,
    ) -> None:
        expected = _decision_denylists_for_test(decision)

        authorities = derive_denylist_authorities(
            username=username,
            reason=f"import preview rejected: {decision}",
            history=[],
        )

        self.assertEqual(decision in authorities, expected)

    @given(
        denied_username=_SEGMENT,
        history_username=_SEGMENT,
        decision=st.sampled_from((
            "downgrade",
            "audio_corrupt",
            "spectral_reject",
            "mixed_source",
            "nested_layout",
            "empty_fileset",
        )),
        canonical_reason=st.booleans(),
    )
    def test_multi_peer_import_authority_requires_canonical_source_reason(
        self,
        denied_username: str,
        history_username: str,
        decision: str,
        canonical_reason: bool,
    ) -> None:
        authorities = derive_denylist_authorities(
            username=denied_username,
            reason=(
                "beets validation rejected"
                if canonical_reason
                else "manual note"
            ),
            history=[{
                "outcome": "rejected",
                "soulseek_username": history_username,
                "validation_result": {"valid": True},
                "import_result": {"version": 4, "decision": decision},
            }],
        )
        expected = _decision_denylists_for_test(decision) and (
            canonical_reason or denied_username == history_username
        )

        self.assertEqual(decision in authorities, expected)


if __name__ == "__main__":
    unittest.main()
