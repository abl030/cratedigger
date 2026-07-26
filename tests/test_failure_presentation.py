#!/usr/bin/env python3
"""Deterministic pins for lib/failure_presentation.py (issue #868 PR2).

Each pin here has a generated twin in
``tests/test_failure_presentation_generated.py`` — the pin proves the exact
live scenario, the property patrols the world around it.

The anchor scenario is download_log 38272 (Beefeater — *Plays For Lovers &
House Burning Down*, 2026-07-25): 29 transfer records, one peer
``Tymemage``, every one ``Completed, Rejected`` with ``bytes_transferred=0``,
``retry_count=0`` and the exact peer exception ``Verification required``.
The row rendered as ``Download failed: all 29 files errored — 29×
'Verification required'``, which reads like Cratedigger's own validation
failing rather than a peer refusing to upload.
"""

import os
import sys
import unittest

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import lib.download_materialization as materialization
from lib.failure_presentation import (
    FAMILY_LOCAL_STORAGE,
    FAMILY_PEER_FILE,
    FAMILY_REFUSAL,
    FAMILY_TRANSPORT,
    FAMILY_UNKNOWN,
    MAX_RAW_MESSAGE_CHARS,
    TRANSFER_MESSAGE_LABEL_MIXED,
    TRANSFER_MESSAGE_LABEL_PEER,
    TRANSFER_MESSAGE_LABEL_STORAGE,
    FailureEvidence,
    bounded_text,
    decode_transfer_detail,
    materialize_reason_copy,
    peer_failure_family,
    present_failure,
)
from lib.quality import FileFailureDetail
from web.classify import LogEntry, classify_log_entry


# ---------------------------------------------------------------------------
# Live-shaped fixtures
# ---------------------------------------------------------------------------

def _transfer_row(
    username: str,
    filename: str,
    *,
    last_state: str = "Completed, Rejected",
    last_exception: str | None = None,
    bytes_transferred: int = 0,
    retry_count: int = 0,
) -> dict[str, object]:
    """One ``download_log.transfer_detail`` element, production shape."""
    return {
        "username": username,
        "filename": filename,
        "last_state": last_state,
        "last_exception": last_exception,
        "bytes_transferred": bytes_transferred,
        "retry_count": retry_count,
    }


def _log_38272_transfer_detail() -> list[dict[str, object]]:
    return [
        _transfer_row(
            "Tymemage",
            f"@@share\\Beefeater\\Plays For Lovers\\{index:02d} - Track.flac",
            last_exception="Verification required",
        )
        for index in range(1, 30)
    ]


LOG_38272_ERROR_MESSAGE = "all 29 files errored — 29× 'Verification required'"


def _evidence(
    *,
    outcome: str = "timeout",
    error_message: str | None = None,
    beets_detail: str | None = None,
    beets_scenario: str | None = None,
    soulseek_username: str | None = None,
    transfer_detail: tuple[FileFailureDetail, ...] = (),
) -> FailureEvidence:
    return FailureEvidence(
        outcome=outcome,
        error_message=error_message,
        beets_detail=beets_detail,
        beets_scenario=beets_scenario,
        soulseek_username=soulseek_username,
        transfer_detail=transfer_detail,
    )


# ---------------------------------------------------------------------------
# The anchor pin
# ---------------------------------------------------------------------------

class TestDownloadLog38272(unittest.TestCase):
    """The exact live row this issue was filed against."""

    def test_verdict_names_the_peer_the_refusal_and_the_zero_bytes(self):
        presentation = present_failure(_evidence(
            error_message=LOG_38272_ERROR_MESSAGE,
            soulseek_username="Tymemage",
            transfer_detail=decode_transfer_detail(_log_38272_transfer_detail()),
        ))

        self.assertEqual(
            presentation.verdict,
            'Peer Tymemage rejected all 29 files before transfer '
            '— "Verification required"',
        )
        self.assertEqual(
            presentation.transfer_message, '29× "Verification required"')
        self.assertEqual(
            presentation.transfer_message_label, TRANSFER_MESSAGE_LABEL_PEER)

    def test_rendering_leaves_the_persisted_evidence_byte_identical(self):
        raw = _log_38272_transfer_detail()
        before = msgspec.json.encode(raw)

        presentation = present_failure(_evidence(
            error_message=LOG_38272_ERROR_MESSAGE,
            soulseek_username="Tymemage",
            transfer_detail=decode_transfer_detail(raw),
        ))

        self.assertIsNotNone(presentation.verdict)
        self.assertEqual(msgspec.json.encode(raw), before)
        # And the raw peer string still reaches the operator verbatim.
        self.assertIn("Verification required", presentation.transfer_message or "")

    def test_classified_entry_carries_the_humane_verdict_and_raw_text(self):
        entry = LogEntry(
            id=38272,
            request_id=2785,
            outcome="timeout",
            soulseek_username="Tymemage",
            error_message=LOG_38272_ERROR_MESSAGE,
            transfer_detail=_log_38272_transfer_detail(),
        )
        before = msgspec.json.encode(entry.transfer_detail)

        classified = classify_log_entry(entry)

        self.assertEqual(classified.badge, "Failed")
        self.assertEqual(
            classified.verdict,
            'Peer Tymemage rejected all 29 files before transfer '
            '— "Verification required"',
        )
        self.assertEqual(
            classified.transfer_message, '29× "Verification required"')
        self.assertEqual(
            classified.transfer_message_label, TRANSFER_MESSAGE_LABEL_PEER)
        # The list-row summary attributes the peer once, not twice.
        self.assertEqual(classified.summary, classified.verdict)
        # Presentation is a read: the row's audit blob is untouched.
        self.assertEqual(msgspec.json.encode(entry.transfer_detail), before)
        self.assertEqual(entry.error_message, LOG_38272_ERROR_MESSAGE)


# ---------------------------------------------------------------------------
# The family classifier
# ---------------------------------------------------------------------------

class TestPeerFailureFamily(unittest.TestCase):
    """Every message in the issue's 45-day live census."""

    CASES = [
        # --- refusal before transfer ---
        ("Verification required", FAMILY_REFUSAL),
        ("Transfer rejected: File not shared.", FAMILY_REFUSAL),
        ("Transfer rejected: Too many files", FAMILY_REFUSAL),
        ("Too many files", FAMILY_REFUSAL),
        ("Transfer rejected: Too many megabytes", FAMILY_REFUSAL),
        ("Too many megabytes", FAMILY_REFUSAL),
        ("Transfer rejected: Banned", FAMILY_REFUSAL),
        ("Transfer rejected: Banned (Country banned)", FAMILY_REFUSAL),
        ("Transfer rejected: Overwhelmed with requests; try again later.",
         FAMILY_REFUSAL),
        ("Pending shutdown.", FAMILY_REFUSAL),
        ("Completed, Rejected", FAMILY_REFUSAL),
        # --- transport / connection ---
        ("Inactivity timeout of 15000 milliseconds was reached",
         FAMILY_TRANSPORT),
        ("Transfer failed: Read error: Remote connection closed",
         FAMILY_TRANSPORT),
        ("Transfer failed: Read error: Unable to read data from the "
         "transport connection", FAMILY_TRANSPORT),
        ("Download reported as failed by remote client", FAMILY_TRANSPORT),
        ("Download of c:\\music\\a\\b.mp3 reported as failed by jaswal",
         FAMILY_TRANSPORT),
        ("The wait timed out after 30000 milliseconds", FAMILY_TRANSPORT),
        ("Failed to establish a direct or indirect transfer connection to "
         "KingKaNeN (1.2.3.4:5678)", FAMILY_TRANSPORT),
        ('enqueue failed: "The wait timed out after 5000 milliseconds"',
         FAMILY_TRANSPORT),
        ("Application shut down", FAMILY_TRANSPORT),
        ("A task was canceled.", FAMILY_TRANSPORT),
        ("The operation was canceled.", FAMILY_TRANSPORT),
        ("Failed to read 16384 bytes from 1.2.3.4:5: Remote connection closed",
         FAMILY_TRANSPORT),
        ("Download failed to enqueue remotely after hard time limit of 60 secs",
         FAMILY_TRANSPORT),
        ("Completed, TimedOut", FAMILY_TRANSPORT),
        ("Completed, Cancelled", FAMILY_TRANSPORT),
        # --- peer-side file problem ---
        ("File read error.", FAMILY_PEER_FILE),
        ("Transfer aborted: the remote size of 100 does not match expected "
         "size 200", FAMILY_PEER_FILE),
        # --- local storage (ours, not the peer's) ---
        ("Failed to create file 01 - x.flac: Stale file handle : "
         "'/mnt/virtio/music/slskd/incomplete/y'", FAMILY_LOCAL_STORAGE),
        ("Failed to create file 01 - x.flac: Could not find a part of the "
         "path '/mnt/virtio/music/slskd/incomplete/y'", FAMILY_LOCAL_STORAGE),
        ("Could not find a part of the path.", FAMILY_LOCAL_STORAGE),
        # --- unknown ---
        ("", FAMILY_UNKNOWN),
        ("something a peer invented", FAMILY_UNKNOWN),
        ("Completed, Errored", FAMILY_UNKNOWN),
    ]

    def test_family_table(self):
        for message, expected in self.CASES:
            with self.subTest(message=message):
                self.assertEqual(peer_failure_family(message), expected)

    def test_matching_is_case_and_whitespace_insensitive(self):
        self.assertEqual(
            peer_failure_family("  VERIFICATION REQUIRED  "), FAMILY_REFUSAL)

    def test_none_is_unknown(self):
        self.assertEqual(peer_failure_family(None), FAMILY_UNKNOWN)


# ---------------------------------------------------------------------------
# Per-family copy
# ---------------------------------------------------------------------------

class TestPeerFamilyCopy(unittest.TestCase):

    def _present(
        self,
        details: list[dict[str, object]],
        *,
        username: str | None = "bob",
    ):
        return present_failure(_evidence(
            error_message="all files errored",
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(details),
        ))

    def test_transport_zero_bytes_says_before_any_data_arrived(self):
        presentation = self._present([
            _transfer_row(
                "bob", f"f{i}", last_state="Completed, Errored",
                last_exception="Inactivity timeout of 15000 milliseconds "
                               "was reached")
            for i in range(4)
        ])
        self.assertEqual(
            presentation.verdict,
            'Transfer from peer bob failed before any data arrived — '
            '"Inactivity timeout of 15000 milliseconds was reached"',
        )

    def test_transport_with_bytes_says_dropped_mid_download(self):
        presentation = self._present([
            _transfer_row(
                "bob", f"f{i}", last_state="Completed, Errored",
                last_exception="Transfer failed: Read error: Remote "
                               "connection closed",
                bytes_transferred=4096)
            for i in range(2)
        ])
        self.assertEqual(
            presentation.verdict,
            'Transfer from peer bob dropped mid-download — '
            '"Transfer failed: Read error: Remote connection closed"',
        )

    def test_peer_file_problem_blames_the_peers_own_files(self):
        presentation = self._present([
            _transfer_row(
                "bob", f"f{i}", last_state="Completed, Errored",
                last_exception="File read error.")
            for i in range(12)
        ])
        self.assertEqual(
            presentation.verdict,
            'Peer bob could not read 12 of its own files — "File read error."',
        )

    def test_local_storage_is_never_attributed_to_the_peer(self):
        """The highest-value correction in the issue: our storage, our fault."""
        presentation = self._present([
            _transfer_row(
                "bob", f"f{i}", last_state="Completed, Errored",
                last_exception="Failed to create file 0{}.flac: Stale file "
                               "handle : '/mnt/virtio/music/slskd/incomplete/x'"
                               .format(i))
            for i in range(3)
        ])
        verdict = presentation.verdict or ""
        self.assertTrue(verdict.startswith("Local storage error writing 3 files"))
        self.assertNotIn("bob", verdict)
        self.assertNotIn("peer", verdict.lower())
        self.assertEqual(
            presentation.transfer_message_label, TRANSFER_MESSAGE_LABEL_STORAGE)

    def test_mixed_families_across_peers_count_instead_of_quoting(self):
        details = [
            _transfer_row("alice", f"a{i}", last_exception="Verification required")
            for i in range(20)
        ] + [
            _transfer_row(
                f"peer{i}", f"b{i}", last_state="Completed, Errored",
                last_exception="Transfer failed: Read error: Remote "
                               "connection closed")
            for i in range(3)
        ]
        presentation = self._present(details)
        self.assertEqual(
            presentation.verdict,
            "23 files failed across 4 peers — "
            "20 rejected before transfer, 3 connection lost",
        )
        self.assertEqual(
            presentation.transfer_message,
            '20× "Verification required"; '
            '3× "Transfer failed: Read error: Remote connection closed"',
        )

    def test_unknown_text_is_quoted_bounded_and_attributed(self):
        raw = "peer said " + ("x" * 400) + "\nsecond line"
        presentation = self._present([
            _transfer_row("bob", f"f{i}", last_exception=raw)
            for i in range(29)
        ])
        verdict = presentation.verdict or ""
        self.assertTrue(verdict.startswith('Peer bob failed all 29 files — "'))
        self.assertNotIn("\n", verdict)
        self.assertIn("\u2026", verdict)
        self.assertLess(len(verdict), 200)

    def test_more_than_three_reasons_are_summarised(self):
        details = [
            _transfer_row("bob", f"f{i}", last_exception=f"reason {i}")
            for i in range(5)
        ]
        presentation = self._present(details)
        self.assertTrue(
            (presentation.transfer_message or "").endswith("; +2 more"),
            presentation.transfer_message,
        )

    def test_single_family_extra_reasons_are_counted_not_dropped(self):
        details = [
            _transfer_row("bob", f"a{i}", last_exception="Verification required")
            for i in range(3)
        ] + [
            _transfer_row("bob", "b0", last_exception="Transfer rejected: Banned"),
        ]
        presentation = self._present(details)
        self.assertEqual(
            presentation.verdict,
            'Peer bob rejected all 4 files before transfer — '
            '"Verification required" (+1 other reason)',
        )

    def test_missing_per_file_username_falls_back_to_the_row_peer(self):
        presentation = self._present(
            [_transfer_row("", "f0", last_exception="Verification required")],
            username="rowpeer",
        )
        self.assertEqual(
            presentation.verdict,
            'Peer rowpeer rejected 1 file before transfer — '
            '"Verification required"',
        )

    def test_mixed_families_including_local_storage_use_a_neutral_label(self):
        details = [
            _transfer_row("alice", "a0", last_exception="Verification required"),
            _transfer_row(
                "alice", "a1",
                last_exception="Failed to create file a1: Stale file handle"),
        ]
        presentation = self._present(details)
        self.assertEqual(
            presentation.transfer_message_label, TRANSFER_MESSAGE_LABEL_MIXED)
        self.assertIn("1 local storage error", presentation.verdict or "")

    def test_files_without_evidence_contribute_nothing(self):
        presentation = self._present([
            _transfer_row("bob", "f0", last_state="InProgress"),
            _transfer_row("bob", "f1", last_state="Completed, Succeeded"),
        ])
        self.assertIsNone(presentation.transfer_message)


# ---------------------------------------------------------------------------
# Cratedigger's own download-phase messages
# ---------------------------------------------------------------------------

class TestOwnDownloadMessages(unittest.TestCase):

    CASES = [
        (
            "retry limit keeps only the basename",
            "file exceeded retry limit after 3 retries: d:\\new music\\my "
            "music\\ambient; dark ambient; drone\\05 - The Rooster Moans.flac",
            'Gave up on "05 - The Rooster Moans.flac" after 3 failed attempts',
        ),
        (
            "retry limit with a posix path",
            "file exceeded retry limit after 1 retries: /music/x/y.mp3",
            'Gave up on "y.mp3" after 1 failed attempt',
        ),
        (
            "stalled timeout loses the config token",
            "no download progress for 600s (stalled_timeout 600s)",
            "Transfer stalled — no progress for 10 minutes",
        ),
        (
            "remote queue timeout is named in minutes",
            "remote_queue_timeout 3600s exceeded",
            "Peer never started the transfer — still queued after 60 minutes",
        ),
        (
            "bare all-errored admits it has no reason",
            "all 12 files errored",
            "All 12 files failed; slskd reported no reason",
        ),
        (
            "vanished drops the slskd-restart guess",
            "transfers vanished from slskd before any status was observed "
            "(slskd restart?)",
            "Transfers disappeared from slskd before the download finished",
        ),
        (
            "legacy vanished phrasing",
            "all transfers vanished from slskd",
            "Transfers disappeared from slskd before the download finished",
        ),
        (
            "vanished with last-observed evidence keeps the evidence",
            "transfers no longer in slskd — last observed: 2× 'File read error.'",
            "Transfers disappeared from slskd — last observed: "
            "2× 'File read error.'",
        ),
    ]

    def test_message_table(self):
        for description, message, expected in self.CASES:
            with self.subTest(description):
                presentation = present_failure(
                    _evidence(error_message=message))
                self.assertEqual(presentation.verdict, expected)

    def test_retry_limit_strips_the_appended_evidence_summary(self):
        """Live rows 37535 / 38203 / 37483: ``_enrich_timeout_reason`` appends
        ``— N× '<reason>'`` AFTER the peer's path, so a naive parse produced a
        "filename" made of half a path and someone else's exception text."""
        presentation = present_failure(_evidence(
            error_message=(
                "file exceeded retry limit after 5 retries: "
                "@@jbkaj\\Musique\\Sodastream\\10 - saturday's ash.flac — "
                "3× 'Download reported as failed by remote client', "
                "1× 'A task was canceled.'"
            ),
        ))
        self.assertEqual(
            presentation.verdict,
            'Gave up on "10 - saturday\'s ash.flac" after 5 failed attempts',
        )

    def test_retry_limit_with_an_unusable_path_names_no_file(self):
        """Shrunk from the fuzz tier: a path with no usable segment must not
        fall back to the raw path — that is exactly the leak this copy
        exists to prevent."""
        presentation = present_failure(_evidence(
            error_message="file exceeded retry limit after 1 retries:  \\ ",
        ))
        self.assertEqual(presentation.verdict, "Gave up after 1 failed attempt")

    def test_stall_that_overran_its_threshold_reads_as_minutes(self):
        """Live row 38245: the poll cycle notices at 622s, not at 600s."""
        presentation = present_failure(_evidence(
            error_message="no download progress for 622s (stalled_timeout 600s)"))
        self.assertEqual(
            presentation.verdict,
            "Transfer stalled — no progress for about 10 minutes",
        )

    def test_unmapped_message_is_bounded_but_kept(self):
        presentation = present_failure(_evidence(
            error_message="some future timeout reason"))
        self.assertEqual(
            presentation.verdict, "Download failed: some future timeout reason")

    def test_no_message_at_all(self):
        self.assertEqual(
            present_failure(_evidence()).verdict, "Download failed")

    def test_retry_limit_leads_but_still_names_the_cause(self):
        """Giving up is OUR decision, so it leads — but it is context, not a
        substitute for the cause, which is appended (I6)."""
        presentation = present_failure(_evidence(
            error_message="file exceeded retry limit after 3 retries: "
                          "d:\\x\\05 - The Rooster Moans.flac",
            soulseek_username="bob",
            transfer_detail=decode_transfer_detail([
                _transfer_row(
                    "bob", "05 - The Rooster Moans.flac",
                    last_state="Completed, Errored",
                    last_exception="Download of d:\\x\\05 - The Rooster "
                                   "Moans.flac reported as failed by bob"),
            ]),
        ))
        self.assertEqual(
            presentation.verdict,
            'Gave up on "05 - The Rooster Moans.flac" after 3 failed attempts '
            "— transfer from peer bob failed before any data arrived",
        )
        self.assertIn("reported as failed by bob",
                      presentation.transfer_message or "")

    def test_remote_queue_timeout_leads_but_still_names_the_cause(self):
        presentation = present_failure(_evidence(
            error_message="remote_queue_timeout 3600s exceeded",
            soulseek_username="bob",
            transfer_detail=decode_transfer_detail([
                _transfer_row("bob", "f0", last_exception="Verification required"),
            ]),
        ))
        self.assertEqual(
            presentation.verdict,
            "Peer never started the transfer — still queued after 60 minutes "
            "— peer bob rejected 1 file before transfer",
        )
        self.assertEqual(
            presentation.transfer_message, '1× "Verification required"')


# ---------------------------------------------------------------------------
# Materialize / staging reasons (PR1's persisted evidence)
# ---------------------------------------------------------------------------

class TestDecisionHeadlinesNeverSuppressTheCause(unittest.TestCase):
    """I6, from live-data review of 400 rows.

    Ten of the fourteen local-storage rows rendered as ``Gave up on "05
    Seventeen.flac" after 5 failed attempts`` — a sentence an operator reads
    as "flaky peer, retry it" — while the evidence underneath was our own
    virtiofs share refusing the write (live rows 38203 / 38187 / 38186 /
    38185 / 38184 / 38183 / 38176 / 38173 / 38160 / 38119). Our decision to
    stop retrying is context; the cause is the story.
    """

    def test_live_38203_retry_limit_over_storage_names_the_storage(self):
        presentation = present_failure(_evidence(
            error_message=(
                "file exceeded retry limit after 5 retries: "
                "Master\\~ J ~\\Jimmy Eat World\\[1996] Static Prevails\\"
                "05 Seventeen.flac — 1× 'Failed to create file 05 "
                "Seventeen.flac: Stale file handle : "
                "'/mnt/virtio/music/slskd/incomplete/x''"
            ),
            soulseek_username="Tymemage",
            transfer_detail=decode_transfer_detail([
                _transfer_row(
                    "Tymemage", "05 Seventeen.flac",
                    last_state="Completed, Errored",
                    last_exception=(
                        "Failed to create file 05 Seventeen.flac: Stale file "
                        "handle : '/mnt/virtio/music/slskd/incomplete/x'"
                    )),
            ]),
        ))
        verdict = presentation.verdict or ""
        self.assertEqual(
            verdict,
            'Gave up on "05 Seventeen.flac" after 5 failed attempts '
            "— local storage error writing 1 file",
        )
        self.assertNotIn("Tymemage", verdict)
        self.assertNotIn("peer", verdict.lower())
        self.assertEqual(
            presentation.transfer_message_label, TRANSFER_MESSAGE_LABEL_STORAGE)

    def test_live_38184_pure_storage_evidence_is_never_silent(self):
        """The starkest live row: ten files, all `Could not find a part of
        the path.`, and the old verdict said nothing about storage."""
        presentation = present_failure(_evidence(
            error_message=(
                "file exceeded retry limit after 5 retries: "
                "d:\\music\\x\\03 - Track.flac"
            ),
            soulseek_username="Tymemage",
            transfer_detail=decode_transfer_detail([
                _transfer_row(
                    "Tymemage", f"{index:02d} - Track.flac",
                    last_state="Completed, Errored",
                    last_exception="Could not find a part of the path.")
                for index in range(10)
            ]),
        ))
        verdict = presentation.verdict or ""
        self.assertEqual(
            verdict,
            'Gave up on "03 - Track.flac" after 5 failed attempts '
            "— local storage error writing 10 files",
        )
        self.assertNotIn("peer", verdict.lower())

    def test_live_38283_retry_limit_over_a_refusal_names_the_refusal(self):
        """Same rule, no special case: the peer was refusing, not the
        transfer being flaky."""
        presentation = present_failure(_evidence(
            error_message=(
                "file exceeded retry limit after 5 retries: "
                "@@vdrdb\\_Lossless\\_Temp\\121 - Bob B. Soxx and the Blue "
                "Jeans - Not Too Young To Get Married.flac"
            ),
            soulseek_username="phil",
            transfer_detail=decode_transfer_detail(
                [
                    _transfer_row(
                        "phil", f"{index:03d}.flac",
                        last_exception="Pending shutdown.")
                    for index in range(57)
                ] + [
                    _transfer_row(
                        "phil", "x.flac", last_exception="Too many files"),
                ]
            ),
        ))
        self.assertEqual(
            presentation.verdict,
            'Gave up on "121 - Bob B. Soxx and the Blue Jeans - Not Too Young '
            'To Get Married.flac" after 5 failed attempts '
            "— peer phil rejected all 58 files before transfer",
        )

    def test_decision_headline_over_mixed_families_lists_them(self):
        presentation = present_failure(_evidence(
            error_message="remote_queue_timeout 3600s exceeded",
            soulseek_username="phil",
            transfer_detail=decode_transfer_detail([
                _transfer_row("phil", "a", last_exception="Verification required"),
                _transfer_row(
                    "phil", "b",
                    last_exception="Failed to create file b: Stale file handle"),
            ]),
        ))
        self.assertEqual(
            presentation.verdict,
            "Peer never started the transfer — still queued after 60 minutes "
            "— 1 local storage error, 1 rejected before transfer",
        )

    def test_mixed_evidence_with_storage_drops_the_peer_headline(self):
        """Heading a sentence with one peer while part of the failure was our
        own share is the same fuzzy attribution one step removed."""
        presentation = present_failure(_evidence(
            error_message="all 4 files errored",
            soulseek_username="phil",
            transfer_detail=decode_transfer_detail(
                [_transfer_row(
                    "phil", "a", last_exception="Verification required")]
                + [
                    _transfer_row(
                        "phil", f"b{index}",
                        last_exception=(
                            "Failed to create file b: Stale file handle"))
                    for index in range(3)
                ]
            ),
        ))
        verdict = presentation.verdict or ""
        self.assertIn("local storage error", verdict)
        self.assertNotIn("phil", verdict)
        self.assertNotIn("from peer", verdict)


class TestMaterializeReasonCopy(unittest.TestCase):

    def test_every_reason_constant_has_copy(self):
        """A new PR1 reason cannot ship without operator copy."""
        constants = {
            name: value for name, value in vars(materialization).items()
            if name.startswith("REASON_") and isinstance(value, str)
        }
        self.assertGreater(len(constants), 10)
        for name, value in constants.items():
            with self.subTest(name):
                probe = value + "ESTALE" if name.endswith("_PREFIX") else value
                self.assertIsNotNone(
                    materialize_reason_copy(probe),
                    f"{name} has no operator copy",
                )

    def test_containment_reasons_read_as_a_deliberate_refusal(self):
        for reason in (
            materialization.REASON_UNSAFE_SOURCE_PATH,
            materialization.REASON_SLSKD_ROOT_UNSAFE,
            materialization.REASON_PROCESSING_AUTHORITY_UNSAFE,
        ):
            with self.subTest(reason):
                copy = materialize_reason_copy(reason) or ""
                self.assertTrue(copy.startswith("Refused to import:"), copy)

    def test_storage_errnos_never_read_as_a_security_finding(self):
        for prefix in (
            materialization.REASON_SOURCE_OPEN_FAILED_PREFIX,
            materialization.REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
            materialization.REASON_PROCESSING_OPEN_FAILED_PREFIX,
        ):
            with self.subTest(prefix):
                copy = materialize_reason_copy(prefix + "ESTALE") or ""
                self.assertIn("ESTALE", copy)
                self.assertNotIn("Refused to import:", copy)
                self.assertNotIn("symlink", copy)

    def test_the_three_subjects_keep_their_own_nouns(self):
        self.assertIn(
            "slskd download share",
            materialize_reason_copy(materialization.REASON_SLSKD_ROOT_MISSING)
            or "",
        )
        self.assertIn(
            "processing storage",
            materialize_reason_copy(
                materialization.REASON_PROCESSING_PATH_MISSING) or "",
        )
        self.assertIn(
            "slskd never reported",
            materialize_reason_copy(
                materialization.REASON_EVENT_PATH_NEVER_STAMPED) or "",
        )

    def test_unknown_reason_has_no_copy(self):
        self.assertIsNone(materialize_reason_copy("something_new"))
        self.assertIsNone(materialize_reason_copy(None))
        self.assertIsNone(materialize_reason_copy(""))


class TestFailedRowCopy(unittest.TestCase):

    def test_grace_expiry_row_is_not_labelled_an_import_error(self):
        presentation = present_failure(_evidence(
            outcome="failed",
            error_message="Completed download could not be materialized "
                          "within 3600s of processing start; resetting to "
                          "wanted for re-download",
        ))
        self.assertEqual(
            presentation.verdict,
            "Download could not be staged for import in time; "
            "returned to the queue",
        )
        self.assertNotIn("Import error", presentation.verdict or "")

    def test_persisted_reason_outranks_the_generic_grace_sentence(self):
        presentation = present_failure(_evidence(
            outcome="failed",
            error_message="Completed download could not be materialized "
                          "within 3600s of processing start; resetting to "
                          "wanted for re-download",
            beets_detail=materialization.REASON_EVENT_PATH_NEVER_STAMPED,
        ))
        self.assertEqual(
            presentation.verdict,
            "Download finished but slskd never reported where the files "
            "landed; requeued",
        )

    def test_completion_path_reason_in_error_message(self):
        presentation = present_failure(_evidence(
            outcome="failed",
            error_message=materialization.REASON_EVENT_PATH_GONE_FROM_DISK,
        ))
        self.assertEqual(
            presentation.verdict,
            "Downloaded files disappeared before import; requeued",
        )

    def test_historical_fused_reason_claims_neither_cause(self):
        presentation = present_failure(_evidence(
            outcome="failed",
            error_message="event_path_missing",
            beets_detail="event_path_missing",
        ))
        self.assertEqual(
            presentation.verdict,
            "Downloaded files could not be located for import; requeued",
        )
        self.assertNotIn("never reported", presentation.verdict or "")
        self.assertNotIn("disappeared", presentation.verdict or "")

    def test_abandoned_auto_import(self):
        presentation = present_failure(_evidence(
            outcome="failed",
            error_message="Abandoned interrupted auto-import; queued for "
                          "redownload",
        ))
        self.assertEqual(
            presentation.verdict, "Interrupted import abandoned and requeued")

    def test_real_import_errors_keep_the_import_error_label(self):
        presentation = present_failure(_evidence(
            outcome="failed", error_message="Harness returned rc=2"))
        self.assertEqual(presentation.verdict, "Import error: Harness returned rc=2")

    def test_beets_scenario_timeout(self):
        presentation = present_failure(_evidence(
            outcome="failed", beets_scenario="timeout",
            error_message="whatever"))
        self.assertEqual(presentation.verdict, "Import timed out")

    def test_no_message_leaves_the_verdict_to_the_caller(self):
        self.assertIsNone(
            present_failure(_evidence(outcome="failed")).verdict)


class TestMeasurementFailureCopy(unittest.TestCase):

    CASES = [
        (
            "doubled failed prefix",
            "failed: current Beets authority resolution raised",
            "Measurement failed: could not read the installed library copy",
        ),
        (
            "authority path escape",
            "FilesystemAuthorityError: path is outside the library root",
            "Measurement failed: installed path is outside the library root",
        ),
        (
            "other authority errors lose only the class name",
            "FilesystemAuthorityError: open failed on the album directory",
            "Measurement failed: open failed on the album directory",
        ),
        (
            "already-readable diagnostics pass through",
            "candidate source changed since evidence capture",
            "Measurement failed: candidate source changed since evidence capture",
        ),
    ]

    def test_message_table(self):
        for description, message, expected in self.CASES:
            with self.subTest(description):
                presentation = present_failure(_evidence(
                    outcome="measurement_failed", error_message=message))
                self.assertEqual(presentation.verdict, expected)

    def test_legacy_rows_fall_back_to_beets_detail(self):
        presentation = present_failure(_evidence(
            outcome="measurement_failed", error_message=None,
            beets_detail="lossless spectral analysis returned no usable grade"))
        self.assertEqual(
            presentation.verdict,
            "Measurement failed: lossless spectral analysis returned no "
            "usable grade",
        )

    def test_no_diagnostic_at_all(self):
        presentation = present_failure(_evidence(outcome="measurement_failed"))
        self.assertEqual(presentation.verdict, "Measurement failed")


# ---------------------------------------------------------------------------
# Decoding and bounding
# ---------------------------------------------------------------------------

class TestDecodeAndBound(unittest.TestCase):

    def test_decode_transfer_detail_round_trips_production_shape(self):
        decoded = decode_transfer_detail(_log_38272_transfer_detail())
        self.assertEqual(len(decoded), 29)
        self.assertIsInstance(decoded[0], FileFailureDetail)
        self.assertEqual(decoded[0].username, "Tymemage")
        self.assertEqual(decoded[0].bytes_transferred, 0)
        self.assertEqual(decoded[0].retry_count, 0)

    def test_malformed_blob_degrades_instead_of_raising(self):
        self.assertEqual(decode_transfer_detail(None), ())
        self.assertEqual(decode_transfer_detail({"not": "a list"}), ())
        self.assertEqual(decode_transfer_detail([{"username": 7}]), ())

    def test_bounded_text_collapses_control_characters(self):
        self.assertEqual(bounded_text("a\nb\tc  d"), "a b c d")

    def test_bounded_text_truncates_with_an_ellipsis(self):
        bounded = bounded_text("x" * (MAX_RAW_MESSAGE_CHARS + 50))
        self.assertEqual(len(bounded), MAX_RAW_MESSAGE_CHARS)
        self.assertTrue(bounded.endswith("\u2026"))


class TestNonFailureOutcomes(unittest.TestCase):

    def test_success_and_rejected_get_no_opinion(self):
        for outcome in ("success", "rejected", "force_import", "user_offline"):
            with self.subTest(outcome):
                presentation = present_failure(_evidence(
                    outcome=outcome, error_message="anything"))
                self.assertIsNone(presentation.verdict)
                self.assertIsNone(presentation.transfer_message)


class TestFailureEvidenceFromRow(unittest.TestCase):
    """The CLI adapter reads the same row shape the DB returns."""

    def test_from_row_reads_the_live_columns(self):
        evidence = FailureEvidence.from_row({
            "outcome": "timeout",
            "error_message": LOG_38272_ERROR_MESSAGE,
            "beets_detail": None,
            "beets_scenario": None,
            "soulseek_username": "Tymemage",
            "transfer_detail": _log_38272_transfer_detail(),
            "import_result": {"unrelated": True},
        })
        self.assertEqual(evidence.outcome, "timeout")
        self.assertEqual(evidence.soulseek_username, "Tymemage")
        self.assertEqual(len(evidence.transfer_detail), 29)

    def test_from_row_tolerates_missing_and_mistyped_columns(self):
        evidence = FailureEvidence.from_row({"outcome": "timeout",
                                             "error_message": 7})
        self.assertEqual(evidence.outcome, "timeout")
        self.assertIsNone(evidence.error_message)
        self.assertEqual(evidence.transfer_detail, ())


if __name__ == "__main__":
    unittest.main()
