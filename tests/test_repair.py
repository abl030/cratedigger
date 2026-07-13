"""Tests for repair/orphan-recovery pure functions."""

import unittest

from lib.repair import (
    OrphanInfo,
    find_completed_transfers_to_purge,
    find_inconsistencies,
    find_orphaned_downloads,
    find_slskd_orphans,
    suggest_repair,
)
from tests.helpers import make_download_directory, make_download_user, make_transfer_snapshot


class TestFindInconsistencies(unittest.TestCase):
    """Detect inconsistent pipeline DB rows."""

    def test_downloading_no_state(self):
        rows = [{"id": 1, "status": "downloading", "active_download_state": None,
                 "imported_path": None}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "corrupt_downloading")
        self.assertEqual(issues[0].request_id, 1)

    def test_downloading_with_state_is_fine(self):
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {"filetype": "flac"},
                 "imported_path": None}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_wanted_with_imported_path_is_fine(self):
        """Issue #93: transcode_upgrade / quality-gate upgrade flows
        mark_done (persisting imported_path to the real beets destination),
        then re-queue the row to ``wanted`` to search for something better.
        The files genuinely live at imported_path during that search, so
        flagging the row as stale would wipe correct data on the next
        ``repair.py fix``.
        """
        rows = [{"id": 2, "status": "wanted",
                 "active_download_state": None,
                 "imported_path": "/Beets/Artist/Album"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0,
                         "wanted + imported_path is a valid upgrade-search state")

    def test_manual_with_imported_path_is_fine(self):
        """Same rationale as wanted: manual status after a force-import
        could legitimately carry imported_path until the row is cleared."""
        rows = [{"id": 4, "status": "manual",
                 "active_download_state": None,
                 "imported_path": "/Beets/Artist/Album"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_imported_with_path_is_fine(self):
        rows = [{"id": 3, "status": "imported",
                 "active_download_state": None,
                 "imported_path": "/some/path"}]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)

    def test_multiple_issues(self):
        rows = [
            {"id": 1, "status": "downloading", "active_download_state": None,
             "imported_path": None},
            {"id": 2, "status": "downloading", "active_download_state": None,
             "imported_path": None},
        ]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 2)

    def test_clean_rows(self):
        rows = [
            {"id": 1, "status": "wanted", "active_download_state": None,
             "imported_path": None},
            {"id": 2, "status": "imported", "active_download_state": None,
             "imported_path": "/valid"},
        ]
        issues = find_inconsistencies(rows)
        self.assertEqual(len(issues), 0)


class TestFindOrphanedDownloads(unittest.TestCase):
    """Detect downloading rows whose slskd transfers no longer exist."""

    def test_orphaned_when_no_transfers_match(self):
        """All files missing from slskd → orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        active = set()  # no active transfers
        issues = find_orphaned_downloads(rows, active, existing_local_paths=None)
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "orphaned_download")
        self.assertEqual(issues[0].request_id, 1)

    def test_not_orphaned_when_transfer_exists(self):
        """At least one file still active → not orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        active = {("user1", "track.flac")}
        issues = find_orphaned_downloads(rows, active, existing_local_paths=None)
        self.assertEqual(len(issues), 0)

    def test_skips_non_downloading_rows(self):
        """Only downloading rows should be checked."""
        rows = [{"id": 1, "status": "wanted",
                 "active_download_state": None}]
        issues = find_orphaned_downloads(rows, set(), existing_local_paths=None)
        self.assertEqual(len(issues), 0)

    def test_skips_downloading_without_state(self):
        """corrupt_downloading (no state) handled by find_inconsistencies."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": None}]
        issues = find_orphaned_downloads(rows, set(), existing_local_paths=None)
        self.assertEqual(len(issues), 0)

    def test_partial_match_not_orphaned(self):
        """Some files transferred, some still active → not orphaned."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "files": [
                         {"username": "user1", "filename": "01.flac"},
                         {"username": "user1", "filename": "02.flac"},
                     ]}}]
        active = {("user1", "02.flac")}  # only 1 of 2 still active
        issues = find_orphaned_downloads(rows, active, existing_local_paths=None)
        self.assertEqual(len(issues), 0)

    def test_skips_local_processing_rows_without_active_transfers(self):
        """Rows already in local processing are not orphaned downloads."""
        current_path = "/tmp/staging/auto-import/Test/Album [request-1]"
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "current_path": current_path,
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(
            rows,
            set(),
            existing_local_paths={current_path},
        )
        self.assertEqual(len(issues), 0)

    def test_processing_started_without_current_path_is_not_orphaned(self):
        """Recovery-owned rows must not be reset ahead of poll recovery."""
        rows = [{"id": 1, "status": "downloading",
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(rows, set(), existing_local_paths=None)
        self.assertEqual(issues, [])

    def test_reports_missing_local_processing_path_for_manual_review(self):
        """Blocked post-move rows should be surfaced to repair tooling."""
        rows = [{"id": 1, "status": "downloading",
                 "artist_name": "Test",
                 "album_title": "Album",
                 "year": 2026,
                 "active_download_state": {
                     "filetype": "flac",
                     "processing_started_at": "2026-04-22T00:00:00+00:00",
                     "current_path": "/tmp/staging/auto-import/Test/Album [request-1]",
                     "files": [{"username": "user1", "filename": "track.flac"}]}}]
        issues = find_orphaned_downloads(
            rows,
            set(),
            existing_local_paths=set(),
        )
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].issue_type, "blocked_post_move")
        self.assertIn("persisted processing path missing", issues[0].detail)

    def test_suggest_repair_orphaned(self):
        """Orphaned download should suggest reset_to_wanted."""
        issue = OrphanInfo(request_id=1, issue_type="orphaned_download",
                           detail="transfers gone")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "reset_to_wanted")

    def test_suggest_repair_blocked_post_move(self):
        issue = OrphanInfo(
            request_id=1,
            issue_type="blocked_post_move",
            detail="missing staged path",
        )
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")

    def test_suggest_repair_blocked_recovery(self):
        issue = OrphanInfo(
            request_id=1,
            issue_type="blocked_recovery",
            detail="ambiguous legacy shared staged path",
        )
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")

    def test_suggest_repair_auto_abandon_import(self):
        issue = OrphanInfo(
            request_id=1,
            issue_type="auto_abandon_import",
            detail="auto-abandonable request-scoped auto-import",
        )
        action = suggest_repair(issue)
        self.assertEqual(action.action, "wait_for_automatic_recovery")
        self.assertIn("quarantine", action.detail)


class TestFindSlskdOrphans(unittest.TestCase):
    """Inverse orphan direction (#278), ledger-positive since #571 PR 3:
    a live slskd transfer is only ever an orphan when it's IN
    cratedigger's write-ahead ledger. Unledgered transfers are foreign —
    counted, never reported as an orphan (C1). Ledgered-but-unbacked
    transfers are cratedigger's own strays (C2)."""

    FILENAME = "Music\\Album\\01 - Track.flac"

    @staticmethod
    def _snapshot(username="peer1", directory="Music\\Album",
                  filename="Music\\Album\\01 - Track.flac",
                  transfer_id="t-1", state="InProgress"):
        return [make_download_user(username=username, directories=[
            make_download_directory(directory=directory, files=[
                make_transfer_snapshot(
                    filename=filename, id=transfer_id, state=state),
            ]),
        ])]

    @staticmethod
    def _owning_row(status="downloading", username="peer1",
                    filename="Music\\Album\\01 - Track.flac", row_id=1):
        return {"id": row_id, "status": status,
                "active_download_state": {
                    "filetype": "flac",
                    "files": [{"username": username, "filename": filename}]}}

    @staticmethod
    def _ledgered(*pairs):
        return set(pairs)

    def test_live_unledgered_transfer_is_foreign_not_orphan(self):
        """C1, the flip of the old doctrine: with zero ledger knowledge, a
        live transfer with no owning row used to be reported as an
        orphan (and convergence would cancel it — a human's transfer on
        a shared instance). Now it's foreign: never reported, only
        counted."""
        ownership = find_slskd_orphans(self._snapshot(), [], set())
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 1)

    def test_live_ledgered_unbacked_transfer_is_orphan(self):
        """C2: ledgered AND not backed by a downloading row IS the stray
        this convergence targets."""
        ownership = find_slskd_orphans(
            self._snapshot(), [], self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)
        self.assertEqual(ownership.orphans[0].username, "peer1")
        self.assertEqual(ownership.orphans[0].transfer_id, "t-1")
        self.assertEqual(ownership.orphans[0].filename, self.FILENAME)
        self.assertEqual(ownership.orphans[0].state, "InProgress")
        self.assertEqual(ownership.foreign_count, 0)

    def test_live_ledgered_backed_transfer_is_not_orphan(self):
        ownership = find_slskd_orphans(
            self._snapshot(), [self._owning_row()],
            self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 0)

    def test_backed_but_unledgered_transfer_is_foreign_not_shielded(self):
        """Priority ordering: ledger membership decides C1 BEFORE the
        backed check runs. A transfer no ledger row proves cratedigger
        created is foreign even if it happens to match a downloading
        row's active_download_state (legacy pre-ledger data, or a stray
        DB entry) — never reported as an orphan either way."""
        ownership = find_slskd_orphans(
            self._snapshot(), [self._owning_row()], set())
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 1)

    def test_completed_unowned_transfer_is_not_orphan(self):
        """Terminal transfers have nothing to cancel — the completed-
        transfer purge (#571 PR 5) reaps their UI entries. Skipped before
        classification, so it doesn't even count toward foreign_count."""
        ownership = find_slskd_orphans(
            self._snapshot(state="Completed, Succeeded"), [], set())
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 0)

    def test_non_downloading_row_does_not_own(self):
        """A replaced row's frozen active_download_state must NOT shield
        its stranded, ledgered transfer — that's the exact case this
        converges (the canonical Replace scenario)."""
        ownership = find_slskd_orphans(
            self._snapshot(), [self._owning_row(status="replaced")],
            self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)

    def test_downloading_row_without_state_owns_nothing(self):
        row = {"id": 1, "status": "downloading",
               "active_download_state": None}
        ownership = find_slskd_orphans(
            self._snapshot(), [row], self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)

    def test_processing_phase_row_still_shields_its_transfers(self):
        """A downloading row mid-local-processing owns its files like any
        other — ownership never branches on processing_started_at."""
        row = self._owning_row()
        row["active_download_state"]["processing_started_at"] = (
            "2026-07-03T00:00:00+00:00")
        ownership = find_slskd_orphans(
            self._snapshot(), [row], self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(ownership.orphans, [])

    def test_username_must_match(self):
        """Same filename from a different peer is a different transfer —
        must be ledgered under its OWN (username, filename) key."""
        ownership = find_slskd_orphans(
            self._snapshot(username="peer2"), [self._owning_row()],
            self._ledgered(("peer2", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)
        self.assertEqual(ownership.orphans[0].username, "peer2")

    def test_queued_state_is_live(self):
        ownership = find_slskd_orphans(
            self._snapshot(state="Queued, Remotely"), [],
            self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)

    def test_missing_state_treated_as_live(self):
        # state="" is TransferSnapshot's own default — mirrors an entry
        # where slskd omitted the field entirely (issue #507: the
        # envelope's file entries decode through the same Struct).
        snapshot = self._snapshot(state="")
        ownership = find_slskd_orphans(
            snapshot, [], self._ledgered(("peer1", self.FILENAME)))
        self.assertEqual(len(ownership.orphans), 1)
        self.assertEqual(ownership.orphans[0].state, "")

    def test_file_without_filename_skipped(self):
        snapshot = self._snapshot(filename="")
        ownership = find_slskd_orphans(snapshot, [], set())
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 0)

    def test_mixed_snapshot_classifies_orphan_and_foreign_independently(self):
        snapshot = (
            self._snapshot()  # live, ledgered + owned below -> in flight
            + self._snapshot(username="peer2", transfer_id="t-2",
                             filename="Music\\Other\\02.flac")  # live, ledgered, unbacked -> stray
            + self._snapshot(username="peer3", transfer_id="t-3",
                             filename="Music\\Foreign\\03.flac")  # live, unledgered -> foreign
            + self._snapshot(username="peer4", transfer_id="t-4",
                             state="Completed, Errored")  # terminal
        )
        ownership = find_slskd_orphans(
            snapshot, [self._owning_row()],
            self._ledgered(
                ("peer1", self.FILENAME),
                ("peer2", "Music\\Other\\02.flac"),
            ))
        self.assertEqual(len(ownership.orphans), 1)
        self.assertEqual(ownership.orphans[0].transfer_id, "t-2")
        self.assertEqual(ownership.foreign_count, 1)

    def test_empty_snapshot(self):
        ownership = find_slskd_orphans([], [self._owning_row()], set())
        self.assertEqual(ownership.orphans, [])
        self.assertEqual(ownership.foreign_count, 0)


class TestFindCompletedTransfersToPurge(unittest.TestCase):
    """Completed-transfer purge classification (#571 PR 5): a live
    COMPLETED transfer is only ever removable when its ``transfer_id`` is
    BOTH ledger-owned AND completion-stamped (P1 good-citizen, P2 stamp-
    before-remove). Matching is by transfer_id, never (username,
    filename) — a retried file's completed and in-flight attempts can
    share that pair."""

    FILENAME = "Music\\Album\\01 - Track.flac"

    @staticmethod
    def _snapshot(username="peer1", directory="Music\\Album",
                  filename="Music\\Album\\01 - Track.flac",
                  transfer_id="t-1", state="Completed, Succeeded",
                  requested_at="2026-07-13T01:00:00+00:00"):
        return [make_download_user(username=username, directories=[
            make_download_directory(directory=directory, files=[
                make_transfer_snapshot(
                    filename=filename, id=transfer_id, state=state,
                    requested_at=requested_at),
            ]),
        ])]

    def test_stamped_owned_transfer_is_removable(self):
        ownership = find_completed_transfers_to_purge(
            self._snapshot(), {"t-1"}, set())
        self.assertEqual(len(ownership.to_remove), 1)
        self.assertEqual(ownership.to_remove[0].username, "peer1")
        self.assertEqual(ownership.to_remove[0].transfer_id, "t-1")
        self.assertEqual(ownership.to_remove[0].filename, self.FILENAME)
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.to_claim_failures, [])
        self.assertEqual(ownership.success_waiting_count, 0)
        self.assertEqual(ownership.foreign_count, 0)

    def test_unstamped_owned_success_waits_for_authoritative_event(self):
        """An unstamped success still needs the event/local-path stamp."""
        ownership = find_completed_transfers_to_purge(
            self._snapshot(), set(), {"t-1"})
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.success_waiting_count, 1)
        self.assertEqual(ownership.foreign_count, 0)

    def test_every_unstamped_owned_failure_is_selected_for_terminal_stamp(self):
        """Every known non-success terminal mode gets exact-id stamping."""
        for state in (
            "Completed, Aborted",
            "Completed, Cancelled",
            "Completed, Errored",
            "Completed, Rejected",
            "Completed, TimedOut",
        ):
            with self.subTest(state=state):
                ownership = find_completed_transfers_to_purge(
                    self._snapshot(state=state), set(), {"t-1"})
                self.assertEqual(ownership.to_remove, [])
                self.assertEqual(
                    [item.transfer_id for item in ownership.to_stamp_failures],
                    ["t-1"],
                )
                self.assertEqual(ownership.success_waiting_count, 0)
                self.assertEqual(ownership.foreign_count, 0)

    def test_unbound_terminal_failure_is_an_atomic_claim_candidate(self):
        """A failed T1.5 can claim an exact-key causal open T1 row in DB."""
        ownership = find_completed_transfers_to_purge(
            self._snapshot(state="Completed, Errored"), set(), set())
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(
            [item.transfer_id for item in ownership.to_claim_failures],
            ["t-1"],
        )
        self.assertEqual(ownership.foreign_count, 0)

    def test_foreign_completed_transfer_is_never_removed(self):
        """P1: absent from both sets entirely -- a human's completed
        download on a shared instance."""
        ownership = find_completed_transfers_to_purge(
            self._snapshot(), set(), set())
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.to_claim_failures, [])
        self.assertEqual(ownership.success_waiting_count, 0)
        self.assertEqual(ownership.foreign_count, 1)

    def test_live_non_terminal_transfer_is_skipped_entirely(self):
        """This classifier only ever reasons about completed records --
        the disjoint half of find_slskd_orphans' live-transfer scope.
        Even a stamped-owned id doesn't count if the transfer hasn't
        reached a terminal state (shouldn't happen in practice, but the
        classifier must not misreport it either way)."""
        ownership = find_completed_transfers_to_purge(
            self._snapshot(state="InProgress"), {"t-1"}, set())
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.success_waiting_count, 0)
        self.assertEqual(ownership.foreign_count, 0)
        self.assertEqual(ownership.nonterminal_count, 1)

    def test_transfer_id_disambiguates_retried_attempts(self):
        """The brief's exact scenario: two completed records share the
        SAME (username, filename) -- a retried file -- but only one
        transfer_id is stamped-owned. (username, filename) matching alone
        would be ambiguous; transfer_id matching is not."""
        snapshot = (
            self._snapshot(transfer_id="t-old", state="Completed, Cancelled")
            + self._snapshot(transfer_id="t-new", state="Completed, Succeeded")
        )
        ownership = find_completed_transfers_to_purge(
            snapshot, {"t-new"}, set())
        self.assertEqual(len(ownership.to_remove), 1)
        self.assertEqual(ownership.to_remove[0].transfer_id, "t-new")
        self.assertEqual(
            [item.transfer_id for item in ownership.to_claim_failures],
            ["t-old"],
        )

    def test_completed_transfer_with_no_id_is_skipped(self):
        snapshot = self._snapshot(transfer_id="")
        ownership = find_completed_transfers_to_purge(snapshot, set(), set())
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_claim_failures, [])
        self.assertEqual(ownership.foreign_count, 0)
        self.assertEqual(ownership.nonterminal_count, 0)

    def test_mixed_snapshot_classifies_all_three_tiers_independently(self):
        snapshot = (
            self._snapshot()  # stamped-owned -> removable
            + self._snapshot(username="peer2", transfer_id="t-2",
                             filename="Music\\Other\\02.flac")  # unstamped-owned
            + self._snapshot(username="peer3", transfer_id="t-3",
                             filename="Music\\Foreign\\03.flac")  # foreign
            + self._snapshot(username="peer4", transfer_id="t-4",
                             state="InProgress")  # live, not our concern
        )
        ownership = find_completed_transfers_to_purge(
            snapshot, {"t-1"}, {"t-2"})
        self.assertEqual(len(ownership.to_remove), 1)
        self.assertEqual(ownership.to_remove[0].transfer_id, "t-1")
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.to_claim_failures, [])
        self.assertEqual(ownership.success_waiting_count, 1)
        self.assertEqual(ownership.foreign_count, 1)
        self.assertEqual(ownership.nonterminal_count, 1)

    def test_empty_snapshot(self):
        ownership = find_completed_transfers_to_purge([], {"t-1"}, {"t-2"})
        self.assertEqual(ownership.to_remove, [])
        self.assertEqual(ownership.to_stamp_failures, [])
        self.assertEqual(ownership.to_claim_failures, [])
        self.assertEqual(ownership.success_waiting_count, 0)
        self.assertEqual(ownership.foreign_count, 0)
        self.assertEqual(ownership.nonterminal_count, 0)


class TestSuggestRepair(unittest.TestCase):
    """Map issues to repair actions."""

    def test_corrupt_downloading(self):
        issue = OrphanInfo(request_id=1, issue_type="corrupt_downloading",
                           detail="no active_download_state")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "reset_to_wanted")

    def test_unknown_issue_type(self):
        issue = OrphanInfo(request_id=3, issue_type="unknown",
                           detail="something")
        action = suggest_repair(issue)
        self.assertEqual(action.action, "manual_review")


if __name__ == "__main__":
    unittest.main()
