"""Self-tests for ``tests/fakes/slskd.py``'s FakeSlskdAPI and its sub-APIs.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

from tests.fakes import (
    FakeSlskdAPI,
)


class TestFakeSlskdAPI(unittest.TestCase):
    def test_get_downloads_returns_queued_snapshots(self):
        """#507: get_all_downloads() now runs the raw JSON snapshot through
        parse_downloads_envelope(), the same as production — mirroring the
        real decode is the point (test-fidelity Rule B)."""
        from lib.slskd_client import parse_downloads_envelope
        first = [{"username": "user1", "directories": [{"files": []}]}]
        second = [{"username": "user1", "directories": [{"files": [
            {"filename": "track.mp3", "id": "tid-1"},
        ]}]}]
        slskd = FakeSlskdAPI(download_snapshots=[first, second])

        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(first))
        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(second))
        self.assertEqual(
            slskd.transfers.get_all_downloads(includeRemoved=True),
            parse_downloads_envelope(second))
        self.assertEqual(slskd.transfers.get_all_downloads_calls, [True, True, True])

    def test_records_enqueue_and_cancel_calls(self):
        slskd = FakeSlskdAPI()
        files = [{"filename": "track.mp3", "size": 1000}]

        self.assertTrue(slskd.transfers.enqueue("user1", files))
        self.assertTrue(slskd.transfers.cancel_download("user1", "tid-1"))

        self.assertEqual(slskd.transfers.enqueue_calls[0].username, "user1")
        self.assertEqual(slskd.transfers.enqueue_calls[0].files, files)
        self.assertEqual(slskd.transfers.cancel_download_calls[0].id, "tid-1")

    def test_cancel_false_return_keeps_only_rejected_transfer_resident(self):
        """Per-ID cancellation outcomes preserve the fake's live state."""
        slskd = FakeSlskdAPI()
        for transfer_id in ("tid-false", "tid-success"):
            slskd.add_transfer(
                username="user1", directory="Music\\Album",
                filename=f"Music\\Album\\{transfer_id}.flac",
                id=transfer_id, state="Completed, Succeeded",
            )
        slskd.transfers.cancel_download_results_by_id["tid-false"] = False

        self.assertFalse(slskd.transfers.cancel_download(
            "user1", "tid-false", remove=True))
        self.assertTrue(slskd.transfers.cancel_download(
            "user1", "tid-success", remove=True))

        remaining_ids = {
            transfer.id
            for user in slskd.transfers.get_all_downloads()
            for directory in user.directories
            for transfer in directory.files
        }
        self.assertEqual(remaining_ids, {"tid-false"})

    def test_user_directories_record_results_and_errors(self):
        slskd = FakeSlskdAPI()
        directory = [{"directory": "Music\\Album", "files": []}]
        slskd.users.set_directory("user1", "Music\\Album", directory)
        slskd.users.set_directory_error(
            "user1",
            "Music\\Broken",
            RuntimeError("Peer offline"),
        )

        self.assertEqual(slskd.users.directory("user1", "Music\\Album"), directory)
        with self.assertRaises(RuntimeError):
            slskd.users.directory("user1", "Music\\Broken")
        self.assertEqual(slskd.users.directory_calls, [
            ("user1", "Music\\Album"),
            ("user1", "Music\\Broken"),
        ])

    def test_user_status_default_is_online(self):
        """Unset users default to Online so legacy tests stay green."""
        slskd = FakeSlskdAPI()

        result = slskd.users.status("never_set")

        self.assertEqual(result["presence"], "Online")
        self.assertEqual(slskd.users.status_calls, ["never_set"])

    def test_user_status_returns_configured_presence(self):
        slskd = FakeSlskdAPI()
        slskd.users.set_status("alice", "Online")
        slskd.users.set_status("bob", "Away")
        slskd.users.set_status("carol", "Offline")

        self.assertEqual(slskd.users.status("alice")["presence"], "Online")
        self.assertEqual(slskd.users.status("bob")["presence"], "Away")
        self.assertEqual(slskd.users.status("carol")["presence"], "Offline")
        self.assertEqual(
            slskd.users.status_calls, ["alice", "bob", "carol"],
        )

    def test_user_status_raises_configured_error(self):
        slskd = FakeSlskdAPI()
        boom = RuntimeError("slskd unreachable")
        slskd.users.set_status_error("flaky", boom)

        with self.assertRaises(RuntimeError):
            slskd.users.status("flaky")
        # The call is still recorded so tests can assert ordering.
        self.assertEqual(slskd.users.status_calls, ["flaky"])

    def test_user_status_payload_shape_matches_slskd_api(self):
        """Returned dict mirrors slskd-api UserStatus TypedDict shape:
        {presence: str, isPrivileged: bool}."""
        slskd = FakeSlskdAPI()
        slskd.users.set_status("alice", "Online")

        result = slskd.users.status("alice")

        self.assertIn("presence", result)
        self.assertIn("isPrivileged", result)
        self.assertIsInstance(result["isPrivileged"], bool)

    def test_add_transfer_can_carry_exception_reason(self):
        """Issue #564: seeded transfers can carry slskd's real failure
        reason so poll/harvest tests can drive it through the same
        parse_downloads_envelope() decode production uses."""
        slskd = FakeSlskdAPI()
        slskd.add_transfer(
            username="user1", directory="user1\\Music",
            filename="user1\\Music\\01.flac", id="tid-1",
            state="Completed, Rejected",
            exception="Transfer rejected: Banned",
        )

        downloads = slskd.transfers.get_all_downloads(includeRemoved=True)

        snap = downloads[0].directories[0].files[0]
        self.assertEqual(snap.exception, "Transfer rejected: Banned")


class TestFakeSlskdSearches(unittest.TestCase):
    """Self-test for the FakeSlskdSearches stub introduced in U5."""

    def test_search_text_records_kwargs_and_returns_id(self):
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_id_sequence = [101]
        result = slskd.searches.search_text(
            searchText="*rtist Album",
            searchTimeout=30000,
            filterResponses=True,
            maximumPeerQueueLength=5,
            minimumPeerUploadSpeed=0,
            responseLimit=1000,
        )
        self.assertEqual(result, {"id": 101})
        call = slskd.searches.search_text_calls[0]
        self.assertEqual(call.search_text, "*rtist Album")
        self.assertEqual(call.kwargs["responseLimit"], 1000)
        self.assertEqual(call.kwargs["searchTimeout"], 30000)

    def test_state_returns_canned_terminal_state(self):
        slskd = FakeSlskdAPI()
        slskd.searches.add_search(search_id=7, state="ResponseLimitReached")

        state = slskd.searches.state(7, False)

        self.assertEqual(state["state"], "ResponseLimitReached")
        self.assertEqual(slskd.searches.state_calls, [(7, False)])

    def test_search_responses_returns_canned_payload(self):
        slskd = FakeSlskdAPI()
        responses = [
            {"username": "u1", "uploadSpeed": 100, "files": [
                {"filename": "u1\\Music\\01.flac"},
            ]},
        ]
        slskd.searches.add_search(search_id=11, responses=responses)

        out = slskd.searches.search_responses(11)

        self.assertEqual(out, responses)
        # Response list must be a deep copy — tests can mutate freely.
        out[0]["files"].append({"filename": "tampered.flac"})
        again = slskd.searches.search_responses(11)
        self.assertEqual(len(again[0]["files"]), 1)

    def test_search_text_error_propagates(self):
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error = RuntimeError("slskd offline")
        with self.assertRaises(RuntimeError):
            slskd.searches.search_text(searchText="x", responseLimit=1000)

    def test_unknown_search_id_returns_completed_with_no_responses(self):
        slskd = FakeSlskdAPI()
        # No add_search() call — the fake should still answer politely.
        state = slskd.searches.search_text(
            searchText="x", responseLimit=1000)
        sid = state["id"]
        self.assertEqual(slskd.searches.state(sid)["state"], "Completed")
        self.assertEqual(slskd.searches.search_responses(sid), [])

    def test_search_text_error_by_query_targets_exact_searchtext(self):
        """Issue #1090 NIT-9: per-searchText keyed injection is
        independent of call order/count across OTHER distinct
        searchText values -- a candidate that never calls search_text at
        all (e.g. an empty-artist_name guard) cannot desynchronise a
        keyed queue meant for a different candidate's text."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [
            RuntimeError("A fails once"), None,
        ]
        slskd.searches.search_text_error_by_query["Artist B"] = [
            RuntimeError("B always fails"),
        ]
        # B's queue is untouched by A's calls.
        with self.assertRaises(RuntimeError) as caught_a1:
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertEqual(str(caught_a1.exception), "A fails once")
        result = slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertIn("id", result)
        with self.assertRaises(RuntimeError) as caught_b:
            slskd.searches.search_text(searchText="Artist B", responseLimit=1000)
        self.assertEqual(str(caught_b.exception), "B always fails")

    def test_search_text_error_by_query_takes_priority_over_blanket_error(self):
        """Issue #1112: with the flat-FIFO ``search_text_error_sequence``
        mechanism removed, the by-query queue is the only per-call
        injection left besides the blanket ``search_text_error`` poison --
        confirm it still wins when both are configured for the same
        query."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [
            RuntimeError("keyed error"),
        ]
        slskd.searches.search_text_error = RuntimeError("blanket error")
        with self.assertRaises(RuntimeError) as caught:
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        self.assertEqual(str(caught.exception), "keyed error")

    def test_search_text_error_by_query_exhausted_falls_back_to_blanket_error(
        self,
    ):
        """Once a query's own queue is exhausted, ``search_text_error``
        (if set) resumes poisoning THAT query's later calls -- the
        per-query queue is a prefix override, not a replacement for the
        blanket-error knob."""
        slskd = FakeSlskdAPI()
        slskd.searches.search_text_error_by_query["Artist A"] = [None]
        slskd.searches.search_text_error = RuntimeError("blanket failure")
        # First call consumes the queue's lone None -- succeeds.
        slskd.searches.search_text(searchText="Artist A", responseLimit=1000)
        # Queue now empty -- falls back to search_text_error.
        with self.assertRaises(RuntimeError):
            slskd.searches.search_text(searchText="Artist A", responseLimit=1000)


class TestFakeSlskdServer(unittest.TestCase):
    """Self-test for the FakeSlskdServer stub introduced for issue #1090."""

    def test_defaults_to_ready(self):
        from lib.slskd_client import SlskdServerState
        slskd = FakeSlskdAPI()
        state = slskd.server.state()
        self.assertIsInstance(state, SlskdServerState)
        self.assertTrue(state.is_connected)
        self.assertTrue(state.is_logged_in)
        self.assertEqual(slskd.server.state_calls, 1)

    def test_set_ready_reports_reconnect_window(self):
        slskd = FakeSlskdAPI()
        slskd.server.set_ready(is_connected=True, is_logged_in=False)
        state = slskd.server.state()
        self.assertTrue(state.is_connected)
        self.assertFalse(state.is_logged_in)

    def test_state_error_propagates(self):
        slskd = FakeSlskdAPI()
        slskd.server.state_error = RuntimeError("server endpoint down")
        with self.assertRaises(RuntimeError):
            slskd.server.state()


class TestFakeSlskdEvents(unittest.TestCase):
    """Self-tests for the events sub-API fake (issue #146)."""

    def _api(self):
        from tests.fakes import FakeSlskdAPI
        return FakeSlskdAPI()

    def test_pagination_slices_newest_first_feed(self):
        api = self._api()
        events = [
            api.events.make_event(
                id=f"ev-{i}", timestamp="2026-07-01T00:00:00.0000000Z",
                type="Noise", data="{}")
            for i in range(5)
        ]
        api.events.set_events(events)

        page = api.events.list(limit=2, offset=1)

        self.assertEqual([e.id for e in page.events], ["ev-1", "ev-2"])
        self.assertEqual(page.total_count, 5)
        self.assertEqual(api.events.list_calls, [(2, 1)])

    def test_total_count_override(self):
        api = self._api()
        api.events.total_count_override = 389110

        page = api.events.list()

        self.assertEqual(page.total_count, 389110)
        self.assertEqual(page.events, [])

    def test_list_error_injection(self):
        api = self._api()
        api.events.list_error = RuntimeError("events API down")

        with self.assertRaises(RuntimeError):
            api.events.list()

    def test_call_log_records_cross_api_ordering(self):
        api = self._api()

        api.transfers.get_all_downloads()
        api.events.list()

        self.assertEqual(
            api.call_log, ["transfers.get_all_downloads", "events.list"])


if __name__ == "__main__":
    unittest.main()
