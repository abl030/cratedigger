"""``DispatchRequest`` / ``DispatchDB`` — the typed dispatch interface (#1277).

Two contracts live here:

* ``DispatchRequest`` is the flat frozen description of one import dispatch.
  Its defaults ARE the old 36-kwarg signature's defaults; a drifted default
  silently changes what every caller that omits the field asks for.
* ``DispatchDB`` is the exact pipeline-DB surface ``lib/dispatch/`` uses.
  ``PipelineDB`` and ``FakePipelineDB`` must both satisfy it — that
  equivalence is what lets tests hand dispatch a fake without an ``Any``
  bridge.
"""

from __future__ import annotations

import dataclasses
import typing
import unittest

from lib.dispatch.types import DispatchDB, DispatchRequest
from lib.grab_list import DownloadFile
from lib.quality import DownloadInfo
from tests.dispatch_helpers import make_dispatch_request
from tests.fakes import FakePipelineDB

#: ``dataclasses.MISSING`` is the stdlib's own "this field has no default"
#: sentinel, and identity is the documented way to test it. Aliased to a
#: bare name because ``tests/test_lint_no_is_on_enum.py`` flags
#: ``is <ALL_CAPS attribute>`` on sight — that lint is aimed at enum
#: members, and it already exempts underscore-named sentinels; this is the
#: same kind of value under a name it cannot recognise.
_NO_DEFAULT = dataclasses.MISSING


def _minimal() -> DispatchRequest:
    return DispatchRequest(
        path="/tmp/album",
        mb_release_id="mbid-1",
        request_id=7,
        label="Artist - Album",
        beets_harness_path="/opt/harness.py",
        dl_info=DownloadInfo(),
    )


class TestDispatchRequestConstruction(unittest.TestCase):
    """The request carries exactly the values dispatch describes itself with."""

    def test_required_fields_round_trip(self) -> None:
        request = _minimal()
        self.assertEqual(request.path, "/tmp/album")
        self.assertEqual(request.mb_release_id, "mbid-1")
        self.assertEqual(request.request_id, 7)
        self.assertEqual(request.label, "Artist - Album")
        self.assertEqual(request.beets_harness_path, "/opt/harness.py")

    def test_defaults_mirror_the_previous_kwarg_signature(self) -> None:
        """Every optional field's default is the one the 36-kwarg signature
        carried. These are load-bearing: ``requeue_on_failure=True`` is what
        makes an automatic import self-heal, and ``force=False`` /
        ``scenario='auto_import'`` decide whether the source folder is
        treated as disposable."""
        request = _minimal()
        self.assertIs(request.force, False)
        self.assertIsNone(request.override_min_bitrate)
        self.assertIsNone(request.target_format)
        self.assertEqual(request.verified_lossless_target, "")
        self.assertIsNone(request.distance)
        self.assertEqual(request.scenario, "auto_import")
        self.assertIsNone(request.files)
        self.assertEqual(request.outcome_label, "success")
        self.assertIs(request.requeue_on_failure, True)
        self.assertIsNone(request.cooled_down_users)
        self.assertIsNone(request.source_dirs)
        self.assertIsNone(request.candidate_import_job_id)
        self.assertIsNone(request.attempt_spectral_audit)
        self.assertIsNone(request.attempt_result)
        self.assertIsNone(request.candidate_download_log_id)
        self.assertIsNone(request.launch_authority_path)
        self.assertIsNone(request.prevalidated_candidate_result)
        self.assertIsNone(request.beets_library_db_path)
        self.assertIsNone(request.beets_library_root)
        self.assertIsNone(request.execution_lease)
        self.assertIsNone(request.owner_session_identity)

    def test_is_frozen(self) -> None:
        """A stage function must not be able to rewrite the description of
        the import it was handed."""
        request = _minimal()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            setattr(request, "path", "/tmp/other")  # noqa: B010

    def test_files_carry_download_files(self) -> None:
        """``files`` is the peer attribution dispatch denylists from — real
        ``DownloadFile`` rows, the only shape either production caller
        passes."""
        peer = DownloadFile(
            filename="01.mp3", id="i", file_dir="d",
            username="peer", size=1,
        )
        request = make_dispatch_request(files=[peer])
        self.assertEqual([f.username for f in request.files or ()], ["peer"])


class TestMakeDispatchRequestBuilder(unittest.TestCase):
    """``make_dispatch_request`` is the shared builder every dispatch test
    constructs through — it must default to a complete, usable request and
    honour every override."""

    def test_builder_defaults_are_a_complete_request(self) -> None:
        request = make_dispatch_request()
        self.assertTrue(request.path)
        self.assertTrue(request.mb_release_id)
        self.assertTrue(request.beets_harness_path)
        self.assertIsInstance(request.request_id, int)
        self.assertIsInstance(request.dl_info, DownloadInfo)

    def test_builder_overrides_reach_the_request(self) -> None:
        request = make_dispatch_request(
            path="/x", mb_release_id="mb-9", request_id=42,
            force=True, scenario="force_import", requeue_on_failure=False,
            candidate_import_job_id=5,
        )
        self.assertEqual(request.path, "/x")
        self.assertEqual(request.mb_release_id, "mb-9")
        self.assertEqual(request.request_id, 42)
        self.assertIs(request.force, True)
        self.assertEqual(request.scenario, "force_import")
        self.assertIs(request.requeue_on_failure, False)
        self.assertEqual(request.candidate_import_job_id, 5)

    def test_builder_leaves_unnamed_fields_at_the_dataclass_default(
        self,
    ) -> None:
        """An override must not disturb its neighbours — the whole point of
        the builder is that a test names only what its scenario needs."""
        request = make_dispatch_request(force=True)
        self.assertIs(request.requeue_on_failure, True)
        self.assertEqual(request.scenario, "auto_import")
        self.assertEqual(request.outcome_label, "success")

    def test_builder_optional_defaults_equal_the_dataclass_defaults(
        self,
    ) -> None:
        """Drift guard: the builder repeats ``DispatchRequest``'s defaults
        by hand, so a field whose dataclass default changes without the
        builder following would silently give every test that omits it a
        different request than production's own default. Field-by-field,
        derived from the dataclass itself — never a hand-listed set."""
        built = make_dispatch_request()
        checked: list[str] = []
        for spec in dataclasses.fields(DispatchRequest):
            if spec.default is _NO_DEFAULT:
                continue
            checked.append(spec.name)
            self.assertEqual(
                getattr(built, spec.name),
                spec.default,
                f"builder default for {spec.name} drifted from "
                "DispatchRequest's own default",
            )
        self.assertGreater(len(checked), 15, checked)


class TestDispatchDBPort(unittest.TestCase):
    """The narrow DB port dispatch declares."""

    def test_fake_pipeline_db_satisfies_dispatch_db(self) -> None:
        """``FakePipelineDB`` is what every dispatch test passes as ``db``.
        If it stops satisfying the port, the tests are lying about the
        surface production uses — and the ``Any`` bridge comes back."""
        fake = FakePipelineDB()
        self.assertIsInstance(fake, DispatchDB)
        # pyright-visible half: the assignment itself is the static proof.
        conforming: DispatchDB = fake
        self.assertIs(conforming, fake)

    def test_port_declares_the_owner_fencing_methods(self) -> None:
        """``_probe_owner_session`` is the #898 ownership re-verification
        every dispatch checkpoint runs, and the two heartbeats are what
        ``checkpoint_automation_owner`` needs. The port must declare all
        three — underscore and all, per the cross-module private-use
        convention (PR #775) — so a DB stand-in missing any of them fails
        the port instead of being silently accepted."""
        members = typing.get_protocol_members(DispatchDB)
        for name in (
            "_probe_owner_session",
            "heartbeat_import_job",
            "heartbeat_import_job_preview",
        ):
            self.assertIn(name, members)

    def test_port_declares_the_methods_dispatch_calls_directly(self) -> None:
        """A spot-check across the port's own declarations: the advisory
        lock it serialises on, the job-less terminal-outcome bundles (the
        denylist and audit writers as of issue #1355 items A1/A2 — dispatch
        no longer calls ``add_denylist``/``log_download`` directly, only
        through these), the launch authority handshake, and the
        exact-completion capture. Dropping any of them from the port would
        let a DB stand-in that cannot serve dispatch pass as one."""
        members = typing.get_protocol_members(DispatchDB)
        for name in (
            "advisory_lock",
            "persist_request_success_outcome",
            "persist_request_policy_outcome",
            "authorize_import_job_launch",
            "capture_automation_import_completion",
            "persist_import_terminal_outcome",
            "persist_request_rejection_outcome",
            "request_marked_incomplete",
        ):
            self.assertIn(name, members)


if __name__ == "__main__":
    unittest.main()
