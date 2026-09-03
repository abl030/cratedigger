"""Tests for the shared fakes and builders that belong to no one cluster.

Since the #1313 split this module keeps the fake-to-production signature
contract (``TestPipelineDBFakeContract`` and its module-level diff
helpers), the seams the shared ``_base``/``_core`` modules own, the shared
builders, and the ``FakePipelineDBSource`` gating tests.
A cluster's own self-tests live beside it in ``tests/test_fakes_<cluster>.py``,
which is also what ``scripts/targeted_test_selection.py`` derives from a
changed ``tests/fakes/pipeline_db/<cluster>.py`` or
``lib/pipeline_db/<cluster>.py``. A cluster with no such module falls back
here, which is where the ``_base`` and ``_core`` tests below stay by design
and where ``source``'s gating tests already were. ``cleanup_journal``,
``convergence`` and ``terminal_outcomes`` also have no sibling yet and would
land here too.

A new ``PipelineDB`` method owes an equivalent stub on ``FakePipelineDB``
plus a self-test — in the cluster's own module when it has one, here when
it does not. That is the New Work Checklist row in
``.claude/rules/code-quality.md``.
"""

import inspect
import unittest
from unittest.mock import MagicMock

from lib.grab_list import DownloadFile, GrabListEntry
from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionOwnerProof,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.pipeline_db import PipelineDB
from lib.quality import ValidationResult
from tests.fakes import (
    FakeCursor,
    FakePipelineDB,
    RecordingProcessAlbum,
)
from tests.helpers import (
    make_ctx_with_fake_db,
    make_download_file,
    make_grab_list_entry,
    make_request_row,
    make_validation_result,
)


class TestRecordingProcessAlbum(unittest.TestCase):
    def test_records_exact_call_and_returns_configured_result(self) -> None:
        from lib.download_processing import CompletionDeferred

        entry = make_grab_list_entry()
        db = FakePipelineDB()
        ctx = make_ctx_with_fake_db(db)
        outcome = CompletionDeferred(detail="release_lock_contention")
        recorder = RecordingProcessAlbum(outcome=outcome)
        owner_proof = ExecutionOwnerProof(
            execution_lease=ExecutionLeaseSnapshot(
                host_boot_id="boot-a",
                invocation_id="invocation-a",
                systemd_unit="cratedigger-importer.service",
                worker=ProcessIdentity(pid=101, start_ticks=1001),
            ),
            owner_session_identity=OwnerSessionIdentity(
                connection_object_id=1, backend_pid=2,
            ),
        )

        result = recorder(entry, ctx, import_job_id=73, owner_proof=owner_proof)

        self.assertIs(result, outcome)
        self.assertEqual(len(recorder.calls), 1)
        call = recorder.calls[0]
        self.assertIs(call.album_data, entry)
        self.assertIs(call.ctx, ctx)
        self.assertEqual(call.import_job_id, 73)
        self.assertIsNone(call.validate_fn)
        self.assertIsNone(call.handle_valid_fn)
        self.assertIsNone(call.dispatch_fn)
        self.assertIs(call.owner_proof, owner_proof)


class TestFakePipelineDBCoreSeams(unittest.TestCase):
    """The shared connection seams: the ``_base`` DSN, and ``_core``'s
    ``close``, queued-cursor ``execute`` stub, read-only query cursor, and
    advisory-lock knob.

    These stay in ``tests/test_fakes.py`` because their verbs live in
    ``tests/fakes/pipeline_db/_base.py`` and ``_core.py``, whose derived
    sibling names (``tests.test_fakes__base``, ``tests.test_fakes__core``)
    are not modules anyone would write. Selection falls back to this file
    for the underscore clusters.
    """

    def test_exposes_configured_connection_identity(self) -> None:
        db = FakePipelineDB(dsn="postgresql://contract-test")

        self.assertEqual(db.dsn, "postgresql://contract-test")

    def test_close_marks_flag(self):
        db = FakePipelineDB()
        self.assertFalse(db.closed)
        db.close()
        self.assertTrue(db.closed)

    def test_execute_records_calls_and_returns_queued_cursors(self):
        """``queue_execute_results`` registers a deterministic cursor sequence;
        each ``_execute`` call pops the next entry and records the call."""
        db = FakePipelineDB()
        cur1 = MagicMock(name="cur1")
        cur2 = MagicMock(name="cur2")
        db.queue_execute_results(cur1, cur2)

        result1 = db._execute("SELECT 1")
        result2 = db._execute("SELECT 2", (42,))

        self.assertIs(result1, cur1)
        self.assertIs(result2, cur2)
        self.assertEqual(
            db.execute_calls,
            [("SELECT 1", ()), ("SELECT 2", (42,))],
        )

    def test_execute_raises_when_queued_entry_is_exception(self):
        """Queued ``Exception`` entries are raised, not returned — replaces
        ``side_effect=[..., ProgrammingError(...), ...]`` from MagicMock."""
        db = FakePipelineDB()
        boom = RuntimeError("syntax error")
        db.queue_execute_results(MagicMock(), boom)

        db._execute("SELECT 1")
        with self.assertRaises(RuntimeError) as raised:
            db._execute("BOOM")
        self.assertIs(raised.exception, boom)

    def test_execute_with_empty_queue_returns_default(self):
        """Empty queue returns an empty cursor (production's "query ran,
        zero rows" shape) so tests that don't care about the cursor
        result can still call ``_execute`` without setup."""
        db = FakePipelineDB()
        self.assertEqual(db._execute("SELECT 1").fetchall(), [])
        self.assertEqual(db.execute_calls, [("SELECT 1", ())])

    def test_read_only_query_cursor_brackets_query_with_setup_and_rollback(self):
        db = FakePipelineDB()
        query_cursor = FakeCursor([{"id": 1}])
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            query_cursor, MagicMock(name="rollback"),
        )

        with db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchall(), [{"id": 1}])

        self.assertEqual(
            db.execute_calls,
            [
                ("BEGIN TRANSACTION READ ONLY", ()),
                ("SET LOCAL standard_conforming_strings = on", ()),
                ("SELECT 1", ()),
                ("ROLLBACK", ()),
            ],
        )

    def test_read_only_query_cursor_rolls_back_after_query_error(self):
        db = FakePipelineDB()
        error = RuntimeError("query failed")
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"), error,
            MagicMock(name="rollback"),
        )

        with self.assertRaisesRegex(RuntimeError, "query failed"), db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT broken")

        self.assertEqual(
            db.execute_calls,
            [
                ("BEGIN TRANSACTION READ ONLY", ()),
                ("SET LOCAL standard_conforming_strings = on", ()),
                ("SELECT broken", ()),
                ("ROLLBACK", ()),
            ],
        )

    def test_read_only_query_cursor_suppresses_connection_lost_during_cleanup(self):
        import psycopg2

        db = FakePipelineDB()
        query_cursor = FakeCursor([{"id": 1}])
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            query_cursor, psycopg2.InterfaceError("connection lost"),
        )

        with db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")
            rows = cursor.fetchall()

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(db.execute_calls[-1], ("ROLLBACK", ()))

    def test_read_only_query_cursor_propagates_non_connection_cleanup_error(self):
        db = FakePipelineDB()
        db.queue_execute_results(
            MagicMock(name="begin"), MagicMock(name="string_mode"),
            FakeCursor(), RuntimeError("rollback failed"),
        )

        with self.assertRaisesRegex(RuntimeError, "rollback failed"), db.read_only_query_cursor() as cursor:
            cursor.execute("SELECT 1")

    def test_advisory_lock_default_yields_true(self):
        db = FakePipelineDB()
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertTrue(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])

    def test_advisory_lock_configurable(self):
        db = FakePipelineDB()
        db.set_advisory_lock_result(False)
        with db.advisory_lock(0x1234, 42) as acquired:
            self.assertFalse(acquired)
        self.assertEqual(db.advisory_lock_calls, [(0x1234, 42)])


class TestFakeAssertLog(unittest.TestCase):
    """``assert_log`` is the fake's own download-log assertion helper.

    It lives in ``tests/fakes/pipeline_db/_base.py`` rather than in the
    ``download_log`` cluster, so its self-tests stay here with the rest
    of the shared base.
    """

    def test_assert_log_passes(self):
        db = FakePipelineDB()
        log_id = db.log_download(42, outcome="success", soulseek_username="user1")

        # Should not raise
        self.assertEqual(log_id, db.download_logs[0].id)
        db.assert_log(self, 0, outcome="success", request_id=42)

    def test_assert_log_checks_extra_fields(self):
        """A field that is not a DownloadLogRow attribute lands in ``.extra``,
        and assert_log must read it back from there.

        The name promised this and the body used to read ``.extra`` directly,
        so the getattr default in assert_log was unconstrained: the #1313
        review runner dropped the fallback to a bare ``None`` and the whole
        suite stayed green. Every one of the 33 assert_log call sites in the
        tree passes a real column, so nothing else reaches this branch.
        """
        db = FakePipelineDB()
        db.log_download(42, outcome="success", spectral_grade="genuine")

        self.assertFalse(
            hasattr(db.download_logs[0], "spectral_grade"),
            "pick a field the row does not declare, or this pins nothing")
        db.assert_log(self, 0, outcome="success", spectral_grade="genuine")
        with self.assertRaisesRegex(AssertionError, r"spectral_grade"):
            db.assert_log(self, 0, spectral_grade="lossy_upscale")

    def test_assert_log_failure_message_names_the_real_field(self):
        """Issue #1211 review F4 regression pin: assert_log's f-string used
        to interpolate ``{field}`` instead of ``{field_name}``. ``field``
        resolved to the module-scope ``dataclasses.field`` import the fake
        carried at the time, so every failure printed that function's repr
        instead of the column name. Assert the real field name appears in
        the message, not just that it raises."""
        db = FakePipelineDB()
        db.log_download(42, outcome="success", beets_distance=None)

        with self.assertRaisesRegex(AssertionError, r"beets_distance"):
            db.assert_log(self, 0, beets_distance=0.01)


class TestBuilders(unittest.TestCase):
    def test_make_download_file_defaults(self):
        f = make_download_file()
        self.assertIsInstance(f, DownloadFile)
        self.assertEqual(f.filename, "01 - Track.mp3")
        self.assertEqual(f.username, "user1")
        self.assertEqual(f.size, 5_000_000)

    def test_make_download_file_overrides(self):
        f = make_download_file(username="beta", bitRate=192)
        self.assertEqual(f.username, "beta")
        self.assertEqual(f.bitRate, 192)

    def test_make_grab_list_entry_defaults(self):
        entry = make_grab_list_entry()
        self.assertIsInstance(entry, GrabListEntry)
        self.assertEqual(entry.artist, "Test Artist")
        self.assertEqual(len(entry.files), 1)
        self.assertIsInstance(entry.files[0], DownloadFile)

    def test_make_grab_list_entry_overrides(self):
        files = [make_download_file(username="a"), make_download_file(username="b")]
        entry = make_grab_list_entry(files=files, db_request_id=42, db_source="request")
        self.assertEqual(len(entry.files), 2)
        self.assertEqual(entry.db_request_id, 42)

    def test_make_validation_result_defaults(self):
        vr = make_validation_result()
        self.assertIsInstance(vr, ValidationResult)
        self.assertTrue(vr.valid)
        self.assertEqual(vr.distance, 0.05)
        self.assertEqual(vr.scenario, "strong_match")

    def test_make_validation_result_overrides(self):
        vr = make_validation_result(valid=False, distance=0.5, scenario="bad_match",
                                     failed_path="/tmp/failed")
        self.assertFalse(vr.valid)
        self.assertEqual(vr.distance, 0.5)
        self.assertEqual(vr.failed_path, "/tmp/failed")




def _public_methods(cls: type) -> set[str]:
    """Return the set of non-underscore method names provided by ``cls``,
    including those contributed by base classes / mixins.

    BOTH classes are composed from cluster mixins now: ``PipelineDB`` from
    ``lib/pipeline_db/`` since #379, ``FakePipelineDB`` from
    ``tests/fakes/pipeline_db/`` since #1313. Neither keeps its public API
    in ``vars(cls)``: measured on the fake, ``vars`` yields 0 public
    callables where this MRO walk (skipping ``object``) yields 203.

    Degrading this to ``vars(cls)`` does NOT make the contract test below
    report the fake as missing, which is the intuitive but wrong
    prediction. It empties BOTH sides, so ``real - fake`` stays empty and
    the comparison passes vacuously. Measured: that mutant survived every
    test in ``TestPipelineDBFakeContract`` until
    ``test_recovered_surfaces_are_not_empty`` was added to catch it."""
    names: set[str] = set()
    for klass in cls.__mro__:
        if klass is object:
            continue
        for name, obj in vars(klass).items():
            if callable(obj) and not name.startswith("_"):
                names.add(name)
    return names


class TestPipelineDBFakeContract(unittest.TestCase):
    """Enforce FakePipelineDB stays in lockstep with PipelineDB.

    Models ``TestRouteContractAudit`` (tests/web/test_route_audit.py). The
    New Work Checklist row in ``.claude/rules/code-quality.md`` asks a new
    ``PipelineDB`` method for a matching stub on ``FakePipelineDB`` and a
    self-test in that cluster's test module; the stub half is enforced
    here at test time, not at review time.

    A new kwarg on a real method can otherwise be silently swallowed if the
    fake accepts ``**kwargs``.
    """

    def test_recovered_surfaces_are_not_empty(self) -> None:
        """``real - fake`` is empty when BOTH sides are empty.

        Both classes are mixin-composed now (``PipelineDB`` since #379,
        ``FakePipelineDB`` since #1313), so neither keeps a public method
        in ``vars(cls)``. Degrading ``_public_methods`` to a plain
        ``vars(cls)`` scan therefore leaves every other test in this class
        green while it compares two empty sets. Measured: that mutant
        survived all three tests here before this one existed.
        """
        for cls in (PipelineDB, FakePipelineDB):
            with self.subTest(cls=cls.__name__):
                self.assertGreater(
                    len(_public_methods(cls)), 100,
                    f"{cls.__name__} recovered a near-empty public surface, "
                    "so every comparison in this class is vacuous",
                )

    def test_fake_exposes_every_public_method_of_real(self) -> None:
        """Every non-underscore method on ``PipelineDB`` must exist on
        ``FakePipelineDB``."""
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        missing = real - fake
        self.assertEqual(
            missing, set(),
            f"FakePipelineDB is missing stubs for: {sorted(missing)}. "
            "See .claude/rules/code-quality.md 'New PipelineDB method' "
            "in the new-work checklist.",
        )

    def test_fake_only_methods_stay_on_the_allowlist(self) -> None:
        """Methods on ``FakePipelineDB`` that don't mirror ``PipelineDB``
        must be intentional test helpers on an explicit allowlist.

        Catches typos in new stub names
        (``update_importred_path_by_release_id`` would pass the
        ``real - fake`` check because the method isn't on real, but
        the sigcheck never exercises it). Without this inverse
        enforcement, a typo'd stub would compile and tests against it
        would crash with ``AttributeError`` — the exact silent-drift
        vector this contract is meant to prevent.
        """
        allowed_fake_only = {
            "seed_request",
            "request",
            "assert_log",
            "set_advisory_lock_result",
            "set_cooldown_result",
            "set_update_download_state_error",
            "arm_request_creation_race",
            "queue_execute_results",
            "seed_youtube_album_mapping",
        }
        real = _public_methods(PipelineDB)
        fake = _public_methods(FakePipelineDB)
        unexpected = fake - real - allowed_fake_only
        self.assertEqual(
            unexpected, set(),
            f"FakePipelineDB has methods not on PipelineDB and not on "
            f"the allowlist: {sorted(unexpected)}. If these are "
            "intentional test helpers, add them to "
            "``allowed_fake_only``. If they're typo'd stubs meant to "
            "mirror a real method, rename them.",
        )

    def test_fake_signatures_compatible_with_real(self) -> None:
        """For every shared method, each named parameter on the real
        method must be declared by name on the fake with a compatible
        kind and no stricter requiredness.

        This catches "real added a new kwarg; fake silently ignored it"
        drift. Crucially, a bare ``**kwargs`` on the fake is NOT allowed
        to absorb a named real parameter — otherwise a fake that
        accepts ``**kwargs`` would pass this check for any real
        signature, reproducing the exact silent-drift failure mode the
        contract is meant to prevent.

        ``**kwargs`` on the fake may still absorb test-only extras and
        matches the real's own ``**kwargs`` when present. Return types
        and type annotations are not checked — the fake is free to use
        ``Any`` for brevity.
        """
        mismatches = _diff_signatures(PipelineDB, FakePipelineDB)
        self.assertEqual(
            mismatches, [],
            "FakePipelineDB signatures drifted from PipelineDB. "
            "Every real parameter must be named explicitly on the fake "
            "(bare **kwargs does NOT satisfy the contract). "
            "Mismatches:\n  "
            + "\n  ".join(mismatches),
        )


_POSITIONAL_KINDS = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _diff_signatures(real_cls: type, fake_cls: type) -> list[str]:
    """Return a list of signature drift messages between two classes.

    The invariant the reviewers kept circling: the fake must be
    substitutable for the real in every production-valid call pattern.
    Checks, in order, what a caller could observe:

    1. Positional layout must match exactly. Any reorder, insertion,
       or rename at a positional slot would bind ``add_request("A",
       "B", "request")`` to the wrong parameter on the fake (codex R4).
    2. Every named real parameter must be declared by name on the
       fake. ``**kwargs`` absorption is NOT sufficient — a fake that
       absorbs a renamed kwarg silently reproduces the drift this
       contract is meant to prevent (round 1).
    3. Kinds must match exactly. Narrowing positional-or-keyword to
       keyword-only breaks positional callers (codex R3).
    4. Requiredness drift in both directions: real required → fake
       optional lets the fake accept calls real would reject; real
       optional → fake required crashes calls real would handle
       (codex R3).
    5. ``*args`` / ``**kwargs`` on real require equivalents on fake
       so variadic callers don't silently lose arguments.

    The fake may add trailing keyword-only parameters with defaults
    (for test-only bookkeeping) and absorb test-only extras with
    ``**kwargs`` — those are not visible to any real-valid caller so
    they do not need to be mirrored back onto real.
    """
    real_methods = _public_methods(real_cls)
    fake_methods = _public_methods(fake_cls)
    shared = real_methods & fake_methods

    mismatches: list[str] = []
    for name in sorted(shared):
        real_sig = inspect.signature(getattr(real_cls, name))
        fake_sig = inspect.signature(getattr(fake_cls, name))

        mismatches.extend(_diff_positional_layout(name, real_sig, fake_sig))
        mismatches.extend(_diff_named_params(name, real_sig, fake_sig))
        mismatches.extend(_diff_variadic(name, real_sig, fake_sig))
        mismatches.extend(_diff_fake_only_required(name, real_sig, fake_sig))
    return mismatches


def _positional_params(
    sig: inspect.Signature,
) -> list[inspect.Parameter]:
    return [
        p for p in sig.parameters.values()
        if p.name != "self" and p.kind in _POSITIONAL_KINDS
    ]


def _diff_positional_layout(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Positional slots must match real exactly — no reorder, no extras.

    Python binds positional args by index; a fake that adds
    ``add_request(album_title, artist_name, source)`` would satisfy the
    name-matching check while binding ``add_request("Artist", "Album",
    "request")`` to the wrong parameters (codex R4).
    """
    out: list[str] = []
    real_pos = _positional_params(real_sig)
    fake_pos = _positional_params(fake_sig)

    for i, rp in enumerate(real_pos):
        if i >= len(fake_pos):
            out.append(
                f"{method}: positional slot {i} ('{rp.name}') "
                "present on real but missing from fake's positional "
                "sequence")
            continue
        fp = fake_pos[i]
        if fp.name != rp.name:
            out.append(
                f"{method}: positional slot {i} — real='{rp.name}', "
                f"fake='{fp.name}' (reorder, rename, or inserted "
                "parameter would break positional callers)")
    if len(fake_pos) > len(real_pos):
        extras = [fp.name for fp in fake_pos[len(real_pos):]]
        out.append(
            f"{method}: fake has extra positional parameters beyond "
            f"real: {extras} (a positional call on real would bind "
            "nothing to these slots on the fake)")
    return out


def _diff_named_params(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Every named real param must be declared on the fake with a
    compatible kind and requiredness."""
    out: list[str] = []
    fake_params = fake_sig.parameters
    for pname, param in real_sig.parameters.items():
        if pname == "self":
            continue
        if param.kind in (inspect.Parameter.VAR_POSITIONAL,
                          inspect.Parameter.VAR_KEYWORD):
            continue
        if pname not in fake_params:
            out.append(
                f"{method}: param '{pname}' present on real but "
                "not declared on fake (declare it explicitly — "
                "**kwargs does not count)")
            continue
        fp = fake_params[pname]
        if fp.kind != param.kind:
            out.append(
                f"{method}({pname}): kind mismatch — "
                f"real={param.kind.name}, fake={fp.kind.name}")
            continue
        real_required = param.default is inspect.Parameter.empty
        fake_required = fp.default is inspect.Parameter.empty
        if real_required and not fake_required:
            out.append(
                f"{method}({pname}): real requires this param but "
                "fake gives it a default (silently makes it optional)")
        elif fake_required and not real_required:
            out.append(
                f"{method}({pname}): real has a default but fake "
                "requires this param (production calls that omit it "
                "would crash against the fake)")
    return out


def _diff_fake_only_required(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """Fake params absent from real must have defaults.

    A fake that adds a required keyword-only parameter
    (e.g. ``def m(self, request_id, *, new_required):``) has no match
    in ``_diff_named_params`` — that helper walks only real params.
    Every production call that omits the new kwarg works against real
    but raises ``TypeError`` against the fake. Codex R5.

    Optional extras (with defaults) are fine — they represent
    test-only bookkeeping the fake may accept.
    """
    out: list[str] = []
    real_names = {p.name for p in real_sig.parameters.values()}
    for fp in fake_sig.parameters.values():
        if fp.name == "self":
            continue
        if fp.kind in (inspect.Parameter.VAR_POSITIONAL,
                       inspect.Parameter.VAR_KEYWORD):
            continue
        if fp.name in real_names:
            continue
        # Fake-only parameter. Required → crashes real-valid callers.
        if fp.default is inspect.Parameter.empty:
            out.append(
                f"{method}({fp.name}): fake requires a parameter not "
                "on real — production calls that omit it would crash "
                "against the fake (give it a default, or remove it)")
    return out


def _diff_variadic(
    method: str,
    real_sig: inspect.Signature,
    fake_sig: inspect.Signature,
) -> list[str]:
    """``*args`` / ``**kwargs`` on real require equivalents on fake."""
    out: list[str] = []
    fake_accepts_varargs = any(
        p.kind == inspect.Parameter.VAR_POSITIONAL
        for p in fake_sig.parameters.values())
    fake_accepts_kwargs = any(
        p.kind == inspect.Parameter.VAR_KEYWORD
        for p in fake_sig.parameters.values())
    for param in real_sig.parameters.values():
        if (param.kind == inspect.Parameter.VAR_POSITIONAL
                and not fake_accepts_varargs):
            out.append(
                f"{method}: real has *{param.name} but fake does "
                "not accept variable positional args")
        elif (param.kind == inspect.Parameter.VAR_KEYWORD
                and not fake_accepts_kwargs):
            out.append(
                f"{method}: real has **{param.name} but fake does "
                "not accept variable keyword args")
    return out


class TestPipelineDBFakeContractInternals(unittest.TestCase):
    """Regression tests for the drift detector itself.

    The detector must fail when real and fake disagree, otherwise the
    outer contract test is a silent no-op. Exercise the drift cases
    directly.
    """

    def test_kwargs_does_not_absorb_named_param(self):
        """Bare **kwargs on fake must NOT satisfy a named real param."""
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, **kwargs: object) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'flag'" in m for m in diff),
            f"Expected drift for named param 'flag', got: {diff}")

    def test_annotations_are_not_compared(self):
        """The contract's docstring says a fake may annotate as loosely as it
        likes. That was only ever illustrated by the fixtures' own spelling,
        which asserted nothing; assert it instead, so the fixtures above are
        free to use whatever annotation reads best.
        """
        class Real:
            def m(self, request_id: int, **extra: int) -> None:
                ...
        class Fake:
            def m(self, request_id: str, **extra: str) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])

    def test_renamed_param_is_caught(self):
        class Real:
            def m(self, spectral_grade: str | None = None) -> None:
                ...
        class Fake:
            def m(self, grade: str | None = None) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("'spectral_grade'" in m for m in diff),
            f"Expected drift for renamed param, got: {diff}")

    def test_required_becoming_optional_is_caught(self):
        class Real:
            def m(self, release_id: str) -> None:
                ...
        class Fake:
            def m(self, release_id: str = "") -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("release_id" in m and "optional" in m for m in diff),
            f"Expected requiredness drift, got: {diff}")

    def test_clean_signature_yields_no_diff(self):
        class Real:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, request_id: int, flag: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])

    def test_star_kwargs_on_real_still_requires_fake_kwargs(self):
        class Real:
            def m(self, **extra: object) -> None:
                ...
        class Fake:
            def m(self) -> None:  # no **kwargs
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("**extra" in m for m in diff),
            f"Expected drift when fake drops **kwargs, got: {diff}")

    def test_positional_or_keyword_narrowed_to_keyword_only_is_caught(self):
        """Codex R3: a fake that narrows pos-or-keyword to keyword-only
        would break every caller using positional args — must fail the
        contract so fake-backed tests cannot silently green."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, *, artist_name: str, album_title: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("kind mismatch" in m for m in diff),
            f"Expected drift for narrowed kind, got: {diff}")

    def test_optional_becoming_required_on_fake_is_caught(self):
        """Codex R3: a fake that drops a default would force production
        callers to pass the arg — production calls that omit it would
        work against real but crash the fake."""
        class Real:
            def m(self, flag: bool = False) -> None:
                ...
        class Fake:
            def m(self, flag: bool) -> None:  # no default
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("fake requires this param" in m for m in diff),
            f"Expected drift for tightened requiredness, got: {diff}")

    def test_positional_reorder_is_caught(self):
        """Codex R4: a fake that swaps positional parameter order
        would bind positional args to the wrong params. Name-matching
        alone cannot catch this — the positional layout must be
        checked by index."""
        class Real:
            def m(self, artist_name: str, album_title: str,
                  source: str) -> None:
                ...
        class Fake:
            def m(self, album_title: str, artist_name: str,
                  source: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("positional slot" in m for m in diff),
            f"Expected drift for reordered positional params, got: "
            f"{diff}")

    def test_fake_with_extra_positional_param_is_caught(self):
        """Codex R4: a fake that adds an extra positional parameter
        beyond real breaks positional callers — real's call pattern
        would leave that slot unbound on the fake."""
        class Real:
            def m(self, artist_name: str, album_title: str) -> None:
                ...
        class Fake:
            def m(self, artist_name: str, album_title: str,
                  new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("extra positional parameters" in m for m in diff),
            f"Expected drift for fake with extra positional, got: "
            f"{diff}")

    def test_fake_with_required_keyword_only_not_on_real_is_caught(self):
        """Codex R5: a fake that adds a required keyword-only
        parameter real doesn't have would crash any production-valid
        call that omits it."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *, new_required: str) -> None:
                ...
        diff = _diff_signatures(Real, Fake)
        self.assertTrue(
            any("new_required" in m and "not on real" in m
                for m in diff),
            f"Expected drift for required fake-only kwarg, got: "
            f"{diff}")

    def test_fake_with_optional_keyword_only_not_on_real_is_allowed(self):
        """Optional fake-only params (for test-only bookkeeping) are
        permitted — real-valid callers never pass them, so they don't
        affect call compatibility."""
        class Real:
            def m(self, request_id: int) -> None:
                ...
        class Fake:
            def m(self, request_id: int, *,
                  test_only: bool = False) -> None:
                ...
        self.assertEqual(_diff_signatures(Real, Fake), [])


class TestFakePipelineDBSourceRejectAndRequeueGating(unittest.TestCase):
    """Issue #1077, R4-5 (round-4 review): ``FakePipelineDBSource.
    reject_and_requeue`` must gate identically to the real
    ``album_source.DatabaseSource.reject_and_requeue`` it stands in for —
    a single falsy ``request_id`` check before branching on
    ``import_job_id``, not a per-branch ``isinstance(request_id, int)``
    re-check that treats ``request_id=0`` as valid, and not an
    additional ``get_import_job(...) is not None`` requirement production
    never applies before taking the deferred path."""

    def _source(self):
        from tests.fakes import FakePipelineDB, FakePipelineDBSource
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="downloading"))
        return FakePipelineDBSource(db), db

    def test_falsy_request_id_writes_nothing_on_the_sync_branch(self) -> None:
        """``request_id=0`` is falsy — production's own ``if not
        request_id: return None`` (``album_source.py``) writes nothing for
        it. An ``isinstance(0, int)`` check would wrongly treat it as a
        valid request and write a full requeue+log+denylist."""
        from lib.quality import ValidationResult

        source, db = self._source()
        album = MagicMock(db_request_id=0)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(album, result)

        self.assertIsNone(outcome)
        self.assertEqual(db.download_logs, [])
        self.assertEqual(db.denylist, [])

    def test_falsy_request_id_writes_nothing_on_the_deferred_branch(self) -> None:
        """Same falsy gate applies before the ``import_job_id`` branch
        decision is even made — ``request_id=0`` (falsy but ``isinstance``-
        valid, same distinguishing case as the sync-branch pin above) must
        not reach the deferred path either."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
        from lib.quality import ValidationResult

        source, db = self._source()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(
                download_log_id=1, failed_path="/tmp/cratedigger-r4-5-test"),
        )
        album = MagicMock(db_request_id=0)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(
            album, result, import_job_id=job.id)

        self.assertIsNone(outcome)

    def test_unseeded_import_job_id_still_takes_the_deferred_path(self) -> None:
        """Production takes the deferred path on ``import_job_id is not
        None`` alone (``album_source.py``) — it never checks the job
        exists first. The fake used to require
        ``get_import_job(...) is not None``, which made an unseeded job id
        silently fall through to the SYNC branch instead — a materially
        different code path than production would take for the same
        input. This proves the fake now takes the SAME (deferred) path
        regardless of whether the id happens to be seeded."""
        from lib.quality import ValidationResult
        from lib.terminal_outcomes import PendingImportTerminalOutcome

        source, db = self._source()
        self.assertIsNone(db.get_import_job(999999))
        album = MagicMock(db_request_id=42)
        result = ValidationResult(
            valid=False, distance=0.4, scenario="high_distance",
            detail="test",
        )

        outcome = source.reject_and_requeue(
            album, result, import_job_id=999999)

        # The deferred path returns a PendingImportTerminalOutcome command
        # bundle, never a plain int/None sync-path return.
        self.assertIsInstance(outcome, PendingImportTerminalOutcome)
        # And critically: no download_log row was written directly — a
        # sync-branch fallthrough would have written one immediately.
        self.assertEqual(db.download_logs, [])


if __name__ == "__main__":
    unittest.main()
