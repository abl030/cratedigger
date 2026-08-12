"""Deterministic pins for the live-corpus render differential harness."""

from __future__ import annotations

import contextlib
import datetime
import io
import json
import os
import subprocess
import sys
import unittest
import unittest.mock
from collections.abc import Iterator, Mapping
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import msgspec
import msgspec.inspect

from lib.quality import (
    AudioQualityMeasurement,
    ImportResult,
    QualityComparisonBasis,
    TargetQualityContract,
)
from scripts.render_differential import (
    _EXPLORER_UNWATCHED,
    _EXPLORER_WATCHED,
    DEFAULT_TARGET_SPEC,
    ClassifyRenderTarget,
    DiffReport,
    FieldChange,
    RenderDifferentialError,
    RenderedRow,
    contains_text,
    format_report,
    load_render_target,
    main,
    project_output_fields,
    read_rendered,
    summarize_render_diff,
    unwatched_field_names,
    watched_field_names,
)
from web.classify import ClassifiedEntry, LogEntry, classify_log_entry
from web.download_history_view import (
    _classify_pipeline_log_item,
    build_recents_download_log_rows,
)


def _row(row_id: int, **fields: object) -> RenderedRow:
    return RenderedRow(id=row_id, fields=dict(fields))


@contextlib.contextmanager
def _quiet() -> Iterator[io.StringIO]:
    """Run the CLI without spraying its report across the suite output."""
    out = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(out):
        yield out


def _render_one(row: Mapping[str, object]) -> RenderedRow:
    """Render a single row through the real default target."""
    target = ClassifyRenderTarget()
    target.prepare([row])
    return target.render(row)


def upper_verdict_target(row: Mapping[str, object]) -> RenderedRow:
    """A second render target, resolved through ``--target`` in tests."""
    rendered = _render_one(row)
    upper = dict(rendered.fields)
    verdict = upper["verdict"]
    upper["verdict"] = verdict.upper() if isinstance(verdict, str) else verdict
    return RenderedRow(id=rendered.id, fields=upper)


def not_a_render_target(row: Mapping[str, object]) -> str:
    """A ``--target`` that returns the wrong type, on purpose."""
    return str(row.get("id"))


def _download_log_row(**overrides: object) -> dict[str, object]:
    """A production-shaped ``download_log`` row from the read seam."""
    row: dict[str, object] = {
        "id": 4242,
        "request_id": 77,
        "outcome": "rejected",
        "created_at": datetime.datetime(
            2026, 7, 26, 4, 30, tzinfo=datetime.UTC),
        "beets_scenario": "mbid_not_found",
        "beets_distance": 0.04,
        "soulseek_username": "peer",
        "album_title": "Beelzebub",
        "artist_name": "Kikagaku Moyo",
        "request_status": "wanted",
        "_request_mb_release_id": "render-differential-release",
        "_evidence_mb_release_id": "render-differential-release",
        "_current_evidence_mb_release_id": "render-differential-release",
    }
    row.update(overrides)
    return row


def _import_result_json(result: ImportResult) -> dict[str, object]:
    """The JSONB shape production persists for an ImportResult."""
    payload: dict[str, object] = msgspec.to_builtins(result)
    return payload


_BASIS = QualityComparisonBasis(
    verdict="worse", branch="rank", new_rank="good", existing_rank="lossless",
    new_metric="avg", existing_metric="avg",
    new_value_kbps=320, existing_value_kbps=1000,
    new_format="MP3", existing_format="FLAC",
)


class _NestedText(msgspec.Struct):
    """A nested Struct whose text a fail-open derivation would miss."""

    note: str


class _DerivationProbe(msgspec.Struct):
    """Declared-type shapes the derivation must fail closed on."""

    nested: _NestedText
    optional_nested: _NestedText | None
    anything: object
    loose: dict[str, object]
    number: int
    flag: bool | None


class TestWatchedFieldDerivation(unittest.TestCase):
    """The watched set comes from the output type and fails CLOSED."""

    def setUp(self) -> None:
        self.watched = watched_field_names(ClassifiedEntry)
        self.unwatched = unwatched_field_names(ClassifiedEntry)
        info = msgspec.inspect.type_info(ClassifiedEntry)
        assert isinstance(info, msgspec.inspect.StructType)
        self.declared = tuple(field.encode_name for field in info.fields)

    def test_watched_and_unwatched_partition_the_struct(self) -> None:
        self.assertEqual(
            sorted([*self.watched, *self.unwatched]), sorted(self.declared))
        self.assertEqual(set(self.watched) & set(self.unwatched), set())

    def test_declaration_order_is_preserved(self) -> None:
        self.assertEqual(
            list(self.watched),
            [name for name in self.declared if name in set(self.watched)],
        )

    def test_the_885_evidence_fields_are_all_watched(self) -> None:
        for name in ("badge", "badge_class", "border_color",
                     "downloaded_label", "verdict", "summary"):
            with self.subTest(field=name):
                self.assertIn(name, self.watched)

    def test_list_of_str_fields_are_watched(self) -> None:
        self.assertIn("bad_extensions", self.watched)
        self.assertIn("wrong_match_triage_stage_chain", self.watched)

    def test_comparison_basis_is_watched(self) -> None:
        # dict[str, object] carrying eight operator-visible strings, rendered
        # as the card's "Compared" evidence row. The earlier fail-open
        # derivation skipped it, so nulling every basis rendered as zero
        # changes over the whole live corpus.
        self.assertIn("comparison_basis", self.watched)

    def test_only_provably_numeric_fields_are_unwatched(self) -> None:
        # Derived from the declared type, not from a hand-list: every
        # unwatched field must be a number/bool, optionally optional.
        info = msgspec.inspect.type_info(ClassifiedEntry)
        assert isinstance(info, msgspec.inspect.StructType)
        by_name = {field.encode_name: field.type for field in info.fields}
        numeric = (msgspec.inspect.IntType, msgspec.inspect.FloatType,
                   msgspec.inspect.BoolType, msgspec.inspect.NoneType)
        for name in self.unwatched:
            with self.subTest(field=name):
                declared = by_name[name]
                members = (
                    declared.types
                    if isinstance(declared, msgspec.inspect.UnionType)
                    else (declared,)
                )
                self.assertTrue(all(isinstance(m, numeric) for m in members))

    def test_unknown_declared_types_fail_closed_into_the_watched_set(
        self,
    ) -> None:
        watched = watched_field_names(_DerivationProbe)
        for name in ("nested", "optional_nested", "anything", "loose"):
            with self.subTest(field=name):
                self.assertIn(name, watched)
        self.assertEqual(
            unwatched_field_names(_DerivationProbe), ("number", "flag"))

    def test_non_struct_output_type_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            watched_field_names(LogEntry)  # pyright: ignore[reportArgumentType]


class TestConverseOracle(unittest.TestCase):
    """No unwatched field may hold text — the check that was missing."""

    def test_real_classify_output_never_puts_text_outside_the_watched_set(
        self,
    ) -> None:
        # project_output_fields raises if it does, so a clean render IS the
        # assertion. Drive several real production scenarios.
        for row in (
            _download_log_row(),
            _download_log_row(outcome="success", was_converted=True,
                              original_filetype="flac",
                              spectral_grade="genuine"),
            _download_log_row(outcome="timeout",
                              error_message="peer went offline"),
            _download_log_row(outcome="have_analysis_error"),
            _download_log_row(import_result=_import_result_json(ImportResult(
                decision="downgrade", comparison_basis=_BASIS))),
        ):
            with self.subTest(outcome=row["outcome"]):
                self.assertTrue(_render_one(row).fields)

    def test_text_in_an_unwatched_field_fails_closed(self) -> None:
        # msgspec Structs do not validate on construction, so this is the
        # exact runtime shape the converse check exists to catch.
        entry = ClassifiedEntry(
            badge="b", badge_class="c", border_color="#fff",
            verdict="v", summary="s",
            actual_min_bitrate="320",  # pyright: ignore[reportArgumentType]
        )
        item: dict[str, object] = msgspec.to_builtins(entry)
        with self.assertRaises(RenderDifferentialError) as caught:
            project_output_fields(
                item,
                watched_field_names(ClassifiedEntry),
                unwatched_field_names(ClassifiedEntry),
            )
        self.assertIn("actual_min_bitrate", str(caught.exception))

    def test_an_undercounting_watched_set_is_caught_on_real_output(
        self,
    ) -> None:
        # The exact defect this fixes: drop comparison_basis from the watched
        # set on a real row that has one, and the converse check trips.
        item = _classify_pipeline_log_item(_download_log_row(
            import_result=_import_result_json(ImportResult(
                decision="downgrade", comparison_basis=_BASIS))))
        self.assertIsNotNone(item["comparison_basis"])
        watched = tuple(
            name for name in watched_field_names(ClassifiedEntry)
            if name != "comparison_basis")
        unwatched = (*unwatched_field_names(ClassifiedEntry),
                     "comparison_basis")
        with self.assertRaises(RenderDifferentialError) as caught:
            project_output_fields(item, watched, unwatched)
        self.assertIn("comparison_basis", str(caught.exception))

    def test_contains_text_oracle(self) -> None:
        for value, expected in (
            ("x", True), ("", True), (None, False), (0, False), (True, False),
            ([], False), (["a"], True), ([1, 2], False), ({}, False),
            ({"k": 1}, True), ([[["deep"]]], True),
        ):
            with self.subTest(value=value):
                self.assertEqual(contains_text(value), expected)

    def test_missing_watched_field_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            project_output_fields({}, ("verdict",), ())

    def test_non_json_value_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            project_output_fields(
                {"verdict": datetime.datetime(2026, 7, 26, tzinfo=datetime.UTC)},
                ("verdict",),
                (),
            )


class TestClassifyRenderTargetIsTheProductionPath(unittest.TestCase):
    """The default target runs every production stage Recents runs."""

    def test_rendered_fields_match_the_real_classifier_for_a_plain_row(
        self,
    ) -> None:
        row = _download_log_row()
        rendered = _render_one(row)
        self.assertEqual(rendered.id, 4242)
        classified = classify_log_entry(LogEntry.from_row(dict(row)))
        payload: dict[str, object] = msgspec.to_builtins(classified)
        for name in watched_field_names(ClassifiedEntry):
            with self.subTest(field=name):
                self.assertEqual(rendered.fields[name], payload[name])

    def test_rendering_does_not_mutate_the_corpus_row(self) -> None:
        row = _download_log_row(_evidence_spectral_grade="genuine")
        before = dict(row)
        _render_one(row)
        self.assertEqual(row, before)

    def test_the_production_evidence_overlay_runs(self) -> None:
        with_evidence = _render_one(_download_log_row(
            outcome="success",
            spectral_grade=None,
            _evidence_spectral_grade="genuine",
            _evidence_spectral_bitrate=880,
        ))
        without_evidence = _render_one(
            _download_log_row(outcome="success", spectral_grade=None))
        self.assertEqual(with_evidence.fields["spectral_grade"], "genuine")
        self.assertIsNone(without_evidence.fields["spectral_grade"])

    def test_project_current_library_have_runs(self) -> None:
        # Measured live: disabling this route stage moves 2,603 of 36,312
        # corpus rows. Without it the differential compares existing_format
        # against a value production never shows.
        overridden = _render_one(_download_log_row(
            outcome="rejected",
            _current_evidence_id=7,
            _current_evidence_is_pre_attempt=True,
            _current_evidence_format="MP3",
            _current_evidence_min_bitrate=245,
            _current_evidence_avg_bitrate=260,
            _current_evidence_spectral_grade="transparent",
            _current_evidence_v0_probe_kind="lossless_source_v0",
        ))
        plain = _render_one(_download_log_row(outcome="rejected"))
        self.assertEqual(overridden.fields["existing_format"], "MP3")
        self.assertEqual(
            overridden.fields["existing_spectral_grade"], "transparent")
        self.assertEqual(
            overridden.fields["existing_v0_probe_kind"], "lossless_source_v0")
        self.assertIsNone(plain.fields["existing_format"])
        self.assertIsNone(plain.fields["existing_spectral_grade"])

    def test_project_current_library_have_respects_its_own_guards(
        self,
    ) -> None:
        # A successful import never receives the overlay — production's
        # first early return, driven for real rather than reimplemented.
        successful = _render_one(_download_log_row(
            outcome="success",
            _current_evidence_id=7,
            _current_evidence_is_pre_attempt=True,
            _current_evidence_format="MP3",
            _current_evidence_min_bitrate=245,
            _current_evidence_avg_bitrate=260,
        ))
        self.assertIsNone(successful.fields["existing_format"])

    def _origin_and_successor(self) -> list[dict[str, object]]:
        origin = _download_log_row(id=100, outcome="rejected")
        successor = _download_log_row(
            id=101,
            outcome="force_import",
            source_download_log_id=100,
            import_result=_import_result_json(ImportResult(
                decision="import",
                materialized_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245, avg_bitrate_kbps=260,
                    median_bitrate_kbps=255, format="MP3", is_cbr=False),
                target_quality_contract=TargetQualityContract(
                    format="MP3 V0", is_cbr=False),
            )),
        )
        return [origin, successor]

    def test_newest_successor_wins_across_all_batch_shapes(self) -> None:
        origin = _download_log_row(id=100, outcome="rejected")
        older = _download_log_row(
            id=101,
            outcome="force_import",
            source_download_log_id=100,
            import_result=_import_result_json(ImportResult(
                decision="import",
                materialized_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=245,
                    avg_bitrate_kbps=260,
                    median_bitrate_kbps=255,
                    format="MP3",
                    is_cbr=False,
                ),
            )),
        )
        newer = _download_log_row(
            id=102,
            outcome="force_import",
            source_download_log_id=100,
            import_result=_import_result_json(ImportResult(
                decision="import",
                materialized_measurement=AudioQualityMeasurement(
                    min_bitrate_kbps=118,
                    avg_bitrate_kbps=124,
                    median_bitrate_kbps=123,
                    format="Opus",
                    is_cbr=False,
                ),
            )),
        )
        cases = (
            ("page ascending", [origin, older, newer], []),
            ("page shuffled", [newer, origin, older], []),
            ("separately linked", [origin], [older, newer]),
            ("linked shuffled", [origin], [newer, older]),
            (
                "page and linked duplicates",
                [origin, older, newer],
                [newer, older],
            ),
        )
        for label, rows, linked in cases:
            with self.subTest(label=label):
                rendered = build_recents_download_log_rows(
                    rows,
                    linked_successor_rows=linked,
                )
                self.assertEqual(
                    [item["id"] for item in rendered],
                    [row["id"] for row in rows],
                )
                rendered_origin = next(
                    item for item in rendered if item["id"] == 100
                )
                self.assertEqual(
                    rendered_origin["materialized_format"],
                    "Opus",
                )

        conflicting_duplicate = {**older, "id": 102}
        with self.assertRaisesRegex(ValueError, "conflicting projection data"):
            build_recents_download_log_rows(
                [origin, newer],
                linked_successor_rows=[conflicting_duplicate],
            )

    def test_project_linked_import_evidence_runs(self) -> None:
        rows = self._origin_and_successor()
        target = ClassifyRenderTarget()
        target.prepare(rows)
        origin = target.render(rows[0])
        self.assertEqual(origin.fields["materialized_format"], "MP3")
        self.assertEqual(origin.fields["target_contract_format"], "MP3 V0")

    def test_linked_evidence_is_absent_without_the_successor(self) -> None:
        rows = self._origin_and_successor()
        target = ClassifyRenderTarget()
        target.prepare([rows[0]])
        origin = target.render(rows[0])
        self.assertIsNone(origin.fields["materialized_format"])
        self.assertIsNone(origin.fields["target_contract_format"])

    def test_default_target_equals_the_production_batch_owner(self) -> None:
        rows = self._origin_and_successor()
        expected = build_recents_download_log_rows(
            rows,
            linked_successor_rows=rows,
        )
        target = ClassifyRenderTarget()
        target.prepare(rows)
        for row, expected_item in zip(rows, expected, strict=True):
            rendered = target.render(row)
            for name in watched_field_names(ClassifiedEntry):
                with self.subTest(row=row["id"], field=name):
                    self.assertEqual(
                        rendered.fields[name],
                        expected_item[name],
                    )

    def test_row_without_an_integer_id_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            _render_one(_download_log_row(id="4242"))

    def test_boolean_id_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            _render_one(_download_log_row(id=True))


class TestWrongMatchExplorerRenderTargetIsTheProductionPath(unittest.TestCase):
    """Issue #1086: the Wrong Matches explorer's OWN render target.

    Unlike :class:`ClassifyRenderTarget`, this target's output depends on
    LIVE filesystem state rather than stored columns — proportionate
    coverage here is: the target's fields match calling
    ``build_wrong_match_explorer`` directly over the SAME real folder,
    the error branch is reached honestly, and the row-id contract fails
    closed the same way every other target does.
    """

    def _quarantine_env(self, processing_dir: str) -> dict[str, str]:
        return {
            "CRATEDIGGER_QUARANTINE_SLSKD_DIR": "",
            "CRATEDIGGER_QUARANTINE_STAGING_DIR": "",
            "CRATEDIGGER_QUARANTINE_PROCESSING_DIR": processing_dir,
        }

    def test_rendered_fields_match_the_real_explorer_for_a_refused_entry(
        self,
    ) -> None:
        from scripts.render_differential import WrongMatchExplorerRenderTarget
        from web.wrong_match_file_service import build_wrong_match_explorer

        with TemporaryDirectory() as root:
            processing_dir = os.path.join(root, "processing")
            album = os.path.join(
                processing_dir, "albums", "wrong_matches", "Album")
            os.makedirs(album)
            readable = os.path.join(album, "01 - Readable.mp3")
            with open(readable, "wb") as handle:
                handle.write(b"\x00" * 16)
            locked = os.path.join(album, "02 - Locked.mp3")
            with open(locked, "wb") as handle:
                handle.write(b"\x00" * 16)
            os.chmod(locked, 0o000)
            try:
                row = {
                    "id": 909,
                    "validation_result": json.dumps({"failed_path": album}),
                }
                with unittest.mock.patch.dict(
                    os.environ, self._quarantine_env(processing_dir),
                    clear=False,
                ):
                    target = WrongMatchExplorerRenderTarget()
                    target.prepare([row])
                    rendered = target.render(row)
                    from lib.config import CratediggerConfig
                    expected = build_wrong_match_explorer(
                        download_log_id=909, entry=row,
                        cfg=CratediggerConfig(processing_dir=processing_dir),
                    )
            finally:
                os.chmod(locked, 0o600)

        self.assertEqual(rendered.id, 909)
        self.assertEqual(rendered.fields["status"], expected["status"])
        self.assertEqual(
            rendered.fields["unreadable_reason"],
            expected["unreadable_reason"])
        self.assertEqual(
            rendered.fields["failed_path"], expected["failed_path"])
        self.assertEqual(rendered.fields["files"], expected["files"])
        # ``unreadable_entry_count`` and ``partial`` are provably
        # numeric/boolean (``WrongMatchExplorerPayload``), so they are
        # UNWATCHED and never reach ``rendered.fields`` — assert the
        # underlying fact directly against the real production payload
        # instead of through the differential's watched projection.
        self.assertEqual(expected["unreadable_entry_count"], 1)
        self.assertEqual(expected["partial"], True)
        # The load-bearing fact this whole issue is about: a refusal is
        # counted and honestly worded, never silently dropped.
        assert isinstance(rendered.fields["unreadable_reason"], str)
        self.assertIn("may be transient", rendered.fields["unreadable_reason"])
        # Blocker 3: the field set is exactly what the Struct DERIVES,
        # not a hand-picked subset — the old 5-key hand-pick hid 10 keys,
        # including ``files``/``failed_path``, from every future
        # differential run against this target.
        self.assertEqual(set(rendered.fields), set(_EXPLORER_WATCHED))
        # "Also fix" (#1086 review, round 2): ``WrongMatchExplorerPayload``
        # is a SEPARATE declaration from the production return dict, kept
        # in sync only by convention — its own module docstring admits a
        # key added to ``build_wrong_match_explorer`` and not here
        # silently drops out of every future differential rather than
        # failing loudly, because ``project_output_fields`` only ever
        # iterates the fields THIS Struct declares. Close that gap here:
        # the real production function's key set must equal the Struct's
        # full declared field set (watched + unwatched together), so a
        # key added to one and not the other fails this assertion instead
        # of silently escaping every future watched-set derivation.
        self.assertEqual(
            set(expected.keys()),
            set(_EXPLORER_WATCHED) | set(_EXPLORER_UNWATCHED),
            "build_wrong_match_explorer's real return-dict keys have "
            "drifted from WrongMatchExplorerPayload's declared fields — "
            "update the Struct to match",
        )

    def test_a_missing_folder_renders_as_its_own_error_shape(self) -> None:
        from scripts.render_differential import WrongMatchExplorerRenderTarget

        with TemporaryDirectory() as root:
            processing_dir = os.path.join(root, "processing")
            os.makedirs(os.path.join(processing_dir, "albums"))
            row = {
                "id": 910,
                "validation_result": json.dumps({
                    "failed_path": os.path.join(
                        processing_dir, "albums", "wrong_matches", "Gone"),
                }),
            }
            with unittest.mock.patch.dict(
                os.environ, self._quarantine_env(processing_dir),
                clear=False,
            ):
                target = WrongMatchExplorerRenderTarget()
                target.prepare([row])
                rendered = target.render(row)
        self.assertEqual(rendered.id, 910)
        self.assertEqual(rendered.fields["status"], "error")
        assert isinstance(rendered.fields["unreadable_reason"], str)
        self.assertIn("not found", rendered.fields["unreadable_reason"])

    def test_row_without_an_integer_id_fails_closed(self) -> None:
        from scripts.render_differential import WrongMatchExplorerRenderTarget

        target = WrongMatchExplorerRenderTarget()
        with self.assertRaises(RenderDifferentialError):
            target.render({"id": "909", "validation_result": "{}"})

    def test_dotted_spec_resolves_this_target(self) -> None:
        target = load_render_target(
            "scripts.render_differential:WrongMatchExplorerRenderTarget")
        from scripts.render_differential import WrongMatchExplorerRenderTarget
        self.assertIsInstance(target, WrongMatchExplorerRenderTarget)


class TestSummarizeRenderDiff(unittest.TestCase):
    """The summarizer is a pure function over two rendered corpora."""

    def test_identical_corpora_report_no_change(self) -> None:
        rows = [_row(1, verdict="a", summary="b"),
                _row(2, verdict="c", summary="d")]
        report = summarize_render_diff(rows, list(rows))
        self.assertEqual(report.total_rows, 2)
        self.assertEqual(report.changed_rows, 0)
        self.assertEqual(report.changed_by_field, {"summary": 0, "verdict": 0})
        self.assertEqual(report.samples, [])

    def test_every_field_is_reported_including_the_unchanged_ones(self) -> None:
        base = [_row(1, badge="Rejected", verdict="old", summary="s")]
        current = [_row(1, badge="Rejected", verdict="new", summary="s")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            report.changed_by_field, {"badge": 0, "summary": 0, "verdict": 1})

    def test_nested_object_values_are_compared(self) -> None:
        base = [_row(1, comparison_basis={"verdict": "worse", "new_rank": "good"})]
        current = [_row(1, comparison_basis={"verdict": "worse", "new_rank": "bad"})]
        report = summarize_render_diff(base, current)
        self.assertEqual(report.changed_by_field, {"comparison_basis": 1})

    def test_nulling_a_nested_object_is_a_change(self) -> None:
        base = [_row(1, comparison_basis={"verdict": "worse"})]
        current = [_row(1, comparison_basis=None)]
        self.assertEqual(summarize_render_diff(base, current).changed_rows, 1)

    def test_one_row_changing_two_fields_counts_one_changed_row(self) -> None:
        base = [_row(1, verdict="old", summary="old")]
        current = [_row(1, verdict="new", summary="new")]
        report = summarize_render_diff(base, current)
        self.assertEqual(report.changed_rows, 1)
        self.assertEqual(report.changed_by_field, {"summary": 1, "verdict": 1})

    def test_two_rows_changing_one_field_counts_two_changed_rows(self) -> None:
        base = [_row(1, verdict="old"), _row(2, verdict="old")]
        current = [_row(1, verdict="new"), _row(2, verdict="new")]
        report = summarize_render_diff(base, current)
        self.assertEqual(report.changed_rows, 2)
        self.assertEqual(report.changed_by_field, {"verdict": 2})

    def test_rows_are_matched_by_id_not_by_position(self) -> None:
        base = [_row(1, verdict="a"), _row(2, verdict="b")]
        current = [_row(2, verdict="b"), _row(1, verdict="a")]
        self.assertEqual(summarize_render_diff(base, current).changed_rows, 0)

    def test_list_and_null_valued_changes_are_counted(self) -> None:
        base = [_row(1, bad_extensions=["a.cue"], transfer_message=None)]
        current = [_row(1, bad_extensions=[], transfer_message="Peer said no")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            report.changed_by_field, {"bad_extensions": 1, "transfer_message": 1})

    def test_samples_are_bounded_per_field_and_carry_both_sides(self) -> None:
        base = [_row(i, verdict="old") for i in range(1, 6)]
        current = [_row(i, verdict=f"new-{i}") for i in range(1, 6)]
        report = summarize_render_diff(base, current, samples_per_field=2)
        self.assertEqual(report.changed_by_field, {"verdict": 5})
        self.assertEqual(len(report.samples), 2)
        self.assertEqual(
            [(s.id, s.base, s.current) for s in report.samples],
            [(1, "old", "new-1"), (2, "old", "new-2")],
        )

    def test_samples_are_ordered_by_field_then_id(self) -> None:
        base = [_row(2, alpha="a", zeta="z"), _row(1, alpha="a", zeta="z")]
        current = [_row(2, alpha="A", zeta="Z"), _row(1, alpha="A", zeta="Z")]
        report = summarize_render_diff(base, current)
        self.assertEqual(
            [(s.field, s.id) for s in report.samples],
            [("alpha", 1), ("alpha", 2), ("zeta", 1), ("zeta", 2)],
        )

    def test_empty_corpora_report_nothing(self) -> None:
        report = summarize_render_diff([], [])
        self.assertEqual(
            (report.total_rows, report.changed_rows, report.changed_by_field),
            (0, 0, {}),
        )


class TestSummarizeFailsClosed(unittest.TestCase):
    """Every shape that could hide a changed row raises instead."""

    def test_row_missing_from_current(self) -> None:
        with self.assertRaises(RenderDifferentialError) as caught:
            summarize_render_diff(
                [_row(1, verdict="a"), _row(2, verdict="b")],
                [_row(1, verdict="a")])
        self.assertIn("2", str(caught.exception))

    def test_row_absent_from_base(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a")],
                [_row(1, verdict="a"), _row(2, verdict="b")])

    def test_duplicate_id_in_base(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a"), _row(1, verdict="a")],
                [_row(1, verdict="a")])

    def test_duplicate_id_in_current(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a")],
                [_row(1, verdict="a"), _row(1, verdict="a")])

    def test_field_set_drift_between_sides(self) -> None:
        with self.assertRaises(RenderDifferentialError) as caught:
            summarize_render_diff(
                [_row(1, verdict="a", summary="b")],
                [_row(1, verdict="a")])
        self.assertIn("summary", str(caught.exception))
        self.assertIn("--allow-field-drift", str(caught.exception))

    def test_field_set_drift_within_one_side(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            summarize_render_diff(
                [_row(1, verdict="a"), _row(2, verdict="a", summary="b")],
                [_row(1, verdict="a"), _row(2, verdict="a", summary="b")])

    def test_negative_sample_budget(self) -> None:
        with self.assertRaises(ValueError):
            summarize_render_diff([], [], samples_per_field=-1)


class TestAllowedFieldDrift(unittest.TestCase):
    """A PR that adds an output field can still compare the shared ones."""

    def test_added_field_is_named_and_the_rest_compared(self) -> None:
        base = [_row(1, verdict="a"), _row(2, verdict="b")]
        current = [_row(1, verdict="a", new_field="x"),
                   _row(2, verdict="B", new_field="y")]
        report = summarize_render_diff(base, current, allow_field_drift=True)
        self.assertEqual(report.current_only_fields, ["new_field"])
        self.assertEqual(report.base_only_fields, [])
        self.assertEqual(report.changed_by_field, {"verdict": 1})
        self.assertEqual(report.changed_rows, 1)

    def test_removed_field_is_named_too(self) -> None:
        base = [_row(1, verdict="a", gone="x")]
        current = [_row(1, verdict="a")]
        report = summarize_render_diff(base, current, allow_field_drift=True)
        self.assertEqual(report.base_only_fields, ["gone"])
        self.assertEqual(report.changed_by_field, {"verdict": 0})

    def test_unshared_fields_are_printed_loudly(self) -> None:
        text = format_report(DiffReport(
            total_rows=1, changed_rows=0, changed_by_field={"verdict": 0},
            samples=[], base_only_fields=[], current_only_fields=["added"],
        ))
        self.assertIn("NOT COMPARED", text)
        self.assertIn("added", text)


class TestRenderTargetLoading(unittest.TestCase):
    """``--target`` resolves a dotted spec and validates what it returns."""

    def test_default_spec_returns_the_classify_target(self) -> None:
        self.assertIsInstance(load_render_target(None), ClassifyRenderTarget)
        self.assertIsInstance(
            load_render_target(DEFAULT_TARGET_SPEC), ClassifyRenderTarget)

    def test_dotted_spec_resolves_a_plain_function(self) -> None:
        target = load_render_target(
            "tests.test_render_differential:upper_verdict_target")
        target.prepare([])
        rendered = target.render(_download_log_row())
        verdict = _render_one(_download_log_row()).fields["verdict"]
        assert isinstance(verdict, str)
        self.assertEqual(rendered.fields["verdict"], verdict.upper())

    def test_dotted_spec_resolves_a_render_target_class(self) -> None:
        target = load_render_target(
            "scripts.render_differential:ClassifyRenderTarget")
        self.assertIsInstance(target, ClassifyRenderTarget)

    def test_malformed_spec_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("web.classify")

    def test_unimportable_module_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("lib.no_such_module_888:target")

    def test_missing_attribute_fails_closed(self) -> None:
        with self.assertRaises(RenderDifferentialError):
            load_render_target("web.classify:no_such_function")

    def test_target_returning_the_wrong_type_fails_closed(self) -> None:
        target = load_render_target(
            "tests.test_render_differential:not_a_render_target")
        with self.assertRaises(RenderDifferentialError):
            target.render(_download_log_row())


class TestCommandLine(unittest.TestCase):
    """render → render → diff, end to end, on real classify output."""

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)
        self.corpus = self.dir / "corpus.jsonl"
        self.corpus.write_text(
            "\n".join(
                json.dumps(_download_log_row(id=row_id), default=str)
                for row_id in (1, 2, 3)
            ) + "\n",
            encoding="utf-8",
        )

    def _render(self, name: str, target: str | None = None) -> Path:
        out = self.dir / name
        argv = ["render", "--corpus", str(self.corpus), "--out", str(out)]
        if target is not None:
            argv += ["--target", target]
        with _quiet():
            self.assertEqual(main(argv), 0)
        return out

    def _diff(self, *argv: str) -> tuple[int, str]:
        with _quiet() as out:
            code = main(["diff", *argv])
        return code, out.getvalue()

    def test_identical_renders_report_zero_changes(self) -> None:
        base = self._render("base.jsonl")
        current = self._render("current.jsonl")
        report = summarize_render_diff(
            read_rendered(str(base)), read_rendered(str(current)))
        self.assertEqual(report.total_rows, 3)
        self.assertEqual(report.changed_rows, 0)
        self.assertEqual(
            len(report.changed_by_field),
            len(watched_field_names(ClassifiedEntry)))
        self.assertEqual(set(report.changed_by_field.values()), {0})
        code, printed = self._diff("--base", str(base), "--current", str(current))
        self.assertEqual(code, 0)
        self.assertIn("changed rows: 0", printed)

    def test_a_changed_target_is_reported_by_field(self) -> None:
        base = self._render("base.jsonl")
        current = self._render(
            "current.jsonl",
            target="tests.test_render_differential:upper_verdict_target")
        report = summarize_render_diff(
            read_rendered(str(base)), read_rendered(str(current)))
        self.assertEqual(report.changed_rows, 3)
        self.assertEqual(report.changed_by_field["verdict"], 3)
        self.assertEqual(report.changed_by_field["badge"], 0)
        self.assertEqual(report.changed_by_field["summary"], 0)

    def test_plain_function_target_works_through_the_real_script(self) -> None:
        out = self.dir / "subprocess.jsonl"
        proc = subprocess.run(
            [
                sys.executable,
                "scripts/render_differential.py",
                "render",
                "--corpus",
                str(self.corpus),
                "--out",
                str(out),
                "--target",
                "tests.test_render_differential:upper_verdict_target",
            ],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        rendered = read_rendered(str(out))
        self.assertEqual(len(rendered), 3)
        self.assertTrue(all(
            isinstance(row.fields["verdict"], str)
            and row.fields["verdict"] == row.fields["verdict"].upper()
            for row in rendered
        ))

    def test_blank_corpus_lines_are_skipped(self) -> None:
        self.corpus.write_text(
            json.dumps(_download_log_row(id=9), default=str) + "\n\n",
            encoding="utf-8")
        self.assertEqual(len(read_rendered(str(self._render("r.jsonl")))), 1)

    def test_corpus_line_that_is_not_an_object_fails_closed(self) -> None:
        self.corpus.write_text("[1, 2]\n", encoding="utf-8")
        with _quiet():
            self.assertEqual(
                main(["render", "--corpus", str(self.corpus),
                      "--out", str(self.dir / "r.jsonl")]), 1)

    def test_rendered_file_that_is_not_a_rendered_row_fails_closed(
        self,
    ) -> None:
        broken = self.dir / "broken.jsonl"
        broken.write_text('{"id": "one", "fields": {}}\n', encoding="utf-8")
        with self.assertRaises(RenderDifferentialError):
            read_rendered(str(broken))

    def test_diff_over_mismatched_corpora_exits_nonzero(self) -> None:
        base = self._render("base.jsonl")
        short = self.dir / "short.jsonl"
        short.write_text(
            msgspec.json.encode(read_rendered(str(base))[0]).decode() + "\n",
            encoding="utf-8")
        code, printed = self._diff("--base", str(base), "--current", str(short))
        self.assertEqual(code, 1)
        self.assertIn("different rows", printed)

    def test_missing_corpus_file_exits_nonzero(self) -> None:
        with _quiet():
            self.assertEqual(
                main(["render", "--corpus", str(self.dir / "absent.jsonl"),
                      "--out", str(self.dir / "r.jsonl")]), 1)

    def test_negative_sample_budget_is_rejected_by_the_parser(self) -> None:
        base = self._render("base.jsonl")
        with _quiet(), self.assertRaises(SystemExit):
            main(["diff", "--base", str(base), "--current", str(base),
                  "--samples", "-1"])

    def test_allow_field_drift_flag_is_wired(self) -> None:
        base = self._render("base.jsonl")
        widened = self.dir / "widened.jsonl"
        widened.write_text(
            "\n".join(
                msgspec.json.encode(RenderedRow(
                    id=row.id, fields={**row.fields, "brand_new": "x"},
                )).decode()
                for row in read_rendered(str(base))
            ) + "\n",
            encoding="utf-8")
        code, _ = self._diff("--base", str(base), "--current", str(widened))
        self.assertEqual(code, 1)
        code, printed = self._diff(
            "--base", str(base), "--current", str(widened),
            "--allow-field-drift")
        self.assertEqual(code, 0)
        self.assertIn("NOT COMPARED", printed)
        self.assertIn("brand_new", printed)

    def test_json_report_round_trips(self) -> None:
        base = self._render("base.jsonl")
        code, printed = self._diff(
            "--base", str(base), "--current", str(base), "--json")
        self.assertEqual(code, 0)
        decoded = msgspec.json.decode(printed, type=DiffReport)
        self.assertEqual((decoded.total_rows, decoded.changed_rows), (3, 0))


class TestFormatReport(unittest.TestCase):
    """The printed report is the operator-facing evidence."""

    def test_zero_change_report_still_names_every_field(self) -> None:
        text = format_report(DiffReport(
            total_rows=36312,
            changed_rows=0,
            changed_by_field={"badge": 0, "verdict": 0},
            samples=[],
        ))
        self.assertIn("rows: 36312", text)
        self.assertIn("changed rows: 0", text)
        self.assertIn("badge", text)
        self.assertIn("verdict", text)

    def test_samples_show_both_sides(self) -> None:
        text = format_report(DiffReport(
            total_rows=1,
            changed_rows=1,
            changed_by_field={"verdict": 1},
            samples=[FieldChange(
                id=7, field="verdict", base="mbid_not_found",
                current="Requested release ID not among the match candidates")],
        ))
        self.assertIn("[verdict] id=7", text)
        self.assertIn("mbid_not_found", text)
        self.assertIn("Requested release ID not among the match candidates",
                      text)


if __name__ == "__main__":
    unittest.main()
