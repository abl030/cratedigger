"""Live-corpus differential for the QUALITY DECISION itself.

``scripts/render_differential.py`` measures what a copy change does to
operator-facing derived TEXT. This measures what a policy change does to
the DECISION: it re-decides real persisted ``album_quality_evidence``
rows through the real decider and reports changed rows by field.

The two are deliberately the same shape and share one diff engine
(:mod:`scripts.render_differential`'s ``summarize_render_diff``,
``RenderedRow``, ``format_report``), because the question is the same one
— "on the rows that actually exist, what does this change?" — and because
a second copy of that engine would be a parallel code path.

Four modes, with ``decide``/``diff`` sharing Rule D's two-tree runbook:

* ``export`` reads the live source graph into a corpus plus its coverage
  manifest. ``verify`` recomputes that manifest before either tree trusts it.

* ``decide`` reads a corpus JSONL containing complete candidate and current
  ``album_quality_evidence`` rows (with their snapshot files under ``files``).
  A candidate's nullable ``current_evidence_id`` and request ``mb_release_id``
  are the request-owned pairing authority; the referenced current row must be
  present in that same corpus and both evidence rows must match that release.
  It runs ``full_pipeline_decision_from_evidence`` for every candidate and
  writes one decided JSONL row per candidate.
* ``diff`` compares two decided JSONL files field by field.

    git archive <base-ref> | tar -x -C /tmp/dd-base
    cp scripts/decision_differential.py /tmp/dd-base/scripts/   # if newer
    nix-shell --run "python3 /tmp/dd-base/scripts/decision_differential.py \\
      decide --corpus /tmp/evidence.jsonl --coverage /tmp/evidence-coverage.json \\
      --out /tmp/base.jsonl"
    nix-shell --run "python3 scripts/decision_differential.py \\
      decide --corpus /tmp/evidence.jsonl --coverage /tmp/evidence-coverage.json \\
      --out /tmp/current.jsonl"
    nix-shell --run "python3 scripts/decision_differential.py diff \\
      --base /tmp/base.jsonl --current /tmp/current.jsonl"

**Copying this harness into a base tree that predates the change.** The
copy runs against the BASE tree's production code, so anything the base
does not have has to come out of the copy: for a base ref older than the
v3 ultrasonic leg or the v4 AAC-lattice leg that is the leg import, its
``*_leg_for_evidence`` helper, and the ``PROOF_FIELDS`` entries it feeds.
The two sides then emit different
field sets, which the diff engine refuses by default and correctly so —
re-run the ``diff`` with ``--allow-field-drift``, which compares the shared
fields and prints the unshared ones under ``NOT COMPARED``. Say in the PR
body which fields had no base value.

**Two arms, and the second is the one that measures a proof-gate change.**

* ``decide`` alone re-decides each row AS PERSISTED. A row that already
  holds a verified-lossless proof starts from that proof, so the
  PROMOTION the gate governs never runs: this arm answers "does the
  change disturb the library as it stands", and for a change that only
  moves whether a promotion is GRANTED its honest result is zero.

  It is NOT zero for a change that moves what a proof is CALLED.
  ``decide_row`` re-mints the proof through ``mint_verified_lossless_
  proof`` on every row, so a classifier change — a new leg composing into
  the name, say — lands in ``verified_lossless_classifier`` on this arm
  too, on exactly the rows whose legs adjudicate. Read the per-field
  table, not the headline count: "0 decision fields, N classifier rows"
  and "0 rows" are different findings.
* ``decide --counterfactual`` drops each candidate's persisted proof
  columns first, asking the question the gate actually decides — if this
  exact album arrived now, would it be promoted? This is the arm where a
  proof-gate change shows its real blast radius.

**Native current-side pairing.** The corpus carries each candidate's exact
``album_requests.current_evidence_id`` and request ``mb_release_id`` beside
the candidate evidence row, and the complete referenced evidence row elsewhere
in the same corpus. The replay validates the complete JSON wire shape, builds
an ID index after loading it, then resolves that foreign key exactly as
production does. It fails closed on missing/malformed pairing or evidence
columns, duplicate evidence IDs, dangling references, and either evidence row
belonging to a sibling pressing; a null ``current_evidence_id`` means the
candidate has no installed album. This matters because a fresh request cannot
reach branches that compare against a HAVE, including the provisional-lossless
lane's confident rejects.

The current side's OWN proof is never stripped: ``--counterfactual`` removes
only the candidate proof, because installed proof is real evidence and the
acquisition ceiling. Corpus order is irrelevant, but the complete corpus must
be assembled before either tree runs the replay so a current row emitted in a
different export batch remains resolvable.

**Routing changes owe both arms.** A lane-membership, entry-condition, or
bypass change is not measured by a fresh-world run alone: run ``decide`` as
persisted AND ``decide --counterfactual``. The former finds cohorts created
by durable state (historical proofs or incomplete evidence) that fresh-world
tests do not construct; the latter measures a new arrival. A route that
compares a candidate with a HAVE must consume the mandatory native current
side described above, rather than reconstructing a parallel pairing.

**Action-time facts are not on the row.** ``target_format`` falls back to
the candidate's own persisted column, but ``verified_lossless_target`` —
the operator's configured stored format for lossless sources — is
configuration the evidence row never carries. Pass ``--verified-lossless-
target "$(the live value)"``, or every row decides as if nothing were
configured and ``target_final_format`` plus the gate format it feeds are
identical on both trees no matter what changed.

**What this instrument CANNOT see: measurement-time changes.** The corpus
is persisted evidence — grades, buckets, deficits, captures — so a change
to how those values are PRODUCED (``lib/spectral_check.py``'s
``classify_track``, the HF-deficit ladder, the slice windows, the cliff
detector) moves nothing here by construction: both trees re-decide the
same stored numbers. That is a structural blind spot, not a zero. Such a
change owes a live grade-count instead, before and after, run on doc2
(``pipeline-cli query -``, SQL on stdin):

```sql
SELECT spectral_grade, spectral_measurement_version, COUNT(*)
FROM album_quality_evidence
WHERE spectral_grade IS NOT NULL
GROUP BY 1, 2 ORDER BY 3 DESC;
```

The version column separates the measurement era, so a ladder change can
be reported against the cohort it will actually reach. Name the
population that will be re-measured explicitly —
rows are re-graded under the CURRENT ladder whenever they are measured
again (a re-download, a re-preview, a force-import retry), and only rows
that are never re-measured keep their old-era grade.

Three properties keep the measurement honest, and each is the mirror of a
way a differential can lie:

* **The corpus is decoded by PRODUCTION'S decoder.** Rows go through
  ``PipelineDB._album_quality_evidence_from_row``, the same function the
  importer's reads use. A bespoke decoder here would measure a world the
  pipeline never sees — the test-fidelity.md Rule B failure, one layer up.
* **The watched field set is DERIVED from the decision dict, not chosen.**
  Every key the decider emits is compared, plus the two proof facts the
  dict does not carry (``verified_lossless_classifier`` and the
  ultrasonic leg's own outcome/reason), which is where a proof-gate
  change actually lands.
* **A row the decider refuses is recorded, never dropped.** An evidence
  row that is not action-ready raises in production too; silently
  skipping it would shrink the denominator and flatter the result.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import os
import sys
import tempfile
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeGuard

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec
import psycopg2
import psycopg2.extras

from lib.json_narrow import is_object_list, is_str_object_dict
from lib.quality import (
    AacLatticeProofLeg,
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
    AlbumQualityEvidenceFile,
    CodecFamily,
    EvidenceProvenance,
    EvidenceSubject,
    UltrasonicProofLeg,
    aac_lattice_proof_leg,
    full_pipeline_decision_from_evidence,
    is_preserved_source_spectral,
    mint_verified_lossless_proof,
    ultrasonic_proof_leg,
)
from lib.quality.audio_validation import (
    AudioToolDiagnosticCategory,
    AudioValidationOutcome,
)
from lib.quality.pipeline import evidence_spectral_context
from scripts.render_differential import (
    DEFAULT_SAMPLES_PER_FIELD,
    RenderDifferentialError,
    RenderedRow,
    format_report,
    read_rendered,
    summarize_render_diff,
)

if TYPE_CHECKING:
    from lib.pipeline_db.evidence import _EvidenceMixin


def _export_evidence_contract() -> tuple[tuple[str, ...], tuple[str, ...], type[object], type[object]]:
    """Load #999's DB-export-only helpers only for ``export``.

    The two-tree differential copies this script into historical trees.  Those
    trees must still be able to parse ``--help`` and replay ``decide`` using
    their own quality library, even when they predate this export projection.
    """
    from lib.pipeline_db.evidence import (
        EVIDENCE_FILE_PROJECTION_COLUMNS,
        EVIDENCE_PROJECTION_COLUMNS,
        PersistedAlbumQualityEvidenceRow,
        _EvidenceMixin,
    )

    return (
        EVIDENCE_PROJECTION_COLUMNS,
        EVIDENCE_FILE_PROJECTION_COLUMNS,
        PersistedAlbumQualityEvidenceRow,
        _EvidenceMixin,
    )


def _production_evidence_mixin() -> type[_EvidenceMixin]:
    """Load the historical production mapper required by ``decide`` only."""
    from lib.pipeline_db.evidence import _EvidenceMixin

    return _EvidenceMixin

#: Decision-dict keys that are not decisions: the Stage-1 counterfactual
#: is audit-only reporting, and ``comparison_basis`` is a nested display
#: payload whose own differential is ``render_differential``'s job. They
#: are excluded by NAME rather than by type so adding a decision key can
#: never silently fall out of the watched set.
_AUDIT_ONLY_DECISION_KEYS: frozenset[str] = frozenset(
    {
        "stage2_import_if_stage1_deferred",
        "comparison_basis_if_stage1_deferred",
    }
)

#: Facts the decision dict does not carry but a proof-gate change moves.
#: ``verified_lossless_classifier`` is minted beside the decision, and the
#: leg's outcome/reason are the three-state fact the decision reduces to a
#: boolean. Reporting only the boolean would show a proof-gate change as
#: fewer moved rows than it really moved.
PROOF_FIELDS: tuple[str, ...] = (
    "verified_lossless_classifier",
    "ultrasonic_leg_outcome",
    "ultrasonic_leg_reason",
    "aac_lattice_leg_outcome",
    "aac_lattice_leg_reason",
)

#: Recorded in place of a decision when the decider refuses the row.
DECISION_ERROR_FIELD = "decision_error"


def _decision_keys() -> tuple[str, ...]:
    """Every decision key, DERIVED from production's own dict contract.

    Taken from the flat simulator twin on a trivial world rather than
    hand-listed: the twins are contractually one dict shape (the parity
    property enforces it), so a new decision key joins the differential
    automatically. Deriving also keeps a REFUSED row comparable — it has
    no decision of its own, and a row carrying a different field set from
    its neighbours is a row the diff engine cannot compare at all.
    """
    from lib.quality import full_pipeline_decision

    shape = full_pipeline_decision(
        is_flac=False,
        min_bitrate=192,
        is_cbr=True,
    )
    return tuple(
        key
        for key in sorted(shape)
        if key not in _AUDIT_ONLY_DECISION_KEYS and key != "comparison_basis"
    )


DECISION_KEYS: tuple[str, ...] = _decision_keys()


def _evidence_from_corpus_row(
    row: Mapping[str, object],
) -> AlbumQualityEvidence:
    """Decode one corpus row with PRODUCTION's evidence decoder.

    ``files`` carries the snapshot rows the DB read joins in. The decoder
    is a ``@staticmethod`` for exactly this reason — the same shape
    ``render_differential`` uses for
    ``_overlay_evidence_onto_download_log_row``.
    """
    payload = dict(row)
    # Corpus pairing is an outer grammar, not evidence state.  Remove its
    # explicitly allowed keys before the exact evidence wire conversion so a
    # foreign top-level key cannot be silently discarded.
    for pairing_key in (
        "is_candidate",
        "current_evidence_id",
        "request_mb_release_id",
    ):
        payload.pop(pairing_key, None)
    try:
        # ``_album_quality_evidence_from_row`` deliberately accepts values
        # from psycopg2's already-typed result rows.  A JSONL corpus is a
        # separate wire boundary: validate every column it consumes before
        # handing the row to that production mapper, rather than allowing its
        # legacy ``bool()``/``int()`` coercions to decide a different world.
        wire = msgspec.convert(payload, type=DecisionCorpusEvidenceWire)
    except msgspec.ValidationError as exc:
        raise RenderDifferentialError(
            f"corpus row {payload.get('id')!r} has an invalid evidence wire "
            f"shape: {exc}"
        ) from exc
    # Conversion is the boundary, not a validate-and-ignore side quest.  The
    # production mapper receives exactly the typed contract's builtins so a
    # nullable/type/schema drift cannot be normalized away by its legacy
    # ``bool``/``int`` conveniences.
    payload = msgspec.to_builtins(wire)
    raw_files = payload.pop("files")
    # The strict Struct conversion above establishes these assertions; spelling
    # them keeps the raw Mapping passed to production type-safe without
    # introducing a second semantic evidence mapper.
    assert is_object_list(raw_files)
    file_rows: list[dict[str, object]] = []
    for raw_file in raw_files:
        assert is_str_object_dict(raw_file)
        file_rows.append(dict(raw_file))
    measured_at = payload.get("measured_at")
    if isinstance(measured_at, str):
        # PG hands psycopg2 a datetime; a JSONL export hands us its ISO
        # text. Restore the production type rather than letting a string
        # ride into a Struct that declares datetime.
        payload["measured_at"] = _parse_timestamp(measured_at)
    try:
        evidence_mixin = _production_evidence_mixin()
        return evidence_mixin._album_quality_evidence_from_row(
            payload,
            file_rows,
        )
    except (KeyError, TypeError, ValueError, msgspec.ValidationError) as exc:
        raise RenderDifferentialError(
            f"corpus row {payload.get('id')!r} is not an "
            f"album_quality_evidence row: {exc}"
        ) from exc


def _parse_timestamp(raw: str) -> object:
    from datetime import datetime

    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RenderDifferentialError(
            f"measured_at is not an ISO timestamp: {raw!r}"
        ) from exc


def leg_for_evidence(evidence: AlbumQualityEvidence) -> UltrasonicProofLeg:
    """The ultrasonic proof leg the decider will build for this row.

    Built through the SAME context adapter the decider uses
    (``evidence_spectral_context``), so the differential cannot report a
    leg the decision never saw.
    """
    context = evidence_spectral_context(evidence)
    return ultrasonic_proof_leg(
        deficit_db=context.ultrasonic_deficit_db,
        spectral_measurement_version=context.spectral_measurement_version,
        decode_path=context.spectral_decode_path,
        preserved_source_spectral=is_preserved_source_spectral(
            context.spectral_subject,
            context.was_converted_from,
        ),
    )


def lattice_leg_for_evidence(
    evidence: AlbumQualityEvidence,
) -> AacLatticeProofLeg:
    """The AAC frame-lattice proof leg the decider will build for this row.

    The decider reads the persisted capture straight off the evidence row
    with no adapter in between, so this is the same one-liner it runs.
    """
    return aac_lattice_proof_leg(evidence.aac_lattice)


#: The persisted verified-lossless proof columns, dropped by the
#: counterfactual arm. Spelled here rather than derived because they are DB
#: column names on the corpus row, not fields of any type this module holds.
_PERSISTED_PROOF_COLUMNS: tuple[str, ...] = (
    "verified_lossless_provenance",
    "verified_lossless_source",
    "verified_lossless_classifier",
    "verified_lossless_detail",
)


def without_persisted_proof(row: Mapping[str, object]) -> dict[str, object]:
    """The same corpus row as it would have arrived before it was proved.

    Re-deciding a persisted row starts from the proof it already holds, so
    the promotion path a proof gate governs is never exercised. Dropping
    the proof asks the question the gate actually answers: if this exact
    album were acquired now, would it be promoted?

    Only the four proof columns and the boolean move. Nothing measured is
    touched — the counterfactual is about what the row was GRANTED, never
    about what it IS.
    """
    counterfactual = dict(row)
    counterfactual["verified_lossless"] = False
    for column in _PERSISTED_PROOF_COLUMNS:
        counterfactual[column] = None
    return counterfactual


def decide_row(
    row: Mapping[str, object],
    *,
    current: Mapping[str, object] | None = None,
    counterfactual: bool = False,
    verified_lossless_target: str | None = None,
) -> RenderedRow:
    """Re-decide one corpus row through the real decider.

    ``current`` is the installed album's evidence row, paired the way
    production pairs it. Without one the row is decided as a fresh request
    with nothing installed, which no branch that compares against a HAVE
    can reach. ``counterfactual`` drops the candidate's persisted proof
    first (see ``without_persisted_proof``); the current side keeps its
    own, because an installed proof is real evidence.

    ``verified_lossless_target`` is the operator's configured stored
    format for lossless sources (``[Beets] verified_lossless_target``).
    It is an action-time FACT, not an evidence column, so a run that omits
    it decides every row as if nothing were configured — which makes
    ``target_final_format`` and the gate format it feeds trivially
    identical on both trees. Pass the live value when the change under
    measurement can touch them, or read a false zero.
    """
    row_id = row.get("id")
    if not isinstance(row_id, int) or isinstance(row_id, bool):
        raise RenderDifferentialError(f"corpus row has no integer id: {row_id!r}")
    evidence = _evidence_from_corpus_row(
        without_persisted_proof(row) if counterfactual else row,
    )
    current_evidence = (
        _evidence_from_corpus_row(current) if current is not None else None
    )
    fields: dict[str, object] = {}
    facts = AlbumQualityEvidenceDecisionFacts(
        verified_lossless_target=verified_lossless_target,
    )
    try:
        decision = full_pipeline_decision_from_evidence(
            evidence,
            current_evidence,
            facts=facts,
        )
    except ValueError as exc:
        # A row the decider refuses is a real outcome and is compared as
        # one; dropping it would shrink the denominator. It still carries
        # the full key set, with no decision, so the diff engine can put
        # it beside its neighbours.
        fields[DECISION_ERROR_FIELD] = str(exc)
        decision = {}
    else:
        fields[DECISION_ERROR_FIELD] = None
        unknown = (
            set(decision)
            - set(DECISION_KEYS)
            - _AUDIT_ONLY_DECISION_KEYS
            - {"comparison_basis"}
        )
        if unknown:
            raise RenderDifferentialError(
                "the evidence decider emitted keys the flat twin does not: "
                f"{sorted(unknown)} — the dict-shape contract has drifted "
                "and this differential would silently not compare them"
            )
    for key in DECISION_KEYS:
        value = decision.get(key)
        fields[key] = value if _is_json_scalar(value) else repr(value)
    leg = leg_for_evidence(evidence)
    lattice_leg = lattice_leg_for_evidence(evidence)
    proof = mint_verified_lossless_proof(
        bool(decision.get("verified_lossless")),
        was_converted_from=evidence.measurement.was_converted_from,
        detected_source_format=evidence.storage_format,
        spectral_grade=evidence.measurement.spectral_grade,
        ultrasonic_leg=leg,
        aac_lattice_leg=lattice_leg,
    )
    fields.update(
        {
            "verified_lossless_classifier": (
                proof.classifier if proof is not None else None
            ),
            "ultrasonic_leg_outcome": leg.outcome,
            "ultrasonic_leg_reason": leg.reason,
            "aac_lattice_leg_outcome": lattice_leg.outcome,
            "aac_lattice_leg_reason": lattice_leg.reason,
        }
    )
    # The emitted field set is checked against the declared contract on
    # every row rather than trusted: a proof fact added above without
    # joining ``PROOF_FIELDS`` would be compared by the diff engine but
    # invisible to anyone reading what this harness claims to watch, and
    # one dropped would be silently uncompared.
    expected = {DECISION_ERROR_FIELD, *DECISION_KEYS, *PROOF_FIELDS}
    if set(fields) != expected:
        raise RenderDifferentialError(
            "decided row field set does not match the declared contract: "
            f"extra {sorted(set(fields) - expected)}, "
            f"missing {sorted(expected - set(fields))}"
        )
    return RenderedRow(id=row_id, fields=fields)


def _is_json_scalar(value: object) -> bool:
    return value is None or isinstance(value, (str, bool, int, float))


def _corpus_rows(path: str) -> Iterator[dict[str, object]]:
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                yield msgspec.json.decode(line, type=dict[str, object])
            except (msgspec.DecodeError, msgspec.ValidationError) as exc:
                raise RenderDifferentialError(
                    f"{path}:{line_number}: corpus line is not a JSON object: {exc}"
                ) from exc


CORPUS_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "id",
        "is_candidate",
        "current_evidence_id",
        "request_mb_release_id",
        "files",
    }
)


class DecisionCorpusEvidenceFileWire(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """Exact JSON shape of one exported evidence-file row."""

    relative_path: str
    size_bytes: int
    mtime_ns: int
    extension: str
    container: str
    codec: str | None
    decode_ok: bool


class DecisionCorpusAudioDiagnosticWire(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """Exact JSON shape of a persisted audio-validation diagnostic."""

    relative_path: str
    category: AudioToolDiagnosticCategory
    return_code: int | None
    stderr_excerpt: str
    stderr_bytes: int
    stderr_sha256: str
    stderr_truncated: bool


class DecisionCorpusAudioValidationWire(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """Exact JSON shape consumed by production's audio-report decoder."""

    policy_id: str
    tool: str
    tool_version: str
    outcome: AudioValidationOutcome
    files_checked: int
    files_failed: int
    diagnostics: list[DecisionCorpusAudioDiagnosticWire]
    omitted_diagnostics: int


class DecisionCorpusAacLatticeTrackWire(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """Exact JSON shape of one persisted AAC-lattice track capture."""

    filename: str
    offset: int | None
    z: float | None
    proba: float | None
    error: str | None


class DecisionCorpusEvidenceWire(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    """Exact JSON shape consumed by production's evidence row decoder.

    This is intentionally a wire schema only.  It validates the complete
    export before the sole semantic mapper,
    ``PipelineDB._album_quality_evidence_from_row``, reconstructs the typed
    evidence object production itself uses.
    """

    id: int
    mb_release_id: str
    snapshot_fingerprint: str
    source_path: str
    measured_at: str
    min_bitrate_kbps: int | None
    avg_bitrate_kbps: int | None
    median_bitrate_kbps: int | None
    format: str | None
    is_cbr: bool
    spectral_grade: str | None
    spectral_bitrate_kbps: int | None
    spectral_subject: EvidenceSubject | None
    spectral_provenance: EvidenceProvenance | None
    was_converted_from: str | None
    cliff_hz: int | None
    codec_family: CodecFamily | None
    ultrasonic_deficit_db: float | None
    spectral_measurement_version: int | None
    codec: str | None
    container: str | None
    storage_format: str | None
    target_format: str | None
    target_is_cbr: bool | None
    lineage_version: int
    v0_min_bitrate_kbps: int | None
    v0_avg_bitrate_kbps: int | None
    v0_median_bitrate_kbps: int | None
    v0_subject: EvidenceSubject | None
    v0_provenance: EvidenceProvenance | None
    on_disk_v0_research_attempted: bool
    current_enrichment_required: bool
    verified_lossless: bool
    verified_lossless_provenance: EvidenceProvenance | None
    verified_lossless_source: str | None
    verified_lossless_classifier: str | None
    verified_lossless_detail: str | None
    audio_validation: DecisionCorpusAudioValidationWire
    audio_corrupt: bool
    audio_error: str | None
    folder_layout: str
    audio_file_count: int
    filetype_band: str
    matched_bad_audio_hash_id: int | None
    matched_bad_audio_hash_path: str | None
    aac_lattice_tracks: list[DecisionCorpusAacLatticeTrackWire] | None
    aac_lattice_modal_offset: int | None
    aac_lattice_modal_count: int | None
    aac_lattice_scored_tracks: int | None
    aac_lattice_max_z: float | None
    files: list[DecisionCorpusEvidenceFileWire]


class DecisionCorpusEvidence(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """One complete evidence row plus its corpus-level pairing metadata."""

    evidence_id: int
    is_candidate: bool
    current_evidence_id: int | None
    request_mb_release_id: str | None
    row: dict[str, object]


def _is_exact_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)


def _corpus_evidence(row: Mapping[str, object]) -> DecisionCorpusEvidence:
    """Validate the native pairing columns that the SQL export must emit."""
    missing = CORPUS_REQUIRED_COLUMNS - set(row)
    if missing:
        raise RenderDifferentialError(
            f"corpus row is missing required columns: {sorted(missing)}"
        )
    evidence_id = row["id"]
    if not _is_exact_int(evidence_id):
        raise RenderDifferentialError(f"corpus row has no integer id: {evidence_id!r}")
    is_candidate = row["is_candidate"]
    if not isinstance(is_candidate, bool):
        raise RenderDifferentialError("corpus row 'is_candidate' must be a boolean")
    current_evidence_id = row["current_evidence_id"]
    if current_evidence_id is not None and not _is_exact_int(current_evidence_id):
        raise RenderDifferentialError(
            "corpus row 'current_evidence_id' must be an integer or null"
        )
    request_mb_release_id = row["request_mb_release_id"]
    if request_mb_release_id is not None and not isinstance(
        request_mb_release_id,
        str,
    ):
        raise RenderDifferentialError(
            "corpus row 'request_mb_release_id' must be a string or null"
        )
    if not is_candidate and current_evidence_id is not None:
        raise RenderDifferentialError(
            "current-only corpus row has a current_evidence_id"
        )
    if is_candidate and request_mb_release_id is None:
        raise RenderDifferentialError(
            "candidate corpus row has no request_mb_release_id"
        )
    if not is_candidate and request_mb_release_id is not None:
        raise RenderDifferentialError(
            "current-only corpus row has a request_mb_release_id"
        )
    return DecisionCorpusEvidence(
        evidence_id=evidence_id,
        is_candidate=is_candidate,
        current_evidence_id=current_evidence_id,
        request_mb_release_id=request_mb_release_id,
        row=dict(row),
    )


def _entry_release_id(entry: DecisionCorpusEvidence) -> str:
    release_id = entry.row.get("mb_release_id")
    if not isinstance(release_id, str):
        raise RenderDifferentialError(
            f"corpus evidence {entry.evidence_id} has no string mb_release_id"
        )
    return release_id


def resolve_native_current_pairs(
    entries: Sequence[DecisionCorpusEvidence],
) -> list[tuple[DecisionCorpusEvidence, DecisionCorpusEvidence | None]]:
    """Resolve candidate FKs after the complete corpus is available.

    This is deliberately an ID lookup rather than a parallel sidecar mapping:
    ``album_requests.current_evidence_id`` is production's one pairing
    authority. The resolver's output follows the candidate records' input
    order, but each current reference is order-independent.
    """
    by_id: dict[int, DecisionCorpusEvidence] = {}
    for entry in entries:
        if entry.evidence_id in by_id:
            raise RenderDifferentialError(
                f"corpus has duplicate evidence id {entry.evidence_id}"
            )
        by_id[entry.evidence_id] = entry

    pairs: list[tuple[DecisionCorpusEvidence, DecisionCorpusEvidence | None]] = []
    for entry in entries:
        if not entry.is_candidate:
            continue
        request_release_id = entry.request_mb_release_id
        if request_release_id is None:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} has no request_mb_release_id"
            )
        candidate_release_id = _entry_release_id(entry)
        if candidate_release_id != request_release_id:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} release "
                f"{candidate_release_id!r} does not match request "
                f"release {request_release_id!r}"
            )
        current_id = entry.current_evidence_id
        if current_id is None:
            pairs.append((entry, None))
            continue
        current = by_id.get(current_id)
        if current is None:
            raise RenderDifferentialError(
                "candidate evidence "
                f"{entry.evidence_id} has dangling current_evidence_id "
                f"{current_id}"
            )
        current_release_id = _entry_release_id(current)
        if current_release_id != request_release_id:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} current evidence "
                f"{current.evidence_id} release {current_release_id!r} "
                f"does not match request release {request_release_id!r}"
            )
        pairs.append((entry, current))
    return pairs


def read_decision_corpus(path: str) -> list[DecisionCorpusEvidence]:
    """Load and validate the complete native-pairing corpus.

    Both candidate and current rows pass through production's evidence decoder
    before any decision is emitted. That rejects a malformed current row even
    if it appears before its candidate in one batch or is not reached until a
    later one.
    """
    entries = [_corpus_evidence(row) for row in _corpus_rows(path)]
    for entry in entries:
        _evidence_from_corpus_row(entry.row)
    resolve_native_current_pairs(entries)
    return entries


def decide_corpus(
    corpus_path: str,
    out_path: str | None,
    *,
    counterfactual: bool = False,
    verified_lossless_target: str | None = None,
) -> int:
    """Decide every candidate after resolving the complete native corpus."""
    count = 0
    pairs = resolve_native_current_pairs(read_decision_corpus(corpus_path))
    output = (
        contextlib.nullcontext(sys.stdout)
        if out_path is None
        else open(out_path, "w", encoding="utf-8")  # noqa: SIM115 - managed below
    )
    with output as handle:
        for candidate, current in pairs:
            decided = decide_row(
                candidate.row,
                current=current.row if current is not None else None,
                counterfactual=counterfactual,
                verified_lossless_target=verified_lossless_target,
            )
            handle.write(msgspec.json.encode(decided).decode())
            handle.write("\n")
            count += 1
    return count


@dataclass(frozen=True)
class DecisionCorpusExportResult:
    """Outcome of one read-only decision-corpus export."""

    green: bool
    debt_count: int


@dataclass(frozen=True)
class _DecisionCorpusSnapshot:
    """Qualified PG transaction facts exposed only to the concurrency pin."""

    isolation: str
    read_only: str
    snapshot: str


class _DecisionCorpusSnapshotRow(msgspec.Struct, frozen=True):
    isolation: str
    read_only: str
    snapshot: str


DecisionCorpusSource = Literal["download_log", "import_jobs"]


class _DecisionCorpusSourceLink(msgspec.Struct, frozen=True):
    """One exact candidate-evidence foreign-key reference from PostgreSQL."""

    source: DecisionCorpusSource
    source_id: int
    evidence_id: int
    request_id: int | None
    request_exists: bool
    request_mb_release_id: str | None
    current_evidence_id: int | None


class _CoverageSourceLink(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """The complete, canonical source-arm ledger committed to coverage."""

    source: DecisionCorpusSource
    source_id: int
    evidence_id: int
    request_id: int | None
    request_exists: bool
    request_mb_release_id: str | None
    current_evidence_id: int | None
    authority_reason: str | None


class _CoverageObservedEvidence(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Compact evidence facts sufficient to audit every required ID."""

    evidence_id: int
    mb_release_id: str
    stored_snapshot_fingerprint: str
    files_snapshot_fingerprint: str
    files_sha256: str


class _CoverageAssociation(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    candidate_evidence_id: int
    current_evidence_id: int | None
    request_mb_release_id: str


class _CoverageAuthorityless(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    source: str
    source_id: int
    evidence_id: int
    request_id: int | None
    reason: str


class _CoverageConflict(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    evidence_id: int
    associations: list[_CoverageAssociation]


class _CoverageCandidateMismatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    evidence_id: int
    evidence_mb_release_id: str
    request_mb_release_id: str


class _CoverageCurrentMismatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    candidate_evidence_id: int
    current_evidence_id: int
    current_mb_release_id: str
    request_mb_release_id: str


class _CoverageContentMismatch(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    evidence_id: int
    stored_snapshot_fingerprint: str
    files_snapshot_fingerprint: str


class _CoverageFileContentConflict(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    mb_release_id: str
    snapshot_fingerprint: str
    evidence_ids: list[int]


class _CoverageAddress(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    id: int
    mb_release_id: str
    snapshot_fingerprint: str
    files_sha256: str


class _CoverageWrittenAddressConflict(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    mb_release_id: str
    snapshot_fingerprint: str


class _CoverageCounts(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    download_log: int
    import_jobs: int


class _CoverageValidCandidates(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    paired: int
    unpaired: int


class _CoverageCorpusOutput(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    expected_candidate_ids: list[int]
    written_candidate_ids: list[int]
    expected_associations: list[_CoverageAssociation]
    written_associations: list[_CoverageAssociation]
    expected_referenced_current_ids: list[int]
    written_referenced_current_ids: list[int]
    expected_current_only_ids: list[int]
    written_current_only_ids: list[int]
    dual_role_ids: list[int]
    expected_evidence_ids: list[int]
    written_evidence_ids: list[int]
    exact_match: bool
    content_addresses: list[_CoverageAddress]
    sha256: str


class _CoverageOutputs(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    corpus: _CoverageCorpusOutput


class DecisionCorpusCoverage(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    """Strict, self-recomputing coverage artifact schema (v2)."""

    schema_version: int
    source_links: list[_CoverageSourceLink]
    observed_evidence: list[_CoverageObservedEvidence]
    total_source_links: int
    source_link_counts: _CoverageCounts
    source_distinct_candidate_id_counts: _CoverageCounts
    total_distinct_candidate_ids: int
    identical_associations_collapsed: int
    authorityless_source_links: list[_CoverageAuthorityless]
    authorityless_candidate_ids: list[int]
    association_conflicts: list[_CoverageConflict]
    missing_candidate_evidence_ids: list[int]
    candidate_release_mismatches: list[_CoverageCandidateMismatch]
    missing_current_evidence_ids: list[int]
    current_release_mismatches: list[_CoverageCurrentMismatch]
    referenced_current_ids: list[int]
    valid_candidates: _CoverageValidCandidates
    content_address_mismatches: list[_CoverageContentMismatch]
    file_content_conflicts: list[_CoverageFileContentConflict]
    written_content_address_conflicts: list[_CoverageWrittenAddressConflict]
    outputs: _CoverageOutputs
    green: bool
    debt_count: int


# Do not replace this projection with ``e.*``.  The JSONL wire and the real
# evidence mapper consume this exact surface; an add/drop/rename/type/nullability
# change must break the export until its projection and Struct move together.
def _decision_corpus_evidence_columns() -> tuple[str, ...]:
    """Shared production decoder projection; corpus adds JSON-aggregated files."""
    columns, _file_columns, _persisted_row, _evidence_mixin = _export_evidence_contract()
    return columns


def _decision_corpus_evidence_file_columns() -> tuple[str, ...]:
    """The file projection used by the export's joined JSON aggregation."""
    _columns, file_columns, _persisted_row, _evidence_mixin = _export_evidence_contract()
    return file_columns


def _db_source_links(cursor: object) -> list[_DecisionCorpusSourceLink]:
    """Read both durable source arms under the export's one PG snapshot."""
    assert isinstance(cursor, psycopg2.extensions.cursor)
    cursor.execute("""
        WITH links AS (
            SELECT 'import_jobs'::text AS source, job.id AS source_id,
                   job.candidate_evidence_id AS evidence_id, job.request_id
            FROM import_jobs AS job
            WHERE job.candidate_evidence_id IS NOT NULL
            UNION ALL
            SELECT 'download_log'::text AS source, log.id AS source_id,
                   log.candidate_evidence_id AS evidence_id, log.request_id
            FROM download_log AS log
            WHERE log.candidate_evidence_id IS NOT NULL
        )
        SELECT links.source, links.source_id, links.evidence_id,
               links.request_id, request.id IS NOT NULL AS request_exists,
               request.mb_release_id AS request_mb_release_id,
               request.current_evidence_id
        FROM links
        LEFT JOIN album_requests AS request ON request.id = links.request_id
        ORDER BY links.source, links.source_id
    """)
    links: list[_DecisionCorpusSourceLink] = []
    for raw in cursor.fetchall():
        try:
            links.append(
                msgspec.convert(
                    dict(raw),
                    type=_DecisionCorpusSourceLink,
                )
            )
        except msgspec.ValidationError as exc:
            raise RenderDifferentialError(
                f"decision-corpus source-link projection drift: {exc}"
            ) from exc
    return links


def _qualify_read_snapshot(cursor: object) -> _DecisionCorpusSnapshot:
    """Prove the collector's transaction settings before any source read."""
    assert isinstance(cursor, psycopg2.extensions.cursor)
    cursor.execute(
        "SELECT current_setting('transaction_isolation') AS isolation, "
        "current_setting('transaction_read_only') AS read_only, "
        "txid_current_snapshot() AS snapshot"
    )
    raw = cursor.fetchone()
    if not isinstance(raw, Mapping):
        raise RenderDifferentialError("decision-corpus snapshot qualification returned no row")
    row = msgspec.convert(dict(raw), type=_DecisionCorpusSnapshotRow)
    if row.isolation != "repeatable read" or row.read_only != "on":
        raise RenderDifferentialError(
            "decision-corpus export requires a repeatable-read, read-only snapshot"
        )
    return _DecisionCorpusSnapshot(
        isolation=row.isolation, read_only=row.read_only, snapshot=row.snapshot
    )


_EVIDENCE_SCHEMA_TYPES: dict[str, tuple[str, bool]] = {
    "id": ("bigint", False),
    "mb_release_id": ("text", False),
    "snapshot_fingerprint": ("text", False),
    "source_path": ("text", False),
    "measured_at": ("timestamp with time zone", False),
    "is_cbr": ("boolean", False),
    "lineage_version": ("smallint", False),
    "on_disk_v0_research_attempted": ("boolean", False),
    "current_enrichment_required": ("boolean", False),
    "verified_lossless": ("boolean", False),
    "audio_validation": ("jsonb", False),
    "audio_corrupt": ("boolean", False),
    "folder_layout": ("text", False),
    "audio_file_count": ("integer", False),
    "filetype_band": ("text", False),
    "codec": ("text", True),
    "container": ("text", True),
    "storage_format": ("text", True),
    "target_format": ("text", True),
    "min_bitrate_kbps": ("integer", True),
    "avg_bitrate_kbps": ("integer", True),
    "median_bitrate_kbps": ("integer", True),
    "format": ("text", True),
    "spectral_grade": ("text", True),
    "spectral_bitrate_kbps": ("integer", True),
    "was_converted_from": ("text", True),
    "v0_min_bitrate_kbps": ("integer", True),
    "v0_avg_bitrate_kbps": ("integer", True),
    "v0_median_bitrate_kbps": ("integer", True),
    "v0_subject": ("text", True),
    "v0_provenance": ("text", True),
    "verified_lossless_provenance": ("text", True),
    "verified_lossless_source": ("text", True),
    "verified_lossless_classifier": ("text", True),
    "verified_lossless_detail": ("text", True),
    "matched_bad_audio_hash_id": ("bigint", True),
    "matched_bad_audio_hash_path": ("text", True),
    "target_is_cbr": ("boolean", True),
    "spectral_subject": ("text", True),
    "spectral_provenance": ("text", True),
    "audio_error": ("text", True),
    "cliff_hz": ("integer", True),
    "codec_family": ("text", True),
    "ultrasonic_deficit_db": ("double precision", True),
    "spectral_measurement_version": ("smallint", True),
    "aac_lattice_tracks": ("jsonb", True),
    "aac_lattice_modal_offset": ("integer", True),
    "aac_lattice_modal_count": ("integer", True),
    "aac_lattice_scored_tracks": ("integer", True),
    "aac_lattice_max_z": ("double precision", True),
}
_FILE_SCHEMA_TYPES: dict[str, tuple[str, bool]] = {
    # Join/filter/order dependencies are part of the authoritative export
    # contract just as much as decoded file payload fields are.
    "evidence_id": ("bigint", False),
    "ordinal": ("integer", False),
    "relative_path": ("text", False),
    "size_bytes": ("bigint", False),
    "mtime_ns": ("bigint", False),
    "extension": ("text", False),
    "container": ("text", False),
    "codec": ("text", True),
    "decode_ok": ("boolean", False),
}

_SOURCE_SCHEMA_TYPES: dict[str, dict[str, tuple[str, bool]]] = {
    "import_jobs": {
        "id": ("integer", False),
        "candidate_evidence_id": ("bigint", True),
        "request_id": ("integer", True),
    },
    "download_log": {
        "id": ("integer", False),
        "candidate_evidence_id": ("bigint", True),
        "request_id": ("integer", False),
    },
    "album_requests": {
        "id": ("integer", False),
        "mb_release_id": ("text", True),
        "current_evidence_id": ("bigint", True),
    },
}


def assert_decision_corpus_schema(
    descriptions: Mapping[str, Mapping[str, tuple[str, bool]]],
) -> None:
    """Fail closed when a consumed PG column's name/type/nullability drifts."""
    for table, columns, overrides in (
        (
            "album_quality_evidence",
            _decision_corpus_evidence_columns(),
            _EVIDENCE_SCHEMA_TYPES,
        ),
        (
            "album_quality_evidence_files",
            (*_decision_corpus_evidence_file_columns(), "evidence_id", "ordinal"),
            _FILE_SCHEMA_TYPES,
        ),
        *(
            (table, tuple(contract), contract)
            for table, contract in _SOURCE_SCHEMA_TYPES.items()
        ),
    ):
        if set(columns) != set(overrides):
            raise RenderDifferentialError(
                f"{table} schema contract keys differ from consumed projection"
            )
        observed = descriptions.get(table, {})
        for column in columns:
            actual = observed.get(column)
            if actual is None:
                raise RenderDifferentialError(
                    f"{table}.{column} is missing from schema"
                )
            expected = overrides.get(column)
            if expected is not None and actual != expected:
                raise RenderDifferentialError(
                    f"{table}.{column} schema drift: expected {expected}, got {actual}"
                )


def _assert_live_decision_corpus_schema(cursor: object) -> None:
    assert isinstance(cursor, psycopg2.extensions.cursor)
    cursor.execute("""
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN (
              'album_quality_evidence', 'album_quality_evidence_files',
              'import_jobs', 'download_log', 'album_requests'
          )
    """)
    descriptions: dict[str, dict[str, tuple[str, bool]]] = {}
    assert cursor.description is not None
    names = [description.name for description in cursor.description]
    for fetched in cursor.fetchall():
        raw: object = fetched
        if is_str_object_dict(raw):
            row = raw
        else:
            try:
                values = msgspec.convert(raw, type=tuple[object, ...])
            except msgspec.ValidationError as exc:
                raise RenderDifferentialError(
                    "decision-corpus schema qualification returned an invalid row"
                ) from exc
            row = dict(zip(names, values, strict=True))
        table, column = row["table_name"], row["column_name"]
        data_type, nullable = row["data_type"], row["is_nullable"]
        assert isinstance(table, str) and isinstance(column, str)
        assert isinstance(data_type, str) and isinstance(nullable, str)
        descriptions.setdefault(table, {})[column] = (data_type, nullable == "YES")
    assert_decision_corpus_schema(descriptions)


def _db_evidence_rows(
    cursor: object,
    evidence_ids: Sequence[int],
    batch_size: int,
) -> dict[int, dict[str, object]]:
    """Read explicit evidence/file projections and validate the export wire."""
    assert isinstance(cursor, psycopg2.extensions.cursor)
    rows: dict[int, dict[str, object]] = {}
    column_sql = ", ".join(
        f"e.{column}" for column in _decision_corpus_evidence_columns()
    )
    for start in range(0, len(evidence_ids), batch_size):
        batch = list(evidence_ids[start : start + batch_size])
        if not batch:
            continue
        cursor.execute(
            f"""
            SELECT {column_sql},
                   COALESCE(
                       jsonb_agg(jsonb_build_object(
                           'relative_path', file.relative_path,
                           'size_bytes', file.size_bytes,
                           'mtime_ns', file.mtime_ns,
                           'extension', file.extension,
                           'container', file.container,
                           'codec', file.codec,
                           'decode_ok', file.decode_ok
                       ) ORDER BY file.ordinal)
                       FILTER (WHERE file.evidence_id IS NOT NULL),
                       '[]'::jsonb
                   ) AS files
            FROM album_quality_evidence AS e
            LEFT JOIN album_quality_evidence_files AS file ON file.evidence_id = e.id
            WHERE e.id = ANY(%s)
            GROUP BY {column_sql}
            ORDER BY e.id
        """,
            (batch,),
        )
        for raw in cursor.fetchall():
            payload = dict(raw)
            measured_at = payload.get("measured_at")
            if not isinstance(measured_at, datetime):
                raise RenderDifferentialError(
                    "decision-corpus evidence projection has non-timestamp "
                    f"measured_at for evidence {payload.get('id')!r}"
                )
            payload["measured_at"] = measured_at.isoformat()
            try:
                # The exporter first validates the exact production PG row
                # contract; JSONL conversion below only serializes that same
                # contract for replay.
                files = payload.pop("files")
                _columns, _file_columns, persisted_row_type, _evidence_mixin = (
                    _export_evidence_contract()
                )
                pg_row = msgspec.convert(
                    payload,
                    type=persisted_row_type,
                )
                payload = msgspec.to_builtins(pg_row)
                payload["measured_at"] = measured_at.isoformat()
                payload["files"] = files
                wire = msgspec.convert(payload, type=DecisionCorpusEvidenceWire)
            except msgspec.ValidationError as exc:
                raise RenderDifferentialError(
                    "decision-corpus evidence projection/wire drift for "
                    f"evidence {payload.get('id')!r}: {exc}"
                ) from exc
            normalized: dict[str, object] = msgspec.to_builtins(wire)
            evidence_id = normalized["id"]
            assert _is_exact_int(evidence_id)
            rows[evidence_id] = normalized
    return rows


def _atomic_write(path: Path, content: bytes) -> None:
    """Publish one complete deterministic artifact, never a partial file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as handle:
        tmp_path = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.replace(tmp_path, path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise


def _address(row: Mapping[str, object]) -> dict[str, object]:
    """The evidence content address included in coverage's exactness proof."""
    return {
        "id": row["id"],
        "mb_release_id": row["mb_release_id"],
        "snapshot_fingerprint": row["snapshot_fingerprint"],
        "files_sha256": hashlib.sha256(
            msgspec.json.encode(row["files"]),
        ).hexdigest(),
    }


def _authority_reason(link: _DecisionCorpusSourceLink) -> str | None:
    if link.request_mb_release_id is not None:
        return None
    if link.request_id is None:
        return "null_request_id"
    if not link.request_exists:
        return "missing_request"
    return "request_missing_release_authority"


def _coverage_source_links(
    links: Sequence[_DecisionCorpusSourceLink],
) -> list[_CoverageSourceLink]:
    ledger = [
        _CoverageSourceLink(
            source=link.source,
            source_id=link.source_id,
            evidence_id=link.evidence_id,
            request_id=link.request_id,
            request_exists=link.request_exists,
            request_mb_release_id=link.request_mb_release_id,
            current_evidence_id=link.current_evidence_id,
            authority_reason=_authority_reason(link),
        )
        for link in links
    ]
    if ledger != sorted(ledger, key=lambda item: (item.source, item.source_id)):
        raise RenderDifferentialError("source-links ledger is not canonically sorted")
    return ledger


def _observed_evidence(row: Mapping[str, object]) -> _CoverageObservedEvidence:
    """Record the reproducible content address for every fetched evidence ID."""
    evidence_id = row.get("id")
    release_id = row.get("mb_release_id")
    stored = row.get("snapshot_fingerprint")
    files = row.get("files")
    if (
        not _is_exact_int(evidence_id)
        or not isinstance(release_id, str)
        or not isinstance(stored, str)
        or not isinstance(files, list)
    ):
        raise RenderDifferentialError("observed evidence ledger has invalid source row")
    try:
        file_rows = msgspec.convert(files, type=list[AlbumQualityEvidenceFile])
    except msgspec.ValidationError as exc:
        raise RenderDifferentialError(
            f"decision-corpus file projection/wire drift for evidence {evidence_id}: {exc}"
        ) from exc
    from lib.quality_evidence import snapshot_fingerprint

    return _CoverageObservedEvidence(
        evidence_id=evidence_id,
        mb_release_id=release_id,
        stored_snapshot_fingerprint=stored,
        files_snapshot_fingerprint=snapshot_fingerprint(file_rows),
        files_sha256=hashlib.sha256(msgspec.json.encode(files)).hexdigest(),
    )


def _export_debt_count(coverage: DecisionCorpusCoverage) -> int:
    """Count every non-empty debt list without silently reclassifying it."""
    debt_fields = (
        "authorityless_source_links",
        "authorityless_candidate_ids",
        "association_conflicts",
        "missing_candidate_evidence_ids",
        "candidate_release_mismatches",
        "missing_current_evidence_ids",
        "current_release_mismatches",
        "content_address_mismatches",
        "file_content_conflicts",
        "written_content_address_conflicts",
    )
    total = 0
    for field in debt_fields:
        value = getattr(coverage, field)
        total += len(value)
    return total


def duplicate_content_addresses(
    addresses: Sequence[_CoverageAddress],
) -> list[tuple[str, str]]:
    """Return duplicate content-address keys in one linear pass."""
    counts = Counter(
        (item.mb_release_id, item.snapshot_fingerprint) for item in addresses
    )
    return sorted(key for key, count in counts.items() if count > 1)


def _association_order_key(
    association: tuple[int | None, str],
) -> tuple[bool, int, str]:
    """Order nullable-current associations independently of hash seed."""
    current_id, release_id = association
    return (current_id is not None, current_id or -1, release_id)


def recompute_decision_corpus_coverage(
    source_links: Sequence[_CoverageSourceLink],
    observed_evidence: Sequence[_CoverageObservedEvidence],
    corpus_rows: Sequence[Mapping[str, object]],
    corpus_bytes: bytes,
) -> DecisionCorpusCoverage:
    """Derive *all* coverage values from the two ledgers and corpus.

    Export and ``verify`` both call this one checker.  The artifact's human
    readable counts and debt lists are therefore assertions, never authority:
    changing any copied field makes this recomputation disagree.
    """
    links = list(source_links)
    if links != sorted(links, key=lambda item: (item.source, item.source_id)):
        raise RenderDifferentialError("coverage source_links are not canonically sorted")
    evidence_rows = {item.evidence_id: item for item in observed_evidence}
    if len(evidence_rows) != len(observed_evidence):
        raise RenderDifferentialError("coverage observed_evidence duplicates an ID")
    if list(observed_evidence) != sorted(observed_evidence, key=lambda item: item.evidence_id):
        raise RenderDifferentialError("coverage observed_evidence is not canonically sorted")

    source_link_counts = _CoverageCounts(
        download_log=sum(link.source == "download_log" for link in links),
        import_jobs=sum(link.source == "import_jobs" for link in links),
    )
    source_distinct_candidate_id_counts = _CoverageCounts(
        download_log=len({link.evidence_id for link in links if link.source == "download_log"}),
        import_jobs=len({link.evidence_id for link in links if link.source == "import_jobs"}),
    )
    authorityless = [
        _CoverageAuthorityless(
            source=link.source,
            source_id=link.source_id,
            evidence_id=link.evidence_id,
            request_id=link.request_id,
            reason=link.authority_reason,
        )
        for link in links
        if link.authority_reason is not None
    ]
    authoritative = [link for link in links if link.authority_reason is None]
    associations: dict[int, set[tuple[int | None, str]]] = {}
    for link in authoritative:
        assert link.request_mb_release_id is not None
        associations.setdefault(link.evidence_id, set()).add(
            (link.current_evidence_id, link.request_mb_release_id)
        )
    conflicts = [
        _CoverageConflict(
            evidence_id=evidence_id,
            associations=[
                _CoverageAssociation(
                    candidate_evidence_id=evidence_id,
                    current_evidence_id=current_id,
                    request_mb_release_id=release_id,
                )
                for current_id, release_id in sorted(values, key=_association_order_key)
            ],
        )
        for evidence_id, values in sorted(associations.items())
        if len(values) > 1
    ]
    conflict_ids = {item.evidence_id for item in conflicts}
    candidate_associations = {
        evidence_id: next(iter(sorted(values, key=_association_order_key)))
        for evidence_id, values in associations.items()
        if evidence_id not in conflict_ids
    }
    # Every non-null source-link current ID is observed and audited, even when
    # its candidate has no request authority and is excluded from replay.
    referenced_current_ids = sorted(
        {link.current_evidence_id for link in links if link.current_evidence_id is not None}
    )
    all_candidate_ids = {link.evidence_id for link in links}
    missing_candidates = sorted(all_candidate_ids - set(evidence_rows))
    missing_currents = sorted(set(referenced_current_ids) - set(evidence_rows))
    mismatches = [
        _CoverageContentMismatch(
            evidence_id=item.evidence_id,
            stored_snapshot_fingerprint=item.stored_snapshot_fingerprint,
            files_snapshot_fingerprint=item.files_snapshot_fingerprint,
        )
        for item in observed_evidence
        if item.stored_snapshot_fingerprint != item.files_snapshot_fingerprint
    ]
    content_mismatch_ids = {item.evidence_id for item in mismatches}
    content_groups: dict[tuple[str, str], list[int]] = {}
    for item in observed_evidence:
        content_groups.setdefault(
            (item.mb_release_id, item.files_snapshot_fingerprint), []
        ).append(item.evidence_id)
    file_content_conflicts = [
        _CoverageFileContentConflict(
            mb_release_id=release_id,
            snapshot_fingerprint=fingerprint,
            evidence_ids=evidence_ids,
        )
        for (release_id, fingerprint), evidence_ids in sorted(content_groups.items())
        if len(evidence_ids) > 1
    ]
    candidate_mismatches: list[_CoverageCandidateMismatch] = []
    current_mismatches: list[_CoverageCurrentMismatch] = []
    invalid: set[tuple[int, int | None, str]] = set()
    for evidence_id, values in sorted(associations.items()):
        candidate = evidence_rows.get(evidence_id)
        for current_id, release_id in sorted(values, key=_association_order_key):
            association = (evidence_id, current_id, release_id)
            if candidate is None or evidence_id in content_mismatch_ids:
                invalid.add(association)
                continue
            if candidate.mb_release_id != release_id:
                candidate_mismatches.append(
                    _CoverageCandidateMismatch(
                        evidence_id=evidence_id,
                        evidence_mb_release_id=candidate.mb_release_id,
                        request_mb_release_id=release_id,
                    )
                )
                invalid.add(association)
            if current_id is not None:
                current = evidence_rows.get(current_id)
                if current is None or current_id in content_mismatch_ids:
                    invalid.add(association)
                    continue
                if current.mb_release_id != release_id:
                    current_mismatches.append(
                        _CoverageCurrentMismatch(
                            candidate_evidence_id=evidence_id,
                            current_evidence_id=current_id,
                            current_mb_release_id=current.mb_release_id,
                            request_mb_release_id=release_id,
                        )
                    )
                    invalid.add(association)
    valid_candidates = {
        evidence_id: association
        for evidence_id, association in candidate_associations.items()
        if (evidence_id, association[0], association[1]) not in invalid
    }
    valid_current_ids = {
        current_id for current_id, _release in valid_candidates.values()
        if current_id is not None
    }
    expected_associations = [
        _CoverageAssociation(
            candidate_evidence_id=evidence_id,
            current_evidence_id=current_id,
            request_mb_release_id=release_id,
        )
        for evidence_id, (current_id, release_id) in sorted(valid_candidates.items())
    ]
    expected_candidate_ids = [item.candidate_evidence_id for item in expected_associations]
    expected_current_only_ids = sorted(valid_current_ids - set(valid_candidates))
    dual_role_ids = sorted(valid_current_ids & set(valid_candidates))
    expected_evidence_ids = sorted(set(valid_candidates) | valid_current_ids)
    assert_export_output_exact(
        corpus_rows,
        [
            (item.candidate_evidence_id, item.current_evidence_id, item.request_mb_release_id)
            for item in expected_associations
        ],
        sorted(valid_current_ids),
    )
    entries = [_corpus_evidence(row) for row in corpus_rows]
    written_candidate_ids = [entry.evidence_id for entry in entries if entry.is_candidate]
    written_evidence_ids = [entry.evidence_id for entry in entries]
    written_associations = [
        _CoverageAssociation(
            candidate_evidence_id=entry.evidence_id,
            current_evidence_id=entry.current_evidence_id,
            request_mb_release_id=entry.request_mb_release_id or "",
        )
        for entry in entries if entry.is_candidate
    ]
    addresses = [
        msgspec.convert(_address(entry.row), type=_CoverageAddress)
        for entry in entries
    ]
    for address in addresses:
        observed = evidence_rows.get(address.id)
        if observed is None or (
            address.mb_release_id != observed.mb_release_id
            or address.snapshot_fingerprint != observed.stored_snapshot_fingerprint
            or address.files_sha256 != observed.files_sha256
        ):
            raise RenderDifferentialError("corpus content address disagrees with observed evidence ledger")
    address_conflicts = duplicate_content_addresses(addresses)
    output = _CoverageCorpusOutput(
        expected_candidate_ids=expected_candidate_ids,
        written_candidate_ids=written_candidate_ids,
        expected_associations=expected_associations,
        written_associations=written_associations,
        expected_referenced_current_ids=sorted(valid_current_ids),
        written_referenced_current_ids=sorted(valid_current_ids),
        expected_current_only_ids=expected_current_only_ids,
        written_current_only_ids=[entry.evidence_id for entry in entries if not entry.is_candidate],
        dual_role_ids=dual_role_ids,
        expected_evidence_ids=expected_evidence_ids,
        written_evidence_ids=written_evidence_ids,
        exact_match=(expected_candidate_ids == written_candidate_ids and expected_evidence_ids == written_evidence_ids),
        content_addresses=addresses,
        sha256=hashlib.sha256(corpus_bytes).hexdigest(),
    )
    coverage = DecisionCorpusCoverage(
        schema_version=2,
        source_links=list(source_links),
        observed_evidence=list(observed_evidence),
        total_source_links=len(links),
        source_link_counts=source_link_counts,
        source_distinct_candidate_id_counts=source_distinct_candidate_id_counts,
        total_distinct_candidate_ids=len(all_candidate_ids),
        identical_associations_collapsed=len(authoritative) - sum(len(values) for values in associations.values()),
        authorityless_source_links=authorityless,
        authorityless_candidate_ids=sorted({item.evidence_id for item in authorityless}),
        association_conflicts=conflicts,
        missing_candidate_evidence_ids=missing_candidates,
        candidate_release_mismatches=candidate_mismatches,
        missing_current_evidence_ids=missing_currents,
        current_release_mismatches=current_mismatches,
        referenced_current_ids=referenced_current_ids,
        valid_candidates=_CoverageValidCandidates(
            paired=sum(item.current_evidence_id is not None for item in expected_associations),
            unpaired=sum(item.current_evidence_id is None for item in expected_associations),
        ),
        content_address_mismatches=mismatches,
        file_content_conflicts=file_content_conflicts,
        written_content_address_conflicts=[
            _CoverageWrittenAddressConflict(mb_release_id=release, snapshot_fingerprint=fingerprint)
            for release, fingerprint in address_conflicts
        ],
        outputs=_CoverageOutputs(corpus=output),
        green=False,
        debt_count=0,
    )
    debt_count = _export_debt_count(coverage)
    return msgspec.structs.replace(
        coverage,
        green=debt_count == 0,
        debt_count=debt_count,
    )


def assert_export_output_exact(
    rows: Sequence[Mapping[str, object]],
    expected_associations: Sequence[tuple[int, int | None, str]],
    expected_referenced_current_ids: Sequence[int],
) -> None:
    """Fail closed on omissions, role changes, or substituted pairings."""
    ids = [row["id"] for row in rows]
    if any(not _is_exact_int(value) for value in ids):
        raise RenderDifferentialError("corpus output has a non-integer evidence id")
    if len(ids) != len(set(ids)):
        raise RenderDifferentialError("corpus output duplicates an evidence id")
    entries = [_corpus_evidence(row) for row in rows]
    written_associations = [
        (entry.evidence_id, entry.current_evidence_id, entry.request_mb_release_id)
        for entry in entries
        if entry.is_candidate
    ]
    expected_associations = list(expected_associations)
    if written_associations != expected_associations:
        raise RenderDifferentialError(
            "corpus candidate association triples differ from expected: "
            f"{written_associations!r} != {expected_associations!r}"
        )
    written_referenced_current_ids = sorted(
        {
            current_id
            for _candidate_id, current_id, _release in written_associations
            if current_id is not None
        }
    )
    if written_referenced_current_ids != list(expected_referenced_current_ids):
        raise RenderDifferentialError(
            "corpus referenced current IDs differ from expected: "
            f"{written_referenced_current_ids!r} != "
            f"{list(expected_referenced_current_ids)!r}"
        )
    expected_ids = sorted(
        {candidate_id for candidate_id, _current_id, _release in expected_associations}
        | set(expected_referenced_current_ids)
    )
    written_ids = [entry.evidence_id for entry in entries]
    if sorted(written_ids) != expected_ids:
        raise RenderDifferentialError(
            f"corpus evidence IDs {sorted(written_ids)!r} != expected {expected_ids!r}"
        )
    expected_current_only = sorted(
        set(expected_referenced_current_ids)
        - {
            candidate_id
            for candidate_id, _current_id, _release in expected_associations
        }
    )
    written_current_only = [
        entry.evidence_id for entry in entries if not entry.is_candidate
    ]
    if written_current_only != expected_current_only:
        raise RenderDifferentialError(
            f"corpus current-only IDs {written_current_only!r} != expected "
            f"{expected_current_only!r}"
        )


def export_decision_corpus(
    dsn: str | None,
    corpus_path: str | Path,
    coverage_path: str | Path,
    *,
    batch_size: int = 1000,
    _after_source_links: Callable[[_DecisionCorpusSnapshot], None] | None = None,
) -> DecisionCorpusExportResult:
    """Export the complete authoritative decision corpus from PostgreSQL.

    This is deliberately developer tooling, not a ``pipeline-cli`` mutation or
    operator command.  It opens precisely one repeatable-read, read-only
    snapshot; batching changes transport, never membership or ordering.
    Historical debt is reported in coverage and returns non-green, while every
    independently valid candidate still appears in the corpus.
    """
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    # Export-only dependency: leave ``--help`` and historical ``decide``
    # usable when this script is copied into a tree predating #999.
    from lib.quality_evidence import snapshot_fingerprint

    corpus_file, coverage_file = Path(corpus_path), Path(coverage_path)
    if corpus_file.resolve() == coverage_file.resolve():
        raise RenderDifferentialError(
            "corpus and coverage destinations resolve to the same path"
        )
    connection = psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        connection.set_session(
            readonly=True,
            autocommit=False,
            isolation_level="REPEATABLE READ",
        )
        with connection.cursor() as cursor:
            _assert_live_decision_corpus_schema(cursor)
            snapshot = _qualify_read_snapshot(cursor)
            links = _db_source_links(cursor)
            if _after_source_links is not None:
                _after_source_links(snapshot)
            authoritative = [
                link for link in links if link.request_mb_release_id is not None
            ]
            associations: dict[int, set[tuple[int | None, str]]] = {}
            for link in authoritative:
                assert link.request_mb_release_id is not None
                associations.setdefault(link.evidence_id, set()).add(
                    (
                        link.current_evidence_id,
                        link.request_mb_release_id,
                    )
                )
            conflict_ids = {
                evidence_id
                for evidence_id, values in associations.items()
                if len(values) > 1
            }
            # Collapse identical source associations only.  Two distinct
            # associations for the same candidate are named conflict debt,
            # never selected with DISTINCT ON or quietly treated as one.
            candidate_associations = {
                evidence_id: next(iter(sorted(values, key=_association_order_key)))
                for evidence_id, values in associations.items()
                if evidence_id not in conflict_ids
            }
            referenced_current_ids = sorted(
                {
                    link.current_evidence_id
                    for link in links
                    if link.current_evidence_id is not None
                }
            )
            # Account every candidate ID before choosing the valid replay
            # cohort; conflicts are debt, not permission to skip existence or
            # content-address validation.
            all_candidate_ids = {link.evidence_id for link in links}
            required_ids = sorted(all_candidate_ids | set(referenced_current_ids))
            evidence_rows = _db_evidence_rows(cursor, required_ids, batch_size)
            content_mismatches: list[dict[str, object]] = []
            for evidence_id, evidence in evidence_rows.items():
                try:
                    files = msgspec.convert(
                        evidence["files"],
                        type=list[AlbumQualityEvidenceFile],
                    )
                except msgspec.ValidationError as exc:
                    raise RenderDifferentialError(
                        "decision-corpus file projection/wire drift for "
                        f"evidence {evidence_id}: {exc}"
                    ) from exc
                actual_fingerprint = snapshot_fingerprint(files)
                stored_fingerprint = evidence["snapshot_fingerprint"]
                assert isinstance(stored_fingerprint, str)
                assert isinstance(evidence["mb_release_id"], str)
                if stored_fingerprint != actual_fingerprint:
                    content_mismatches.append(
                        {
                            "evidence_id": evidence_id,
                            "stored_snapshot_fingerprint": stored_fingerprint,
                            "files_snapshot_fingerprint": actual_fingerprint,
                        }
                    )
            content_mismatch_ids = {item["evidence_id"] for item in content_mismatches}
            candidate_mismatches: list[dict[str, object]] = []
            current_mismatches: list[dict[str, object]] = []
            # Inspect every authoritative association before conflicts remove
            # any candidate from replay eligibility.  A conflict is never a
            # licence to conceal a release/content mismatch on one of its
            # source links.
            invalid_associations: set[tuple[int, int | None, str]] = set()
            for evidence_id, association_values in sorted(associations.items()):
                candidate = evidence_rows.get(evidence_id)
                for current_id, release_id in sorted(
                    association_values, key=_association_order_key
                ):
                    association = (evidence_id, current_id, release_id)
                    if candidate is None or evidence_id in content_mismatch_ids:
                        invalid_associations.add(association)
                        continue
                    if candidate["mb_release_id"] != release_id:
                        candidate_mismatches.append(
                            {
                                "evidence_id": evidence_id,
                                "evidence_mb_release_id": candidate["mb_release_id"],
                                "request_mb_release_id": release_id,
                            }
                        )
                        invalid_associations.add(association)
                    if current_id is not None:
                        current = evidence_rows.get(current_id)
                        if current is None or current_id in content_mismatch_ids:
                            invalid_associations.add(association)
                            continue
                        if current["mb_release_id"] != release_id:
                            current_mismatches.append(
                                {
                                    "candidate_evidence_id": evidence_id,
                                    "current_evidence_id": current_id,
                                    "current_mb_release_id": current["mb_release_id"],
                                    "request_mb_release_id": release_id,
                                }
                            )
                            invalid_associations.add(association)
            valid_candidates: dict[int, tuple[int | None, str]] = {}
            for evidence_id, (current_id, release_id) in candidate_associations.items():
                if (evidence_id, current_id, release_id) in invalid_associations:
                    continue
                valid_candidates[evidence_id] = (current_id, release_id)
            valid_current_ids = {
                current_id
                for current_id, _release in valid_candidates.values()
                if current_id is not None
            }
            corpus_ids = sorted(set(valid_candidates) | valid_current_ids)
            corpus_rows: list[dict[str, object]] = []
            for evidence_id in corpus_ids:
                row = dict(evidence_rows[evidence_id])
                if evidence_id in valid_candidates:
                    current_id, release_id = valid_candidates[evidence_id]
                    row.update(
                        {
                            "is_candidate": True,
                            "current_evidence_id": current_id,
                            "request_mb_release_id": release_id,
                        }
                    )
                else:
                    row.update(
                        {
                            "is_candidate": False,
                            "current_evidence_id": None,
                            "request_mb_release_id": None,
                        }
                    )
                # One shared strict contract, then production's only semantic
                # decoder.  Do not validate one dict and feed another.
                _evidence_from_corpus_row(row)
                corpus_rows.append(row)
            corpus_bytes = b"".join(
                msgspec.json.encode(row) + b"\n" for row in corpus_rows
            )
            coverage = recompute_decision_corpus_coverage(
                _coverage_source_links(links),
                sorted(
                    (_observed_evidence(row) for row in evidence_rows.values()),
                    key=lambda item: item.evidence_id,
                ),
                corpus_rows,
                corpus_bytes,
            )
            debt_count = coverage.debt_count
            # The manifest binds to corpus bytes. Each artifact is atomically
            # replaced independently; consumers reject a stale pair by digest.
            coverage_bytes = msgspec.json.encode(coverage) + b"\n"
        connection.rollback()  # read-only snapshot: never retain an idle txn.
    finally:
        connection.close()
    _atomic_write(corpus_file, corpus_bytes)
    _atomic_write(coverage_file, coverage_bytes)
    return DecisionCorpusExportResult(green=debt_count == 0, debt_count=debt_count)


def verify_decision_corpus_pair(
    corpus_path: str | Path,
    coverage_path: str | Path,
) -> None:
    """Verify the mandatory corpus/coverage pairing before either render."""
    corpus_file, coverage_file = Path(corpus_path), Path(coverage_path)
    if corpus_file.resolve() == coverage_file.resolve():
        raise RenderDifferentialError("corpus and coverage resolve to the same path")
    corpus_bytes = corpus_file.read_bytes()
    try:
        coverage = msgspec.json.decode(
            coverage_file.read_bytes(), type=DecisionCorpusCoverage
        )
    except (msgspec.DecodeError, msgspec.ValidationError) as exc:
        raise RenderDifferentialError(f"coverage violates its strict schema: {exc}") from exc
    if coverage.schema_version != 2:
        raise RenderDifferentialError(
            f"coverage schema_version must be exactly 2, got {coverage.schema_version!r}"
        )
    rows = list(_corpus_rows(str(corpus_file)))
    for row in rows:
        _evidence_from_corpus_row(row)
    recomputed = recompute_decision_corpus_coverage(
        coverage.source_links,
        coverage.observed_evidence,
        rows,
        corpus_bytes,
    )
    if coverage != recomputed:
        raise RenderDifferentialError(
            "coverage does not reconcile with its source/evidence ledgers and corpus"
        )


def _non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def _positive_int(raw: str) -> int:
    value = _non_negative_int(raw)
    if value == 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="decision_differential.py",
        description=(
            "Re-decide a corpus of album_quality_evidence rows through the "
            "real decider, so a policy change is measured against real rows "
            "instead of asserted."
        ),
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    decide = sub.add_parser(
        "decide", help="Decide a corpus JSONL through the real decider"
    )
    decide.add_argument(
        "--corpus",
        required=True,
        help="Corpus JSONL: one album_quality_evidence row object per line",
    )
    decide.add_argument(
        "--coverage", required=True, help="Matching verified coverage manifest"
    )

    verify = sub.add_parser("verify", help="Verify an exported corpus/coverage pair")
    verify.add_argument("--corpus", required=True, help="Exported corpus JSONL")
    verify.add_argument("--coverage", required=True, help="Matching coverage manifest")
    decide.add_argument(
        "--out", default=None, help="Decided JSONL output path (default: stdout)"
    )
    decide.add_argument(
        "--verified-lossless-target",
        default=None,
        dest="verified_lossless_target",
        help=(
            "The operator's configured stored format for lossless "
            "sources (doc2 config.ini [Beets] verified_lossless_target, "
            "e.g. 'opus 128'). Omitting it decides every row as if "
            "nothing were configured, which makes target_final_format "
            "and the gate format it feeds unmeasurable"
        ),
    )
    decide.add_argument(
        "--counterfactual",
        action="store_true",
        help=(
            "Drop each candidate's persisted verified-lossless proof "
            "first — the fresh-mint arm, where a promotion-gate change "
            "shows its real blast radius"
        ),
    )

    export = sub.add_parser(
        "export", help="Read PostgreSQL once and materialize corpus plus coverage"
    )
    export.add_argument(
        "--dsn",
        required=True,
        help="PostgreSQL DSN for the read-only repeatable-read snapshot",
    )
    export.add_argument(
        "--corpus", required=True, help="Destination corpus.jsonl (atomically replaced)"
    )
    export.add_argument(
        "--coverage",
        required=True,
        help="Destination coverage.json (atomically replaced even with debt)",
    )
    export.add_argument(
        "--batch-size",
        type=_positive_int,
        default=1000,
        help="Evidence IDs per internal fetch; membership and output are invariant",
    )

    diff = sub.add_parser("diff", help="Compare two decided JSONL files field by field")
    diff.add_argument("--base", required=True, help="Base decided JSONL")
    diff.add_argument("--current", required=True, help="Current decided JSONL")
    diff.add_argument(
        "--samples",
        type=_non_negative_int,
        default=DEFAULT_SAMPLES_PER_FIELD,
        help="Concrete before/after pairs to show per changed field",
    )
    diff.add_argument(
        "--allow-field-drift",
        action="store_true",
        help=(
            "Compare the shared fields when the decision field set "
            "changed, naming the unshared fields in the report"
        ),
    )
    diff.add_argument("--json", action="store_true", help="Print the report as JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.mode == "export":
            result = export_decision_corpus(
                args.dsn,
                args.corpus,
                args.coverage,
                batch_size=args.batch_size,
            )
            state = "green" if result.green else "debt"
            print(
                f"exported decision corpus: {state}, {result.debt_count} debt item(s)",
                file=sys.stderr,
            )
            return 0 if result.green else 2
        if args.mode == "decide":
            verify_decision_corpus_pair(args.corpus, args.coverage)
            count = decide_corpus(
                args.corpus,
                args.out,
                counterfactual=args.counterfactual,
                verified_lossless_target=args.verified_lossless_target,
            )
            print(f"decided {count} rows", file=sys.stderr)
            return 0
        if args.mode == "verify":
            verify_decision_corpus_pair(args.corpus, args.coverage)
            print("verified decision corpus pair", file=sys.stderr)
            return 0
        report = summarize_render_diff(
            read_rendered(args.base),
            read_rendered(args.current),
            samples_per_field=args.samples,
            allow_field_drift=args.allow_field_drift,
        )
        if args.json:
            print(msgspec.json.encode(report).decode())
        else:
            print(format_report(report))
        return 0
    except (RenderDifferentialError, OSError) as exc:
        print(f"decision-differential: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
