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

Two modes, same two-tree runbook as Rule D:

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
      decide --corpus /tmp/evidence.jsonl --out /tmp/base.jsonl"
    nix-shell --run "python3 scripts/decision_differential.py \\
      decide --corpus /tmp/evidence.jsonl --out /tmp/current.jsonl"
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
import os
import sys
from collections.abc import Iterator, Mapping, Sequence
from typing import TypeGuard

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import msgspec

from lib.json_narrow import is_object_list, is_str_object_dict
from lib.pipeline_db.evidence import _EvidenceMixin
from lib.quality import (
    AacLatticeProofLeg,
    AlbumQualityEvidence,
    AlbumQualityEvidenceDecisionFacts,
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

#: Decision-dict keys that are not decisions: the Stage-1 counterfactual
#: is audit-only reporting, and ``comparison_basis`` is a nested display
#: payload whose own differential is ``render_differential``'s job. They
#: are excluded by NAME rather than by type so adding a decision key can
#: never silently fall out of the watched set.
_AUDIT_ONLY_DECISION_KEYS: frozenset[str] = frozenset({
    "stage2_import_if_stage1_deferred",
    "comparison_basis_if_stage1_deferred",
})

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
        is_flac=False, min_bitrate=192, is_cbr=True,
    )
    return tuple(
        key for key in sorted(shape)
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
    try:
        # ``_album_quality_evidence_from_row`` deliberately accepts values
        # from psycopg2's already-typed result rows.  A JSONL corpus is a
        # separate wire boundary: validate every column it consumes before
        # handing the row to that production mapper, rather than allowing its
        # legacy ``bool()``/``int()`` coercions to decide a different world.
        msgspec.convert(payload, type=DecisionCorpusEvidenceWire)
    except msgspec.ValidationError as exc:
        raise RenderDifferentialError(
            f"corpus row {payload.get('id')!r} has an invalid evidence wire "
            f"shape: {exc}") from exc
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
        return _EvidenceMixin._album_quality_evidence_from_row(
            payload, file_rows,
        )
    except (KeyError, TypeError, ValueError, msgspec.ValidationError) as exc:
        raise RenderDifferentialError(
            f"corpus row {payload.get('id')!r} is not an "
            f"album_quality_evidence row: {exc}") from exc


def _parse_timestamp(raw: str) -> object:
    from datetime import datetime

    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise RenderDifferentialError(
            f"measured_at is not an ISO timestamp: {raw!r}") from exc


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
            context.spectral_subject, context.was_converted_from,
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
        raise RenderDifferentialError(
            f"corpus row has no integer id: {row_id!r}")
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
            evidence, current_evidence, facts=facts,
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
                "and this differential would silently not compare them")
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
    fields.update({
        "verified_lossless_classifier": (
            proof.classifier if proof is not None else None
        ),
        "ultrasonic_leg_outcome": leg.outcome,
        "ultrasonic_leg_reason": leg.reason,
        "aac_lattice_leg_outcome": lattice_leg.outcome,
        "aac_lattice_leg_reason": lattice_leg.reason,
    })
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
            f"missing {sorted(expected - set(fields))}")
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
                    f"{path}:{line_number}: corpus line is not a JSON object: "
                    f"{exc}") from exc


CORPUS_REQUIRED_COLUMNS: frozenset[str] = frozenset({
    "id",
    "is_candidate",
    "current_evidence_id",
    "request_mb_release_id",
    "files",
})


class DecisionCorpusEvidenceFileWire(msgspec.Struct, frozen=True):
    """Exact JSON shape of one exported evidence-file row."""

    relative_path: str
    size_bytes: int
    mtime_ns: int
    extension: str
    container: str
    codec: str | None
    decode_ok: bool


class DecisionCorpusAudioDiagnosticWire(msgspec.Struct, frozen=True):
    """Exact JSON shape of a persisted audio-validation diagnostic."""

    relative_path: str
    category: AudioToolDiagnosticCategory
    return_code: int | None
    stderr_excerpt: str
    stderr_bytes: int
    stderr_sha256: str
    stderr_truncated: bool


class DecisionCorpusAudioValidationWire(msgspec.Struct, frozen=True):
    """Exact JSON shape consumed by production's audio-report decoder."""

    policy_id: str
    tool: str
    tool_version: str
    outcome: AudioValidationOutcome
    files_checked: int
    files_failed: int
    diagnostics: list[DecisionCorpusAudioDiagnosticWire]
    omitted_diagnostics: int


class DecisionCorpusAacLatticeTrackWire(msgspec.Struct, frozen=True):
    """Exact JSON shape of one persisted AAC-lattice track capture."""

    filename: str
    offset: int | None
    z: float | None
    proba: float | None
    error: str | None


class DecisionCorpusEvidenceWire(msgspec.Struct, frozen=True):
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


class DecisionCorpusEvidence(msgspec.Struct, frozen=True):
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
            "corpus row is missing required columns: "
            f"{sorted(missing)}")
    evidence_id = row["id"]
    if not _is_exact_int(evidence_id):
        raise RenderDifferentialError(
            f"corpus row has no integer id: {evidence_id!r}")
    is_candidate = row["is_candidate"]
    if not isinstance(is_candidate, bool):
        raise RenderDifferentialError(
            "corpus row 'is_candidate' must be a boolean")
    current_evidence_id = row["current_evidence_id"]
    if current_evidence_id is not None and not _is_exact_int(current_evidence_id):
        raise RenderDifferentialError(
            "corpus row 'current_evidence_id' must be an integer or null")
    request_mb_release_id = row["request_mb_release_id"]
    if request_mb_release_id is not None and not isinstance(
        request_mb_release_id, str,
    ):
        raise RenderDifferentialError(
            "corpus row 'request_mb_release_id' must be a string or null")
    if not is_candidate and current_evidence_id is not None:
        raise RenderDifferentialError(
            "current-only corpus row has a current_evidence_id")
    if is_candidate and request_mb_release_id is None:
        raise RenderDifferentialError(
            "candidate corpus row has no request_mb_release_id")
    if not is_candidate and request_mb_release_id is not None:
        raise RenderDifferentialError(
            "current-only corpus row has a request_mb_release_id")
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
            f"corpus evidence {entry.evidence_id} has no string "
            "mb_release_id")
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
                f"corpus has duplicate evidence id {entry.evidence_id}")
        by_id[entry.evidence_id] = entry

    pairs: list[tuple[DecisionCorpusEvidence, DecisionCorpusEvidence | None]] = []
    for entry in entries:
        if not entry.is_candidate:
            continue
        request_release_id = entry.request_mb_release_id
        if request_release_id is None:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} has no "
                "request_mb_release_id")
        candidate_release_id = _entry_release_id(entry)
        if candidate_release_id != request_release_id:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} release "
                f"{candidate_release_id!r} does not match request "
                f"release {request_release_id!r}")
        current_id = entry.current_evidence_id
        if current_id is None:
            pairs.append((entry, None))
            continue
        current = by_id.get(current_id)
        if current is None:
            raise RenderDifferentialError(
                "candidate evidence "
                f"{entry.evidence_id} has dangling current_evidence_id "
                f"{current_id}")
        current_release_id = _entry_release_id(current)
        if current_release_id != request_release_id:
            raise RenderDifferentialError(
                f"candidate evidence {entry.evidence_id} current evidence "
                f"{current.evidence_id} release {current_release_id!r} "
                f"does not match request release {request_release_id!r}")
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


def _non_negative_int(raw: str) -> int:
    value = int(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="decision_differential.py",
        description=(
            "Re-decide a corpus of album_quality_evidence rows through the "
            "real decider, so a policy change is measured against real rows "
            "instead of asserted."),
    )
    sub = parser.add_subparsers(dest="mode", required=True)

    decide = sub.add_parser(
        "decide", help="Decide a corpus JSONL through the real decider")
    decide.add_argument(
        "--corpus", required=True,
        help="Corpus JSONL: one album_quality_evidence row object per line")
    decide.add_argument(
        "--out", default=None,
        help="Decided JSONL output path (default: stdout)")
    decide.add_argument(
        "--verified-lossless-target", default=None,
        dest="verified_lossless_target",
        help=("The operator's configured stored format for lossless "
              "sources (doc2 config.ini [Beets] verified_lossless_target, "
              "e.g. 'opus 128'). Omitting it decides every row as if "
              "nothing were configured, which makes target_final_format "
              "and the gate format it feeds unmeasurable"))
    decide.add_argument(
        "--counterfactual", action="store_true",
        help=("Drop each candidate's persisted verified-lossless proof "
              "first — the fresh-mint arm, where a promotion-gate change "
              "shows its real blast radius"))

    diff = sub.add_parser(
        "diff", help="Compare two decided JSONL files field by field")
    diff.add_argument("--base", required=True, help="Base decided JSONL")
    diff.add_argument("--current", required=True, help="Current decided JSONL")
    diff.add_argument(
        "--samples", type=_non_negative_int, default=DEFAULT_SAMPLES_PER_FIELD,
        help="Concrete before/after pairs to show per changed field")
    diff.add_argument(
        "--allow-field-drift", action="store_true",
        help=("Compare the shared fields when the decision field set "
              "changed, naming the unshared fields in the report"))
    diff.add_argument(
        "--json", action="store_true", help="Print the report as JSON")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.mode == "decide":
            count = decide_corpus(
                args.corpus, args.out,
                counterfactual=args.counterfactual,
                verified_lossless_target=args.verified_lossless_target,
            )
            print(f"decided {count} rows", file=sys.stderr)
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
