#!/usr/bin/env python3
"""Generated properties for lib/failure_presentation.py (issue #868 PR2).

The pins in ``tests/test_failure_presentation.py`` prove the exact live
scenarios; these properties patrol the world space around them, per
``.claude/rules/code-quality.md`` § "Pin+fuzz PAIR rule". Worlds vary file
counts, peer counts, failure families, bytes transferred, terminal states
and arbitrary unknown/bounded peer text.

Invariants (numbered as in ``lib/failure_presentation.py``):

I1. Presenting a failure never mutates the persisted evidence, and never
    drops it: whenever any file carried a reason, the presentation still
    carries that reason's text.
I2. A world whose every failure is a local storage fault is never
    attributed to a peer — no peer name, no peer vocabulary, and the
    server-owned label says storage.
I3. A materialize reason that names a storage errno never renders as a
    security finding, and a containment refusal never renders as ordinary
    storage trouble. Expectations are derived from the PRODUCER's own
    vocabulary structure (``lib.download_materialization._ReasonVocabulary``),
    not from the presenter's table, so the two must independently agree.
I4. Unknown text is always bounded, stripped of control characters, and
    quoted as the peer's words rather than re-voiced as our diagnosis.
I5. Presentation is pure: same evidence in, same copy out, and the row it
    was derived from is unchanged.

Every checker is a module-level function returning a violation string (or
``None``), so ``TestInvariantCheckersTripOnViolations`` can call it
directly with a planted violation.
"""

from __future__ import annotations

import os
import sys
import unittest
from collections.abc import Sequence

import msgspec
from hypothesis import assume, example, given, strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

from lib.download_materialization import (
    _PRIVATE_TREE_VOCABULARY,
    _SHARED_ROOT_VOCABULARY,
    _SOURCE_FILE_VOCABULARY,
    _ReasonVocabulary,
)
from lib.failure_presentation import (
    FAMILY_LOCAL_STORAGE,
    FAMILY_PEER_FILE,
    FAMILY_REFUSAL,
    FAMILY_TRANSPORT,
    FAMILY_UNKNOWN,
    MAX_RAW_MESSAGE_CHARS,
    MAX_RAW_MESSAGE_GROUPS,
    TRANSFER_MESSAGE_LABEL_PEER,
    TRANSFER_MESSAGE_LABEL_STATE,
    TRANSFER_MESSAGE_LABEL_STORAGE,
    FailureEvidence,
    FailurePresentation,
    PeerFailureFamily,
    bounded_text,
    decode_transfer_detail,
    materialize_reason_copy,
    peer_failure_family,
    present_failure,
)
from lib.failure_presentation import (
    _FAMILY_BREAKDOWN_LABELS as _BREAKDOWN_LABELS,
)
from web.classify import LogEntry, classify_log_entry


# ---------------------------------------------------------------------------
# Worlds
# ---------------------------------------------------------------------------

# Peer names deliberately drawn from an alphabet no template word or census
# message contains, so "does the peer name appear in this sentence?" is a
# decidable question (I2).
_PEER_NAMES = st.text(
    alphabet="QXZJKVW0123456789", min_size=5, max_size=12,
)

_TERMINAL_STATES = (
    "Completed, Errored",
    "Completed, Rejected",
    "Completed, Cancelled",
    "Completed, TimedOut",
    # Neither of these is evidence of failure, so worlds containing them
    # exercise the "this file contributes nothing" path.
    "Completed, Succeeded",
    "InProgress",
)

_FAMILY_MESSAGES: dict[PeerFailureFamily, tuple[str, ...]] = {
    FAMILY_REFUSAL: (
        "Verification required",
        "Transfer rejected: File not shared.",
        "Transfer rejected: Too many files",
        "Too many files",
        "Transfer rejected: Too many megabytes",
        "Transfer rejected: Banned",
        "Transfer rejected: Banned (Country banned)",
        "Transfer rejected: Overwhelmed with requests; try again later.",
        "Pending shutdown.",
    ),
    FAMILY_TRANSPORT: (
        "Inactivity timeout of 15000 milliseconds was reached",
        "Transfer failed: Read error: Remote connection closed",
        "Download reported as failed by remote client",
        "The wait timed out after 30000 milliseconds",
        'enqueue failed: "The wait timed out after 5000 milliseconds"',
        "Application shut down",
        "A task was canceled.",
    ),
    FAMILY_PEER_FILE: (
        "File read error.",
        "Transfer aborted: the remote size of 100 does not match expected size 200",
    ),
    FAMILY_LOCAL_STORAGE: (
        "Failed to create file 01 - track.flac: Stale file handle : "
        "'/mnt/virtio/music/slskd/incomplete/x'",
        "Failed to create file 02 - track.flac: Could not find a part of the "
        "path '/mnt/virtio/music/slskd/incomplete/x'",
        "Could not find a part of the path.",
    ),
}

_TERMINAL_ERROR_STATES = tuple(
    state for state in _TERMINAL_STATES
    if state.startswith("Completed,") and state != "Completed, Succeeded"
)

# Everything the measurement path recognises; anything else must survive
# verbatim. Kept as data so a new copy entry has to be considered here too.
_KNOWN_MEASUREMENT_TRIGGERS = frozenset(
    {"current beets authority resolution raised",
     "current beets path was not returned"}
)

_KNOWN_MESSAGES = st.sampled_from(
    tuple(message for pool in _FAMILY_MESSAGES.values() for message in pool)
)
_UNKNOWN_MESSAGES = st.text(min_size=1, max_size=300)
_ANY_MESSAGE = st.one_of(_KNOWN_MESSAGES, _UNKNOWN_MESSAGES, st.none())


def _same_message_world(
    peer: str, message: str, count: int, transferred: int,
) -> list[dict[str, object]]:
    """``count`` files from one peer, all failing the same way."""
    return [
        _transfer_file(peer, message, transferred, "Completed, Errored")
        for _index in range(count)
    ]


def _transfer_file(
    username: str,
    message: str | None,
    transferred: int,
    state: str,
) -> dict[str, object]:
    return {
        "username": username,
        "filename": f"{username}-{abs(hash(message)) % 1000}.flac",
        "last_state": state,
        "last_exception": message,
        "bytes_transferred": transferred,
        "retry_count": 0,
    }


def _files(
    message_strategy: st.SearchStrategy[str | None],
    *,
    peers: st.SearchStrategy[str] = _PEER_NAMES,
    min_size: int = 0,
    max_size: int = 30,
) -> st.SearchStrategy[list[dict[str, object]]]:
    return st.lists(
        st.builds(
            _transfer_file,
            peers,
            message_strategy,
            st.integers(min_value=0, max_value=10**9),
            st.sampled_from(_TERMINAL_STATES),
        ),
        min_size=min_size,
        max_size=max_size,
    )


_OWN_MESSAGES = st.sampled_from((
    None,
    "all 12 files errored",
    "no download progress for 600s (stalled_timeout 600s)",
    "remote_queue_timeout 3600s exceeded",
    "file exceeded retry limit after 3 retries: d:\\music\\x\\05 - Track.flac",
    "transfers vanished from slskd before any status was observed "
    "(slskd restart?)",
    "all 29 files errored — 29× 'Verification required'",
))

_OUTCOMES = st.sampled_from((
    "timeout", "failed", "measurement_failed", "success", "rejected",
))


# ---------------------------------------------------------------------------
# Checkers (module-level so the known-bad self-tests can call them)
# ---------------------------------------------------------------------------

_CONTROL_FREE_FAILURE = "rendered text contains a control character: {value!r}"


def _observed_reason(row: dict[str, object]) -> str | None:
    """The documented evidence rule, restated independently of the presenter.

    slskd's per-transfer exception if there is one; otherwise a terminal
    ``Completed, *`` state that is not a success. Same rule
    ``lib.download.summarize_file_failures`` has used since #564.
    """
    exception = row.get("last_exception")
    if isinstance(exception, str) and exception:
        return exception
    state = row.get("last_state")
    if (
        isinstance(state, str)
        and state.startswith("Completed,")
        and state != "Completed, Succeeded"
    ):
        return state
    return None


def check_evidence_survives_rendering(
    raw_files: Sequence[dict[str, object]],
    presentation: FailurePresentation,
) -> str | None:
    """I1 — no failing file's evidence is silently dropped.

    The dominant reason is always quoted, and any reason beyond the
    rendering bound is still counted in the ``+N more`` tail.
    """
    counts: dict[str, int] = {}
    for row in raw_files:
        reason = _observed_reason(dict(row))
        if reason:
            counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return None
    message = presentation.transfer_message
    if not message:
        return "per-file evidence existed but no transfer_message was rendered"
    dominant = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    if bounded_text(dominant) not in message:
        return (
            "the dominant peer message is missing from transfer_message: "
            f"{dominant!r}"
        )
    hidden = len(counts) - MAX_RAW_MESSAGE_GROUPS
    if hidden > 0 and not message.endswith(f"+{hidden} more"):
        return (
            f"{hidden} further distinct reasons went unaccounted for: {message!r}"
        )
    return None


# A family clause is a claim about EVERY message in the family. These are
# the words each family may NOT use, because at least one census member
# contradicts them — the defect: "could not read N of its own files" was
# false for ``Transfer aborted: the remote size of N does not match
# expected size N``, where the peer read its file perfectly well and the
# share index was stale (25 of 39 live rows, issue #868 review B1).
_FORBIDDEN_FAMILY_CLAIMS: dict[PeerFailureFamily, tuple[tuple[str, str], ...]] = {
    FAMILY_PEER_FILE: (
        ("could not read",
         "the size-mismatch member read its file fine; slskd aborted"),
        ("unreadable",
         "the adjective form of the same retracted claim — this is how it "
         "survived B1 in the breakdown label table"),
        ("its own files",
         "a size mismatch is about what was advertised, not ownership"),
        ("rejected", "nothing in this family is a refusal"),
    ),
    FAMILY_REFUSAL: (
        ("could not read", "a refusal never got as far as reading"),
        ("dropped mid-download", "a refusal is not a dropped transfer"),
    ),
    FAMILY_TRANSPORT: (
        ("rejected", "a transport failure is not a refusal"),
        ("could not read", "the peer's own read is not what failed"),
    ),
    FAMILY_LOCAL_STORAGE: (
        ("peer", "our storage, our fault"),
        ("rejected", "nobody refused anything"),
    ),
    FAMILY_UNKNOWN: (
        ("without a reason",
         "the row HAS a reason, quoted beside it; we failed to classify it"),
        ("could not read", "nothing here says what failed"),
        ("rejected", "nothing here says a peer refused"),
    ),
}


def check_family_claim(
    family: PeerFailureFamily,
    claim: str,
    *,
    where: str,
    bytes_moved: bool = False,
) -> str | None:
    """B1/F1 — any string that renders a family claim must be family-true.

    ``claim`` is deliberately not "the verdict": the same retracted claim
    survived in ``_FAMILY_BREAKDOWN_LABELS`` in adjective form, and a
    verdict-scoped check could not see it because every generated world
    carried one family and never reached the breakdown (issue #868 review
    F1). Every string that speaks for a family answers here.
    """
    lowered = claim.casefold()
    for forbidden, why in _FORBIDDEN_FAMILY_CLAIMS.get(family, ()):
        if forbidden in lowered:
            return (
                f"{where} for {family} claims {forbidden!r} ({why}): {claim!r}"
            )
    if bytes_moved and "before transfer" in lowered:
        return (
            f"{where} for {family} claims 'before transfer' after bytes "
            f"moved: {claim!r}"
        )
    return None


def check_local_storage_not_peer_attributed(
    peers: Sequence[str],
    presentation: FailurePresentation,
    summary: str | None = None,
) -> str | None:
    """I2 — our own storage failing is never rendered as peer behaviour.

    ``summary`` is the list-row line the operator actually reads: it
    appends the row's peer as a trailing attribution, which put the peer's
    name back onto our own fault on 25 live rows while this checker was
    looking only at the verdict (issue #868 review #12).
    """
    verdict = presentation.verdict or ""
    if "peer" in verdict.casefold():
        return f"local-storage verdict uses peer vocabulary: {verdict!r}"
    for peer in peers:
        if peer and peer in verdict:
            return f"local-storage verdict names peer {peer!r}: {verdict!r}"
    if presentation.transfer_message_label != TRANSFER_MESSAGE_LABEL_STORAGE:
        return (
            "local-storage evidence was labelled "
            f"{presentation.transfer_message_label!r}"
        )
    if summary is not None:
        if "peer" in summary.casefold():
            return f"local-storage summary uses peer vocabulary: {summary!r}"
        for peer in peers:
            if peer and peer in summary:
                return f"local-storage summary names peer {peer!r}: {summary!r}"
    return None


def dominant_family(raw_files: Sequence[dict[str, object]]) -> str | None:
    """The family of the most-reported reason, derived independently."""
    counts: dict[str, int] = {}
    for row in raw_files:
        reason = _observed_reason(dict(row))
        if reason:
            counts[reason] = counts.get(reason, 0) + 1
    if not counts:
        return None
    dominant = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]
    return peer_failure_family(dominant)


def check_storage_cause_is_never_suppressed(
    raw_files: Sequence[dict[str, object]],
    peers: Sequence[str],
    presentation: FailurePresentation,
) -> str | None:
    """I6 — a Cratedigger-decision headline never hides a storage cause.

    Live-data review of 400 rows: 10 of 14 local-storage rows led with
    "Gave up on <file> after 5 failed attempts" and never mentioned storage
    at all, because the retry-limit message outranked the evidence. The
    operator's next action (retry the peer / fix the mount) hangs entirely
    on this sentence.
    """
    if dominant_family(raw_files) != FAMILY_LOCAL_STORAGE:
        return None
    verdict = presentation.verdict or ""
    if "local storage" not in verdict.casefold():
        return f"storage cause suppressed from the verdict: {verdict!r}"
    for peer in peers:
        if peer and peer in verdict:
            return f"storage-caused verdict names peer {peer!r}: {verdict!r}"
    return None


def check_unrecognised_text_is_passed_through(
    diagnostic: str,
    presentation: FailurePresentation,
) -> str | None:
    """I4 for measurement rows — copy is substituted only for text a
    producer actually emits; everything else survives verbatim.

    The defect this patrols: a copy table entry keyed on a string no
    producer emits ("path is outside the library root") replaced the
    producer's real words with a different, wrong fact. Anything the
    presenter does not recognise must reach the operator unedited.
    """
    verdict = presentation.verdict or ""
    expected = bounded_text(diagnostic, limit=200)
    if expected and expected not in verdict:
        return (
            f"unrecognised diagnostic was rewritten: {diagnostic!r} -> "
            f"{verdict!r}"
        )
    return None


def check_state_tokens_are_not_peer_speech(
    presentation: FailurePresentation,
) -> str | None:
    """Review finding #5 — slskd's state machine is not a peer talking."""
    if presentation.transfer_message_label == TRANSFER_MESSAGE_LABEL_PEER:
        return (
            "an slskd state token was labelled as a peer message: "
            f"{presentation.transfer_message!r}"
        )
    verdict = presentation.verdict or ""
    # A breakdown verdict quotes nothing, so there is nothing to attribute;
    # the moment it DOES quote, it must say the words are slskd's.
    if '"' in verdict and "slskd state" not in verdict:
        return (
            "a state-derived verdict quotes without saying whose words "
            f"they are: {verdict!r}"
        )
    return None


CONTAINMENT_MARKERS = ("refused to import", "symlink", "escaped", "containment")
STORAGE_MARKERS = ("storage", "could not read", "could not be opened")


def check_reason_partition(
    kind: str,
    copy: str | None,
) -> str | None:
    """I3 — containment copy and storage copy never borrow each other's words.

    ``kind`` comes from the PRODUCER's vocabulary slot, not from the
    presenter: ``unsafe`` is a containment refusal, ``open_failed`` is a
    storage errno, and everything else must at least not read as a
    security finding.
    """
    if copy is None:
        return f"{kind} reason has no operator copy"
    lowered = copy.casefold()
    has_containment = any(marker in lowered for marker in CONTAINMENT_MARKERS)
    has_storage = any(marker in lowered for marker in STORAGE_MARKERS)
    if kind == "unsafe":
        if not has_containment:
            return f"containment refusal does not read as one: {copy!r}"
        if has_storage:
            return f"containment refusal reads as a storage fault: {copy!r}"
        return None
    if kind == "open_failed":
        if not has_storage:
            return f"storage failure does not read as one: {copy!r}"
        if has_containment:
            return f"storage failure reads as a security finding: {copy!r}"
        return None
    if has_containment:
        return f"{kind} reason manufactures a security finding: {copy!r}"
    return None


_GAVE_UP_PREFIX = 'Gave up on "'


def check_gave_up_names_only_a_filename(verdict: str | None) -> str | None:
    """I4 — the give-up sentence names a file, not a peer's whole path.

    The persisted message is ``file exceeded retry limit after N retries:
    <peer path>``, optionally followed by ``— N× '<reason>'``. Both the
    Windows drive letters and the appended evidence summary must stay out
    of the quoted name.
    """
    if verdict is None or not verdict.startswith(_GAVE_UP_PREFIX):
        return None
    body = verdict[len(_GAVE_UP_PREFIX):]
    name = body.rsplit('" after ', 1)[0]
    for forbidden in ("\\", "/", "× '"):
        if forbidden in name:
            return f"give-up copy leaked {forbidden!r}: {verdict!r}"
    return None


def check_rendered_text_is_bounded(
    presentation: FailurePresentation,
) -> str | None:
    """I4 — nothing rendered carries control characters or grows unbounded."""
    for value in (presentation.verdict, presentation.transfer_message):
        if value is None:
            continue
        if any(not ch.isprintable() and ch != " " for ch in value):
            return _CONTROL_FREE_FAILURE.format(value=value)
    # A verdict is at most one bounded own-message, one family clause and
    # one bounded quote — never an unbounded peer string. Live p95 is 163.
    if presentation.verdict is not None and len(presentation.verdict) > 600:
        return (
            f"verdict exceeded its bound ({len(presentation.verdict)} chars)"
        )
    message = presentation.transfer_message
    if message is None:
        return None
    # Bounded by construction: at most N groups, each a bounded message
    # plus its "123× " prefix and quotes, plus a "+N more" tail.
    ceiling = MAX_RAW_MESSAGE_GROUPS * (MAX_RAW_MESSAGE_CHARS + 40) + 40
    if len(message) > ceiling:
        return f"transfer_message exceeded its bound ({len(message)} chars)"
    return None


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

class TestEvidenceSurvivesRendering(unittest.TestCase):
    """I1."""

    @given(
        files=_files(_ANY_MESSAGE),
        error_message=_OWN_MESSAGES,
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    @example(
        files=[
            _transfer_file("Tymemage", "Verification required", 0,
                           "Completed, Rejected")
        ] * 29,
        error_message="all 29 files errored — 29× 'Verification required'",
        username="Tymemage",
    )
    def test_raw_evidence_is_never_mutated_or_dropped(
        self,
        files: list[dict[str, object]],
        error_message: str | None,
        username: str | None,
    ) -> None:
        before = msgspec.json.encode(files)
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message=error_message,
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(files),
        ))
        self.assertEqual(msgspec.json.encode(files), before)
        violation = check_evidence_survives_rendering(files, presentation)
        self.assertIsNone(violation, violation)

    @given(files=_files(_ANY_MESSAGE, min_size=1), outcome=_OUTCOMES)
    def test_presentation_never_raises(
        self, files: list[dict[str, object]], outcome: str,
    ) -> None:
        present_failure(FailureEvidence(
            outcome=outcome,
            error_message="something",
            transfer_detail=decode_transfer_detail(files),
        ))


class TestLocalStorageIsNeverPeerAttributed(unittest.TestCase):
    """I2 — the correction the issue calls highest-value."""

    @given(
        files=_files(
            st.sampled_from(_FAMILY_MESSAGES[FAMILY_LOCAL_STORAGE]),
            min_size=1,
        ),
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    def test_storage_worlds_blame_storage(
        self, files: list[dict[str, object]], username: str | None,
    ) -> None:
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message="all files errored",
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(files),
        ))
        peers = [str(row["username"]) for row in files]
        if username:
            peers.append(username)
        # The list row, not just the verdict — that is the line the
        # operator reads (review #12).
        classified = classify_log_entry(LogEntry(
            id=1, request_id=2, outcome="timeout",
            error_message="all files errored",
            soulseek_username=username,
            transfer_detail=files,
        ))
        violation = check_local_storage_not_peer_attributed(
            peers, presentation, classified.summary)
        self.assertIsNone(violation, violation)

    @given(
        storage_files=_files(
            st.sampled_from(_FAMILY_MESSAGES[FAMILY_LOCAL_STORAGE]),
            min_size=1, max_size=10,
        ),
        peer_files=_files(
            st.sampled_from(_FAMILY_MESSAGES[FAMILY_REFUSAL]),
            min_size=1, max_size=10,
        ),
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    def test_mixed_worlds_name_storage_separately_and_agree_on_attribution(
        self,
        storage_files: list[dict[str, object]],
        peer_files: list[dict[str, object]],
        username: str | None,
    ) -> None:
        """Review F6: ONE attribution rule across verdict and summary.

        The verdict dropped its peer phrase as soon as any storage
        evidence appeared while the summary re-attached the peer unless
        EVERY group was storage, so the two layers disagreed about the
        same row.
        """
        files = storage_files + peer_files
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message="all files errored",
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(files),
        ))
        verdict = presentation.verdict or ""
        self.assertIn("local storage error", verdict)
        self.assertIn("rejected by the peer", verdict)
        self.assertFalse(presentation.peer_attributable)
        classified = classify_log_entry(LogEntry(
            id=1, request_id=2, outcome="timeout",
            error_message="all files errored",
            soulseek_username=username,
            transfer_detail=files,
        ))
        peers = [str(row["username"]) for row in files]
        if username:
            peers.append(username)
        for named in peers:
            if named:
                self.assertNotIn(named, verdict)
                self.assertNotIn(named, classified.summary)

    @given(
        storage_files=_files(
            st.sampled_from(_FAMILY_MESSAGES[FAMILY_LOCAL_STORAGE]),
            min_size=1, max_size=20,
        ),
        other_files=_files(_ANY_MESSAGE, min_size=0, max_size=4),
        error_message=_OWN_MESSAGES,
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    @example(
        storage_files=[
            _transfer_file(
                "QXZJK",
                "Failed to create file 05 Seventeen.flac: Stale file handle : "
                "'/mnt/virtio/music/slskd/incomplete/x'",
                0,
                "Completed, Errored",
            ),
        ],
        other_files=[],
        error_message=(
            "file exceeded retry limit after 3 retries: "
            "d:\\music\\x\\05 - Track.flac"
        ),
        username="QXZJK",
    )
    def test_no_cratedigger_decision_headline_hides_a_storage_cause(
        self,
        storage_files: list[dict[str, object]],
        other_files: list[dict[str, object]],
        error_message: str | None,
        username: str | None,
    ) -> None:
        """I6 — whichever of OUR messages leads, the cause still shows."""
        files = storage_files + other_files
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message=error_message,
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(files),
        ))
        peers = [str(row["username"]) for row in files]
        if username:
            peers.append(username)
        violation = check_storage_cause_is_never_suppressed(
            files, peers, presentation)
        self.assertIsNone(violation, violation)


class TestFamilyClausesHoldForEveryMember(unittest.TestCase):
    """B1/F1 — every string that speaks for a family, over every member."""

    def test_every_breakdown_label_is_true_of_its_whole_family(self) -> None:
        """The table itself: a label is a family claim in adjective form.

        This is the half that was structurally invisible — single-family
        worlds never reach ``_family_breakdown``, so the retracted "could
        not read" claim survived here as "unreadable on the peer" through
        a whole review round.
        """
        for family, labels in _BREAKDOWN_LABELS.items():
            for label in labels:
                violation = check_family_claim(
                    family, label, where="breakdown label", bytes_moved=True)
                self.assertIsNone(violation, violation)

    @given(
        family=st.sampled_from(tuple(_FAMILY_MESSAGES)),
        data=st.data(),
        count=st.integers(min_value=1, max_value=12),
        transferred=st.integers(min_value=0, max_value=10**6),
        peer=_PEER_NAMES,
    )
    @example(
        family=FAMILY_PEER_FILE, data=None, count=12, transferred=0,
        peer="QXZJK",
    )
    def test_no_single_family_clause_claims_what_a_member_contradicts(
        self,
        family: PeerFailureFamily,
        data: st.DataObject | None,
        count: int,
        transferred: int,
        peer: str,
    ) -> None:
        messages = _FAMILY_MESSAGES[family]
        chosen = (
            messages if data is None else (data.draw(st.sampled_from(messages)),)
        )
        for message in chosen:
            presentation = present_failure(FailureEvidence(
                outcome="timeout",
                error_message="all files errored",
                transfer_detail=decode_transfer_detail(
                    _same_message_world(peer, message, count, transferred)),
            ))
            violation = check_family_claim(
                family,
                presentation.verdict or "",
                where="single-family clause",
                bytes_moved=transferred > 0,
            )
            self.assertIsNone(violation, violation)

    @given(
        first=st.sampled_from(tuple(_FAMILY_MESSAGES)),
        second=st.sampled_from(tuple(_FAMILY_MESSAGES)),
        data=st.data(),
        counts=st.tuples(
            st.integers(min_value=1, max_value=8),
            st.integers(min_value=1, max_value=8),
        ),
        transferred=st.integers(min_value=0, max_value=10**6),
        peer=_PEER_NAMES,
    )
    # Live row 37265: ten size mismatches plus one read error, rendered
    # through the breakdown as "10 unreadable on the peer".
    @example(
        first=FAMILY_PEER_FILE, second=FAMILY_TRANSPORT, data=None,
        counts=(10, 1), transferred=0, peer="wheeliewhee",
    )
    def test_no_breakdown_claims_what_a_member_contradicts(
        self,
        first: PeerFailureFamily,
        second: PeerFailureFamily,
        data: st.DataObject | None,
        counts: tuple[int, int],
        transferred: int,
        peer: str,
    ) -> None:
        """Multi-family worlds, so the breakdown actually renders."""
        assume(first != second)
        pick = (
            (lambda fam: _FAMILY_MESSAGES[fam][0])
            if data is None
            else (lambda fam: data.draw(st.sampled_from(_FAMILY_MESSAGES[fam])))
        )
        files: list[dict[str, object]] = []
        for family, count in ((first, counts[0]), (second, counts[1])):
            files.extend(
                _same_message_world(peer, pick(family), count, transferred))
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message="all files errored",
            transfer_detail=decode_transfer_detail(files),
        ))
        verdict = presentation.verdict or ""
        for family in (first, second):
            label = _BREAKDOWN_LABELS[family][0]
            self.assertIn(
                label, verdict,
                f"{family} did not reach the breakdown: {verdict!r}",
            )
            violation = check_family_claim(
                family, label, where="rendered breakdown",
                bytes_moved=transferred > 0,
            )
            self.assertIsNone(violation, violation)


class TestReasonPartition(unittest.TestCase):
    """I3 — driven off the producer's vocabulary, not the presenter's table."""

    VOCABULARIES = (
        ("source file", _SOURCE_FILE_VOCABULARY),
        ("private tree", _PRIVATE_TREE_VOCABULARY),
        ("shared root", _SHARED_ROOT_VOCABULARY),
    )

    @given(errno=st.sampled_from((
        "ESTALE", "EIO", "EACCES", "ENOTDIR", "EPERM", "UNKNOWN",
    )))
    def test_every_vocabulary_slot_keeps_its_meaning(self, errno: str) -> None:
        for name, vocabulary in self.VOCABULARIES:
            self._check_vocabulary(name, vocabulary, errno)

    def _check_vocabulary(
        self, name: str, vocabulary: _ReasonVocabulary, errno: str,
    ) -> None:
        slots = (
            ("unsafe", vocabulary.unsafe),
            ("missing", vocabulary.missing),
            ("open_failed", vocabulary.open_failed_prefix + errno),
            ("unclassified", vocabulary.unclassified),
        )
        for kind, reason in slots:
            copy = materialize_reason_copy(reason)
            violation = check_reason_partition(kind, copy)
            self.assertIsNone(violation, f"{name}/{kind}: {violation}")
            if kind == "open_failed":
                self.assertIn(errno, copy or "", f"{name}/{kind}")


class TestUnknownTextStaysBounded(unittest.TestCase):
    """I4."""

    @given(
        files=_files(_UNKNOWN_MESSAGES, min_size=1),
        error_message=_OWN_MESSAGES,
    )
    @example(
        files=[_transfer_file("QQQQQ", "x" * 500 + "\n\r\t", 0,
                              "Completed, Errored")],
        error_message=None,
    )
    def test_arbitrary_peer_text_is_sanitized_and_truncated(
        self, files: list[dict[str, object]], error_message: str | None,
    ) -> None:
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message=error_message,
            transfer_detail=decode_transfer_detail(files),
        ))
        violation = check_rendered_text_is_bounded(presentation)
        self.assertIsNone(violation, violation)

    @given(
        attempts=st.integers(min_value=1, max_value=9),
        segments=st.lists(
            st.text(alphabet="abcdefg 0123456789-.", min_size=1, max_size=12),
            min_size=1, max_size=5,
        ),
        separator=st.sampled_from(("\\", "/")),
        tail=st.one_of(
            st.just(""),
            st.builds(
                lambda count, reason: f" — {count}× '{reason}'",
                st.integers(min_value=1, max_value=60),
                st.sampled_from(_FAMILY_MESSAGES[FAMILY_TRANSPORT]
                                + _FAMILY_MESSAGES[FAMILY_LOCAL_STORAGE]),
            ),
        ),
    )
    @example(
        attempts=5,
        segments=["@@jbkaj", "musique", "10 - track.flac"],
        separator="\\",
        tail=" — 3× 'Download reported as failed by remote client'",
    )
    # Shrunk from the fuzz tier: a path of nothing but separators and
    # whitespace made the basename empty, and the old fallback answered
    # with the raw path — putting the separator straight back in.
    @example(attempts=1, segments=[" ", " "], separator="\\", tail="")
    def test_give_up_copy_never_leaks_a_path_or_the_evidence_tail(
        self,
        attempts: int,
        segments: list[str],
        separator: str,
        tail: str,
    ) -> None:
        message = (
            f"file exceeded retry limit after {attempts} retries: "
            f"{separator.join(segments)}{tail}"
        )
        presentation = present_failure(FailureEvidence(
            outcome="timeout", error_message=message))
        violation = check_gave_up_names_only_a_filename(presentation.verdict)
        self.assertIsNone(violation, violation)

    @given(diagnostic=st.text(
        alphabet=st.characters(blacklist_characters=":"),
        min_size=1, max_size=200,
    ))
    def test_unrecognised_measurement_text_is_never_rewritten(
        self, diagnostic: str,
    ) -> None:
        assume(bounded_text(diagnostic))
        assume(diagnostic.strip().casefold() not in _KNOWN_MEASUREMENT_TRIGGERS)
        presentation = present_failure(FailureEvidence(
            outcome="measurement_failed", error_message=diagnostic))
        violation = check_unrecognised_text_is_passed_through(
            diagnostic, presentation)
        self.assertIsNone(violation, violation)

    @given(
        states=st.lists(
            st.sampled_from(_TERMINAL_ERROR_STATES), min_size=1, max_size=20),
        peers=_PEER_NAMES,
    )
    def test_state_only_worlds_are_never_labelled_peer_speech(
        self, states: list[str], peers: str,
    ) -> None:
        files = [
            {
                "username": peers,
                "filename": f"{index}.flac",
                "last_state": state,
                "last_exception": None,
                "bytes_transferred": 0,
                "retry_count": 0,
            }
            for index, state in enumerate(states)
        ]
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message="all files errored",
            transfer_detail=decode_transfer_detail(files),
        ))
        violation = check_state_tokens_are_not_peer_speech(presentation)
        self.assertIsNone(violation, violation)

    @given(message=st.text(max_size=500))
    def test_family_classification_is_total(self, message: str) -> None:
        self.assertIn(
            peer_failure_family(message),
            {"refusal", "transport", "peer_file", "local_storage", "unknown"},
        )

    @given(
        family=st.sampled_from(tuple(_FAMILY_MESSAGES)),
        data=st.data(),
    )
    def test_census_messages_keep_their_declared_family(
        self, family: PeerFailureFamily, data: st.DataObject,
    ) -> None:
        message = data.draw(st.sampled_from(_FAMILY_MESSAGES[family]))
        self.assertEqual(peer_failure_family(message), family)


class TestPresentationIsPure(unittest.TestCase):
    """I5."""

    @given(
        files=_files(_ANY_MESSAGE),
        error_message=_OWN_MESSAGES,
        outcome=_OUTCOMES,
        username=st.one_of(st.none(), _PEER_NAMES),
    )
    def test_same_evidence_renders_the_same_copy(
        self,
        files: list[dict[str, object]],
        error_message: str | None,
        outcome: str,
        username: str | None,
    ) -> None:
        evidence = FailureEvidence(
            outcome=outcome,
            error_message=error_message,
            soulseek_username=username,
            transfer_detail=decode_transfer_detail(files),
        )
        self.assertEqual(present_failure(evidence), present_failure(evidence))

    @given(
        files=_files(_ANY_MESSAGE),
        error_message=_OWN_MESSAGES,
        outcome=_OUTCOMES,
    )
    def test_classifying_a_row_does_not_change_the_row(
        self,
        files: list[dict[str, object]],
        error_message: str | None,
        outcome: str,
    ) -> None:
        entry = LogEntry(
            id=1,
            request_id=2,
            outcome=outcome,
            error_message=error_message,
            soulseek_username="QQQQQ",
            transfer_detail=files,
        )
        before = entry.to_json_dict()
        classify_log_entry(entry)
        self.assertEqual(entry.to_json_dict(), before)


# ---------------------------------------------------------------------------
# Known-bad self-tests — a checker that cannot fail proves nothing
# ---------------------------------------------------------------------------

class TestInvariantCheckersTripOnViolations(unittest.TestCase):

    def test_evidence_checker_trips_when_the_raw_text_is_dropped(self) -> None:
        files = [
            _transfer_file("QQQQQ", "Verification required", 0,
                           "Completed, Rejected"),
        ]
        self.assertIsNotNone(check_evidence_survives_rendering(
            files, FailurePresentation(verdict="Peer QQQQQ rejected 1 file"),
        ))

    def test_evidence_checker_trips_when_a_different_message_is_shown(self) -> None:
        files = [
            _transfer_file("QQQQQ", "Verification required", 0,
                           "Completed, Rejected"),
        ]
        self.assertIsNotNone(check_evidence_survives_rendering(
            files,
            FailurePresentation(
                verdict="something", transfer_message='1× "File read error."'),
        ))

    def test_evidence_checker_passes_the_honest_rendering(self) -> None:
        files = [
            _transfer_file("QQQQQ", "Verification required", 0,
                           "Completed, Rejected"),
        ]
        self.assertIsNone(check_evidence_survives_rendering(
            files,
            FailurePresentation(
                verdict="…", transfer_message='1× "Verification required"'),
        ))

    def test_family_claim_checker_trips_on_both_defects_that_shipped(self) -> None:
        # The clause form (B1) …
        self.assertIsNotNone(check_family_claim(
            FAMILY_PEER_FILE,
            'Peer QXZJK could not read 12 of its own files — "…"',
            where="clause",
        ))
        # … and the adjective form that survived it in the label table (F1).
        self.assertIsNotNone(check_family_claim(
            FAMILY_PEER_FILE, "unreadable on the peer", where="label"))
        self.assertIsNotNone(check_family_claim(
            FAMILY_UNKNOWN, "failed without a reason", where="label"))
        self.assertIsNotNone(check_family_claim(
            FAMILY_REFUSAL, "rejected before transfer", where="label",
            bytes_moved=True))
        self.assertIsNone(check_family_claim(
            FAMILY_PEER_FILE, "not delivered by the peer", where="label",
            bytes_moved=True))
        self.assertIsNone(check_family_claim(
            FAMILY_PEER_FILE,
            'Peer QXZJK could not deliver 12 of the files it was sharing — "…"',
            where="clause",
        ))

    def test_local_storage_checker_trips_on_the_summary_suffix(self) -> None:
        self.assertIsNotNone(check_local_storage_not_peer_attributed(
            ["QXZJK"],
            FailurePresentation(
                verdict="Local storage error writing 1 file",
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STORAGE,
            ),
            "Local storage error writing 1 file · QXZJK",
        ))
        self.assertIsNone(check_local_storage_not_peer_attributed(
            ["QXZJK"],
            FailurePresentation(
                verdict="Local storage error writing 1 file",
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STORAGE,
            ),
            "Local storage error writing 1 file",
        ))

    def test_local_storage_checker_trips_on_peer_vocabulary(self) -> None:
        self.assertIsNotNone(check_local_storage_not_peer_attributed(
            ["QQQQQ"],
            FailurePresentation(
                verdict='Peer QQQQQ failed all 3 files — "Failed to create file"',
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STORAGE,
            ),
        ))

    def test_local_storage_checker_trips_on_a_peer_label(self) -> None:
        self.assertIsNotNone(check_local_storage_not_peer_attributed(
            [],
            FailurePresentation(
                verdict="Local storage error writing 3 files",
                transfer_message_label="Peer message",
            ),
        ))

    def test_storage_suppression_checker_trips_on_the_live_defect(self) -> None:
        files = [
            _transfer_file(
                "QXZJK",
                "Failed to create file 05 Seventeen.flac: Stale file handle",
                0,
                "Completed, Errored",
            ),
        ]
        # The exact shape live row 38203 rendered before I6.
        self.assertIsNotNone(check_storage_cause_is_never_suppressed(
            files,
            ["QXZJK"],
            FailurePresentation(
                verdict='Gave up on "05 Seventeen.flac" after 5 failed attempts',
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STORAGE,
            ),
        ))
        # Naming storage but blaming a peer for it is still a violation.
        self.assertIsNotNone(check_storage_cause_is_never_suppressed(
            files,
            ["QXZJK"],
            FailurePresentation(
                verdict="Gave up — local storage error from peer QXZJK"),
        ))
        self.assertIsNone(check_storage_cause_is_never_suppressed(
            files,
            ["QXZJK"],
            FailurePresentation(
                verdict='Gave up on "05 Seventeen.flac" after 5 failed '
                        "attempts — local storage error writing 1 file"),
        ))
        # A world with no storage evidence is not this checker's business.
        self.assertIsNone(check_storage_cause_is_never_suppressed(
            [_transfer_file("QXZJK", "Verification required", 0,
                            "Completed, Rejected")],
            ["QXZJK"],
            FailurePresentation(verdict="Gave up after 5 failed attempts"),
        ))

    def test_reason_partition_checker_trips_on_a_fused_sentence(self) -> None:
        self.assertIsNotNone(check_reason_partition(
            "open_failed",
            "Refused to import: our processing storage could not be opened",
        ))
        self.assertIsNotNone(check_reason_partition(
            "unsafe", "Our processing storage failed; requeued",
        ))
        self.assertIsNotNone(check_reason_partition(
            "missing",
            "Refused to import: a downloaded path escaped the directory",
        ))
        self.assertIsNotNone(check_reason_partition("unsafe", None))

    def test_give_up_checker_trips_on_a_leaked_path_or_tail(self) -> None:
        self.assertIsNotNone(check_gave_up_names_only_a_filename(
            'Gave up on "d:\\music\\x.flac" after 3 failed attempts'))
        self.assertIsNotNone(check_gave_up_names_only_a_filename(
            'Gave up on "x.flac — 3× \'boom\'" after 3 failed attempts'))
        self.assertIsNone(check_gave_up_names_only_a_filename(
            'Gave up on "x.flac" after 3 failed attempts'))
        self.assertIsNone(check_gave_up_names_only_a_filename(
            "Transfer stalled — no progress for 10 minutes"))

    def test_passthrough_checker_trips_on_a_rewritten_diagnostic(self) -> None:
        self.assertIsNotNone(check_unrecognised_text_is_passed_through(
            "path is outside configured quarantine roots",
            FailurePresentation(
                verdict="Measurement failed: installed path is outside the "
                        "library root"),
        ))
        self.assertIsNone(check_unrecognised_text_is_passed_through(
            "path is outside configured quarantine roots",
            FailurePresentation(
                verdict="Measurement failed: path is outside configured "
                        "quarantine roots"),
        ))

    def test_state_checker_trips_on_a_peer_label(self) -> None:
        self.assertIsNotNone(check_state_tokens_are_not_peer_speech(
            FailurePresentation(
                verdict='Peer QQQQQ failed 1 file — "Completed, Errored"',
                transfer_message='1× "Completed, Errored"',
                transfer_message_label=TRANSFER_MESSAGE_LABEL_PEER),
        ))
        self.assertIsNotNone(check_state_tokens_are_not_peer_speech(
            FailurePresentation(
                verdict='Peer QQQQQ failed 1 file — "Completed, Errored"',
                transfer_message='1× "Completed, Errored"',
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STATE),
        ))
        self.assertIsNone(check_state_tokens_are_not_peer_speech(
            FailurePresentation(
                verdict='Peer QQQQQ failed 1 file — slskd state '
                        '"Completed, Errored"',
                transfer_message='1× "Completed, Errored"',
                transfer_message_label=TRANSFER_MESSAGE_LABEL_STATE),
        ))

    def test_bounded_checker_trips_on_control_characters(self) -> None:
        self.assertIsNotNone(check_rendered_text_is_bounded(
            FailurePresentation(verdict="peer said\nmore"),
        ))

    def test_bounded_checker_trips_on_an_unbounded_message(self) -> None:
        self.assertIsNotNone(check_rendered_text_is_bounded(
            FailurePresentation(transfer_message="x" * 10000),
        ))

    def test_bounded_checker_trips_on_an_unbounded_verdict(self) -> None:
        self.assertIsNotNone(check_rendered_text_is_bounded(
            FailurePresentation(verdict="x" * 10000),
        ))


if __name__ == "__main__":
    unittest.main()
