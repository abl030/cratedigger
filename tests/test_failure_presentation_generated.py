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
from hypothesis import example, given, strategies as st

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
    MAX_RAW_MESSAGE_CHARS,
    MAX_RAW_MESSAGE_GROUPS,
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

_KNOWN_MESSAGES = st.sampled_from(
    tuple(message for pool in _FAMILY_MESSAGES.values() for message in pool)
)
_UNKNOWN_MESSAGES = st.text(min_size=1, max_size=300)
_ANY_MESSAGE = st.one_of(_KNOWN_MESSAGES, _UNKNOWN_MESSAGES, st.none())


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


def check_local_storage_not_peer_attributed(
    peers: Sequence[str],
    presentation: FailurePresentation,
) -> str | None:
    """I2 — our own storage failing is never rendered as peer behaviour."""
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
        violation = check_local_storage_not_peer_attributed(peers, presentation)
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
    )
    def test_mixed_worlds_still_name_the_storage_share_separately(
        self,
        storage_files: list[dict[str, object]],
        peer_files: list[dict[str, object]],
    ) -> None:
        presentation = present_failure(FailureEvidence(
            outcome="timeout",
            error_message="all files errored",
            transfer_detail=decode_transfer_detail(storage_files + peer_files),
        ))
        verdict = presentation.verdict or ""
        self.assertIn("local storage error", verdict)
        self.assertIn("rejected before transfer", verdict)


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

    def test_bounded_checker_trips_on_control_characters(self) -> None:
        self.assertIsNotNone(check_rendered_text_is_bounded(
            FailurePresentation(verdict="peer said\nmore"),
        ))

    def test_bounded_checker_trips_on_an_unbounded_message(self) -> None:
        self.assertIsNotNone(check_rendered_text_is_bounded(
            FailurePresentation(transfer_message="x" * 10000),
        ))


if __name__ == "__main__":
    unittest.main()
