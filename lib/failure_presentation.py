"""Operator copy for download/import failures, derived at render time.

Issue #868 PR2. Nothing here writes anything: ``download_log.error_message``
and ``download_log.transfer_detail`` stay exactly as the pipeline recorded
them, and this module turns that persisted evidence into the one sentence
an operator reads. Rendering (rather than writing) humane copy is what
makes the 513 timeout rows that already carry ``transfer_detail`` legible
the moment this deploys, and what makes "humanized copy never discards or
mutates the raw peer evidence" structural instead of test-enforced.

Invariants this module owns (each pinned deterministically AND patrolled by
a generated property in ``tests/test_failure_presentation*.py``):

I1. **Raw evidence survives.** Presenting a failure never mutates its
    inputs, and whenever per-file peer text exists the presentation carries
    a bounded, deduplicated copy of that text (``transfer_message``) — the
    verdict is an interpretation, never the only place the raw string
    appears.
I2. **Local storage is never blamed on a peer.** slskd failing to write to
    our own share is our fault; a presentation whose whole evidence set is
    local-storage failures names no peer and uses no peer vocabulary.
I3. **Storage and containment stay partitioned.** A materialize reason that
    names a storage errno never renders as a security finding, and a
    containment refusal never renders as ordinary storage trouble. (PR1
    spent two review rounds establishing that partition in the producer;
    the copy layer must not re-fuse it.)
I4. **Unknown text stays raw, bounded and attributed.** Text this module
    does not recognise is quoted as peer-supplied evidence, sanitized of
    control characters and truncated — never re-voiced as a Cratedigger
    diagnosis.
I5. **Presentation is a pure function of the evidence.** Same evidence,
    same copy; no clocks, no I/O, no config.
I6. **A Cratedigger decision never suppresses the cause.** Our own
    headline (we stopped retrying; the peer never started) is CONTEXT.
    Whenever per-file evidence exists, the dominant family is named in the
    verdict too — otherwise a fluent sentence derived from a suppressed
    discriminator lies better than the raw one it replaced.

The peer-failure family taxonomy is a census of what Soulseek peers
actually send (issue #868, 45-day live census): refusals before transfer,
transport/connection failures, peer-side file problems, and — the one that
is not about the peer at all — slskd failing to write to our storage.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Final, Literal

import msgspec

from lib.download_materialization import (
    REASON_EVENT_PATH_GONE_FROM_DISK,
    REASON_EVENT_PATH_NEVER_STAMPED,
    REASON_MATERIALIZE_AUTHORITY_FAILED,
    REASON_PRIVATE_MATERIALIZE_FAILED,
    REASON_PROCESSING_AUTHORITY_UNSAFE,
    REASON_PROCESSING_OPEN_FAILED_PREFIX,
    REASON_PROCESSING_PATH_MISSING,
    REASON_PROCESSING_READ_FAILED_PREFIX,
    REASON_PROCESSING_WRITE_FAILED_PREFIX,
    REASON_SLSKD_ROOT_MISSING,
    REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
    REASON_SLSKD_ROOT_READ_FAILED_PREFIX,
    REASON_SLSKD_ROOT_REFUSED,
    REASON_SLSKD_ROOT_UNSAFE,
    REASON_SLSKD_ROOT_WRITE_FAILED_PREFIX,
    REASON_SOURCE_OPEN_FAILED_PREFIX,
    REASON_SOURCE_PREFLIGHT_REFUSED,
    REASON_SOURCE_READ_FAILED_PREFIX,
    REASON_SOURCE_WRITE_FAILED_PREFIX,
    REASON_UNSAFE_SOURCE_PATH,
)
from lib.json_narrow import is_list_like, json_list
from lib.quality import FileFailureDetail

# --------------------------------------------------------------------------
# Bounds
# --------------------------------------------------------------------------

MAX_RAW_MESSAGE_CHARS: Final = 120
"""Longest single peer message rendered anywhere (I4).

Peers send absolute paths (``Download of d:\\new music\\...\\track.flac
reported as failed by X``); the audit row keeps every byte, the card gets a
readable prefix.
"""

MAX_RAW_MESSAGE_GROUPS: Final = 3
"""Distinct peer messages listed in ``transfer_message`` before ``+N more``."""

MAX_PEER_NAME_CHARS: Final = 40
"""Longest peer name rendered inside a sentence."""

MAX_DIAGNOSTIC_CHARS: Final = 200
"""Longest OUR-OWN diagnostic quoted inside a verdict.

Wider than :data:`MAX_RAW_MESSAGE_CHARS` because these are our own
components explaining themselves — a beets exception, a post-import
inconsistency, a measurement failure — where the useful part is often past
where a peer's chatter would have been cut. Still a bound: the verdict is
the collapsed list row, and one long traceback must not become the
operator's whole worklist line. Shared with ``web/classify.py``, which
quotes the same producer text on rejection rows."""

TRANSFER_MESSAGE_LABEL_PEER: Final = "Peer message"
TRANSFER_MESSAGE_LABEL_STORAGE: Final = "Storage error"
TRANSFER_MESSAGE_LABEL_MIXED: Final = "Transfer messages"
TRANSFER_MESSAGE_LABEL_STATE: Final = "Transfer state"

DETAIL_LABEL_REASON_CODE: Final = "Reason code"
"""Forensics-row label for a ``beets_detail`` holding a machine reason.

PR1 started persisting the materialize reason in ``beets_detail``, which
the card already renders as a forensics ``Detail`` row — so those rows now
show ``source_open_failed_ESTALE`` under a label that says "beets detail".
The token belongs there (forensics is where internals go, and the verdict
carries the sentence), but it should say what it is."""

NON_AUTOMATION_IMPORT_FAILURE_PREFIXES: Final[frozenset[str]] = frozenset({
    "Force import attempt failed:",
    "YouTube import attempt failed:",
    "Local import attempt failed:",
})
"""Producer-owned prefixes for failed non-owning import attempts."""

UNLINKED_SOURCE_PROVENANCE_SUFFIX: Final = (
    " Source provenance link was unavailable or refused; terminal audit is unlinked."
)
"""Visible qualifier for a terminal audit whose requested origin was invalid."""


# --------------------------------------------------------------------------
# Peer-failure families
# --------------------------------------------------------------------------

PeerFailureFamily = Literal[
    "refusal", "transport", "peer_file", "local_storage", "unknown"
]

FAMILY_REFUSAL: Final[PeerFailureFamily] = "refusal"
FAMILY_TRANSPORT: Final[PeerFailureFamily] = "transport"
FAMILY_PEER_FILE: Final[PeerFailureFamily] = "peer_file"
FAMILY_LOCAL_STORAGE: Final[PeerFailureFamily] = "local_storage"
FAMILY_UNKNOWN: Final[PeerFailureFamily] = "unknown"

# Ordered, case-insensitive prefix rules over the message slskd recorded —
# no regex archaeology, no per-message sentences. Order is the tiebreak:
# local storage is tested FIRST because misfiling it as peer behaviour is
# the one error that changes what the operator does (I2). Every prefix here
# comes from the live 45-day census in issue #868; anything else is
# deliberately ``unknown`` and gets quoted verbatim rather than guessed at.
_FAMILY_PREFIXES: Final[tuple[tuple[str, PeerFailureFamily], ...]] = (
    # --- 4. Local storage: slskd could not write to OUR share ---
    ("failed to create file", FAMILY_LOCAL_STORAGE),
    ("could not find a part of the path", FAMILY_LOCAL_STORAGE),
    # --- 1. Refusal before transfer ---
    ("transfer rejected:", FAMILY_REFUSAL),
    ("verification required", FAMILY_REFUSAL),
    ("too many files", FAMILY_REFUSAL),
    ("too many megabytes", FAMILY_REFUSAL),
    ("pending shutdown.", FAMILY_REFUSAL),
    ("completed, rejected", FAMILY_REFUSAL),
    # --- 3. Peer-side file problem ---
    ("file read error.", FAMILY_PEER_FILE),
    ("transfer aborted: the remote size of", FAMILY_PEER_FILE),
    # --- 2. Transport / connection ---
    ("a task was canceled.", FAMILY_TRANSPORT),
    ("an attempt was made to transition a task", FAMILY_TRANSPORT),
    ("application shut down", FAMILY_TRANSPORT),
    ("completed, cancelled", FAMILY_TRANSPORT),
    ("completed, canceled", FAMILY_TRANSPORT),
    ("completed, timedout", FAMILY_TRANSPORT),
    ("download failed to enqueue remotely", FAMILY_TRANSPORT),
    ("download of ", FAMILY_TRANSPORT),
    ("download reported as failed by remote client", FAMILY_TRANSPORT),
    ("enqueue failed:", FAMILY_TRANSPORT),
    ("failed to establish a direct or indirect transfer connection",
     FAMILY_TRANSPORT),
    ("failed to read ", FAMILY_TRANSPORT),
    ("failed to write ", FAMILY_TRANSPORT),
    ("inactivity timeout of ", FAMILY_TRANSPORT),
    ("the operation was canceled.", FAMILY_TRANSPORT),
    ("the wait timed out after ", FAMILY_TRANSPORT),
    ("transfer failed:", FAMILY_TRANSPORT),
)


def peer_failure_family(message: str | None) -> PeerFailureFamily:
    """Classify one slskd per-file failure message into its family.

    Matching is a first-hit scan of :data:`_FAMILY_PREFIXES` over the
    stripped, case-folded message. Unrecognised text is ``unknown`` — the
    caller then quotes it verbatim rather than inventing a diagnosis (I4).
    """
    if not message:
        return FAMILY_UNKNOWN
    probe = message.strip().casefold()
    for prefix, family in _FAMILY_PREFIXES:
        if probe.startswith(prefix):
            return family
    return FAMILY_UNKNOWN


# These are family claims in adjective form and answer to exactly the same
# rule as the clauses above: true of EVERY member, or it does not ship.
#
# ``unreadable on the peer`` was the retracted "could not read" claim
# surviving here verbatim (issue #868 review F1) — live row 37265 rendered
# "10 unreadable on the peer" for ten size mismatches the peer had read
# perfectly well. ``failed without a reason`` was false the same way: the
# row HAS a reason, quoted immediately beside it, that we merely failed to
# classify (F2). And ``before transfer`` is licensed by zero bytes, which a
# breakdown group cannot promise.
_FAMILY_BREAKDOWN_LABELS: Final[dict[PeerFailureFamily, tuple[str, str]]] = {
    FAMILY_REFUSAL: ("rejected by the peer", "rejected by the peer"),
    FAMILY_TRANSPORT: ("connection lost", "connection lost"),
    FAMILY_PEER_FILE: ("not delivered by the peer", "not delivered by the peer"),
    FAMILY_LOCAL_STORAGE: ("local storage error", "local storage errors"),
    FAMILY_UNKNOWN: (
        "with an unrecognised reason", "with an unrecognised reason"),
}


# --------------------------------------------------------------------------
# Typed inputs / outputs
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class FailureEvidence:
    """Everything persisted about ONE failed ``download_log`` row.

    Constructed from the row the web classifier or ``pipeline-cli show``
    already holds. Deliberately inert data: presenting it must not need a
    database, a clock, or config (I5).
    """

    outcome: str
    error_message: str | None = None
    beets_detail: str | None = None
    beets_scenario: str | None = None
    soulseek_username: str | None = None
    transfer_detail: tuple[FileFailureDetail, ...] = ()
    transfer_detail_unreadable: bool = False
    """A non-empty audit blob yielded no usable record."""

    @classmethod
    def from_row(cls, row: Mapping[str, object]) -> FailureEvidence:
        """Build from a raw ``download_log`` row mapping (CLI side)."""
        raw_detail = row.get("transfer_detail")
        return cls(
            outcome=_row_str(row, "outcome") or "",
            error_message=_row_str(row, "error_message"),
            beets_detail=_row_str(row, "beets_detail"),
            beets_scenario=_row_str(row, "beets_scenario"),
            soulseek_username=_row_str(row, "soulseek_username"),
            transfer_detail=decode_transfer_detail(raw_detail),
            transfer_detail_unreadable=transfer_detail_unreadable(raw_detail),
        )


@dataclass(frozen=True)
class FailurePresentation:
    """The operator-facing projection of one failure row.

    ``verdict`` is ``None`` when this module has no message-level opinion
    (for example an import failure whose only evidence lives in the
    ``import_result`` JSONB) — the caller keeps its own fallback rather
    than being handed an invented sentence.
    """

    verdict: str | None = None
    transfer_message: str | None = None
    transfer_message_label: str | None = None
    beets_detail_label: str | None = None
    """Label for the row's raw ``beets_detail`` forensics line, when that
    column holds a machine reason code rather than beets prose."""
    peer_attributable: bool = True
    """False when the failure was OURS. The list-row summary appends the
    row's peer as a trailing attribution; on a local-storage verdict that
    put the peer's name back on our own fault in the one line the operator
    reads (issue #868 review #12)."""


def decode_transfer_detail(raw: object) -> tuple[FileFailureDetail, ...]:
    """Decode the ``transfer_detail`` JSONB array into typed records.

    The single wire-boundary decode for this column, PER RECORD: one
    malformed historical element no longer discards its 28 healthy
    siblings. A card degrades rather than 500ing — the same
    display-boundary fail-open the classifier applies to ``import_result``
    / ``validation_result``.
    """
    if not is_list_like(raw):
        return ()
    decoded: list[FileFailureDetail] = []
    for element in json_list(raw):
        try:
            decoded.append(
                msgspec.convert(element, type=FileFailureDetail, strict=True))
        except (msgspec.ValidationError, TypeError):
            continue
    return tuple(decoded)


def transfer_detail_unreadable(raw: object) -> bool:
    """Did a non-empty audit blob fail to yield ANY usable record?

    A fail-open must not be laundered into a claim: without this, a row
    whose evidence could not be decoded still asserted "slskd reported no
    reason" (issue #868 review #6).
    """
    if not is_list_like(raw):
        return raw is not None
    return bool(json_list(raw)) and not decode_transfer_detail(raw)


# --------------------------------------------------------------------------
# Text bounding (I4)
# --------------------------------------------------------------------------

def bounded_text(text: str, *, limit: int = MAX_RAW_MESSAGE_CHARS) -> str:
    """Collapse whitespace, drop unprintables, truncate with an ellipsis."""
    collapsed = _collapsed_text(text)
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(limit - 1, 0)].rstrip() + "\u2026"


def _collapsed_text(text: str) -> str:
    """Remove controls and collapse whitespace without applying a bound."""
    cleaned = "".join(ch if ch.isprintable() else " " for ch in text)
    return " ".join(cleaned.split())


def non_automation_import_failure_message(
    job_type: str,
    diagnostic: str,
    fallback_diagnostic: str = "",
) -> str:
    """Build one bounded force/YouTube Recents failure message.

    Control-only primary diagnostics cannot hide a useful fallback.  The
    bound applies to the complete persisted sentence, including its producer
    identity prefix, so the presenter can reproduce it exactly.
    """
    prefix = {
        "force_import": "Force import attempt failed:",
        "youtube_import": "YouTube import attempt failed:",
        "local_import": "Local import attempt failed:",
    }.get(job_type)
    if prefix is None:
        raise ValueError(f"non-automation failure does not support {job_type!r}")
    detail = _collapsed_text(diagnostic) or _collapsed_text(fallback_diagnostic)
    message = prefix if not detail else f"{prefix} {detail}"
    return bounded_text(message, limit=MAX_DIAGNOSTIC_CHARS)


def unlinked_source_provenance_message(diagnostic: str | None) -> str:
    """Keep a provenance refusal visible without exceeding the row bound."""
    detail_limit = MAX_DIAGNOSTIC_CHARS - len(
        UNLINKED_SOURCE_PROVENANCE_SUFFIX,
    )
    detail = bounded_text(
        _collapsed_text(diagnostic or ""),
        limit=detail_limit,
    )
    return f"{detail}{UNLINKED_SOURCE_PROVENANCE_SUFFIX}"


def _quoted(text: str) -> str:
    return f'"{bounded_text(text)}"'


def _quoted_evidence(group: _ReasonGroup) -> str:
    """Quote one group's text, saying whose words they are."""
    if group.from_state:
        return f"slskd state {_quoted(group.message)}"
    return _quoted(group.message)


def _peer_name(name: str | None) -> str | None:
    if not name:
        return None
    bounded = bounded_text(name, limit=MAX_PEER_NAME_CHARS)
    return bounded or None


def _files(count: int) -> str:
    return "1 file" if count == 1 else f"{count} files"


def _plural(count: int, labels: tuple[str, str]) -> str:
    return labels[0] if count == 1 else labels[1]


def _row_str(row: Mapping[str, object], key: str) -> str | None:
    value = row.get(key)
    return value if isinstance(value, str) else None


# --------------------------------------------------------------------------
# Per-file evidence grouping
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class _ReasonGroup:
    """One distinct failure message and everything observed about it."""

    message: str
    family: PeerFailureFamily
    count: int
    zero_byte_count: int
    peers: tuple[str, ...] = ()
    # True when the message is slskd's own terminal state token rather
    # than something the peer said. An slskd state machine is not peer
    # speech, and rendering ``Completed, Errored`` under a "Peer message"
    # label puts words in a peer's mouth.
    from_state: bool = False


def _file_reason(detail: FileFailureDetail) -> tuple[str, bool] | None:
    """The reason one file failed and whether it is peer speech.

    Mirrors ``summarize_file_failures``: prefer slskd's per-transfer
    exception, fall back to a terminal ``Completed, *`` state that is not a
    success. Files with neither contribute no evidence at all. The second
    element is ``True`` for the state fallback — the caller must not
    present a state token as something a peer said.
    """
    if detail.last_exception:
        return detail.last_exception, False
    state = detail.last_state
    if state and state.startswith("Completed,") and state != "Completed, Succeeded":
        return state, True
    return None


def _group_reasons(
    transfer_detail: Sequence[FileFailureDetail],
    fallback_username: str | None,
) -> tuple[_ReasonGroup, ...]:
    """Group failing files by exact message, most common first.

    Ordering matches ``lib.download.summarize_file_failures`` (count
    descending, then message ascending) so the rendered story and the
    persisted summary agree about which reason dominates.
    """
    counts: dict[str, int] = {}
    zero_bytes: dict[str, int] = {}
    peers: dict[str, list[str]] = {}
    from_state: dict[str, bool] = {}
    for detail in transfer_detail:
        observed = _file_reason(detail)
        if observed is None:
            continue
        reason, state_derived = observed
        counts[reason] = counts.get(reason, 0) + 1
        from_state[reason] = state_derived
        if detail.bytes_transferred <= 0:
            zero_bytes[reason] = zero_bytes.get(reason, 0) + 1
        peer = _peer_name(detail.username) or _peer_name(fallback_username)
        bucket = peers.setdefault(reason, [])
        if peer and peer not in bucket:
            bucket.append(peer)
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return tuple(
        _ReasonGroup(
            message=message,
            family=peer_failure_family(message),
            count=count,
            zero_byte_count=zero_bytes.get(message, 0),
            peers=tuple(sorted(peers.get(message, []))),
            from_state=from_state.get(message, False),
        )
        for message, count in ordered
    )


def _all_peers(groups: Sequence[_ReasonGroup]) -> tuple[str, ...]:
    seen: list[str] = []
    for group in groups:
        for peer in group.peers:
            if peer not in seen:
                seen.append(peer)
    return tuple(seen)


def _transfer_message(groups: Sequence[_ReasonGroup]) -> str:
    """Bounded, deduplicated raw peer text — never one line per file (I1)."""
    listed = groups[:MAX_RAW_MESSAGE_GROUPS]
    rendered = "; ".join(
        f"{group.count}\u00d7 {_quoted(group.message)}" for group in listed
    )
    remaining = len(groups) - len(listed)
    if remaining > 0:
        rendered += f"; +{remaining} more"
    return rendered


def _group_kind(group: _ReasonGroup) -> str:
    """Who this evidence belongs to: our storage, slskd, or the peer."""
    if group.family == FAMILY_LOCAL_STORAGE:
        return "storage"
    if group.from_state:
        return "state"
    return "peer"


def _transfer_message_label(groups: Sequence[_ReasonGroup]) -> str:
    """Name the owner of the raw text — never "Peer message" by default.

    A local write failure is ours, and an slskd terminal state token is
    slskd's; captioning either as something a peer said is the same
    misattribution one field over.
    """
    kinds = {_group_kind(group) for group in groups}
    if kinds == {"storage"}:
        return TRANSFER_MESSAGE_LABEL_STORAGE
    if kinds == {"state"}:
        return TRANSFER_MESSAGE_LABEL_STATE
    if kinds == {"peer"}:
        return TRANSFER_MESSAGE_LABEL_PEER
    return TRANSFER_MESSAGE_LABEL_MIXED


# --------------------------------------------------------------------------
# Peer-failure copy
# --------------------------------------------------------------------------

def _peer_phrase(peers: Sequence[str]) -> str:
    """"from peer X" / "across K peers" / "" when no peer is known."""
    if len(peers) == 1:
        return f" from peer {peers[0]}"
    if len(peers) > 1:
        return f" across {len(peers)} peers"
    return ""


def _single_family_clause(
    family: PeerFailureFamily,
    *,
    count: int,
    zero_bytes: bool,
    peers: Sequence[str],
) -> str:
    """One family's generic cause clause — never a per-message sentence.

    Lower-case and quote-free, because the SAME clause has to serve as the
    head of an evidence-led verdict ("Peer X rejected all 29 files before
    transfer — ...") AND as the cause appended to a Cratedigger-decision
    verdict ("Gave up on ... — local storage error writing 1 file"). One
    vocabulary, two compositions: a cause can never be phrased one way in
    the branch that leads with it and another way in the branch that does
    not (I6).

    The clauses state only what was observed (refused, zero bytes, before
    transfer); the peer's own words live in the quote beside them and in
    ``transfer_message``. They must scale to every message in a family
    without new prose.
    """
    files = _files(count)
    peer = peers[0] if len(peers) == 1 else None
    many_peers = len(peers) > 1

    if family == FAMILY_LOCAL_STORAGE:
        # I2: our storage, our fault — this clause has no place for a peer.
        return f"local storage error writing {files}"
    if family == FAMILY_REFUSAL:
        # "before transfer" is an OBSERVATION, not a property of the
        # family: 11 of 945 live refusal files had already moved bytes.
        # Zero bytes is what licenses the claim.
        when = " before transfer" if zero_bytes else ""
        if peer:
            scope = f"all {files}" if count > 1 else files
            return f"peer {peer} rejected {scope}{when}"
        if many_peers:
            return f"{len(peers)} peers rejected {files}{when}"
        return f"{files} rejected{when}"
    if family == FAMILY_TRANSPORT:
        outcome = (
            "failed before any data arrived" if zero_bytes
            else "dropped mid-download"
        )
        if peer:
            return f"transfer from peer {peer} {outcome}"
        if many_peers:
            return f"transfers from {len(peers)} peers {outcome}"
        return f"{files} {outcome}"
    if family == FAMILY_PEER_FILE:
        # NOT "could not read": the family's other census member is
        # ``Transfer aborted: the remote size of N does not match expected
        # size N``, where the peer read its file perfectly well and slskd
        # aborted because the share index disagreed with what was offered
        # — 25 of 39 live rows carrying the old phrase (issue #868 review
        # B1). "Could not deliver" is true of both members.
        if peer:
            owned = (
                "one of the files it was sharing" if count == 1
                else f"{count} of the files it was sharing"
            )
            return f"peer {peer} could not deliver {owned}"
        if many_peers:
            return (
                f"{len(peers)} peers could not deliver {files} they were sharing"
            )
        return f"{files} could not be delivered by the sharing peer"
    if peer:
        scope = f"all {files}" if count > 1 else files
        return f"peer {peer} failed {scope}"
    if many_peers:
        return f"{files} failed across {len(peers)} peers"
    return f"{files} failed"


def _family_breakdown(groups: Sequence[_ReasonGroup]) -> str:
    """Per-family counts for an evidence set spanning more than one family."""
    per_family: dict[PeerFailureFamily, int] = {}
    for group in groups:
        per_family[group.family] = per_family.get(group.family, 0) + group.count
    ordered = sorted(per_family.items(), key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(
        f"{count} {_plural(count, _FAMILY_BREAKDOWN_LABELS[family])}"
        for family, count in ordered
    )


def _family_clause(groups: Sequence[_ReasonGroup]) -> str:
    """The cause this evidence set describes, as one lower-case clause."""
    families = {group.family for group in groups}
    if len(families) != 1:
        return _family_breakdown(groups)
    return _single_family_clause(
        groups[0].family,
        count=sum(group.count for group in groups),
        zero_bytes=all(
            group.zero_byte_count == group.count for group in groups
        ),
        # I2 is enforced by the local-storage clause itself, which has no
        # place to put a peer — not by filtering the peers here. (A guard
        # here would be unreachable defence: fault injection proved removing
        # it changes nothing, while the generated property kills any clause
        # that starts naming a peer.)
        peers=_all_peers(groups),
    )


def _capitalized(clause: str) -> str:
    return clause[:1].upper() + clause[1:]


def _peer_attributable(groups: Sequence[_ReasonGroup]) -> bool:
    """May this row be attributed to a peer at all?

    ONE rule for every layer. The verdict dropped its peer phrase as soon
    as any local-storage evidence appeared, while the summary re-attached
    the peer unless EVERY group was storage — so the two layers disagreed
    about the same row (issue #868 review F6). If any part of the failure
    was ours, no layer names a peer; the card header still shows the
    row's username, so nothing is lost.
    """
    return not any(group.family == FAMILY_LOCAL_STORAGE for group in groups)


def _mixed_family_sentence(groups: Sequence[_ReasonGroup]) -> str:
    """Two or more families: count them, name none of their messages."""
    total = sum(group.count for group in groups)
    peers = _all_peers(groups) if _peer_attributable(groups) else ()
    return (
        f"{_files(total)} failed{_peer_phrase(peers)} "
        f"\u2014 {_family_breakdown(groups)}"
    )


def _present_transfer_evidence(
    groups: Sequence[_ReasonGroup],
) -> FailurePresentation:
    families = {group.family for group in groups}
    if len(families) == 1:
        verdict = (
            f"{_capitalized(_family_clause(groups))} "
            f"\u2014 {_quoted_evidence(groups[0])}"
        )
        other_reasons = len(groups) - 1
        if other_reasons > 0:
            reasons = "reason" if other_reasons == 1 else "reasons"
            verdict += f" (+{other_reasons} other {reasons})"
    else:
        verdict = _mixed_family_sentence(groups)
    return FailurePresentation(
        verdict=verdict,
        transfer_message=_transfer_message(groups),
        transfer_message_label=_transfer_message_label(groups),
        peer_attributable=_peer_attributable(groups),
    )


# --------------------------------------------------------------------------
# Cratedigger's own download-phase messages
# --------------------------------------------------------------------------

_RETRY_LIMIT_RE: Final = re.compile(
    r"^file exceeded retry limit after (\d+) retries: (.+)$", re.IGNORECASE,
)
_STALLED_RE: Final = re.compile(
    r"^no download progress for (\d+)s\b", re.IGNORECASE,
)
_REMOTE_QUEUE_RE: Final = re.compile(
    r"^remote_queue_timeout (\d+)s exceeded$", re.IGNORECASE,
)
_ALL_ERRORED_RE: Final = re.compile(
    r"^all (\d+) files errored$", re.IGNORECASE,
)
_EVIDENCE_TAIL_RE: Final = re.compile(r" — \d+× '.*$", re.DOTALL)
"""``lib.download._enrich_timeout_reason`` appends ``— N× '<reason>'`` to
every timeout reason. Inside the retry-limit message that tail sits AFTER
the peer's file path, so parsing the path without removing it first yields
a "filename" made of half a path and someone's exception text (live rows
37535 / 38203 / 37483). The grouped reasons are rendered separately."""

def _download_message_context(message: str) -> str | None:
    """A short parenthetical for an evidence-led verdict.

    A stalled-timeout row is evidence-led, so its duration used to vanish
    entirely (issue #868 review #8). The cause still leads; the duration
    rides along.
    """
    probe = message.strip()
    stalled = _STALLED_RE.match(probe)
    if stalled is not None:
        return f"no progress for {_duration_phrase(int(stalled.group(1)))}"
    # Symmetry: #7 stopped this message leading when evidence exists, which
    # would otherwise lose the fact #8 was raised to preserve one message
    # over (issue #868 review F5).
    queued = _REMOTE_QUEUE_RE.match(probe)
    if queued is not None:
        return f"still queued after {_duration_phrase(int(queued.group(1)))}"
    return None


_VANISHED_WITH_EVIDENCE: Final = "transfers no longer in slskd — last observed:"
_VANISHED_PREFIXES: Final = (
    "transfers vanished from slskd",
    "all transfers vanished from slskd",
)


def _duration_phrase(seconds: int) -> str:
    """Minutes, not config tokens; approximate rather than spuriously exact.

    A poll cycle decides these thresholds, so the persisted number is
    "600s" for the configured stall timeout but "622s" for the cycle that
    actually noticed. Both mean "ten minutes" to an operator.
    """
    if seconds >= 60 and seconds % 60 == 0:
        minutes = seconds // 60
        return "1 minute" if minutes == 1 else f"{minutes} minutes"
    if seconds >= 90:
        return f"about {round(seconds / 60)} minutes"
    return "1 second" if seconds == 1 else f"{seconds} seconds"


_PATH_SEPARATORS: Final = re.compile(r"[\\/]+")


def _basename(path: str) -> str:
    """Last non-empty segment of a peer-supplied path (Windows or POSIX).

    Returns ``""`` when the peer's path has no usable segment — a separator
    run, or nothing but whitespace. Falling back to the raw path there
    would put the very drive letters and share names this copy exists to
    keep out straight back into the sentence (found by the generated
    property, shrunk world ``" \\ "``).
    """
    segments = [
        segment.strip()
        for segment in _PATH_SEPARATORS.split(path.strip())
        if segment.strip()
    ]
    return segments[-1] if segments else ""


def _present_download_message(
    message: str, *, evidence_unreadable: bool = False,
) -> str:
    """Humanize one of Cratedigger's OWN download-phase timeout reasons."""
    probe = message.strip()

    retry = _RETRY_LIMIT_RE.match(probe)
    if retry is not None:
        # The producer's number is the configured RETRY cap counted
        # against our own re-enqueue counter, so "N failed attempts"
        # invented both the noun and the value — 5 retries is 6 attempts
        # (issue #868 review #5). Repeat what was recorded.
        retries = int(retry.group(1))
        name = bounded_text(_basename(_EVIDENCE_TAIL_RE.sub("", retry.group(2))))
        tries = "retry" if retries == 1 else "retries"
        if not name:
            return f"Gave up after {retries} {tries}"
        return f'Gave up on "{name}" after {retries} {tries}'

    stalled = _STALLED_RE.match(probe)
    if stalled is not None:
        return (
            "Transfer stalled \u2014 no progress for "
            f"{_duration_phrase(int(stalled.group(1)))}"
        )

    queued = _REMOTE_QUEUE_RE.match(probe)
    if queued is not None:
        return (
            "Peer never started the transfer \u2014 still queued after "
            f"{_duration_phrase(int(queued.group(1)))}"
        )

    errored = _ALL_ERRORED_RE.match(probe)
    if errored is not None:
        count = int(errored.group(1))
        counted = _files(count) if count == 1 else f"All {_files(count)}"
        if evidence_unreadable:
            # A fail-open must never manufacture a negative claim: the row
            # HAS per-file evidence, we just could not decode it (issue
            # #868 review #6).
            return f"{counted} failed"
        return f"{counted} failed; slskd reported no reason"

    lowered = probe.casefold()
    if lowered.startswith(_VANISHED_WITH_EVIDENCE.casefold()):
        observed = probe.split(":", 1)[1].strip()
        return (
            "Transfers disappeared from slskd \u2014 last observed: "
            f"{bounded_text(observed)}"
        )
    if any(lowered.startswith(prefix) for prefix in _VANISHED_PREFIXES):
        # The persisted text guesses ("slskd restart?"); the guess is not
        # evidence, so it does not reach the operator.
        return "Transfers disappeared from slskd before the download finished"

    return f"Download failed: {bounded_text(probe, limit=MAX_DIAGNOSTIC_CHARS)}"


# --------------------------------------------------------------------------
# Materialize / staging failures (PR1's persisted reasons)
# --------------------------------------------------------------------------

_MATERIALIZE_REASON_COPY: Final[dict[str, str]] = {
    # --- one event-stamped source file ---
    REASON_EVENT_PATH_NEVER_STAMPED: (
        "Download finished but slskd never reported where the files landed; "
        "requeued"
    ),
    REASON_EVENT_PATH_GONE_FROM_DISK: (
        "Downloaded files disappeared before import; requeued"
    ),
    REASON_UNSAFE_SOURCE_PATH: (
        # Five codes produce this reason (path_escape, unsafe_symlink,
        # not_a_directory, not_regular_file, untrusted_ownership). Naming
        # two of them reads as exhaustive and is wrong for a FIFO, socket
        # or device node under the adversarial share — and a specific
        # false claim matters most on a security-boundary message (issue
        # #868 review B3).
        "Refused to import: a downloaded path failed the download share's "
        "containment check; requeued"
    ),
    REASON_SOURCE_PREFLIGHT_REFUSED: (
        "A downloaded file was refused before import for an unrecorded "
        "reason; requeued"
    ),
    # --- the shared slskd share ---
    REASON_SLSKD_ROOT_UNSAFE: (
        "Refused to import: the slskd download share failed its containment "
        "check; requeued"
    ),
    REASON_SLSKD_ROOT_MISSING: (
        "The slskd download share was missing; requeued"
    ),
    REASON_SLSKD_ROOT_REFUSED: (
        "The slskd download share refused access for an unrecorded reason; "
        "requeued"
    ),
    # --- our own private processing tree ---
    REASON_PROCESSING_AUTHORITY_UNSAFE: (
        "Refused to import: our processing directory failed its containment "
        "check; requeued"
    ),
    REASON_PROCESSING_PATH_MISSING: (
        # The same reason answers for a missing FILE under that tree, so
        # it must not say "directory" (issue #868 review #10).
        "Our processing storage was missing a required path; requeued"
    ),
    REASON_MATERIALIZE_AUTHORITY_FAILED: (
        "Our processing storage refused the import for an unrecorded reason; "
        "requeued"
    ),
    REASON_PRIVATE_MATERIALIZE_FAILED: (
        "Our processing storage failed with an unclassified error; requeued"
    ),
    # --- staged-path readiness (literal reasons, same persisted column) ---
    "staged_path_missing": (
        "The staged download folder could not be accessed before import "
        "(possible filesystem error); requeued"
    ),
    "staged_path_missing_tracked_files": (
        "Tracked files in the staged download folder could not be accessed "
        "before import (possible filesystem error); requeued"
    ),
    "empty_manifest": (
        "The download attempt tracked no files; requeued"
    ),
    "duplicate_final_basename": (
        # The row records no count, so it must not invent "two".
        "Downloaded files would import to the same filename; requeued"
    ),
    # Historical, retired job-less auto-import recovery reason.
    "abandoned_interrupted_auto_import": (
        "Interrupted import abandoned and requeued"
    ),
    # --- historical, pre-#868 fused reason ---
    # It meant EITHER "slskd never stamped a location" OR "the stamped file
    # was gone". The row cannot say which, so neither does the copy.
    "event_path_missing": (
        "Downloaded files could not be located for import; requeued"
    ),
}

# Open, read and write are three different facts. A destination that ran
# out of space opened perfectly well; "could not be opened" is a specific
# claim, and it was false for every ENOSPC/EIO write and every failed
# flush (issue #868 review B2).
_MATERIALIZE_REASON_PREFIX_COPY: Final[tuple[tuple[str, str], ...]] = (
    (
        REASON_SOURCE_OPEN_FAILED_PREFIX,
        ("A downloaded file on the slskd share could not be opened ({errno}); "
        "requeued"),
    ),
    (
        REASON_SOURCE_READ_FAILED_PREFIX,
        ("A downloaded file could not be read from the slskd share ({errno}); "
        "requeued"),
    ),
    (
        REASON_SOURCE_WRITE_FAILED_PREFIX,
        "A write to the slskd share failed ({errno}); requeued",
    ),
    (
        REASON_SLSKD_ROOT_OPEN_FAILED_PREFIX,
        "The slskd download share could not be opened ({errno}); requeued",
    ),
    (
        REASON_SLSKD_ROOT_READ_FAILED_PREFIX,
        "The slskd download share could not be read ({errno}); requeued",
    ),
    (
        REASON_SLSKD_ROOT_WRITE_FAILED_PREFIX,
        "A write to the slskd download share failed ({errno}); requeued",
    ),
    (
        REASON_PROCESSING_OPEN_FAILED_PREFIX,
        "Our processing storage could not be opened ({errno}); requeued",
    ),
    (
        REASON_PROCESSING_READ_FAILED_PREFIX,
        "Our processing storage could not be read ({errno}); requeued",
    ),
    (
        REASON_PROCESSING_WRITE_FAILED_PREFIX,
        "Our processing storage could not be written ({errno}); requeued",
    ),
)


def materialize_reason_copy(reason: str | None) -> str | None:
    """Operator copy for one persisted materialize reason code, or None.

    I3 lives here: a storage errno reason renders storage vocabulary and a
    containment reason renders a refusal-at-a-boundary sentence. The two
    never borrow each other's words, because PR1 already decided which is
    which and persisted it.
    """
    if not reason:
        return None
    code = reason.strip()
    exact = _MATERIALIZE_REASON_COPY.get(code)
    if exact is not None:
        return exact
    for prefix, template in _MATERIALIZE_REASON_PREFIX_COPY:
        if code.startswith(prefix) and len(code) > len(prefix):
            return template.format(errno=bounded_text(code[len(prefix):], limit=32))
    return None


_MATERIALIZE_GRACE_PREFIX: Final = (
    "completed download could not be materialized within"
)
_ABANDON_PREFIX: Final = "abandoned interrupted auto-import"
_GRACE_COPY: Final = (
    "Download could not be staged for import in time; returned to the queue"
)


def _present_failed_message(
    error_message: str | None,
    beets_detail: str | None,
) -> str | None:
    """Copy for an ``outcome='failed'`` row's persisted message.

    ``beets_detail`` carries PR1's machine reason on the grace-expiry path;
    ``error_message`` carries it on the completion path. Either way the
    reason wins over the generic sentence, because it is the only thing
    that says WHY the import never happened.
    """
    reason_copy = materialize_reason_copy(beets_detail)
    if reason_copy is not None:
        return reason_copy
    reason_copy = materialize_reason_copy(error_message)
    if reason_copy is not None:
        return reason_copy
    if not error_message:
        return None
    probe = error_message.strip()
    if any(probe.startswith(prefix) for prefix in NON_AUTOMATION_IMPORT_FAILURE_PREFIXES):
        return bounded_text(probe, limit=MAX_DIAGNOSTIC_CHARS)
    lowered = probe.casefold()
    if lowered.startswith(_MATERIALIZE_GRACE_PREFIX):
        return _GRACE_COPY
    if lowered.startswith(_ABANDON_PREFIX):
        return "Interrupted import abandoned and requeued"
    # Genuinely import-phase text (harness rc, beets exceptions) keeps the
    # "Import error:" label — it is accurate there, and only there.
    return f"Import error: {bounded_text(probe, limit=MAX_DIAGNOSTIC_CHARS)}"


# --------------------------------------------------------------------------
# Measurement failures
# --------------------------------------------------------------------------

_MEASUREMENT_COPY: Final[dict[str, str]] = {
    "current beets authority resolution raised": (
        "could not read the installed library copy"
    ),
    "current beets path was not returned": (
        "the installed library copy has no path on record"
    ),
}
_MEASUREMENT_AUTHORITY_PREFIX: Final = "filesystemauthorityerror:"
# NOTE: there is deliberately no per-message table for authority failures.
# The one that existed rendered "installed path is outside the library
# root" for the ONLY string production can raise here —
# ``lib.fs_authority``'s "path is outside configured quarantine roots",
# which is the CANDIDATE's quarantine tree, not the installed library copy.
# Wrong root and wrong subject: a containment fact rewritten into a
# different containment fact, which is worse than the raw string it
# replaced (live download_log 38273). Stripping the class name and passing
# the producer's own words through is honest and needs no table.


def _present_measurement_message(diagnostic: str | None) -> str:
    """Copy for a ``measurement_failed`` row.

    Strips the doubled ``failed: `` status prefix and the raw exception
    class name; neither is an operator fact.
    """
    if not diagnostic:
        return "Measurement failed"
    probe = diagnostic.strip()
    if probe.casefold().startswith("failed:"):
        probe = probe.split(":", 1)[1].strip()
    lowered = probe.casefold()
    if lowered.startswith(_MEASUREMENT_AUTHORITY_PREFIX):
        probe = probe.split(":", 1)[1].strip()
        lowered = probe.casefold()
    mapped = _MEASUREMENT_COPY.get(lowered)
    if mapped is not None:
        return f"Measurement failed: {mapped}"
    if not probe:
        return "Measurement failed"
    return f"Measurement failed: {bounded_text(probe, limit=MAX_DIAGNOSTIC_CHARS)}"


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def present_failure(evidence: FailureEvidence) -> FailurePresentation:
    """Derive the operator sentence (and bounded raw evidence) for one row.

    Pure: same evidence in, same copy out (I5). ``verdict is None`` means
    "no message-level evidence here" — never an invented sentence.
    """
    presentation = _present_failure(evidence)
    if materialize_reason_copy(evidence.beets_detail) is None:
        return presentation
    return replace(presentation, beets_detail_label=DETAIL_LABEL_REASON_CODE)


def _present_failure(evidence: FailureEvidence) -> FailurePresentation:
    """The outcome-keyed body of :func:`present_failure`."""
    if evidence.outcome == "measurement_failed":
        return FailurePresentation(
            verdict=_present_measurement_message(
                evidence.error_message or evidence.beets_detail,
            ),
        )

    if evidence.outcome == "timeout":
        groups = _group_reasons(
            evidence.transfer_detail, evidence.soulseek_username,
        )
        message = (evidence.error_message or "").strip()
        # The retry-limit message LEADS because it records a Cratedigger
        # DECISION taken ON TOP of whatever the evidence says: we stopped
        # retrying. Leading is all it does — the cause is appended below.
        #
        # The remote-queue message does NOT lead when evidence exists:
        # ``all_remote_queued`` is a snapshot taken before error handling,
        # so "the peer never started the transfer" can be flatly
        # contradicted by the very clause appended to it (issue #868
        # review #7). The more specific truth wins.
        own_first = bool(message) and (
            _RETRY_LIMIT_RE.match(message) is not None
            or (not groups and _REMOTE_QUEUE_RE.match(message) is not None)
        )
        if groups and not own_first:
            presentation = _present_transfer_evidence(groups)
            context = _download_message_context(message)
            if context is None or presentation.verdict is None:
                return presentation
            # A stalled-timeout row is evidence-led, so its duration would
            # otherwise be lost entirely (review #8).
            return replace(
                presentation, verdict=f"{presentation.verdict} ({context})")
        verdict = (
            _present_download_message(
                message,
                evidence_unreadable=evidence.transfer_detail_unreadable,
            )
            if message else "Download failed"
        )
        if groups:
            # I6: our decision is CONTEXT, never a substitute for the cause.
            # Live-data review of 400 rows found this ranking suppressing the
            # local-storage family in 10 of its 14 occurrences: rows 38203 /
            # 38184 / 38119 read "Gave up on '05 Seventeen.flac' after 5
            # failed attempts" while the evidence underneath was our own
            # virtiofs share refusing the write. An operator concludes
            # "flaky peer" and retries the peer. That is precisely the
            # misattribution PR1 eliminated one layer down, so the dominant
            # family is appended to every decision-led verdict.
            return FailurePresentation(
                verdict=f"{verdict} — {_family_clause(groups)}",
                transfer_message=_transfer_message(groups),
                transfer_message_label=_transfer_message_label(groups),
                peer_attributable=_peer_attributable(groups),
            )
        return FailurePresentation(verdict=verdict)

    if evidence.outcome == "failed":
        if evidence.beets_scenario == "timeout":
            return FailurePresentation(verdict="Import timed out")
        return FailurePresentation(
            verdict=_present_failed_message(
                evidence.error_message, evidence.beets_detail,
            ),
        )

    return FailurePresentation()


__all__ = [
    "DETAIL_LABEL_REASON_CODE",
    "FAMILY_LOCAL_STORAGE",
    "FAMILY_PEER_FILE",
    "FAMILY_REFUSAL",
    "FAMILY_TRANSPORT",
    "FAMILY_UNKNOWN",
    "MAX_DIAGNOSTIC_CHARS",
    "MAX_PEER_NAME_CHARS",
    "MAX_RAW_MESSAGE_CHARS",
    "MAX_RAW_MESSAGE_GROUPS",
    "NON_AUTOMATION_IMPORT_FAILURE_PREFIXES",
    "TRANSFER_MESSAGE_LABEL_MIXED",
    "TRANSFER_MESSAGE_LABEL_PEER",
    "TRANSFER_MESSAGE_LABEL_STATE",
    "TRANSFER_MESSAGE_LABEL_STORAGE",
    "FailureEvidence",
    "FailurePresentation",
    "PeerFailureFamily",
    "bounded_text",
    "decode_transfer_detail",
    "materialize_reason_copy",
    "non_automation_import_failure_message",
    "peer_failure_family",
    "present_failure",
    "transfer_detail_unreadable",
]
