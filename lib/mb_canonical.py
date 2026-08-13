"""Ask MusicBrainz what it calls a release now (#1059).

Cratedigger acquires; Beets manages; MusicBrainz owns release identity above
both. When MusicBrainz editors merge two release entries, the loser's MBID
becomes a permanent ``301`` to the survivor, and the local files have to be
retagged onto the survivor before anything else can move.

This module answers the one question the merge sweep needs: *what does
MusicBrainz call this release now?* A request has exactly ONE release ID, and
this module never creates a second one — it reports the survivor, and the
sweep acts on that answer immediately: retag the installed Beets album onto
the survivor first (Beets keys album duplicate detection on ``mb_albumid``, so
rekeying first would land a second album), then rekey the request. Nothing is
persisted as an alternate identity, and no PASSIVE read/render path ever calls
this.

That placement is the whole lesson of the earlier attempts (PR #1056, and the
six-PR series reverted by PR #1074): resolving at the point of use put a
network call behind twelve consumers, broke four of them across two review
rounds, and cost ~28s on an uncached long-tail render — and preserving both
IDs as simultaneous identities never reached the import-time match, which is
where a merged request actually fails. One sweep, one question, one current
ID.

The one deliberate exception (#1089) is the operator merge-rekey web route: a
single explicit, operator-initiated button click, not a read/render path
behind an arbitrary consumer count — the exact shape the #1056/#1074 lesson
warns against. It asks this module exactly once per click, at request time,
and never during dashboard render.

**Fail-open by contract, with two answer shapes.** :func:`canonical_release_id`
collapses every non-redirect world into ``None`` — a non-MusicBrainz identity,
an unconfigured base, any transport/protocol failure, an unusable response
shape, AND the ordinary case of an id MusicBrainz still considers current all
read identically, because the import-validation seam (#1059/#1080) only ever
needs "keep using the stored id" either way. The operator merge-rekey action
(#1089) cannot use that collapsed answer: a configured-but-unreachable mirror
must never be read as "MusicBrainz confirms this request was never merged" —
that is an operator-facing fact (the #8792 refusal), not a network hiccup.
:func:`canonical_release_status` is the tagged variant that keeps those worlds
apart (:class:`CanonicalReleaseRedirected` / :class:`CanonicalReleaseCurrent` /
:class:`CanonicalReleaseUnavailable`); ``canonical_release_id`` is now defined
in terms of it and its own collapsed contract is unchanged. In particular a
``4xx`` is NEVER read as "this release was deleted": the mirror's WS/2 app
layer has served poisoned 404s that its own PostgreSQL contradicted, and a
bogus UUID answers ``400`` rather than ``404`` at all. Concluding deletion
from this module's silence is a bug.

**No caching, deliberately.** A daily sweep asking a local mirror has nothing
to gain from a 24h cache, and a cached non-answer is exactly the failure this
module must not have. The lookup is direct, or it is not made.

**Inert until configured.** The WS/2 base starts unset, so a process that
never wires it degrades to the literal stored id rather than silently
reaching out to public MusicBrainz.
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lib.json_narrow import json_dict
from lib.release_identity import detect_release_source, normalize_release_id

if TYPE_CHECKING:
    from lib.config import CratediggerConfig

logger = logging.getLogger("cratedigger")

#: A canonical-release resolver: stored release id -> survivor id, or None.
#: Collapses "MusicBrainz answered with no different id" and "no answer was
#: obtained at all" into the same ``None`` — sound for the import-validation
#: seam, which only ever needs "keep using the stored id" either way. A
#: caller that must tell those two worlds apart uses
#: :type:`TaggedCanonicalReleaseFn` / :func:`canonical_release_status`
#: instead (#1089).
type CanonicalReleaseFn = Callable[[str], str | None]

#: A decoded-JSON fetch of one URL. The external HTTP edge, and the only
#: seam tests replace.
type CanonicalFetchFn = Callable[[str], object]

#: WS/2 base (``scheme://host[:port]/ws/2``) for canonical resolution.
#: Unset means inert — see the module docstring. Read it through
#: :func:`configured_canonical_base`; it is process-startup state, not a
#: constant.
_mb_ws2_base: str | None = None

_TIMEOUT_SECONDS = 15
_USER_AGENT = "cratedigger-canonical/1.0"
#: A release document with no ``inc`` clause is a few KB; this bounds a
#: broken or hostile mirror, which the socket timeout alone does not.
_MAX_RESPONSE_BYTES = 1_000_000


def _normalized_base(ws2_base: str | None) -> str | None:
    """One definition of "configured": non-blank, no trailing slash.

    Whitespace is stripped before the emptiness test so a config value that
    is accidentally blank leaves the process inert rather than "configured"
    at a URL that cannot answer. Found by the generated property, which
    treats a blank base as an unconfigured one.
    """
    return (ws2_base or "").strip().rstrip("/") or None


def configure_canonical_base(ws2_base: str | None) -> None:
    """Point canonical resolution at one MusicBrainz WS/2 base."""
    global _mb_ws2_base
    _mb_ws2_base = _normalized_base(ws2_base)


def configured_canonical_base() -> str | None:
    """The wired WS/2 base, or None while this process is inert."""
    return _mb_ws2_base


def _fetch_json(url: str) -> object:
    """Fetch and decode one MB URL, reporting whether it was redirected.

    ``urllib`` follows the merge ``301`` transparently, so the response body
    is already the survivor's document and its top-level ``id`` is the
    canonical MBID. Verified live against the mirror on 2026-08-06.

    ``redirected`` reports the observable fact — ``response.url`` differs
    from the URL we asked for — and nothing more. It is TRUE for any
    redirect the transport followed, a merge ``301`` among them; see
    :func:`canonical_release_status` for why that is sound as a gate rather
    than as proof.

    The body is read under a byte cap: a broken or hostile mirror must not
    be able to stream unbounded bytes into the sweep process.
    """
    request = urllib.request.Request(url)
    request.add_header("User-Agent", _USER_AGENT)
    request.add_header("Connection", "close")
    with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
        body = response.read(_MAX_RESPONSE_BYTES + 1)
        final_url = response.url
    if len(body) > _MAX_RESPONSE_BYTES:
        raise ValueError(
            f"MusicBrainz release document exceeded {_MAX_RESPONSE_BYTES} bytes"
        )
    payload = json.loads(body)
    # A body field alone can NEVER declare a successor: a redirect must
    # also have been observed. urllib rewrites ``response.url`` when it
    # follows one, so a document served straight from the requested URL is
    # gated out no matter what its ``id`` says. This is a necessary
    # condition, not a sufficient one — ``final_url != url`` is true for
    # any redirect the transport followed (host or scheme normalisation, a
    # trailing slash), not only a merge 301. The sufficient half is
    # :func:`canonical_release_status`'s ``canonical == requested`` check: a
    # cosmetic redirect returns the same id and is rejected there. The
    # mirror has served wrong bodies for adversarially-selected MBIDs from
    # a TTL-less cache, and this lookup authorizes a RETAG of installed
    # files plus a rekey of the request, so the two conditions are both
    # required (issue #1049).
    return {"payload": payload, "redirected": final_url != url}


@dataclass(frozen=True)
class CanonicalReleaseRedirected:
    """MusicBrainz names a different, current survivor for the stored id."""

    survivor: str


@dataclass(frozen=True)
class CanonicalReleaseCurrent:
    """MusicBrainz answered, and names no different survivor.

    The stored id is current as far as this lookup can tell — this is the
    genuine "not merged" world (the #8792 Slipknot Vol. 3 refusal: two
    current Beets albums, no MusicBrainz redirect), never a stand-in for a
    lookup that did not answer at all.
    """


@dataclass(frozen=True)
class CanonicalReleaseUnavailable:
    """No answer was obtained at all.

    Covers every reason nothing came back: an unconfigured base, a
    non-MusicBrainz identity (structurally can never answer — no adapter
    between MusicBrainz and Discogs), or the lookup itself failing
    (transport, protocol, or an unusable response shape). Deliberately
    distinct from :class:`CanonicalReleaseCurrent` (#1089 BLOCKING-1): a
    down-but-configured mirror must never be read as "this id is current" —
    that would tell an operator a request was never merged when the truth is
    simply that nobody asked MusicBrainz successfully.
    """


#: Tagged answer from :func:`canonical_release_status`.
type CanonicalReleaseAnswer = (
    CanonicalReleaseRedirected | CanonicalReleaseCurrent | CanonicalReleaseUnavailable
)

#: A canonical-release resolver that keeps "MusicBrainz answered and names no
#: redirect" distinct from "no answer was obtained at all" — see
#: :func:`canonical_release_status`.
type TaggedCanonicalReleaseFn = Callable[[str], CanonicalReleaseAnswer]


def canonical_release_status(
    release_id: str,
    *,
    ws2_base: str | None = None,
    fetch: CanonicalFetchFn | None = None,
) -> CanonicalReleaseAnswer:
    """The tagged sibling of :func:`canonical_release_id` (#1089).

    Three states, never a raise:

    * :class:`CanonicalReleaseRedirected` — MusicBrainz names a different,
      current MusicBrainz survivor.
    * :class:`CanonicalReleaseCurrent` — MusicBrainz answered and names no
      different id (including a cosmetic redirect that lands back on the
      same id — not a merge).
    * :class:`CanonicalReleaseUnavailable` — no answer at all: unconfigured,
      a non-MusicBrainz requested identity, or the lookup failed (transport,
      protocol, or an unusable response shape).

    ``canonical_release_id`` is defined in terms of this function and keeps
    its own collapsed ``str | None`` contract unchanged for the
    import-validation seam.
    """
    requested = normalize_release_id(release_id)
    # MB-only by nature: Discogs release ids have no redirect concept, so
    # this is not an adapter between the two sources. Structurally unable to
    # answer, which is the "unavailable" bucket, not "current" — there is no
    # MusicBrainz identity here to be current about.
    if detect_release_source(requested) != "musicbrainz":
        return CanonicalReleaseUnavailable()

    base = _normalized_base(
        ws2_base if ws2_base is not None else _mb_ws2_base,
    )
    if base is None:
        return CanonicalReleaseUnavailable()

    url = (
        f"{base}/release/"
        f"{urllib.parse.quote(requested, safe='')}?fmt=json"
    )
    try:
        payload = fetch(url) if fetch is not None else _fetch_json(url)
    except Exception as exc:  # noqa: BLE001 - fail-closed-to-unavailable boundary, never raises
        # Deliberately including HTTPError 4xx: a 404 here is not evidence
        # that the release is gone, only that this lookup did not answer.
        logger.debug(
            "canonical release lookup for %s did not answer: %s: %s",
            requested, type(exc).__name__, exc,
        )
        return CanonicalReleaseUnavailable()

    if not isinstance(payload, dict):
        # Not even a JSON object — ``_fetch_json``'s real contract always
        # returns the ``{"payload": ..., "redirected": ...}`` shape, so
        # this can only happen via a malformed injected fetch (tests) or a
        # genuinely broken mirror body. An unusable response shape is never
        # read as MusicBrainz having answered "current" (#1089 MINOR-1) —
        # it is exactly as much "no answer" as a raised exception is.
        return CanonicalReleaseUnavailable()
    # Narrowing an already-decoded value: the shared graceful helper, never
    # a re-``convert`` (`.claude/rules/code-quality.md`).
    envelope = json_dict(payload)
    redirected = envelope.get("redirected")
    if not isinstance(redirected, bool):
        # A missing or wrong-typed "redirected" key is the same unusable
        # shape as above — no genuine transport-level redirect fact was
        # ever observed, so there is nothing to read as an answer.
        return CanonicalReleaseUnavailable()
    if redirected is not True:
        # An explicit, well-formed ``False`` ⇒ MusicBrainz answered and
        # names no successor, whatever the body says. This IS "current",
        # not "unavailable" — the lookup DID answer.
        return CanonicalReleaseCurrent()
    canonical = normalize_release_id(json_dict(envelope.get("payload")).get("id"))
    if detect_release_source(canonical) != "musicbrainz":
        # A redirect WAS observed, but the body's ``id`` is garbage/absent —
        # an unusable response shape, not a confirmed-current answer. The
        # mirror has served wrong bodies for adversarially-selected MBIDs
        # from a TTL-less cache (issue #1049); this must not be read as
        # MusicBrainz having answered anything.
        return CanonicalReleaseUnavailable()
    if canonical == requested:
        # A redirect that lands on the same release id is a cosmetic one
        # (scheme/host normalisation, a trailing slash) — not a merge. This
        # is the half of the gate that makes "redirected" mean "merged".
        return CanonicalReleaseCurrent()
    logger.info(
        "MusicBrainz canonicalizes release %s to %s", requested, canonical,
    )
    return CanonicalReleaseRedirected(canonical)


def canonical_release_id(
    release_id: str,
    *,
    ws2_base: str | None = None,
    fetch: CanonicalFetchFn | None = None,
) -> str | None:
    """The survivor MusicBrainz redirects ``release_id`` to, or ``None``.

    ``None`` means "no different canonical is known" and is returned for
    every non-answer alike: a non-MusicBrainz identity, an unconfigured
    base, any transport or protocol failure, an unusable response shape,
    and the ordinary case of an id MusicBrainz still considers current.
    Callers treat all of them as "keep using the stored id" — sound for the
    import-validation seam this function serves, which never needs to tell
    those worlds apart. A caller that does (#1089) uses
    :func:`canonical_release_status` instead.

    Never raises.
    """
    status = canonical_release_status(release_id, ws2_base=ws2_base, fetch=fetch)
    if isinstance(status, CanonicalReleaseRedirected):
        return status.survivor
    return None


def production_canonical_release_fn() -> CanonicalReleaseFn:
    """The configured resolver, bound late so startup order cannot matter."""

    def resolve(release_id: str) -> str | None:
        return canonical_release_id(release_id)

    return resolve


def production_tagged_canonical_release_fn() -> TaggedCanonicalReleaseFn:
    """The configured tagged resolver, bound late so startup order cannot
    matter. Mirrors :func:`production_canonical_release_fn` for #1089's
    operator merge-rekey action, which needs the tagged answer."""

    def resolve(release_id: str) -> CanonicalReleaseAnswer:
        return canonical_release_status(release_id)

    return resolve


def configure_canonical_release_lookup(cfg: CratediggerConfig) -> None:
    """Point MusicBrainz merge-survivor resolution at the operator's mirror.

    This module starts inert, and an unwired process does not fail loudly —
    it reports "no redirect" forever and looks perfectly healthy. Two
    processes reach a merge seam and must both call this at startup: the
    importer (``lib.download_validation._follow_merged_release``, draining
    automation/force jobs) and the web server (the operator merge-rekey
    route, #1089, reached from ``MergeRekeyService``). Sharing ONE
    implementation is what keeps those two callers from silently drifting
    apart — see CLAUDE.md's "No Parallel Code Paths".

    A blank base leaves resolution inert rather than silently reaching out to
    public MusicBrainz from a deployment that configured a mirror on purpose.
    """
    from web.api_bases import mb_ws2_base

    origin = (cfg.musicbrainz_api_base or "").strip()
    if not origin:
        logger.warning(
            "No [MusicBrainz] api_base configured; MusicBrainz merge "
            "survivors will not be resolved — a merged-away request stays "
            "rejected as mbid_not_found in the importer, and the operator "
            "merge-rekey button reports mirror_unavailable on the web UI",
        )
        configure_canonical_base(None)
        return
    configure_canonical_base(mb_ws2_base(origin))


__all__ = [
    "CanonicalFetchFn",
    "CanonicalReleaseAnswer",
    "CanonicalReleaseCurrent",
    "CanonicalReleaseFn",
    "CanonicalReleaseRedirected",
    "CanonicalReleaseUnavailable",
    "TaggedCanonicalReleaseFn",
    "canonical_release_id",
    "canonical_release_status",
    "configure_canonical_base",
    "configure_canonical_release_lookup",
    "configured_canonical_base",
    "production_canonical_release_fn",
    "production_tagged_canonical_release_fn",
]
