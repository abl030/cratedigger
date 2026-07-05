"""Shared outcome/status string constants for the Replace operator action
and its lazy release-group/master resolver (``POST .../resolve-rg``).

Both ``lib/mbid_replace_service.py`` (``ReplaceResult.outcome`` /
``ReplaceResult.reason``) and ``web/routes/pipeline.py::
post_pipeline_resolve_rg`` (its bare ``status`` field) express failure
modes as plain strings. Before this module they were declared
independently at each call site — a rename of one copy (e.g.
``RESULT_MIRROR_UNCONFIGURED``) could silently diverge from the other
(resolve-rg's hardcoded ``"mirror_unconfigured"`` literal) without any
test catching the drift, since they were never the same symbol. Both
surfaces now import from here (#501 item 2).

``REPLACE_REASON_*`` are typed sub-codes for ``ReplaceResult.reason``,
distinguishing the different rejections ``RESULT_TARGET_INVALID``
collapses. ``error_message`` stays free-text for operator-facing detail;
``reason`` is the stable code CLI/API/tests assert on. Pathway-neutral by
design (no MB/Discogs adapter asymmetry) — the same reason applies
whether the failing lookup was against the MB mirror or the Discogs
mirror:

    cross_pathway_target      target/source pathways differ, or the
                               target id doesn't parse as either shape
    source_no_release_group   the source has no release group/master to
                               anchor siblings against (MB: no RG after
                               lazy-backfill; Discogs: masterless)
    unresolvable_target       the target lookup returned no usable data
                               (empty/falsy payload)
    target_no_release_group   the target resolved but has no release
                               group/master (MB: no release_group_id;
                               Discogs: no master)
    unexpected_lookup_error   the lookup call itself raised something
                               that isn't the known transient set — this
                               ALSO logs a warning, since it may be a
                               real bug rather than expected bad input
"""

from __future__ import annotations

# ReplaceResult.outcome constants. See lib/mbid_replace_service.py's
# module docstring for the full exit-code / HTTP-status convention.
RESULT_REPLACED = "replaced"
RESULT_NOT_FOUND = "not_found"
RESULT_WRONG_STATE = "wrong_state"
RESULT_TARGET_INVALID = "target_invalid"
RESULT_TARGET_RELEASE_GROUP_MISMATCH = "target_release_group_mismatch"
RESULT_TARGET_SAME_AS_CURRENT = "target_same_as_current"
RESULT_TARGET_COLLISION_REQUEST = "target_collision_request"
RESULT_MIRROR_UNCONFIGURED = "mirror_unconfigured"
RESULT_TRANSIENT = "transient"

# ReplaceResult.reason constants — sub-codes for RESULT_TARGET_INVALID.
REPLACE_REASON_CROSS_PATHWAY_TARGET = "cross_pathway_target"
REPLACE_REASON_SOURCE_NO_RELEASE_GROUP = "source_no_release_group"
REPLACE_REASON_UNRESOLVABLE_TARGET = "unresolvable_target"
REPLACE_REASON_TARGET_NO_RELEASE_GROUP = "target_no_release_group"
REPLACE_REASON_UNEXPECTED_LOOKUP_ERROR = "unexpected_lookup_error"

# POST /api/pipeline/<id>/resolve-rg status vocabulary
# (web/routes/pipeline.py::post_pipeline_resolve_rg). Two of these
# (RESOLVE_STATUS_MIRROR_UNCONFIGURED / RESOLVE_STATUS_TRANSIENT) are
# deliberately the SAME string values as RESULT_MIRROR_UNCONFIGURED /
# RESULT_TRANSIENT above — both describe the identical failure mode
# (Discogs mirror not configured / a network blip) on a sibling operator
# action, and web/js/replace_picker.js string-matches both vocabularies.
RESOLVE_STATUS_RESOLVED = "resolved"
RESOLVE_STATUS_MASTERLESS = "masterless"
RESOLVE_STATUS_NOT_FOUND = "not_found"
RESOLVE_STATUS_MISSING_RELEASE_ID = "missing_release_id"
RESOLVE_STATUS_NON_MB_RELEASE_ID = "non_mb_release_id"
RESOLVE_STATUS_NO_RELEASE_GROUP = "no_release_group"
RESOLVE_STATUS_LOOKUP_FAILED = "lookup_failed"
RESOLVE_STATUS_MIRROR_UNCONFIGURED = RESULT_MIRROR_UNCONFIGURED
RESOLVE_STATUS_TRANSIENT = RESULT_TRANSIENT
