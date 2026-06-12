"""Shared scanner for the stateful-MagicMock audit.

Isolated so test_mock_audit.py and the baseline-rebuild helper share one
source of truth for the heuristic. See CLAUDE.md § "Mocks: leaf-seam only"
and issue #290.

The heuristic flags two anti-patterns:

1. **Stateful-collaborator MagicMock by variable name.** Lines that
   assign ``MagicMock(...)`` to a variable whose name implies a stateful
   thing we own (``db``, ``mock_db``, ``ctx``, ``source``, ``beets``,
   ``pipeline_db``, ``slskd``, etc.). The replacement is
   ``FakePipelineDB`` / ``FakeBeetsDB`` / ``FakeSlskdAPI`` / a real
   constructed ``CratediggerContext`` from ``tests/helpers.py``.

2. **Patching our own functions.** Any ``patch("lib.*")`` or
   ``patch("web.*")`` or ``patch("scripts.*")`` or ``patch("harness.*")``
   whose target is **not** on the leaf-seam allowlist. Leaf seams are
   the outermost edge — subprocess, urllib/requests, os.path, time.sleep,
   third-party libs we don't own (``music_tag``, ``redis``), and a small
   set of one-way notifier helpers in ``lib.util``.

The scanner returns a dict ``{relpath: {finding_key: count}}``.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from typing import Dict


TESTS_DIR = os.path.abspath(os.path.dirname(__file__))

# Variables named these and assigned MagicMock(...) on the same line
# strongly suggest a stateful collaborator stand-in.
STATEFUL_VAR_NAMES = {
    "db",
    "mock_db",
    "failing_db",
    "pdb",
    "pipeline_db",
    "ctx",
    "context",
    "beets",
    "beets_db",
    "source",
    "slskd",
    "fake_db",  # the misnomer — sometimes used for MagicMock pretending to be FakePipelineDB
}

_STATEFUL_ASSIGN_RE = re.compile(
    r"^\s*(" + "|".join(sorted(STATEFUL_VAR_NAMES)) + r")\s*=\s*MagicMock\s*\("
)

# patch(...) / @patch("...") / with patch("..."): — first arg is a string
# literal naming the dotted path.
_PATCH_RE = re.compile(r'\bpatch\s*\(\s*["\']([^"\']+)["\']')

# patch.object(MODULE_REF, "name", ...) — first arg is a Python identifier
# (typically a module alias from ``import x.y as alias`` or
# ``from x import y as alias``), second arg is a string naming the
# attribute. The string-first form (``patch.object("module.path", ...)``)
# is also recognised by ``_PATCH_RE`` above.
_PATCH_OBJECT_RE = re.compile(
    r'\bpatch\.object\s*\(\s*([A-Za-z_][A-Za-z0-9_.]*)\s*,\s*["\']([^"\']+)["\']'
)

# Module aliases the audit knows how to resolve to canonical paths.
# Keep narrow — false positives become baseline noise. Detected by
# scanning each file's import statements before classification.
_ALIAS_TO_CANONICAL = {
    "cratedigger": "cratedigger",
    "enqueue_module": "lib.enqueue",
    "dl_mod": "lib.download",
    "srv": "web.server",
    "server": "web.server",
}

# Leaf-seam allowlist. If a patch target matches any of these, the patch
# is legitimate.
_LEAF_SEAM_PATTERNS = [
    # Subprocess
    re.compile(r"\.sp\.(run|Popen|check_output|check_call)$"),
    re.compile(r"\.subprocess\.(run|Popen|check_output|check_call)$"),
    re.compile(r"^subprocess\."),
    # HTTP / URL clients
    re.compile(r"\.urllib\."),
    re.compile(r"\.requests\."),
    re.compile(r"^urllib\."),
    re.compile(r"^requests\."),
    # OS / filesystem leaf seams (stdlib os.*)
    re.compile(r"\.os\.path\."),
    re.compile(r"\.os\.(remove|rename|makedirs|mkdir|listdir|stat|unlink|rmdir|getcwd|getpgid|killpg|kill|chmod|symlink)$"),
    re.compile(r"\.shutil\."),
    re.compile(r"^os\.path\."),
    re.compile(r"^shutil\."),
    # threading / signal primitives
    re.compile(r"\.threading\.(Event|Lock|RLock|Thread|Condition)$"),
    re.compile(r"\.signal\.(signal|SIGINT|SIGTERM|alarm)$"),
    # Time
    re.compile(r"\.time\.(sleep|monotonic|time)$"),
    re.compile(r"^time\."),
    # Third-party libraries we don't own
    re.compile(r"\.music_tag"),
    re.compile(r"^music_tag\."),
    re.compile(r"\.redis\.Redis$"),
    re.compile(r"^redis\."),
    re.compile(r"\.slskd_api"),
    # MusicBrainz / Discogs client objects on the web side
    re.compile(r"^web\.(mb|discogs)\."),
    re.compile(r"^web\.routes\.\w+\.(mb_api|discogs_api)"),
    re.compile(r"^web\.routes\.pipeline\.mb_api"),
    re.compile(r"^web\.server\.(mb_api|discogs_api|_real_beets_db|check_beets_library|check_pipeline|get_library_artist|_beets_db|mb)"),
    # Notifier helpers — fire-and-forget, no return value to mock meaningfully
    re.compile(r"lib\.util\._meelo_"),
    re.compile(r"lib\.util\.trigger_(meelo|plex|jellyfin)_scan$"),
    re.compile(r"lib\.util\.(sp|urllib|os|shutil)\."),
    re.compile(r"lib\.util\.repair_mp3_headers$"),
    re.compile(r"\.trigger_(meelo|plex|jellyfin)_scan$"),
    # builtins / stdlib
    re.compile(r"^builtins\."),
    re.compile(r"\.print$"),
    re.compile(r"^json\."),
    re.compile(r"\.select\.select$"),  # select.select syscall
    # Cratedigger entry-point shims (the top-level cratedigger.py wrapper
    # functions are thin and patched on a per-test basis; the real ones
    # live in lib/* and have their own audit coverage)
    re.compile(r"^cratedigger\.(slskd_api|configure_slskd_http_pool|_create_slskd_client|sp|urllib)"),

    # === Thin seam-wrapper functions in lib/ ===
    # These are functions whose body is mostly "construct args and
    # dispatch to a network/subprocess/filesystem call." Patching them
    # is the most ergonomic point to mock the underlying seam — the
    # alternative (mocking the slskd_api / sox subprocess / harness
    # subprocess at its own boundary) often requires elaborate per-test
    # fixture setup for no additional coverage. Each entry below has a
    # rationale.

    # slskd network wrappers. Each forwards to slskd_api.* and lightly
    # transforms the result; mocking them is morally equivalent to
    # mocking slskd_api directly, which is on the third-party allowlist.
    re.compile(r"^lib\.enqueue\._fanout_browse_users$"),
    re.compile(r"^lib\.enqueue\.slskd_do_enqueue$"),
    re.compile(r"^lib\.enqueue\.slskd_enqueue_with_outcome$"),
    re.compile(r"^lib\.(download|enqueue)\.cancel_and_delete$"),
    re.compile(r"^lib\.download\.slskd_download_status$"),

    # Beets harness subprocess wrapper. ``beets_validate`` invokes
    # ``run_beets_harness.sh`` and parses JSON — equivalent to mocking
    # a subprocess seam.
    re.compile(r"^lib\.beets\.beets_validate$"),

    # ``parse_import_result`` is the parsed-output side of the SAME
    # harness subprocess: scans import_one.py's stdout for the
    # ``__IMPORT_RESULT__`` sentinel and decodes it via msgspec.
    # Patching it is morally equivalent to constructing a fake harness
    # stdout — same wire-boundary seam as ``beets_validate``.
    re.compile(r"^lib\.quality\.parse_import_result$"),
    re.compile(r"^lib\.import_dispatch\.parse_import_result$"),

    # Spectral / audio measurement wrappers. Each invokes sox / ffmpeg /
    # mp3val subprocesses and reads files on disk; equivalent to a
    # subprocess seam. ``inspect_local_files`` reads tag/codec metadata.
    # ``spectral_check.analyze_track`` runs 17 sox commands per file
    # (1 reference band + 16 test slices) — body is all subprocess
    # dispatch despite the length.
    re.compile(r"^lib\.measurement\.spectral_analyze$"),
    re.compile(r"^lib\.measurement\.inspect_local_files$"),
    re.compile(r"^lib\.measurement\.repair_mp3_headers$"),
    re.compile(r"^lib\.measurement\._needs_spectral_check$"),
    re.compile(r"^lib\.measurement\.measure_preimport_state$"),
    re.compile(r"^lib\.measurement\._iter_audio_files$"),
    re.compile(r"^lib\.measurement\.hash_audio_content$"),
    re.compile(r"^lib\.measurement\.validate_audio$"),
    re.compile(r"^lib\.spectral_check\.analyze_track$"),
    re.compile(r"^lib\.audio_hash\.hash_audio_content$"),

    # Re-exports of measurement / harness / dispatch into the
    # import_preview surface — same underlying subprocess seams.
    re.compile(r"^lib\.import_preview\.inspect_local_files$"),
    re.compile(r"^lib\.import_preview\.measure_preimport_state$"),
    re.compile(r"^lib\.import_preview\.run_import_one$"),
    re.compile(r"^lib\.download\.measure_preimport_state$"),

    # Config loader — reads INI from disk. Equivalent to mocking the
    # filesystem read. The replacement (constructing a CratediggerConfig
    # in-memory) is also valid and used in many tests.
    re.compile(r"^lib\.config\.read_runtime_config$"),
    re.compile(r"^scripts\.\w+\.read_runtime_config$"),  # re-exports
    re.compile(r"^lib\.config\.CratediggerConfig\.from_ini$"),

    # Filesystem permission helper — wraps chmod calls.
    re.compile(r"^lib\.permissions\.fix_library_modes$"),
    re.compile(r"^harness\.import_one\.fix_library_modes$"),

    # harness.import_one subprocess wrappers. ``run_import`` invokes
    # ``beet import``; ``convert_lossless`` runs ffmpeg; the probe
    # helpers run ffprobe/sox; ``_get_folder_*`` read tag metadata.
    re.compile(r"^harness\.import_one\.run_import$"),
    re.compile(r"^harness\.import_one\.convert_lossless$"),
    re.compile(r"^harness\.import_one\._probe_lossless_source_as_v0$"),
    re.compile(r"^harness\.import_one\._probe_native_lossy_as_v0$"),
    re.compile(r"^harness\.import_one\._get_folder_bitrates$"),
    re.compile(r"^harness\.import_one\._get_folder_min_bitrate$"),
    re.compile(r"^harness\.import_one\.BeetsDB$"),  # class replacement, see lib.beets_db.BeetsDB

    # Album-level spectral analysis — same sox/ffmpeg seam as
    # analyze_track, just aggregating across an album's tracks.
    re.compile(r"^lib\.spectral_check\.analyze_album$"),

    # Logger objects — patching the module-level logger lets tests
    # assert against log records without subclassing the logger. Also
    # logger.error / .warning / .exception methods directly.
    re.compile(r"^lib\.\w+\.logger$"),
    re.compile(r"^harness\.\w+\.logger$"),
    re.compile(r"^web\.\w+\.logger$"),
    re.compile(r"^scripts\.\w+\.logger$"),
    re.compile(r"^lib\.\w+\.logger\.(error|warning|exception|info|debug)$"),
    re.compile(r"^scripts\.\w+\.logger\.(error|warning|exception|info|debug)$"),
    re.compile(r"^web\.\w+\.logger\.(error|warning|exception|info|debug)$"),
    re.compile(r"^harness\.\w+\.logger\.(error|warning|exception|info|debug)$"),

    # Internal logging helper in the harness — wraps stderr writes.
    re.compile(r"^harness\.import_one\._log$"),

    # Cleanup orchestration that fires shell rm / DB delete; equivalent
    # to a subprocess + DB-mutation seam. The replacement
    # (FakePipelineDB + temp-dir filesystem) is feasible but not always
    # worth the setup cost for tests that aren't testing cleanup itself.
    re.compile(r"^lib\.import_dispatch\._cleanup_staged_dir$"),
    re.compile(r"^lib\.import_dispatch\.cleanup_disambiguation_orphans$"),

    # BeetsDB class itself — patching the class replaces the SQLite
    # boundary at the constructor. Specific methods whose bodies are
    # pure SQLite/filesystem work are also seams. Other BeetsDB methods
    # (album_exists, locate, search, etc.) are read-only query helpers
    # that can be exercised against a real test SQLite DB.
    re.compile(r"^lib\.beets_db\.BeetsDB$"),
    re.compile(r"^lib\.beets_db\.BeetsDB\.delete_album$"),  # SQLite write + file delete seam
    # ``web.server._real_beets_db`` is already covered by the broader
    # ``^web\.server\.(...|_real_beets_db|...)`` pattern higher up
    # (MusicBrainz / Discogs / beets module-level boundaries).

    # PipelineDB class itself — patching the class replaces the
    # PostgreSQL boundary at the constructor. Per-method patches against
    # PipelineDB.<method> stay flagged (FakePipelineDB is the right
    # replacement); the class entry is for tests that swap the
    # constructor wholesale (e.g. ``patch("scripts.X.PipelineDB",
    # return_value=fake_db)``).
    re.compile(r"^lib\.pipeline_db\.PipelineDB$"),
    re.compile(r"^scripts\.\w+\.PipelineDB$"),

    # web.server.db — module-level pipeline DB connection cache.
    # Tests patch.object(server, "db", fake) to inject a per-test DB.
    # Equivalent to the constructor-replacement pattern.
    re.compile(r"^web\.server\.db$"),

    # web.routes re-exports of allowlisted helpers. Same physical
    # function lives in lib.* and is allowlisted there; tests just
    # patch the import binding inside the route module.
    re.compile(r"^web\.routes\.\w+\.resolve_failed_path$"),
    re.compile(r"^web\.routes\.pipeline\.hash_audio_content$"),
    re.compile(r"^web\.routes\.imports\.scan_complete_folder$"),

    # Route-to-transition DI seam. ``web.routes.pipeline.finalize_request``
    # is the module-level swap point for ``transitions.finalize_request``;
    # routes call it through this binding so tests can inject a recorder
    # or no-op without monkey-patching ``lib.transitions``. Same shape as
    # the ``web.server.db`` constructor-replacement entry above — this is
    # how route-scope DI is expressed in this codebase, since route
    # handlers are dispatched by URL and don't take dependency kwargs.
    re.compile(r"^web\.routes\.pipeline\.finalize_request$"),

    # Route-to-service DI seam. ``cleanup_all_wrong_matches`` triggers
    # real DB mutations + filesystem deletes via the wrong-match cleanup
    # service. Service behaviour is tested in
    # ``tests/test_wrong_matches_cleanup.py``; the contract tests in
    # ``tests/web/test_routes_imports.py`` pin the HTTP wire shape (status code,
    # JSON fields, response summary). Patching the route-module binding
    # keeps those contract tests focused on the wire shape.
    re.compile(r"^web\.routes\.imports\.cleanup_all_wrong_matches$"),

    # Web-server module-level swap, same pattern as ``web.server.db``.
    # ``compute_library_rank`` is the in-library rank-badge producer
    # (codec-aware tier label). Tests in ``tests/web/`` patch
    # it via ``side_effect`` to stamp deterministic rank labels into
    # browse / label / artist responses without setting up a real
    # rank-config + beets album fixture for every contract test.
    re.compile(r"^web\.server\.compute_library_rank$"),

    # Module-local DI seams for ``transitions.finalize_request``. Each
    # calling module binds ``finalize_request = transitions.finalize_request``
    # at import time so tests swap the dependency on the route/CLI/harness/
    # dispatch module rather than on ``lib.transitions``. Same shape as
    # ``web.routes.pipeline.finalize_request`` above — route handlers
    # and CLI subcommands are dispatched without keyword args, so
    # module-attribute swap is the established DI shape in this codebase.
    re.compile(r"^lib\.import_dispatch\.finalize_request$"),
    re.compile(r"^harness\.import_one\.finalize_request$"),
    re.compile(r"^scripts\.pipeline_cli\.finalize_request$"),
    re.compile(r"^scripts\.repair\.finalize_request$"),

    # ``lib.download`` formerly had module-local DI seams for the chain
    # ``poll_active_downloads`` → ``process_completed_album`` →
    # ``_process_beets_validation`` → ``_handle_valid_result`` →
    # ``dispatch_import_core``. The chain now exposes opt-in kwarg DI
    # (``validate_fn``, ``handle_valid_fn``, ``dispatch_fn``) on each
    # downstream step; tests pass stubs by value. Defensive guards
    # against future regressions assert on observable state (no new
    # ``import_jobs`` row, no ``download_log`` entry) rather than
    # patching the production binding.

    # ``scripts.repair._collect_issues`` is the argparse-dispatched CLI
    # aggregator (``cmd_fix`` / ``cmd_scan`` call it without an injection
    # path). The orphan and blocked-recovery helpers it composes are
    # injected via kwarg DI (``find_orphaned_fn`` / ``find_blocked_recovery_fn``);
    # tests pass stubs by value, so only ``_collect_issues`` itself
    # retains the module-local seam shape.
    re.compile(r"^scripts\.repair\._collect_issues$"),

    # Service-class constructor replacement. ``MbidReplaceService`` is
    # the operator's MBID-replace surface (CLI + web route both wrap
    # it). The service's own behaviour is covered in
    # ``tests/test_mbid_replace_service.py``; the CLI test in
    # ``test_pipeline_cli.py`` only asserts the wire-shape mapping
    # (exit code per outcome). Same constructor-replacement shape as
    # ``lib.beets_db.BeetsDB`` / ``lib.pipeline_db.PipelineDB`` above.
    re.compile(r"^lib\.mbid_replace_service\.MbidReplaceService$"),

    # ``scripts.import_preview_worker.run_once`` is the preview-worker
    # tick. Tests in ``test_import_queue.py`` stub it to drive the
    # outer loop without going through full preview measurement on
    # every iteration. Worker behaviour is covered by its own dedicated
    # tests; queue tests are about the dispatcher around it.
    re.compile(r"^scripts\.import_preview_worker\.run_once$"),

    # ``lib.download._handle_valid_result`` migrated to kwarg DI alongside
    # the rest of the lib.download chain (see comment above).

    # Broadened ``resolve_failed_path`` re-export allowlist. The pattern
    # already covers ``web.routes.*`` re-exports; ``lib.wrong_matches``
    # also re-exports ``resolve_failed_path`` from ``lib.util``. Same
    # rationale: ``lib.util.resolve_failed_path`` is the actual
    # filesystem boundary; the re-export is the test seam.
    re.compile(r"^lib\.\w+\.resolve_failed_path$"),

    # ``harness.import_one`` RED-guard seams. The test
    # ``test_evidence_backed_import_skips_candidate_measurement_helpers``
    # patches these three pure-decision helpers with ``side_effect=AssertionError``
    # to assert NONE of them run when pre-recorded evidence is supplied.
    # The patch is a regression guard, not a stub — if any helper runs,
    # the test trips. The decisions themselves are tested in
    # ``test_quality_classification.py``.
    re.compile(r"^harness\.import_one\.determine_verified_lossless$"),
    re.compile(r"^harness\.import_one\.provisional_lossless_decision$"),
    re.compile(r"^harness\.import_one\.quality_decision_stage$"),

    # Filesystem-write wrapper. ``log_validation_result`` (defined in
    # ``lib.util``) appends to the beets-tracking JSONL file — a thin
    # filesystem-boundary helper. Tests in ``test_download.py`` patch
    # the ``lib.download`` re-export to skip the write side effect.
    re.compile(r"^lib\.\w+\.log_validation_result$"),

    # Service-layer DI seam (mirrors ``cleanup_all_wrong_matches`` above).
    # ``cleanup_wrong_match`` triggers DB mutations + filesystem deletes;
    # behaviour is tested in ``tests/test_wrong_match_cleanup_service.py``.
    # Tests that exercise the post-rejection triage path in
    # ``lib.download`` stub the service so the wrapper-layer assertion
    # stays focused.
    re.compile(r"^lib\.wrong_match_cleanup_service\.cleanup_wrong_match$"),

    # Deleted-shim regression guard. ``check_beets_by_artist_album``
    # was removed in issue #123; tests patch it with create=True to
    # ensure it stays gone (the patch acts as a RED guard against
    # accidental reintroduction).
    re.compile(r"^web\.server\.check_beets_by_artist_album$"),

    # MusicBrainz / Discogs API fetch helpers — HTTP boundary.
    re.compile(r"^scripts\.pipeline_cli\.fetch_mb_release$"),
    re.compile(r"^scripts\.pipeline_cli\.fetch_mb_release_group_year$"),
    re.compile(r"^lib\.\w+\.fetch_mb_release$"),

    # scripts.pipeline_cli loaders — each is a thin wrapper around a
    # disk/SQLite read in lib.config or lib.beets_db.
    re.compile(r"^scripts\.pipeline_cli\._load_runtime_rank_config$"),
    re.compile(r"^scripts\.pipeline_cli\._load_runtime_verified_lossless_target$"),
    re.compile(r"^scripts\.pipeline_cli\._load_beets_album_info$"),
    re.compile(r"^scripts\.pipeline_cli\._resolve_failed_path$"),

    # scripts.repair helpers that wrap external boundaries.
    # ``_get_slskd_active_transfers`` is a thin slskd_api call;
    # ``_get_all_rows`` runs a single SELECT against the pipeline DB.
    re.compile(r"^scripts\.repair\._get_slskd_active_transfers$"),
    re.compile(r"^scripts\.repair\._get_all_rows$"),

    # DB connection reconnect — network/socket boundary.
    re.compile(r"^web\.server\._try_reconnect_db$"),
]


def _is_leaf_seam(target: str) -> bool:
    for pat in _LEAF_SEAM_PATTERNS:
        if pat.search(target):
            return True
    return False


def _is_repo_target(target: str) -> bool:
    return (
        target.startswith("lib.")
        or target.startswith("web.")
        or target.startswith("scripts.")
        or target.startswith("harness.")
        or target.startswith("cratedigger.")
    )


def scan_file(path: str) -> Dict[str, int]:
    """Return ``{finding_key: count}`` for one test file.

    Finding keys are stable (no line numbers) so the baseline survives
    line shifts from refactors.
    """
    counts: Dict[str, int] = defaultdict(int)
    with open(path, encoding="utf-8") as f:
        for line in f:
            if _STATEFUL_ASSIGN_RE.match(line):
                # Group findings by the assigned name so the baseline is
                # informative when shrinking.
                m = _STATEFUL_ASSIGN_RE.match(line)
                assert m is not None
                counts[f"stateful_mock_assign:{m.group(1)}"] += 1
            for pm in _PATCH_RE.finditer(line):
                target = pm.group(1)
                if not _is_repo_target(target):
                    continue
                if _is_leaf_seam(target):
                    continue
                counts[f"patch:{target}"] += 1
            # patch.object(MODULE_REF, "name", ...) form — the first arg
            # is an identifier (typically a module alias from imports);
            # we resolve it against _ALIAS_TO_CANONICAL to recover the
            # canonical patch target ``<canonical>.<name>``. Unknown
            # aliases are reported verbatim so they show up in the
            # baseline and either land on the allowlist or get migrated.
            for pom in _PATCH_OBJECT_RE.finditer(line):
                module_ref = pom.group(1)
                attr_name = pom.group(2)
                canonical = _ALIAS_TO_CANONICAL.get(module_ref, module_ref)
                target = f"{canonical}.{attr_name}"
                if not _is_repo_target(target):
                    continue
                if _is_leaf_seam(target):
                    continue
                counts[f"patch:{target}"] += 1
    return dict(counts)


def iter_scan_paths():
    """Yield ``(relpath, abspath)`` for every file the audit scans.

    Recursive walk since #408 so subpackages (``tests/web/``) stay under
    audit. Exclusions are keyed by relpath, not basename — a future
    ``tests/web/test_mock_audit.py`` must NOT inherit the self-test's
    exemption. ``web/_harness.py`` is scanned despite the underscore
    prefix: the shared HTTP harness was audited for its whole life inside
    ``test_web_server.py``, and the split must not relax that.
    """
    for dirpath, dirnames, filenames in os.walk(TESTS_DIR):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for fname in sorted(filenames):
            if not fname.endswith(".py"):
                continue
            path = os.path.join(dirpath, fname)
            rel = os.path.relpath(path, TESTS_DIR)
            if rel == "test_mock_audit.py":
                continue  # mentions the patterns in its strings
            if fname.startswith("_") and rel != os.path.join("web", "_harness.py"):
                continue  # this scanner module itself, helpers, etc.
            yield rel, path


def scan_tree() -> Dict[str, Dict[str, int]]:
    """Return ``{relpath: {finding_key: count}}`` for every test file."""
    result: Dict[str, Dict[str, int]] = {}
    for rel, path in iter_scan_paths():
        counts = scan_file(path)
        if counts:
            result[rel] = counts
    return result


# === Web-harness MagicMock ratchet (#430) ===
#
# The tests/web contract harness historically injected a MagicMock
# pipeline DB (now ``MagicMock(wraps=FakePipelineDB)``), so contract
# tests pass whenever the mock's shape matches the assertion's shape —
# production query semantics never enter the loop. Issue #430 migrates
# the harness and every per-route test to a bare ``FakePipelineDB``,
# route-module by route-module.
#
# This ratchet pins the per-file count of remaining MagicMock-harness
# usage OCCURRENCES: every ``mock_db`` reference in a ``tests/web``
# file (dotted, aliased, passed bare — aliasing the mock away is the
# evasion the occurrence count exists to close) plus every
# ``_pipeline_db_test_harness`` reference in ANY scanned test file
# (the transitional wrapped-MagicMock constructor must not leak
# outside tests/web where the per-file mock_db count can't see it).
# The audit test requires the live counts to match this baseline
# EXACTLY:
#
# - a count above baseline fails — new tests must seed FakePipelineDB
#   state instead of configuring mock returns, even in unmigrated files
# - a count below baseline fails — shrink the entry here in the same
#   commit so the baseline always reflects reality
# - a file at zero must have NO entry; delete the line when a module
#   finishes migrating
#
# The migration is done when this dict is empty.

_WEB_HARNESS_MOCK_DB_RE = re.compile(r"\bmock_db\b")
_HARNESS_CTOR_RE = re.compile(r"\b_pipeline_db_test_harness\b")

WEB_HARNESS_MOCK_BASELINE: Dict[str, int] = {
    os.path.join("web", "_harness.py"): 39,
    os.path.join("web", "test_routes_imports.py"): 71,
    os.path.join("web", "test_routes_pipeline.py"): 66,
    os.path.join("web", "test_routes_pipeline_mutations.py"): 100,
}


def count_harness_overrides(text: str, *, web_file: bool) -> int:
    """Count MagicMock-harness usage occurrences in one file's text.

    ``mock_db`` references only count inside ``tests/web`` files (the
    name has no meaning elsewhere); ``_pipeline_db_test_harness``
    references count everywhere the scanner walks.
    """
    n = len(_HARNESS_CTOR_RE.findall(text))
    if web_file:
        n += len(_WEB_HARNESS_MOCK_DB_RE.findall(text))
    return n


def scan_web_harness_overrides() -> Dict[str, int]:
    """Count MagicMock-harness usage occurrences per test file."""
    counts: Dict[str, int] = {}
    web_prefix = "web" + os.sep
    for rel, path in iter_scan_paths():
        with open(path, encoding="utf-8") as f:
            n = count_harness_overrides(
                f.read(), web_file=rel.startswith(web_prefix))
        if n:
            counts[rel] = n
    return counts
