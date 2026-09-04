"""Shared scanner for the stateful-MagicMock audit.

See CLAUDE.md § "Mocks: leaf-seam only" and issue #290.

The heuristic flags three anti-patterns:

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

3. **Retired web MagicMock harness names.** ``tests/web`` must use
   ``FakePipelineDB`` and ``FakeBeetsDB`` state, never the old ``mock_db`` /
   beets-mock names. The deleted ``_pipeline_db_test_harness`` constructor is
   forbidden throughout the scanned test tree.

Patch calls split across physical lines are parsed through the AST. Existing
multiline debt is held to an exact target-count baseline; new occurrences fail
and removals must shrink the baseline.

The scanner returns a dict ``{relpath: {finding_key: count}}``.
"""

from __future__ import annotations

import ast
import io
import os
import re
import tokenize
from collections import Counter, defaultdict

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

# Direct zero-tolerance names left behind by the completed #430 and #445
# migrations. These remain deliberately lexical: the names themselves encode
# the retired test shapes, while current fakes use ``self.db`` /
# ``self.beets_db``.
_WEB_HARNESS_MOCK_DB_RE = re.compile(r"\bmock_db\b")
_HARNESS_CTOR_RE = re.compile(r"\b_pipeline_db_test_harness\b")
_WEB_BEETS_MOCK_RE = re.compile(
    r"\bmock_beets\w*|self\._beets\b|self\.beets\b")

# Module aliases the audit knows how to resolve to canonical paths.
# Keep narrow — false positives become audit noise. Detected by
# scanning each file's import statements before classification.
_ALIAS_TO_CANONICAL = {
    "cratedigger": "cratedigger",
    "enqueue_module": "lib.enqueue",
    "dl_mod": "lib.download",
    "dp_mod": "lib.download_processing",
    "srv": "web.server",
    "server": "web.server",
    # ``patch.object(WebRuntime, "beets_db", ...)`` is the #1313 successor
    # to ``patch.object(srv, "_beets_db", ...)``. Without this row the
    # target resolves to a bare ``WebRuntime.beets_db``, which
    # ``_is_repo_target`` rejects — every such site would silently leave
    # the audit's view.
    "WebRuntime": "web.runtime.WebRuntime",
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
    re.compile(r"\.os\.(remove|rename|makedirs|mkdir|listdir|stat|write|unlink|rmdir|getcwd|getpgid|killpg|kill|chmod|symlink|umask)$"),
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
    # MusicBrainz / Discogs client objects on the web side
    re.compile(r"^web\.(mb|discogs)\."),
    re.compile(r"^web\.routes\.\w+\.(mb_api|discogs_api)"),
    re.compile(r"^web\.server\.(mb_api|discogs_api|mb)"),
    # The same overlay/handle family after #1313 moved it from module
    # functions on web.server onto the frozen runtime.
    re.compile(
        r"^web\.runtime\.WebRuntime\.(beets_db|check_beets_library"
        r"|check_beets_library_detail|check_pipeline|get_library_artist)$"
    ),
    # Notifier helpers — fire-and-forget, no return value to mock meaningfully
    re.compile(r"lib\.util\.trigger_(plex|jellyfin)_scan$"),
    # NOTE: lib.library_delete_notifiers.notify_library_delete is
    # deliberately NOT allowlisted here (issue #1203 item 2 review). It grew
    # real escalation-decision logic (Plex ancestor walk with
    # allow_escalation-gated root-scan refusal, Jellyfin identity lookup +
    # report shaping), well past code-quality.md's "thin wrapper... at most
    # ten lines" leaf-seam allowlist bound.
    # dispatch-level tests inject a recorder through
    # ``dispatch_import_core(media_server_notify_fn=...)`` instead — a
    # kwarg-DI seam, not a module patch — so this scanner never needs to see
    # it at all.
    # Thin urllib GET wrapper (documented "Network leaf seam") — patched so
    # dispatch slices exercise the REAL jellyfin find/children code paths.
    re.compile(r"lib\.util\._jellyfin_get_json$"),
    re.compile(r"lib\.util\.(sp|urllib|os|shutil)\."),
    re.compile(r"lib\.util\.repair_mp3_headers$"),
    re.compile(r"\.trigger_(plex|jellyfin)_scan$"),
    # Curated quarantine mover: the function is a filesystem leaf whose
    # contract is covered separately; generated writer properties patch the
    # move while exercising the real rejection and cleanup orchestration.
    re.compile(r"^lib\.download_rejection\.move_failed_import_curated$"),
    # audio_corrupt's outright-delete helper: a thin ``shutil.rmtree`` /
    # ``os.rmdir`` wrapper (issue #1077, F4) — patching it to raise is the
    # most direct way to prove a failed delete never blocks the rejection
    # record, without reaching into the filesystem itself.
    re.compile(r"^lib\.download_rejection\._cleanup_staged_dir$"),
    # builtins / stdlib
    re.compile(r"^builtins\."),
    re.compile(r"\.print$"),
    re.compile(r"^json\."),
    re.compile(r"\.select\.select$"),  # select.select syscall
    re.compile(r"^socket\.socket$"),
    # Cratedigger entry-point shims (the top-level cratedigger.py wrapper
    # functions are thin and patched on a per-test basis; the real ones
    # live in lib/* and have their own audit coverage)
    re.compile(r"^cratedigger\.(_create_slskd_client|sp|urllib)"),

    # === Thin seam-wrapper functions in lib/ ===
    # These are functions whose body is mostly "construct args and
    # dispatch to a network/subprocess/filesystem call." Patching them
    # is the most ergonomic point to mock the underlying seam — the
    # alternative (mocking the slskd HTTP client / sox subprocess / harness
    # subprocess at its own boundary) often requires elaborate per-test
    # fixture setup for no additional coverage. Each entry below has a
    # rationale.

    # slskd network wrappers. Each forwards to the slskd client and lightly
    # transforms the result; mocking them is morally equivalent to
    # mocking the HTTP boundary directly.
    re.compile(r"^lib\.enqueue\._fanout_browse_users$"),
    re.compile(r"^lib\.enqueue\.slskd_do_enqueue$"),
    re.compile(r"^lib\.enqueue\.slskd_enqueue_with_outcome$"),
    re.compile(r"^lib\.(download|enqueue)\.cancel_and_delete$"),
    # Beets harness subprocess wrapper. ``beets_validate`` invokes
    # ``run_beets_harness.sh`` and parses JSON — equivalent to mocking
    # a subprocess seam.
    re.compile(r"^lib\.beets\.beets_validate$"),

    # ``parse_import_result`` is the parsed-output side of the SAME
    # harness subprocess: scans import_one.py's stdout for the
    # ``__IMPORT_RESULT__`` sentinel and decodes it via msgspec.
    # Patching it is morally equivalent to constructing a fake harness
    # stdout — same wire-boundary seam as ``beets_validate``.
    re.compile(r"^lib\.dispatch\.subprocess_runner\.parse_import_result$"),

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
    # Direct ffprobe wrapper used to distinguish AAC from ALAC in M4A files.
    re.compile(r"^lib\.measurement\.ffprobe_audio_codec_name$"),
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
    # Deployment path table, not logic: the tuple of bases a legacy
    # RELATIVE failed_path is resolved against. Issue #1063's relative-row
    # pin needs a real temp base to build a real EACCES world, and the
    # helper under test deliberately takes no search-dirs parameter
    # (adding one would be production API existing only for tests).
    re.compile(r"^lib\.util\.FAILED_IMPORT_SEARCH_DIRS$"),
    re.compile(r"^scripts\.\w+\.read_runtime_config$"),  # re-exports
    # Route modules bind the loader at import time, so the ``lib.config``
    # patch above cannot reach them — the binding IS the seam (#1063).
    re.compile(r"^web\.routes\.\w+\.read_runtime_config$"),
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
    # ffprobe codec probes over the source folder (same subprocess seam
    # as the _probe_* helpers above).
    re.compile(r"^harness\.import_one\._detect_source_format$"),
    re.compile(r"^harness\.import_one\._detect_native_codec_family$"),
    re.compile(r"^harness\.import_one\.BeetsDB$"),  # class replacement, see lib.beets_db.BeetsDB

    # Album-level spectral analysis — same sox/ffmpeg seam as
    # analyze_track, just aggregating across an album's tracks.
    re.compile(r"^lib\.spectral_check\.analyze_album$"),
    # Two-sided attempt audit — forwards to analyze_spectral_audit_path
    # (the same sox/ffmpeg seam) once per side.
    re.compile(r"^lib\.measurement\.collect_attempt_spectral_audit$"),

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
    # Top-level entrypoint logger is the same assertion-only logging seam.
    re.compile(r"^cratedigger\.logger\.(error|warning|exception|info|debug)$"),

    # Internal logging helper in the harness — wraps stderr writes.
    re.compile(r"^harness\.import_one\._log$"),

    # Cleanup orchestration that fires shell rm / DB delete; equivalent
    # to a subprocess + DB-mutation seam. The replacement
    # (FakePipelineDB + temp-dir filesystem) is feasible but not always
    # worth the setup cost for tests that aren't testing cleanup itself.
    # ``_cleanup_staged_dir`` has two call sites after the #139 split
    # (core + outcome_actions); both bindings are allowlisted.
    re.compile(r"^lib\.dispatch\.core\._cleanup_staged_dir$"),
    re.compile(r"^lib\.dispatch\.outcome_actions\._cleanup_staged_dir$"),

    # BeetsDB class itself — patching the class replaces the SQLite
    # boundary at the constructor. Specific methods whose bodies are
    # pure SQLite/filesystem work are also seams. Other BeetsDB methods
    # (album_exists, locate, search, etc.) are read-only query helpers
    # that can be exercised against a real test SQLite DB.
    re.compile(r"^lib\.beets_db\.BeetsDB$"),
    # PipelineDB class itself — patching the class replaces the
    # PostgreSQL boundary at the constructor. Per-method patches against
    # PipelineDB.<method> stay flagged (FakePipelineDB is the right
    # replacement); the class entry is for tests that swap the
    # constructor wholesale (e.g. ``patch("scripts.X.PipelineDB",
    # return_value=fake_db)``).
    re.compile(r"^lib\.pipeline_db\.PipelineDB$"),
    # ``scripts.pipeline_cli.cli.PipelineDB`` (one extra dotted segment,
    # since #495 split pipeline_cli.py into a package — ``main()``, the
    # only caller, now lives in ``scripts/pipeline_cli/cli.py``) alongside
    # single-segment scripts like ``scripts.repair.PipelineDB``.
    re.compile(r"^scripts\.\w+(\.\w+)?\.PipelineDB$"),
    # The web entrypoint imports the same PipelineDB constructor directly;
    # replacing that binding stops at the PostgreSQL connection boundary.
    re.compile(r"^web\.server\.PipelineDB$"),

    # psycopg's connector is the database socket leaf. Startup placement tests
    # stop there after exercising the real contract and schema-gate code.
    re.compile(r"^lib\.migrator\.psycopg2\.connect$"),

    # web.routes re-exports of allowlisted helpers. Same physical
    # function lives in lib.* and is allowlisted there; tests just
    # patch the import binding inside the route module.
    re.compile(r"^web\.routes\.\w+\.observe_failed_path$"),
    # Same re-export, one module over: the wrong-match queue projection
    # (#1278 extraction from web/routes/imports.py) carries its own
    # binding of the ``lib.util.observe_failed_path`` filesystem leaf.
    re.compile(r"^web\.wrong_match_queue_view\.observe_failed_path$"),
    # Destructive service binding around the ffmpeg-backed audio hash leaf.
    re.compile(r"^lib\.destructive_release_service\.hash_audio_content$"),

    # Route-to-transition DI seam. ``web.routes.pipeline_mutations.finalize_request``
    # is the module-level swap point for ``transitions.finalize_request``;
    # routes call it through this binding so tests can inject a recorder
    # or no-op without monkey-patching ``lib.transitions``. Same shape as
    # the ``web.server.db`` constructor-replacement entry above — this is
    # how route-scope DI is expressed in this codebase, since route
    # handlers are dispatched by URL and don't take dependency kwargs.
    re.compile(r"^web\.routes\.pipeline_mutations\.finalize_request$"),

    # Route-to-service DI seam. ``cleanup_all_wrong_matches`` triggers
    # real DB mutations + filesystem deletes via the wrong-match cleanup
    # service. Service behaviour is tested in
    # ``tests/test_wrong_matches_cleanup.py``; the contract tests in
    # ``tests/web/test_routes_imports.py`` pin the HTTP wire shape (status code,
    # JSON fields, response summary). Patching the route-module binding
    # keeps those contract tests focused on the wire shape.
    re.compile(r"^web\.routes\.imports\.cleanup_all_wrong_matches$"),

    # Route-to-owner DI seam. ``web.routes.browse.parallel_results`` is the
    # module-level binding for ``web.parallel_fanout.parallel_results``
    # (issue #1355 WE5's shared fan-out lifecycle owner — also used by
    # ``web.mb``/``web.discogs``, already leaf-exempt there via the blanket
    # ``web.(mb|discogs).`` pattern above). ``web.routes.browse`` has no
    # such blanket exemption, so the binding needs its own entry. The
    # owner's own lifecycle (success, cancel-on-exception, shutdown
    # ordering) is pinned directly in ``tests/test_web_parallel_fanout.py``;
    # this route module's seam test only proves it reaches that owner
    # rather than a private per-module copy.
    re.compile(r"^web\.routes\.browse\.parallel_results$"),

    # Module-local DI seams for ``transitions.finalize_request``. Each
    # calling module binds ``finalize_request = transitions.finalize_request``
    # at import time so tests swap the dependency on the route/CLI/harness/
    # dispatch module rather than on ``lib.transitions``. Same shape as
    # ``web.routes.pipeline_mutations.finalize_request`` above — route handlers
    # and CLI subcommands are dispatched without keyword args, so
    # module-attribute swap is the established DI shape in this codebase.
    # ``lib.dispatch.outcome_actions`` had one until issue #1355 item A1
    # routed its sole caller through the atomic job-less success bundle
    # instead, deleting the binding along with it.
    re.compile(r"^harness\.import_one\.finalize_request$"),
    # scripts/pipeline_cli.py split into a package (#495) — the single
    # module-level binding split into two independent copies, one per
    # command-family module that calls ``finalize_request``.
    re.compile(r"^scripts\.pipeline_cli\.album_requests\.finalize_request$"),
    re.compile(r"^scripts\.pipeline_cli\.quality\.finalize_request$"),
    re.compile(r"^scripts\.repair\.finalize_request$"),

    # ``lib.download`` formerly had module-local DI seams for the chain
    # ``poll_active_downloads`` → ``process_completed_album`` →
    # ``download_validation._process_beets_validation`` →
    # ``_handle_valid_result`` →
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

    # ``lib.download_validation._handle_valid_result`` uses kwarg DI alongside
    # the rest of the lib.download chain (see comment above).

    # Broadened ``observe_failed_path`` re-export allowlist. The pattern
    # already covers ``web.routes.*`` re-exports; ``lib.wrong_matches``
    # also re-exports ``observe_failed_path`` from ``lib.util``. Same
    # rationale: ``lib.util.observe_failed_path`` is the actual
    # filesystem boundary (issue #1063 renamed it from
    # ``resolve_failed_path`` when it started returning a typed
    # observation instead of ``str | None``); the re-export is the test
    # seam.
    re.compile(r"^lib\.\w+\.observe_failed_path$"),

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
    re.compile(r"^web\.runtime\.WebRuntime\.check_beets_by_artist_album$"),

    # MusicBrainz / Discogs API fetch helpers — HTTP boundary.
    re.compile(r"^scripts\.pipeline_cli\.album_requests\.fetch_mb_release$"),
    re.compile(r"^lib\.\w+\.fetch_mb_release$"),
    # MusicBrainz merge-survivor resolver (#1089) — ``canonical_release_id``
    # is one hop above the raw ``urllib.request`` fetch, same shape as
    # ``fetch_mb_release`` above. ``canonical_release_status`` (the tagged
    # variant, #1089 BLOCKING-1) is NOT allowlisted here — it is ~50 lines
    # of real decision logic, not a thin forwarder (NOTE-2, review round 2)
    # — tests that need to bypass the network patch the TRUE external edge
    # below, ``_fetch_json``, instead.
    re.compile(r"^lib\.mb_canonical\.canonical_release_id$"),
    re.compile(r"^lib\.mb_canonical\._fetch_json$"),

    # scripts.pipeline_cli loaders — each is a thin wrapper around a
    # disk/SQLite read in lib.config or lib.beets_db. Split into
    # scripts/pipeline_cli/quality.py and imports.py (#495).
    re.compile(r"^scripts\.pipeline_cli\.quality\._load_runtime_rank_config$"),
    re.compile(r"^scripts\.pipeline_cli\.quality\._load_runtime_verified_lossless_target$"),
    re.compile(r"^scripts\.pipeline_cli\.quality\._load_beets_album_info$"),
    # Recovery CLI Beets opener: filesystem/SQLite adapter boundary.
    re.compile(r"^scripts\.pipeline_cli\.imports\._open_recovery_beets$"),

    # scripts.repair helpers that wrap external boundaries.
    # ``_fetch_slskd_downloads`` is a thin slskd HTTP call (#479 rename of
    # the former ``_get_slskd_active_transfers``, which flattened to pairs
    # inline — now split so the raw snapshot survives for
    # ``find_slskd_orphans`` too); ``_get_all_rows`` runs a single SELECT
    # against the pipeline DB.
    re.compile(r"^scripts\.repair\._fetch_slskd_downloads$"),
    re.compile(r"^scripts\.repair\._get_all_rows$"),

    # DB connection reconnect — network/socket boundary.
    re.compile(r"^web\.runtime\.WebRuntime\.drop_thread_db$"),
]


# The original patch audit scanned one physical line at a time, so a
# ``patch(\n    "lib.owned.function"\n)`` call evaded it entirely. Switching
# the zero-tolerance audit to AST discovery in one step would surface this
# pre-existing debt across the suite. This exact target-count ratchet closes
# the evasion now: any new multiline owned-function patch fails, and every
# removal must shrink this baseline. New code gets no allowance.
MULTILINE_PATCH_BASELINE: dict[str, int] = {
    'lib.beets_distance.compute_beets_distance': 4,
    'lib.convergence.import_module': 1,
    'lib.dispatch.dispatch_import_from_db': 12,
    'lib.dispatch.entry_points.ensure_candidate_evidence_for_action': 3,
    'lib.download._run_completed_processing': 5,
    'lib.download_validation._handle_valid_result': 1,
    'lib.download_materialization._materialize_processing_dir': 1,
    'lib.download_processing.process_completed_album': 9,
    'lib.import_evidence.ensure_current_evidence_for_action': 6,
    'lib.import_evidence.load_or_backfill_current_evidence': 1,
    'lib.matching._browse_directories': 1,
    'lib.mbid_replace_service.MbidReplaceService.replace_request_mbid': 3,
    'lib.mbid_replace_service.delete_wrong_match_group': 9,
    'lib.merge_rekey_service.MergeRekeyService.rekey_request': 1,
    'lib.quality.full_pipeline_decision_from_evidence': 1,
    'lib.quarantine_triage_service.os.scandir': 1,
    # Leaf filesystem boundary: proves the dashboard's census card reads
    # OSError (denied permissions, IsADirectoryError, ...) as "unreadable"
    # rather than 500ing the whole dashboard (#1142 review N4).
    'lib.retag_divergence_census_snapshot.open': 1,
    'lib.search_plan_service.SearchPlanService.advance_for_request': 5,
    'lib.search_plan_service.SearchPlanService.generate_for_request': 5,
    'lib.search_plan_service.SearchPlanService.history_for_request': 5,
    # Leaf filesystem boundary, mirroring lib.quarantine_triage_service.os.
    # scandir above: proves lib.retag_divergence_census_snapshot's atomic
    # write never touches a prior published snapshot when the final rename
    # fails (#1142 acceptance 1).
    'lib.sidecar_service.os.replace': 1,
    'lib.wrong_match_cleanup_service.cleanup_all_wrong_matches': 1,
    'lib.wrong_match_cleanup_service.cleanup_wrong_match_source': 6,
    'lib.wrong_match_cleanup_service.full_pipeline_decision_from_evidence': 7,
    'lib.wrong_match_cleanup_service.load_current_evidence_for_action': 1,
    'lib.wrong_match_delete_service.delete_wrong_match': 1,
    'lib.wrong_match_delete_service.delete_wrong_match_group': 1,
    'lib.youtube_ingest_service.YoutubeIngestService.submit': 5,
    'scripts.import_preview_worker.measure_and_persist_candidate_evidence': 14,
    'scripts.pipeline_cli.youtube._RedisYoutubeCache': 2,
    'scripts.pipeline_cli.youtube._build_youtube_client': 2,
    'scripts.pipeline_cli.youtube.resolve_youtube_album': 1,
    'scripts.repair.find_blocked_processing_path_issues': 1,
    'web.routes.imports.cleanup_wrong_match': 1,
    # Moved, not added (#1063): ``import-preview --download-log-id``
    # now executes through the route, so the CLI's single patch of the
    # preview service moved from ``lib.import_preview`` to the route
    # module's own binding. Net count unchanged.
    'web.routes.imports.preview_import_from_download_log': 1,
    'web.routes.imports.delete_wrong_match': 1,
    'web.routes.imports.delete_wrong_match_group': 1,
    'web.routes.youtube._RedisYoutubeCache': 2,
    'web.routes.youtube._build_youtube_client': 2,
    'web.routes.youtube.resolve_youtube_album': 4,
    'web.server.ThreadingHTTPServer': 1,
}


def _is_leaf_seam(target: str) -> bool:
    for pat in _LEAF_SEAM_PATTERNS:
        if pat.search(target):
            return True
    return False


def _is_repo_target(target: str) -> bool:
    return (
        target.startswith(("lib.", "web.", "scripts.", "harness.", "cratedigger."))
    )


def _patch_call_names(tree: ast.AST) -> frozenset[str]:
    """Return the narrowly admitted call names for this source file."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "unittest.mock"
            and any(
                name.name == "patch" and name.asname == "_patch"
                for name in node.names
            )
        ):
            return frozenset(("patch", "_patch"))
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "_patch"
            and isinstance(node.value, ast.Name)
            and node.value.id == "patch"
        ):
            return frozenset(("patch", "_patch"))
    return frozenset(("patch",))


def _patch_targets_by_line(source: str, call_name: str) -> dict[int, list[str]]:
    """Return literal bare-patch targets keyed by their physical line."""
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except tokenize.TokenError:
        return {}
    targets: dict[int, list[str]] = defaultdict(list)
    for index, token in enumerate(tokens[:-2]):
        if token.type != tokenize.NAME or token.string != call_name:
            continue
        if index and tokens[index - 1].string == ".":
            continue
        if tokens[index + 1].string != "(":
            continue
        first_arg = tokens[index + 2]
        if first_arg.type != tokenize.STRING:
            continue
        value = ast.literal_eval(first_arg.string)
        if isinstance(value, str):
            targets[token.start[0]].append(value)
    return dict(targets)


def find_multiline_patch_targets(source: str) -> list[str]:
    """Return dotted targets from bare ``patch`` calls spanning lines.

    AST discovery makes physical-line formatting irrelevant. Dynamic targets
    remain outside this heuristic just as they are outside ``_PATCH_RE``.
    """
    tree = ast.parse(source)
    patch_call_names = _patch_call_names(tree)
    targets: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            not isinstance(node.func, ast.Name)
            or node.func.id not in patch_call_names
        ):
            continue
        if node.end_lineno == node.lineno or not node.args:
            continue
        first_arg = node.args[0]
        if not (
            isinstance(first_arg, ast.Constant)
            and isinstance(first_arg.value, str)
        ):
            continue
        targets.append(first_arg.value)
    return targets


def scan_multiline_patch_targets() -> dict[str, int]:
    """Count non-leaf repo patch targets that evade the line scanner."""
    counts: Counter[str] = Counter()
    for _rel, path in iter_scan_paths():
        with open(path, encoding="utf-8") as source_file:
            source = source_file.read()
        for target in find_multiline_patch_targets(source):
            if _is_repo_target(target) and not _is_leaf_seam(target):
                counts[target] += 1
    return dict(counts)


def scan_source(source: str, *, web_file: bool) -> dict[str, int]:
    """Return grouped mock-audit findings for one source string."""
    counts: dict[str, int] = defaultdict(int)
    try:
        patch_call_names = _patch_call_names(ast.parse(source))
    except SyntaxError:
        patch_call_names = frozenset(("patch",))
    alias_targets_by_line = (
        _patch_targets_by_line(source, "_patch")
        if "_patch" in patch_call_names
        else {}
    )
    for line_number, line in enumerate(source.splitlines(), start=1):
        if _STATEFUL_ASSIGN_RE.match(line):
            # Group findings by assigned name so the failure identifies
            # the stateful collaborator shape.
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
        if "_patch" in patch_call_names:
            for target in alias_targets_by_line.get(line_number, []):
                if not _is_repo_target(target):
                    continue
                if _is_leaf_seam(target):
                    continue
                counts[f"patch:{target}"] += 1
        # patch.object(MODULE_REF, "name", ...) form — the first arg
        # is an identifier (typically a module alias from imports);
        # we resolve it against _ALIAS_TO_CANONICAL to recover the
        # canonical patch target ``<canonical>.<name>``. Unknown
        # aliases are reported verbatim so they can be migrated or
        # deliberately allowlisted.
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

    harness_count = len(_HARNESS_CTOR_RE.findall(source))
    if harness_count:
        counts["retired_pipeline_db_harness"] = harness_count
    if web_file:
        db_count = len(_WEB_HARNESS_MOCK_DB_RE.findall(source))
        if db_count:
            counts["web_mock_db"] = db_count
        beets_count = len(_WEB_BEETS_MOCK_RE.findall(source))
        if beets_count:
            counts["web_beets_mock"] = beets_count
    return dict(counts)


def scan_file(path: str) -> dict[str, int]:
    """Return grouped mock-audit findings for one test file."""
    with open(path, encoding="utf-8") as source_file:
        source = source_file.read()
    rel = os.path.relpath(path, TESTS_DIR)
    return scan_source(source, web_file=rel.startswith("web" + os.sep))


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


def scan_tree() -> dict[str, dict[str, int]]:
    """Return ``{relpath: {finding_key: count}}`` for every test file."""
    result: dict[str, dict[str, int]] = {}
    for rel, path in iter_scan_paths():
        counts = scan_file(path)
        if counts:
            result[rel] = counts
    return result
