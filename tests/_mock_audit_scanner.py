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

# patch(...) / @patch(...) / patch.object(target_module, "name") / with patch(...)
# We match either the string-form path ("lib.x.y") or attribute-form
# (target_module="lib.x"). Both forms appear in this repo.
_PATCH_RE = re.compile(r'\bpatch(?:\.object)?\s*\(\s*["\']([^"\']+)["\']')

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

    # Beets harness subprocess wrapper. ``beets_validate`` invokes
    # ``run_beets_harness.sh`` and parses JSON — equivalent to mocking
    # a subprocess seam.
    re.compile(r"^lib\.beets\.beets_validate$"),

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
    re.compile(r"^lib\.spectral_check\.analyze_track$"),

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
    re.compile(r"^lib\.config\.CratediggerConfig\.from_ini$"),

    # Filesystem permission helper — wraps chmod calls.
    re.compile(r"^lib\.permissions\.fix_library_modes$"),

    # Logger objects — patching the module-level logger lets tests
    # assert against log records without subclassing the logger.
    re.compile(r"^lib\.\w+\.logger$"),
    re.compile(r"^harness\.\w+\.logger$"),
    re.compile(r"^web\.\w+\.logger$"),

    # Internal logging helper in the harness — wraps stderr writes.
    re.compile(r"^harness\.import_one\._log$"),

    # Cleanup orchestration that fires shell rm / DB delete; equivalent
    # to a subprocess + DB-mutation seam. The replacement
    # (FakePipelineDB + temp-dir filesystem) is feasible but not always
    # worth the setup cost for tests that aren't testing cleanup itself.
    re.compile(r"^lib\.import_dispatch\._cleanup_staged_dir$"),
    re.compile(r"^lib\.import_dispatch\.cleanup_disambiguation_orphans$"),

    # BeetsDB class itself — patching the class replaces the SQLite
    # boundary at the constructor. Method-level patches against
    # BeetsDB.<method> remain flagged so they get migrated to FakeBeetsDB.
    re.compile(r"^lib\.beets_db\.BeetsDB$"),
    re.compile(r"^web\.server\._real_beets_db$"),

    # MusicBrainz / Discogs API fetch helpers — HTTP boundary.
    re.compile(r"^scripts\.pipeline_cli\.fetch_mb_release$"),
    re.compile(r"^lib\.\w+\.fetch_mb_release$"),

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
    return dict(counts)


def scan_tree() -> Dict[str, Dict[str, int]]:
    """Return ``{relpath: {finding_key: count}}`` for every test file."""
    result: Dict[str, Dict[str, int]] = {}
    for fname in sorted(os.listdir(TESTS_DIR)):
        if not fname.endswith(".py"):
            continue
        if fname.startswith("_"):
            continue  # this scanner module itself, helpers, etc.
        if fname == "test_mock_audit.py":
            continue  # mentions the patterns in its strings
        path = os.path.join(TESTS_DIR, fname)
        counts = scan_file(path)
        if counts:
            result[fname] = counts
    return result
