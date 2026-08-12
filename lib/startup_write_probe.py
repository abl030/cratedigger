"""Startup write-probe: fail loudly if a unit cannot use a required path.

Issue #1085 -- the container-entrypoint pattern. A container that cannot
write to the directory it needs bails instantly, at deploy time, instead of
discovering the problem twenty minutes later one operation at a time.
Cratedigger had no equivalent: the #570/#578 root -> setgid ``group-users``
cutover needed a durable one-time chown because a tmpfiles ``z``-rule turned
out to be impossible across the ownership transition, and a non-root
preStart separately needed the Discogs token readable -- both found by hand.

This module answers exactly one question, once, at startup: can THIS
process reach the paths it is about to use, the way it is about to use
them?

Deliberately narrow, on purpose -- issue #1085's first draft specified a
role -> capability admission model and was rewritten to cut it back to
this. There is no registry, no capability type, and no generalisation
beyond "here is an exact list of paths; prove each one." A caller builds
that list from its own admitted ``CratediggerConfig`` (or, for the
youtube-ingest worker, its own CLI args) and hands it to
:func:`probe_startup_paths`. ``lib.beets_config_contract`` is the shape
precedent: already role-scoped, already invoked once per unit, and this
module composes with it rather than replacing any of it.

Probe once, immediately after strict runtime/Beets configuration admission,
and before any queue recovery, claim, DB lifecycle mutation, or filesystem
mutation. On failure, raise :class:`StartupProbeError` with a message naming
the unit, the path, the operation, and the errno class -- the caller's job
is only to log it and exit non-zero, exactly like
:class:`lib.beets_startup.BeetsStartupError`'s contract.

**Private tree vs. generic paths (issue #1085 review round 2).** The
configured processing root and its ``albums``/``preview`` children are not
ordinary directories: every action-time writer reaches them through
``lib.fs_authority.open_private_processing_root`` /
``open_private_child_directory``, which additionally enforce the private-tree
ownership contract (root owned by the service identity, mode exactly
``0700``, no group/other-writable ancestor). A generic descriptor open
proves only "can I open and write" -- it is GREEN on a root that drifted to
``0750`` or a child a bad chown left at ``0770``, exactly the shape the
#570/#578 cutover produced, and every subsequent materialization / preview
retention / force-action write then fails one album at a time, which is the
outcome this probe exists to eliminate. So this module probes the private
processing tree through the SAME two functions, never the generic
:func:`lib.fs_authority.open_directory_path`, for anything under it.
Quarantine LOOKUPS (``lib.fs_authority.open_configured_quarantine_directory``)
are a different, deliberately looser action-time path -- it opens all three
quarantine roots generically, including the private ``albums`` root -- so
this module's *read* probe of quarantine roots stays on the generic
primitive to match that reality exactly; only *write* access to the private
tree goes through the strict primitives.

**Optional-by-configuration paths (review round 2).** ``beets_staging_dir``
is unset (empty string) on any deployment with Beets validation disabled --
a legitimate, supported configuration
(``lib.download_processing`` never reaches the staging branch in that case,
and ``lib.fs_authority.open_configured_quarantine_directory`` already skips
a non-absolute root). Requiring it unconditionally would crash-loop four
units forever on a deployment that simply never turned validation on, or on
a fresh host mid-bring-up. Every builder below therefore includes
``beets_staging_dir`` in a unit's required paths ONLY when it is configured
(non-empty); an unconfigured path is never probed, never "missing" here.
``slskd_download_dir`` gets no such treatment -- unlike ``stagingDir``,
``services.cratedigger.slskd.downloadDir`` is guarded by an UNCONDITIONAL
module assertion (``nix/module.nix``: ``assertion = cfg.slskd.downloadDir !=
null``, no gating predicate), so any deployment that evaluates at all already
has it set. An empty value here is therefore a genuine misconfiguration this
probe is right to catch.

Every existing action-time authority check -- ``lib.fs_authority``'s
private-root/quarantine checks, and this same module's descriptor
primitives used again at actual mutation time -- stays exactly as it is.
Mounts, ACLs, ownership and modes change after startup; this probe is
never the only check, only an earlier, louder one.

``cratedigger-unfindable`` (``scripts/run_unfindable_detection.py``)
deliberately never calls this module. It has its own systemd unit
specifically so the never-stop-searching invariant is enforceable at the
systemd level; refusing it because storage is unavailable would violate
that invariant at the one place it is currently guaranteed. That is
enforced by never wiring a call in, not by a special case here.

Reuses ``lib.fs_authority``'s descriptor primitives: :func:`observe_directory`
for a friendly presence/absence verdict before the heavier open,
:func:`open_directory_path` / :func:`open_private_processing_root` /
:func:`open_private_child_directory` for the real no-follow descriptors, and
:func:`errno_proves_absence` to phrase "never provisioned" against
"could not be reached." ``os_refusal_in_chain`` is not used here -- it
exists to find an ``OSError`` hiding inside a THIRD-PARTY exception (beets,
mediafile, mutagen); every syscall this module wraps is a direct ``os.*``
call under our own ``except OSError``, so there is no translation to
unwrap. Never ``os.path.isdir()``, ``Path.exists()``, or ``os.access()`` --
conflating "could not look" with "is not there" is precisely the #1063
defect this module's own primitives were hardened against.

**Known, recorded, out of scope (see issue #1085 review):**

* ``open_directory_path`` opens every path component ``O_NOFOLLOW``. A
  single symlinked component in ``slskd_download_dir`` or
  ``beets_staging_dir`` (e.g. a ``/mnt/music -> /pool/music`` bind-mount
  substitute, plausible on another installation) now permanently blocks
  every unit that requires the path, where previously it degraded only the
  one operation that happened to resolve it. No live impact on this
  deployment; changing the shared primitive's symlink posture is out of
  this issue's remit.
* This module cannot verify its required-path lists are COMPLETE -- only
  that each listed path is genuinely usable. Deriving the list from actual
  runtime behaviour (tracing every write production code performs) is a
  real, hard, separate problem and is not attempted here; the lists are
  maintained by hand against the current call graph and can drift.
"""

from __future__ import annotations

import contextlib
import logging
import os
import uuid
from collections.abc import Sequence
from typing import NamedTuple

from lib.config import CratediggerConfig
from lib.fs_authority import (
    FilesystemAuthorityError,
    errno_proves_absence,
    errno_symbol,
    observe_directory,
    open_directory_path,
    open_private_child_directory,
    open_private_processing_root,
    rename_relative_noreplace,
)
from lib.processing_paths import processing_albums_dir

_PROBE_PREFIX = ".cratedigger-startup-probe-"


class StartupProbeError(RuntimeError):
    """A required path could not be used the way its unit needs it.

    The process must exit before creating any application state -- mirrors
    :class:`lib.beets_startup.BeetsStartupError`'s contract exactly, so
    every caller's ``except`` clause is a one-line ``return 1``.
    """


class RequiredPaths(NamedTuple):
    """One unit's exact probe list.

    ``read``/``write`` are ordinary absolute paths probed with the generic
    descriptor primitives. ``private_write_root`` and
    ``private_write_children`` probe the configured processing tree through
    the strict private-root primitives instead
    (``private_processing_dir``/``private_slskd_download_dir`` supply the
    two paths ``open_private_processing_root`` needs); leave them at their
    defaults for a unit that never touches the private tree.
    """

    read: tuple[str, ...] = ()
    write: tuple[str, ...] = ()
    private_processing_dir: str = ""
    private_slskd_download_dir: str = ""
    private_write_root: bool = False
    private_write_children: tuple[str, ...] = ()


def probe_startup_paths(
    *, unit: str, logger: logging.Logger, required: RequiredPaths,
) -> None:
    """Probe every required path for ``unit`` once; fail on the first refusal.

    ``read`` paths get a descriptor open plus an enumerate. ``write`` paths
    additionally get a safely named create -> write -> fsync -> rename ->
    unlink probe, with the temporary artifact always removed -- never left
    as debris. Every listed path must ALREADY exist: each one is either
    externally provisioned (the slskd share, the beets staging root) or
    created once by the deployment's systemd tmpfiles rules (``var_dir``,
    the private processing tree); this probe never creates its own
    required directory. Every write target here is a directory a unit
    owns and writes SIBLINGS of album folders into -- never inside an
    album directory itself (issue #853/#859: ``processing/albums/<album>``
    is an exact media manifest namespace, and a stray probe file there is
    exactly the class of defect that stalled every automation import).
    """
    try:
        for path in required.read:
            _probe_read(unit=unit, path=path)
        for path in required.write:
            _probe_write(unit=unit, path=path)
        _probe_private_tree(
            unit=unit,
            processing_dir=required.private_processing_dir,
            slskd_download_dir=required.private_slskd_download_dir,
            write_root=required.private_write_root,
            write_children=required.private_write_children,
        )
    except StartupProbeError as exc:
        logger.error("%s", exc)
        raise


def _presence_or_raise(*, unit: str, path: str, operation: str) -> None:
    observation = observe_directory(path)
    if observation.present:
        return
    if observation.code is not None and errno_proves_absence(observation.code):
        verdict = "does not exist"
    else:
        verdict = "could not be reached"
    raise StartupProbeError(
        f"{unit}: startup {operation} probe failed at {path} ({verdict}): "
        f"{observation.unavailable_reason()}"
    )


def _probe_read(*, unit: str, path: str) -> None:
    _presence_or_raise(unit=unit, path=path, operation="read")
    try:
        with open_directory_path(path) as fd:
            try:
                os.listdir(fd)
            except OSError as exc:
                raise StartupProbeError(
                    _os_message(unit, path, "enumerate", exc)) from exc
    except FilesystemAuthorityError as exc:
        raise StartupProbeError(
            _authority_message(unit, path, "open", exc)) from exc


def _probe_write(*, unit: str, path: str) -> None:
    _presence_or_raise(unit=unit, path=path, operation="write")
    try:
        with open_directory_path(path) as dir_fd:
            _write_probe_steps(unit=unit, path=path, dir_fd=dir_fd)
    except FilesystemAuthorityError as exc:
        raise StartupProbeError(
            _authority_message(unit, path, "open", exc)) from exc


def _probe_private_tree(
    *,
    unit: str,
    processing_dir: str,
    slskd_download_dir: str,
    write_root: bool,
    write_children: Sequence[str],
) -> None:
    """Probe the configured processing tree through the SAME strict
    private-root primitives every action-time writer uses -- never the
    generic descriptor open (see the module docstring)."""
    if not (write_root or write_children):
        return
    _presence_or_raise(unit=unit, path=processing_dir, operation="read")
    try:
        with open_private_processing_root(
            processing_dir, slskd_download_dir,
        ) as root_fd:
            if write_root:
                _write_probe_steps(
                    unit=unit, path=processing_dir, dir_fd=root_fd)
            for child in write_children:
                _probe_private_write_child(
                    unit=unit, processing_dir=processing_dir,
                    root_fd=root_fd, child=child)
    except FilesystemAuthorityError as exc:
        raise StartupProbeError(
            _authority_message(unit, processing_dir, "open", exc)) from exc


def _probe_private_write_child(
    *, unit: str, processing_dir: str, root_fd: int, child: str,
) -> None:
    child_path = os.path.join(processing_dir, child)
    try:
        with open_private_child_directory(root_fd, child) as child_fd:
            _write_probe_steps(unit=unit, path=child_path, dir_fd=child_fd)
    except FilesystemAuthorityError as exc:
        raise StartupProbeError(
            _authority_message(unit, child_path, "open", exc)) from exc


def _write_probe_steps(*, unit: str, path: str, dir_fd: int) -> None:
    """create -> write -> fsync -> rename -> unlink, always cleaned up.

    ``current_name`` tracks whichever name is currently on disk so the
    ``finally`` clause can remove it regardless of which step failed --
    the artifact is never left behind as debris.
    """
    created_name = f"{_PROBE_PREFIX}{os.getpid()}-{uuid.uuid4().hex}"
    renamed_name = f"{created_name}.moved"
    payload = b"cratedigger startup write probe\n"
    current_name: str | None = None
    probe_fd: int | None = None
    try:
        try:
            probe_fd = os.open(
                created_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                0o600,
                dir_fd=dir_fd,
            )
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "create", exc)) from exc
        current_name = created_name

        try:
            written = os.write(probe_fd, payload)
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "write", exc)) from exc
        if written != len(payload):
            raise StartupProbeError(
                f"{unit}: startup write probe failed at {path}: "
                f"short write ({written}/{len(payload)} bytes)"
            )

        try:
            os.fsync(probe_fd)
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "fsync", exc)) from exc
        try:
            os.close(probe_fd)
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "close", exc)) from exc
        finally:
            # close() consumes the fd number whether or not it raised
            # (POSIX): a second close on failure would target a fd Linux
            # may already have reassigned. The outer cleanup must not
            # retry it either way.
            probe_fd = None

        try:
            renamed = rename_relative_noreplace(dir_fd, created_name, renamed_name)
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "rename", exc)) from exc
        if not renamed:
            raise StartupProbeError(
                f"{unit}: startup rename probe collided at {path} "
                f"(name={renamed_name!r}); a UUID4 collision should never happen"
            )
        current_name = renamed_name

        try:
            os.unlink(renamed_name, dir_fd=dir_fd)
        except OSError as exc:
            raise StartupProbeError(
                _os_message(unit, path, "unlink", exc)) from exc
        current_name = None
    finally:
        if probe_fd is not None:
            with contextlib.suppress(OSError):
                os.close(probe_fd)
        if current_name is not None:
            with contextlib.suppress(OSError):
                os.unlink(current_name, dir_fd=dir_fd)


def _os_message(unit: str, path: str, operation: str, exc: OSError) -> str:
    return (
        f"{unit}: startup {operation} probe failed at {path} "
        f"[{errno_symbol(exc)}]: {exc.strerror}"
    )


def _authority_message(
    unit: str, path: str, operation: str, exc: FilesystemAuthorityError,
) -> str:
    symbol = exc.errno_symbol or exc.code
    return f"{unit}: startup {operation} probe failed at {path} [{symbol}]: {exc}"


# --- Per-unit required-path lists -------------------------------------------
#
# One pure function per gated unit. Production wiring (cratedigger.py,
# scripts/importer.py, scripts/import_preview_worker.py, web/server.py,
# scripts/youtube_ingest_worker.py) and the generated property test both
# call these SAME functions, so a test can never describe a required-path
# list production doesn't actually use (test-fidelity Rule C).


def _quarantine_roots(cfg: CratediggerConfig) -> tuple[str, ...]:
    """The roots ``lib.fs_authority.open_configured_quarantine_directory``
    searches -- generic descriptor opens, matching that function's own
    (deliberately looser than the private-tree contract) action-time
    behaviour exactly, including for its private ``albums`` root. Probed at
    the root, never at an optional child (``wrong_matches``,
    ``failed_imports``) -- those stay optional, proven by the owned
    parent's own authority rather than requiring an empty one to exist.
    ``beets_staging_dir`` is included only when configured (see module
    docstring)."""
    roots = [cfg.slskd_download_dir, processing_albums_dir(cfg.processing_dir)]
    if cfg.beets_staging_dir:
        roots.append(cfg.beets_staging_dir)
    return tuple(roots)


def cratedigger_required_paths(cfg: CratediggerConfig) -> RequiredPaths:
    """Main pipeline loop: reads the shared slskd download share; the disk
    reaper (``lib.slskd_transfers.reap_disk_orphans``) unlinks proven-owned
    completed files and prunes emptied directories back up to that same
    root, so it needs write authority there too, not just read. Writes
    materialized downloads into the private processing tree (via
    ``lib.download_materialization._materialize_processing_dir``, private
    ``albums`` primitives) and, when configured, stages redownload/manual-
    review albums under the beets staging root."""
    write = [cfg.var_dir, cfg.slskd_download_dir]
    if cfg.beets_staging_dir:
        write.append(cfg.beets_staging_dir)
    return RequiredPaths(
        read=(cfg.slskd_download_dir,),
        write=tuple(write),
        private_processing_dir=cfg.processing_dir,
        private_slskd_download_dir=cfg.slskd_download_dir,
        private_write_children=("albums",),
    )


def importer_required_paths(cfg: CratediggerConfig) -> RequiredPaths:
    """Import queue worker: reads quarantine roots (force-import, beets-
    distance), normalizes owned albums in place, and reclaims retained
    force-action copies from the private ``albums`` tree
    (``lib.import_preview.cleanup_force_action_copy_for_job``, private
    primitives). Stages Beets imports under the beets staging root when
    configured."""
    write: list[str] = []
    if cfg.beets_staging_dir:
        write.append(cfg.beets_staging_dir)
    return RequiredPaths(
        read=_quarantine_roots(cfg),
        write=tuple(write),
        private_processing_dir=cfg.processing_dir,
        private_slskd_download_dir=cfg.slskd_download_dir,
        private_write_children=("albums",),
    )


def preview_worker_required_paths(cfg: CratediggerConfig) -> RequiredPaths:
    """Async import preview worker: reads quarantine roots, writes CD-rip
    authenticity cache/spool under ``var_dir``
    (``lib.cd_rip_verifier.verify_cd_rip`` -- silently withheld on an
    unwritable ``var_dir`` via a blanket exception guard, so this is the
    one place that failure mode is caught loudly instead). Writes preview
    snapshots (``preview/``) and retains force-action copies (``albums/``)
    through the private tree; ``.preview-snapshot.lock`` is taken directly
    in the processing root itself (``lib.import_preview._preview_copy_lock``),
    reached via ``snapshot_configured_quarantine_directory``."""
    return RequiredPaths(
        read=_quarantine_roots(cfg),
        write=(cfg.var_dir,),
        private_processing_dir=cfg.processing_dir,
        private_slskd_download_dir=cfg.slskd_download_dir,
        private_write_root=True,
        private_write_children=("albums", "preview"),
    )


def web_required_paths(cfg: CratediggerConfig) -> RequiredPaths:
    """Web UI: reads and deletes from the quarantine roots (Wrong Matches /
    Bad Rip / library-delete converge onto these via ``shutil.rmtree`` on
    the resolved path -- generic, never the private-tree primitives, even
    for the private ``albums`` root); writes CD-rip cache/spool under
    ``var_dir`` (triage sweep) and ``.preview-snapshot.lock`` plus preview
    snapshots through the private tree
    (``preview_import_from_download_log`` -> ``_preview_copy_lock``).

    Deliberately excludes ``slskd_download_dir`` from write: web never
    actually needs write authority there -- it only ever deletes from its
    own ``wrong_matches``/``failed_imports`` quarantine children (which are
    not required to exist). The governing rule is to probe only the
    authority a unit actually uses, not a blanket ban on ever writing into
    the slskd share -- ``cratedigger_required_paths`` DOES require write
    there, correctly, because the disk reaper
    (``lib.slskd_transfers.reap_disk_orphans``) genuinely unlinks proven-
    owned files and prunes emptied directories back up to that same root on
    every cycle.
    """
    write = [cfg.var_dir, processing_albums_dir(cfg.processing_dir)]
    if cfg.beets_staging_dir:
        write.append(cfg.beets_staging_dir)
    return RequiredPaths(
        read=_quarantine_roots(cfg),
        write=tuple(write),
        private_processing_dir=cfg.processing_dir,
        private_slskd_download_dir=cfg.slskd_download_dir,
        private_write_root=True,
        private_write_children=("preview",),
    )


def youtube_ingest_required_paths(
    *, temp_dir: str, staging_dir: str,
) -> RequiredPaths:
    """YouTube-rescue ingest worker: its own yt-dlp scratch directory and
    the shared beets staging root it stages rescued audio into. Never
    touches the private processing tree.

    Has no ``CratediggerConfig`` -- the worker never calls
    ``enforce_beets_startup`` (it defers all Beets mutation to the importer
    queue) and takes both paths directly from its own CLI args, so this
    takes plain strings rather than a config object. Both are
    unconditionally required for this unit: the module's own assertion
    guarantees ``staging_dir`` is set whenever ``youtubeIngest.enable`` is
    true, which is the only way this worker's unit exists at all.
    """
    return RequiredPaths(write=(temp_dir, staging_dir))
