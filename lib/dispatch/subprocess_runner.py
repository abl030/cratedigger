"""import_one.py subprocess orchestration.

Builds and runs the single shared ``import_one.py`` command line and parses
its ``ImportResult`` sentinel. This is the module where ``sp.run`` and
``parse_import_result`` are looked up (tests patch them here).
"""

from __future__ import annotations

import os
import subprocess as sp
import sys
import tempfile
import threading
from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from lib.dispatch.types import ImportOneRun
from lib.import_execution import CancellationToken, MonitoredProcessGroup
from lib.quality import parse_import_result
from lib.util import beets_subprocess_env

if TYPE_CHECKING:
    from lib.quality import V0ProbeEvidence


class SupervisedProcessGroup(Protocol):
    """The child-supervision surface ``run_import_one`` depends on."""

    @property
    def pid(self) -> int: ...

    def terminate_and_wait(self, *, timeout: float = ...) -> int: ...

    def wait(
        self,
        token: CancellationToken,
        *,
        owner_session_probe: Callable[[], bool] | None = ...,
        probe_interval: float = ...,
    ) -> int: ...


ProcessGroupFactory = Callable[["sp.Popen[bytes]"], SupervisedProcessGroup]


def import_one_script_from_harness(beets_harness_path: str) -> str:
    """Resolve import_one.py beside the configured harness wrapper."""
    return os.path.join(os.path.dirname(beets_harness_path), "import_one.py")


def build_import_one_command(
    *,
    path: str,
    mb_release_id: str,
    beets_harness_path: str,
    request_id: int | None = None,
    force: bool = False,
    preserve_source: bool = False,
    dry_run: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    quality_rank_config_json: str | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
    quality_evidence_action_file: str | None = None,
    beets_config_dir: str | None = None,
    beets_python: str | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
) -> list[str]:
    """Build the single shared import_one.py command line."""
    from lib.beets_db import validate_beets_storage_pair

    validate_beets_storage_pair(
        db_path=beets_library_db_path,
        library_root=beets_library_root,
    )
    cmd = [
        sys.executable,
        import_one_script_from_harness(beets_harness_path),
        path,
        mb_release_id,
    ]
    if request_id is not None:
        cmd.extend(["--request-id", str(request_id)])
    if force:
        cmd.append("--force")
    if preserve_source:
        cmd.append("--preserve-source")
    if dry_run:
        cmd.append("--dry-run")
    if verified_lossless_target:
        cmd.extend(["--verified-lossless-target", verified_lossless_target])
    if target_format:
        cmd.extend(["--target-format", target_format])
    if override_min_bitrate is not None:
        cmd.extend(["--override-min-bitrate", str(override_min_bitrate)])
    if quality_rank_config_json:
        cmd.extend(["--quality-rank-config", quality_rank_config_json])
    if quality_evidence_action_file:
        cmd.extend(["--quality-evidence-action-file", quality_evidence_action_file])
    if existing_v0_probe is not None:
        if existing_v0_probe.min_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-min-bitrate",
                str(existing_v0_probe.min_bitrate_kbps),
            ])
        if existing_v0_probe.avg_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-avg-bitrate",
                str(existing_v0_probe.avg_bitrate_kbps),
            ])
        if existing_v0_probe.median_bitrate_kbps is not None:
            cmd.extend([
                "--existing-v0-probe-median-bitrate",
                str(existing_v0_probe.median_bitrate_kbps),
            ])
    if beets_library_db_path is not None:
        cmd.extend(["--beets-library-db", beets_library_db_path])
        assert beets_library_root is not None
        cmd.extend(["--beets-library-root", beets_library_root])
    if beets_config_dir is not None:
        cmd.extend(["--beets-config-dir", beets_config_dir])
    if beets_python is not None:
        cmd.extend(["--beets-python", beets_python])
    return cmd


def run_import_one(
    *,
    path: str,
    mb_release_id: str,
    beets_harness_path: str,
    request_id: int | None = None,
    force: bool = False,
    preserve_source: bool = False,
    dry_run: bool = False,
    override_min_bitrate: int | None = None,
    target_format: str | None = None,
    verified_lossless_target: str = "",
    quality_rank_config_json: str | None = None,
    existing_v0_probe: V0ProbeEvidence | None = None,
    quality_evidence_action_file: str | None = None,
    beets_config_dir: str | None = None,
    beets_python: str | None = None,
    beets_library_db_path: str | None = None,
    beets_library_root: str | None = None,
    timeout: int = 1800,
    cancellation_token: CancellationToken | None = None,
    on_spawn: Callable[[int], None] | None = None,
    owner_session_probe: Callable[[], bool] | None = None,
    process_group_factory: ProcessGroupFactory = MonitoredProcessGroup,
) -> ImportOneRun:
    """Spawn import_one.py as a child and parse its ImportResult sentinel.

    Shares its name with ``harness.import_one.run_import_one``, which runs
    the same import IN PROCESS and returns the result directly. This is the
    one production reaches for; that one is what the child ends up calling.
    """
    cmd = build_import_one_command(
        path=path,
        mb_release_id=mb_release_id,
        beets_harness_path=beets_harness_path,
        request_id=request_id,
        force=force,
        preserve_source=preserve_source,
        dry_run=dry_run,
        override_min_bitrate=override_min_bitrate,
        target_format=target_format,
        verified_lossless_target=verified_lossless_target,
        quality_rank_config_json=quality_rank_config_json,
        existing_v0_probe=existing_v0_probe,
        quality_evidence_action_file=quality_evidence_action_file,
        beets_config_dir=beets_config_dir,
        beets_python=beets_python,
        beets_library_db_path=beets_library_db_path,
        beets_library_root=beets_library_root,
    )
    env = beets_subprocess_env(
        beets_config_dir=beets_config_dir,
        beets_python=beets_python,
        beets_library_db_path=beets_library_db_path,
        beets_library_root=beets_library_root,
    )
    if (
        cancellation_token is None
        and on_spawn is None
        and owner_session_probe is None
    ):
        result = sp.run(
            cmd,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
            env=env,
            check=False,
        )
        returncode = int(result.returncode)
        stdout = result.stdout or ""
        stderr = result.stderr or ""
    else:
        if cancellation_token is None:
            raise ValueError(
                "spawn callbacks and owner-session probes require cancellation"
            )
        cancellation_token.raise_if_cancelled()
        timed_out = threading.Event()
        termination_errors: list[BaseException] = []
        with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
            process = sp.Popen(
                cmd,
                stdout=stdout_file,
                stderr=stderr_file,
                env=env,
                start_new_session=True,
            )
            monitored = process_group_factory(process)
            try:
                if on_spawn is not None:
                    on_spawn(monitored.pid)
            except BaseException:
                monitored.terminate_and_wait()
                raise

            def expire() -> None:
                timed_out.set()
                try:
                    monitored.terminate_and_wait()
                except BaseException as exc:  # noqa: BLE001 - cross-thread handoff
                    termination_errors.append(exc)

            timer = threading.Timer(timeout, expire)
            timer.daemon = True
            timer.start()
            try:
                returncode = monitored.wait(
                    cancellation_token,
                    owner_session_probe=owner_session_probe,
                )
            finally:
                timer.cancel()
                timer.join()
            if termination_errors:
                raise termination_errors[0]
            # Owner/session cancellation is the stronger safety signal when it
            # races the timeout callback. The process group has already been
            # terminated and waited either way; preserve the ownership cause.
            cancellation_token.raise_if_cancelled()
            if timed_out.is_set():
                raise sp.TimeoutExpired(cmd=cmd, timeout=timeout)
            stdout_file.seek(0)
            stderr_file.seek(0)
            stdout = stdout_file.read().decode("utf-8", "replace")
            stderr = stderr_file.read().decode("utf-8", "replace")
    return ImportOneRun(
        command=tuple(cmd),
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        import_result=parse_import_result(stdout),
    )
