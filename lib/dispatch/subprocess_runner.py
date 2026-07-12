"""import_one.py subprocess orchestration.

Builds and runs the single shared ``import_one.py`` command line and parses
its ``ImportResult`` sentinel. This is the module where ``sp.run`` and
``parse_import_result`` are looked up (tests patch them here).
"""

from __future__ import annotations

import os
import subprocess as sp
import sys
from typing import TYPE_CHECKING

from lib.quality import parse_import_result
from lib.util import beets_subprocess_env

from lib.dispatch.types import ImportOneRun

if TYPE_CHECKING:
    from lib.quality import V0ProbeEvidence


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
    beets_library_root: str = "",
) -> list[str]:
    """Build the single shared import_one.py command line."""
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
    if beets_library_root:
        cmd.extend(["--beets-library-root", beets_library_root])
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
    beets_library_root: str = "",
    timeout: int = 1800,
) -> ImportOneRun:
    """Run import_one.py and parse its ImportResult sentinel."""
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
        beets_library_root=beets_library_root,
    )
    result = sp.run(
        cmd,
        capture_output=True,
        text=True,
        errors="replace",
        timeout=timeout,
        env=beets_subprocess_env(),
    )
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    return ImportOneRun(
        command=tuple(cmd),
        returncode=int(result.returncode),
        stdout=stdout,
        stderr=stderr,
        import_result=parse_import_result(stdout),
    )
