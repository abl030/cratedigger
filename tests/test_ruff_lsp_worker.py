"""Contracts for the target-scoped persistent Ruff language server."""

from __future__ import annotations

import ast
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from pathlib import Path
from unittest import mock

from tests.ruff_lsp_worker import RuffLspWorker, RuffLspWorkerError
from tests.test_unused_import_audit import ruff_findings

ROOT = Path(__file__).resolve().parents[1]
_PATHS = ("lib/importing.py", "lib/peer.py")


def _fake_server_command(source: str) -> tuple[str, ...]:
    return (sys.executable, "-u", "-c", textwrap.dedent(source))


_FAKE_PREAMBLE = r"""
import json
import sys
import time

def read_message():
    length = None
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            raise EOFError
        if line == b'\r\n':
            break
        name, value = line.decode('ascii').split(':', 1)
        if name.lower() == 'content-length':
            length = int(value.strip())
    if length is None:
        raise RuntimeError('missing length')
    return json.loads(sys.stdin.buffer.read(length))

def send(message):
    body = json.dumps(message, separators=(',', ':')).encode()
    sys.stdout.buffer.write(
        f'Content-Length: {len(body)}\r\n\r\n'.encode() + body
    )
    sys.stdout.buffer.flush()
"""


def direct_ruff_findings_lines(source: str) -> tuple[int, ...]:
    """Locate the bounded historical per-example Ruff CLI helper call."""
    tree = ast.parse(source)
    return tuple(
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ruff_findings"
    )


def _codes_by_path(
    findings: tuple[dict[str, object], ...],
) -> frozenset[tuple[str, str]]:
    normalized: set[tuple[str, str]] = set()
    for finding in findings:
        filename = Path(str(finding["filename"])).as_posix()
        relative = next(path for path in _PATHS if filename.endswith(path))
        normalized.add((relative, str(finding["code"])))
    return frozenset(normalized)


class TestRuffLspWorker(unittest.TestCase):
    def test_reuses_one_server_and_matches_canonical_cli_findings(self) -> None:
        redundant = {
            "lib/importing.py": "from dependency import name as name\n",
            "lib/peer.py": "name = object()\nprint(name)\n",
        }
        unused = {
            "lib/importing.py": "from dependency import other\n",
            "lib/peer.py": "other = object()\nprint(other)\n",
        }
        with RuffLspWorker(_PATHS, cwd=ROOT) as worker:
            pid = worker.pid
            lsp_redundant = worker.findings(redundant)
            lsp_unused = worker.findings(unused)
            self.assertEqual(worker.pid, pid)

        self.assertEqual(
            _codes_by_path(lsp_redundant),
            _codes_by_path(ruff_findings(redundant)),
        )
        self.assertEqual(
            _codes_by_path(lsp_unused),
            _codes_by_path(ruff_findings(unused)),
        )

    def test_each_update_replaces_the_complete_prior_world(self) -> None:
        world_a = {
            "lib/importing.py": "from dependency import name as name\n",
            "lib/peer.py": "name = object()\nprint(name)\n",
        }
        world_b = {
            "lib/importing.py": "from dependency import name\nprint(name)\n",
            "lib/peer.py": "peer = object()\nprint(peer)\n",
        }
        with RuffLspWorker(_PATHS, cwd=ROOT) as worker:
            first = worker.findings(world_a)
            worker.findings(world_b)
            repeated = worker.findings(world_a)

        self.assertEqual(_codes_by_path(first), _codes_by_path(repeated))

    def test_source_paths_are_an_exact_allowlist(self) -> None:
        with (
            RuffLspWorker(_PATHS, cwd=ROOT) as worker,
            self.assertRaisesRegex(RuffLspWorkerError, "exact configured paths"),
        ):
            worker.findings({"lib/importing.py": "pass\n"})

    def test_close_is_idempotent_and_requests_after_close_fail(self) -> None:
        worker = RuffLspWorker(_PATHS, cwd=ROOT)

        worker.close()
        worker.close()

        with self.assertRaisesRegex(RuffLspWorkerError, "not running"):
            worker.findings({path: "pass\n" for path in _PATHS})

    def test_stale_diagnostic_chatter_cannot_extend_transaction_deadline(self) -> None:
        command = _fake_server_command(
            _FAKE_PREAMBLE
            + r"""
initialize = read_message()
send({'jsonrpc': '2.0', 'id': initialize['id'], 'result': {}})
read_message()
opened = read_message()
uri = opened['params']['textDocument']['uri']
while True:
    send({
        'jsonrpc': '2.0',
        'method': 'textDocument/publishDiagnostics',
        'params': {'uri': uri, 'version': 0, 'diagnostics': []},
    })
    time.sleep(0.01)
""",
        )
        worker = RuffLspWorker(
            ("lib/importing.py",),
            cwd=ROOT,
            timeout_seconds=0.15,
            command=command,
        )
        root = worker.workspace_root
        started = time.monotonic()

        with self.assertRaisesRegex(RuffLspWorkerError, "timed out") as caught:
            worker.findings({"lib/importing.py": "pass\n"})

        self.assertLess(time.monotonic() - started, 1.0)
        self.assertFalse(root.exists())
        with self.assertRaises(RuffLspWorkerError) as repeated:
            worker.findings({"lib/importing.py": "pass\n"})
        self.assertEqual(str(repeated.exception), str(caught.exception))
        worker.close()

    def test_blocked_write_consumes_the_transaction_deadline(self) -> None:
        command = _fake_server_command(
            _FAKE_PREAMBLE
            + """
initialize = read_message()
send({'jsonrpc': '2.0', 'id': initialize['id'], 'result': {}})
import signal
signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(60)
""",
        )
        worker = RuffLspWorker(
            ("lib/importing.py",),
            cwd=ROOT,
            timeout_seconds=0.1,
            command=command,
        )
        root = worker.workspace_root
        started = time.monotonic()

        with self.assertRaisesRegex(RuffLspWorkerError, "request timed out") as caught:
            worker.findings({"lib/importing.py": "x = " + repr("x" * (512 * 1024))})

        self.assertLess(time.monotonic() - started, 0.75)
        self.assertFalse(root.exists())
        with self.assertRaises(RuffLspWorkerError) as repeated:
            worker.findings({"lib/importing.py": "pass\n"})
        self.assertEqual(str(repeated.exception), str(caught.exception))

    def test_child_loss_between_requests_is_poisoned_and_cleans_workspace(self) -> None:
        command = _fake_server_command(
            _FAKE_PREAMBLE
            + """
initialize = read_message()
send({'jsonrpc': '2.0', 'id': initialize['id'], 'result': {}})
read_message()
print('planted Ruff child loss', file=sys.stderr, flush=True)
raise SystemExit(17)
""",
        )
        worker = RuffLspWorker(
            ("lib/importing.py",),
            cwd=ROOT,
            command=command,
        )
        root = worker.workspace_root
        time.sleep(0.2)

        with self.assertRaisesRegex(
            RuffLspWorkerError,
            "planted Ruff child loss",
        ) as caught:
            worker.findings({"lib/importing.py": "pass\n"})
        self.assertFalse(root.exists())
        with self.assertRaises(RuffLspWorkerError) as repeated:
            worker.findings({"lib/importing.py": "pass\n"})
        self.assertEqual(str(repeated.exception), str(caught.exception))

    def test_initialization_child_loss_cleans_temporary_workspace(self) -> None:
        command = _fake_server_command(
            "import sys\n"
            "print('planted init loss', file=sys.stderr, flush=True)\n"
            "raise SystemExit(17)\n"
        )
        temporary_root = Path(tempfile.gettempdir())
        before = set(temporary_root.glob("cratedigger-ruff-lsp-*"))

        with self.assertRaisesRegex(RuffLspWorkerError, "planted init loss"):
            RuffLspWorker(
                ("lib/importing.py",),
                cwd=ROOT,
                command=command,
            )

        self.assertEqual(
            set(temporary_root.glob("cratedigger-ruff-lsp-*")),
            before,
        )

    def test_stderr_thread_start_failure_cleans_temporary_workspace(self) -> None:
        temporary_root = Path(tempfile.gettempdir())
        before = set(temporary_root.glob("cratedigger-ruff-lsp-*"))

        with (
            mock.patch(
                "tests.ruff_lsp_worker.threading.Thread.start",
                side_effect=RuntimeError("planted thread start failure"),
            ),
            self.assertRaisesRegex(RuffLspWorkerError, "thread start failure"),
        ):
            RuffLspWorker(("lib/importing.py",), cwd=ROOT)

        self.assertEqual(
            set(temporary_root.glob("cratedigger-ruff-lsp-*")),
            before,
        )

    def test_stderr_thread_construction_failure_reaps_child_and_closes_pipes(
        self,
    ) -> None:
        process = subprocess.Popen(
            _fake_server_command("import time; time.sleep(60)"),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        temporary_root = Path(tempfile.gettempdir())
        before = set(temporary_root.glob("cratedigger-ruff-lsp-*"))

        with (
            mock.patch(
                "tests.ruff_lsp_worker.subprocess.Popen",
                return_value=process,
            ),
            mock.patch(
                "tests.ruff_lsp_worker.threading.Thread",
                side_effect=RuntimeError("planted thread construction failure"),
            ),
            self.assertRaisesRegex(RuffLspWorkerError, "thread construction failure"),
        ):
            RuffLspWorker(("lib/importing.py",), cwd=ROOT)

        self.assertEqual(process.returncode, -signal.SIGKILL)
        for stream in (process.stdin, process.stdout, process.stderr):
            assert stream is not None
            self.assertTrue(stream.closed)
        self.assertEqual(
            set(temporary_root.glob("cratedigger-ruff-lsp-*")),
            before,
        )

    def test_malformed_jsonrpc_version_fails_initialization_closed(self) -> None:
        command = _fake_server_command(
            _FAKE_PREAMBLE
            + """
initialize = read_message()
send({'jsonrpc': 'bogus', 'id': initialize['id'], 'result': {}})
""",
        )

        with self.assertRaisesRegex(RuffLspWorkerError, "malformed Ruff LSP"):
            RuffLspWorker(("lib/importing.py",), cwd=ROOT, command=command)

    def test_response_requires_exactly_one_result_or_error(self) -> None:
        command = _fake_server_command(
            _FAKE_PREAMBLE
            + """
initialize = read_message()
send({'jsonrpc': '2.0', 'id': initialize['id']})
""",
        )

        with self.assertRaisesRegex(RuffLspWorkerError, "malformed Ruff response"):
            RuffLspWorker(("lib/importing.py",), cwd=ROOT, command=command)


class TestGeneratedRuffWorkerAudit(unittest.TestCase):
    def test_checker_rejects_the_historical_per_example_helper(self) -> None:
        self.assertEqual(
            direct_ruff_findings_lines("result = ruff_findings(sources)\n"),
            (1,),
        )

    def test_generated_unused_import_properties_use_the_persistent_server(self) -> None:
        path = ROOT / "tests" / "test_unused_import_audit_generated.py"

        self.assertEqual(
            direct_ruff_findings_lines(path.read_text(encoding="utf-8")),
            (),
            "generated Ruff properties must not launch the CLI per example",
        )


if __name__ == "__main__":
    unittest.main()
