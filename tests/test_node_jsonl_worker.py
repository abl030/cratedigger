"""Fail-closed contracts for target-scoped Node JSON-lines workers."""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import time
import unittest
from pathlib import Path
from unittest import mock

from tests.node_jsonl_worker import NodeJsonlWorker, NodeJsonlWorkerError

ROOT = Path(__file__).resolve().parents[1]


class TestNodeJsonlWorker(unittest.TestCase):
    @staticmethod
    def _fake_command(source: str) -> tuple[str, ...]:
        return (sys.executable, "-u", "-c", source)

    @staticmethod
    def _startup_frame() -> str:
        return json.dumps({
            "id": 0,
            "ok": True,
            "result": "ready",
            "error": None,
        })

    def test_post_spawn_setup_failure_reaps_child_and_closes_pipes(self) -> None:
        process = subprocess.Popen(
            self._fake_command("import time; time.sleep(60)"),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        with (
            mock.patch(
                "tests.node_jsonl_worker.subprocess.Popen",
                return_value=process,
            ),
            mock.patch(
                "tests.node_jsonl_worker.os.set_blocking",
                side_effect=OSError("planted set_blocking failure"),
            ),
            self.assertRaisesRegex(NodeJsonlWorkerError, "set_blocking failure"),
        ):
            NodeJsonlWorker("", cwd=ROOT)

        self.assertEqual(process.returncode, -signal.SIGKILL)
        for stream in (process.stdin, process.stdout, process.stderr):
            assert stream is not None
            self.assertTrue(stream.closed)

    def test_requests_reuse_one_child_without_crossing_payloads(self) -> None:
        source = """
let calls = 0;
async function handle(operation, payload) {
  calls += 1;
  return { pid: process.pid, calls, operation, value: payload.value };
}
"""
        with NodeJsonlWorker(source, cwd=ROOT) as worker:
            first = worker.request("first", {"value": "alpha"})
            second = worker.request("second", {"value": "beta"})

        self.assertIsInstance(first, dict)
        self.assertIsInstance(second, dict)
        assert isinstance(first, dict)
        assert isinstance(second, dict)
        self.assertEqual(first["pid"], second["pid"])
        self.assertEqual(first["calls"], 1)
        self.assertEqual(second["calls"], 2)
        self.assertEqual(first["operation"], "first")
        self.assertEqual(second["operation"], "second")
        self.assertEqual(first["value"], "alpha")
        self.assertEqual(second["value"], "beta")

    def test_malformed_stdout_fails_closed_and_poisoned_worker_stays_dead(self) -> None:
        source = """
async function handle(_operation, payload) {
  process.stdout.write('not-json\\n');
  return payload;
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "malformed response"):
            worker.request("echo", {"value": 1})

        self.assertFalse(worker.is_running)
        with self.assertRaisesRegex(NodeJsonlWorkerError, "malformed response"):
            worker.request("echo", {"value": 2})

    def test_wrong_response_id_fails_closed(self) -> None:
        source = """
async function handle(_operation, payload) {
  process.stdout.write(JSON.stringify({
    id: 999, ok: true, result: payload, error: null,
  }) + '\\n');
  return payload;
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "response id"):
            worker.request("echo", {"value": 1})

        self.assertFalse(worker.is_running)

    def test_child_exit_before_response_fails_closed_with_stderr(self) -> None:
        source = """
async function handle() {
  process.stderr.write('planted child loss\\n');
  process.exit(17);
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "planted child loss"):
            worker.request("exit", None)

        self.assertFalse(worker.is_running)

    def test_timeout_terminates_child_and_fails_closed(self) -> None:
        """Issue #1156 item 6: the original 0.1s timeout_seconds governs
        BOTH the deliberate hang below AND this worker's own Node startup
        handshake (NodeJsonlWorker.__init__ reuses the same budget for the
        "ready" response) -- at 20-24 workers on a 16-thread host, Node
        interpreter startup alone can exceed 0.1s under real CPU
        contention, raising NodeJsonlWorkerError OUT OF THE CONSTRUCTOR,
        before the `with self.assertRaisesRegex(...)` block below is even
        entered (it only wraps `worker.request`, not construction) --
        reproducing exactly the "fails reproducibly past the worker knee"
        symptom the issue names, as a raw ERROR rather than a clean
        assertion failure. 2.0s (matching this file's own
        test_stderr_flood_is_drained_without_deadlocking_response) gives
        real startup headroom under load while the deliberate hang
        (`await new Promise(() => {})`, which never resolves) still makes
        the timeout the only possible outcome."""
        source = """
async function handle() {
  await new Promise(() => {});
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT, timeout_seconds=2.0)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "timed out"):
            worker.request("hang", None)

        self.assertFalse(worker.is_running)

    def test_blocked_request_write_consumes_the_transaction_deadline(self) -> None:
        command = self._fake_command(
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            f"print({self._startup_frame()!r}, flush=True)\n"
            "time.sleep(60)\n"
        )
        worker = NodeJsonlWorker(
            "",
            cwd=ROOT,
            timeout_seconds=0.1,
            command=command,
        )
        self.addCleanup(worker.close)

        started = time.monotonic()
        with self.assertRaisesRegex(NodeJsonlWorkerError, "timed out") as caught:
            worker.request("blocked", {"text": "x" * (512 * 1024)})
        self.assertLess(time.monotonic() - started, 0.75)
        self.assertFalse(worker.is_running)
        with self.assertRaises(NodeJsonlWorkerError) as repeated:
            worker.request("blocked", {})
        self.assertEqual(str(repeated.exception), str(caught.exception))

    def test_child_loss_between_requests_is_poisoned_with_stderr(self) -> None:
        command = self._fake_command(
            "import sys, time\n"
            f"print({self._startup_frame()!r}, flush=True)\n"
            "time.sleep(0.05)\n"
            "print('planted early loss', file=sys.stderr, flush=True)\n"
            "raise SystemExit(17)\n"
        )
        worker = NodeJsonlWorker("", cwd=ROOT, command=command)
        self.addCleanup(worker.close)
        time.sleep(0.2)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "planted early loss") as caught:
            worker.request("after_exit", {})
        with self.assertRaises(NodeJsonlWorkerError) as repeated:
            worker.request("after_exit", {})
        self.assertEqual(str(repeated.exception), str(caught.exception))

    def test_partial_line_times_out_instead_of_blocking_readline(self) -> None:
        source = r"""
async function handle(_operation, _payload) {
  process.stdout.write('{"id":');
  return await new Promise(() => {});
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT, timeout_seconds=0.1)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "timed out"):
            worker.request("partial", {})

        self.assertFalse(worker.is_running)

    def test_stderr_flood_is_drained_without_deadlocking_response(self) -> None:
        source = r"""
async function handle(_operation, payload) {
  await new Promise((resolve) => {
    process.stderr.write('diagnostic'.repeat(100000), resolve);
  });
  return payload;
}
"""
        with NodeJsonlWorker(source, cwd=ROOT, timeout_seconds=2.0) as worker:
            self.assertEqual(worker.request("flood", {"ok": True}), {"ok": True})

    def test_oversized_response_fails_closed(self) -> None:
        source = r"""
async function handle(_operation, _payload) {
  return 'x'.repeat(2 * 1024 * 1024);
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT, timeout_seconds=2.0)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "response exceeded"):
            worker.request("oversized", {})

        self.assertFalse(worker.is_running)

    def test_invalid_utf8_fails_as_a_protocol_error(self) -> None:
        source = r"""
async function handle(_operation, _payload) {
  process.stdout.write(Buffer.from([0xff, 0xfe, 0x0a]));
  return null;
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "invalid UTF-8"):
            worker.request("invalid_utf8", {})

        self.assertFalse(worker.is_running)

    def test_close_is_idempotent_and_requests_after_close_fail(self) -> None:
        source = "async function handle(_operation, payload) { return payload; }"
        worker = NodeJsonlWorker(source, cwd=ROOT)

        worker.close()
        worker.close()

        with self.assertRaisesRegex(NodeJsonlWorkerError, "not running"):
            worker.request("closed", {})

    def test_javascript_error_is_a_failed_request_not_a_result(self) -> None:
        source = """
async function handle() {
  throw new Error('planted handler failure');
}
"""
        worker = NodeJsonlWorker(source, cwd=ROOT)
        self.addCleanup(worker.close)

        with self.assertRaisesRegex(NodeJsonlWorkerError, "planted handler failure"):
            worker.request("error", None)

        self.assertFalse(worker.is_running)


if __name__ == "__main__":
    unittest.main()
