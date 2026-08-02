"""Target-scoped fail-closed Node worker for generated tests.

The child imports JavaScript once, then serves one strictly framed JSON request at
a time.  A protocol violation poisons the worker: generated tests must never keep
exploring after losing confidence that a response belongs to the current world.
"""

from __future__ import annotations

import os
import selectors
import subprocess
import threading
import time
from pathlib import Path
from typing import Final, Self

import msgspec

MAX_RESPONSE_BYTES: Final = 1024 * 1024
MAX_REQUEST_BYTES: Final = 1024 * 1024
_STDERR_TAIL_BYTES: Final = 16 * 1024


class NodeJsonlWorkerError(RuntimeError):
    """The Node worker could not return one authoritative response."""


class _Request(msgspec.Struct, forbid_unknown_fields=True):
    id: int
    operation: str
    payload: object


class _Response(msgspec.Struct, forbid_unknown_fields=True):
    id: int
    ok: bool
    result: object
    error: str | None


_PROTOCOL: Final = r"""
import readline from 'node:readline';

const lines = readline.createInterface({
  input: process.stdin,
  crlfDelay: Infinity,
  terminal: false,
});

process.stdout.write(JSON.stringify({
  id: 0,
  ok: true,
  result: 'ready',
  error: null,
}) + '\n');

for await (const line of lines) {
  let request = null;
  try {
    request = JSON.parse(line);
    if (
      request === null
      || !Number.isSafeInteger(request.id)
      || request.id < 1
      || typeof request.operation !== 'string'
      || !Object.hasOwn(request, 'payload')
    ) {
      throw new Error('invalid request frame');
    }
    const result = await handle(request.operation, request.payload);
    process.stdout.write(JSON.stringify({
      id: request.id,
      ok: true,
      result,
      error: null,
    }) + '\n');
  } catch (error) {
    const message = error instanceof Error ? error.stack ?? error.message : String(error);
    process.stdout.write(JSON.stringify({
      id: Number.isSafeInteger(request?.id) ? request.id : -1,
      ok: false,
      result: null,
      error: message,
    }) + '\n');
  }
}
"""


class NodeJsonlWorker:
    """One reusable Node child whose lifetime is bounded by one Python target."""

    def __init__(
        self,
        handler_source: str,
        *,
        cwd: Path,
        timeout_seconds: float = 10.0,
        command: tuple[str, ...] | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._timeout_seconds = timeout_seconds
        self._next_id = 1
        self._closed = False
        self._failure: str | None = None
        self._stdout_buffer = bytearray()
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._stderr_thread: threading.Thread | None = None
        server_command = list(command or (
            "node",
            "--input-type=module",
            "--eval",
            f"{handler_source}\n{_PROTOCOL}",
        ))
        self._process: subprocess.Popen[bytes] = subprocess.Popen(
            server_command,
            cwd=cwd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=False,
            bufsize=0,
        )
        try:
            if (
                self._process.stdin is None
                or self._process.stdout is None
                or self._process.stderr is None
            ):
                raise NodeJsonlWorkerError("Node worker pipes were not created")
            os.set_blocking(self._process.stdin.fileno(), False)
            self._stderr_thread = threading.Thread(
                target=self._drain_stderr,
                name=f"node-jsonl-stderr-{self._process.pid}",
                daemon=True,
            )
            self._stderr_thread.start()
            deadline = time.monotonic() + self._timeout_seconds
            startup = self._decode_response(
                0,
                self._read_response_line(
                    0,
                    self._process.stdout.fileno(),
                    deadline,
                ),
            )
            if startup.result != "ready" or self._stdout_buffer:
                detail = self._poison("invalid Node worker startup handshake")
                raise NodeJsonlWorkerError(detail)
        except BaseException as exc:
            if self._failure is None:
                detail = self._poison(f"Node worker initialization failed: {exc}")
                raise NodeJsonlWorkerError(detail) from exc
            raise

    @property
    def is_running(self) -> bool:
        return not self._closed and self._process.poll() is None

    def request(self, operation: str, payload: object) -> object:
        if self._failure is not None:
            raise NodeJsonlWorkerError(self._failure)
        if self._closed:
            raise NodeJsonlWorkerError("Node worker is not running")
        if self._process.poll() is not None:
            detail = self._poison("Node worker exited before request")
            raise NodeJsonlWorkerError(detail)
        deadline = time.monotonic() + self._timeout_seconds
        request_id = self._next_id
        self._next_id += 1
        frame = msgspec.json.encode(
            _Request(id=request_id, operation=operation, payload=payload),
        ) + b"\n"
        if len(frame) > MAX_REQUEST_BYTES:
            detail = self._poison(
                f"request {request_id} exceeded {MAX_REQUEST_BYTES} bytes",
            )
            raise NodeJsonlWorkerError(detail)
        stdout = self._process.stdout
        assert stdout is not None
        self._write_frame(frame, deadline, request_id)
        line = self._read_response_line(request_id, stdout.fileno(), deadline)
        response = self._decode_response(request_id, line)
        if self._stdout_buffer:
            detail = self._poison(
                f"unexpected extra output after response {request_id}",
            )
            raise NodeJsonlWorkerError(detail)
        return response.result

    def _decode_response(self, request_id: int, line: bytes) -> _Response:
        try:
            line_text = line.decode("utf-8")
        except UnicodeDecodeError as exc:
            detail = self._poison(
                f"invalid UTF-8 response for request {request_id}",
            )
            raise NodeJsonlWorkerError(detail) from exc
        try:
            response = msgspec.json.decode(line_text, type=_Response)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            detail = self._poison(
                f"malformed response for request {request_id}: {line_text[:200]!r}",
            )
            raise NodeJsonlWorkerError(detail) from exc
        if response.id != request_id:
            detail = self._poison(
                f"response id {response.id} did not match request {request_id}",
            )
            raise NodeJsonlWorkerError(detail)
        if not response.ok:
            detail = self._poison(
                f"JavaScript request {request_id} failed: {response.error or 'unknown error'}",
            )
            raise NodeJsonlWorkerError(detail)
        if response.error is not None:
            detail = self._poison(
                f"successful response {request_id} carried an error",
            )
            raise NodeJsonlWorkerError(detail)
        return response

    def _write_frame(self, frame: bytes, deadline: float, request_id: int) -> None:
        stdin = self._process.stdin
        assert stdin is not None
        stdin_fd = stdin.fileno()
        remaining_frame = memoryview(frame)
        with selectors.DefaultSelector() as selector:
            selector.register(stdin_fd, selectors.EVENT_WRITE)
            while remaining_frame:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    detail = self._poison(
                        f"request {request_id} timed out after "
                        f"{self._timeout_seconds:g}s",
                    )
                    raise NodeJsonlWorkerError(detail)
                if self._process.poll() is not None:
                    detail = self._poison(
                        f"child exited while writing request {request_id}",
                    )
                    raise NodeJsonlWorkerError(detail)
                try:
                    written = os.write(stdin_fd, remaining_frame)
                except BlockingIOError:
                    continue
                except (BrokenPipeError, OSError) as exc:
                    detail = self._poison("child closed stdin")
                    raise NodeJsonlWorkerError(detail) from exc
                if written < 1:
                    detail = self._poison("child accepted zero request bytes")
                    raise NodeJsonlWorkerError(detail)
                remaining_frame = remaining_frame[written:]

    def _read_response_line(
        self,
        request_id: int,
        stdout_fd: int,
        deadline: float,
    ) -> bytes:
        with selectors.DefaultSelector() as selector:
            selector.register(stdout_fd, selectors.EVENT_READ)
            while True:
                newline = self._stdout_buffer.find(b"\n")
                if newline >= 0:
                    if newline > MAX_RESPONSE_BYTES:
                        detail = self._poison(
                            f"response exceeded {MAX_RESPONSE_BYTES} bytes for "
                            f"request {request_id}",
                        )
                        raise NodeJsonlWorkerError(detail)
                    line = bytes(self._stdout_buffer[:newline])
                    del self._stdout_buffer[: newline + 1]
                    return line
                if len(self._stdout_buffer) > MAX_RESPONSE_BYTES:
                    detail = self._poison(
                        f"response exceeded {MAX_RESPONSE_BYTES} bytes for "
                        f"request {request_id}",
                    )
                    raise NodeJsonlWorkerError(detail)
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    detail = self._poison(
                        f"request {request_id} timed out after "
                        f"{self._timeout_seconds:g}s",
                    )
                    raise NodeJsonlWorkerError(detail)
                try:
                    chunk = os.read(stdout_fd, 64 * 1024)
                except OSError as exc:
                    detail = self._poison(
                        f"failed reading response {request_id}",
                    )
                    raise NodeJsonlWorkerError(detail) from exc
                if not chunk:
                    detail = self._poison(
                        f"child exited before response {request_id}",
                    )
                    raise NodeJsonlWorkerError(detail)
                self._stdout_buffer.extend(chunk)

    def _poison(self, reason: str) -> str:
        self._kill()
        self._join_stderr_thread()
        stderr = self._read_stderr()
        self._close_pipes()
        self._failure = f"{reason}{f'; stderr: {stderr}' if stderr else ''}"
        return self._failure

    def _read_stderr(self) -> str:
        with self._stderr_lock:
            tail = bytes(self._stderr_tail)
        return tail.decode("utf-8", errors="replace").strip()[-2000:]

    def _drain_stderr(self) -> None:
        stderr = self._process.stderr
        if stderr is None:
            return
        while True:
            try:
                chunk = stderr.read(8192)
            except (OSError, ValueError):
                return
            if not chunk:
                return
            with self._stderr_lock:
                self._stderr_tail.extend(chunk)
                excess = len(self._stderr_tail) - _STDERR_TAIL_BYTES
                if excess > 0:
                    del self._stderr_tail[:excess]

    def _join_stderr_thread(self) -> None:
        thread = self._stderr_thread
        if thread is None or thread.ident is None:
            return
        thread.join(timeout=0.1)

    def _kill(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait()

    def _terminate(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=2)

    def _close_pipes(self) -> None:
        for stream in (
            self._process.stdin,
            self._process.stdout,
            self._process.stderr,
        ):
            if stream is not None and not stream.closed:
                try:
                    stream.close()
                except OSError:
                    pass

    def close(self) -> None:
        if self._closed:
            return
        stdin = self._process.stdin
        if stdin is not None:
            try:
                stdin.close()
            except OSError:
                pass
        self._closed = True
        try:
            self._process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=2)
        self._join_stderr_thread()
        self._close_pipes()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
