"""Target-scoped persistent Ruff language-server transport for generated tests."""

from __future__ import annotations

import os
import selectors
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Final, Literal, Self

import msgspec

_MAX_FRAME_BYTES: Final = 2 * 1024 * 1024
_MAX_HEADER_BYTES: Final = 16 * 1024
_STDERR_TAIL_BYTES: Final = 16 * 1024


def _is_unset(value: object) -> bool:
    return isinstance(value, msgspec.UnsetType)


class RuffLspWorkerError(RuntimeError):
    """The Ruff server could not authoritatively diagnose the current world."""


class _LspRequest(msgspec.Struct, kw_only=True):
    id: int
    method: str
    params: object
    jsonrpc: Literal["2.0"] = "2.0"


class _LspNotification(msgspec.Struct, kw_only=True):
    method: str
    params: object
    jsonrpc: Literal["2.0"] = "2.0"


class _LspEnvelope(msgspec.Struct, forbid_unknown_fields=False):
    jsonrpc: Literal["2.0"]
    id: int | None | msgspec.UnsetType = msgspec.UNSET
    method: str | msgspec.UnsetType = msgspec.UNSET
    params: object | msgspec.UnsetType = msgspec.UNSET
    result: object | msgspec.UnsetType = msgspec.UNSET
    error: object | msgspec.UnsetType = msgspec.UNSET


class _Diagnostic(msgspec.Struct, forbid_unknown_fields=False):
    code: str | int | None = None


class _PublishDiagnostics(msgspec.Struct, forbid_unknown_fields=False):
    uri: str
    diagnostics: list[_Diagnostic]
    version: int | None = None


class RuffLspWorker:
    """One Ruff server reused only within one isolated Python test target."""

    def __init__(
        self,
        relative_paths: tuple[str, ...],
        *,
        cwd: Path,
        timeout_seconds: float = 10.0,
        command: tuple[str, ...] | None = None,
    ) -> None:
        if not relative_paths or len(set(relative_paths)) != len(relative_paths):
            raise ValueError("relative_paths must be unique and non-empty")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._paths = relative_paths
        self._timeout_seconds = timeout_seconds
        self._request_id = 1
        self._versions = {path: 0 for path in relative_paths}
        self._buffer = bytearray()
        self._closed = False
        self._failure: str | None = None
        self._stderr_tail = bytearray()
        self._stderr_lock = threading.Lock()
        self._temporary = tempfile.TemporaryDirectory(prefix="cratedigger-ruff-lsp-")
        self._temporary_cleaned = False
        self._root = Path(self._temporary.name)
        for relative_path in relative_paths:
            (self._root / relative_path).parent.mkdir(parents=True, exist_ok=True)
        self._uris = {
            path: (self._root / path).as_uri()
            for path in relative_paths
        }
        self._paths_by_uri = {uri: path for path, uri in self._uris.items()}
        server_command = list(command or ("ruff", "server", "--silent"))
        if command is None:
            config = cwd / "pyproject.toml"
            if config.is_file():
                server_command.extend(("--config", str(config)))
        try:
            self._process: subprocess.Popen[bytes] = subprocess.Popen(
                server_command,
                cwd=cwd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                bufsize=0,
            )
        except BaseException:
            self._cleanup_temporary()
            raise
        self._stderr_thread: threading.Thread | None = None
        try:
            if (
                self._process.stdin is None
                or self._process.stdout is None
                or self._process.stderr is None
            ):
                raise RuffLspWorkerError("Ruff server pipes were not created")
            os.set_blocking(self._process.stdin.fileno(), False)
            self._stderr_thread = threading.Thread(
                target=self._drain_stderr,
                name=f"ruff-lsp-stderr-{self._process.pid}",
                daemon=True,
            )
            self._stderr_thread.start()
            self._initialize()
        except BaseException as exc:
            if self._failure is None:
                detail = self._poison(
                    f"Ruff server initialization failed: {exc}",
                )
                raise RuffLspWorkerError(detail) from exc
            raise

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def workspace_root(self) -> Path:
        return self._root

    @property
    def is_running(self) -> bool:
        return not self._closed and self._process.poll() is None

    def findings(
        self,
        sources: dict[str, str],
    ) -> tuple[dict[str, object], ...]:
        if self._failure is not None:
            raise RuffLspWorkerError(self._failure)
        if self._closed:
            raise RuffLspWorkerError("Ruff server is not running")
        if self._process.poll() is not None:
            detail = self._poison("Ruff server exited before request")
            raise RuffLspWorkerError(detail)
        if set(sources) != set(self._paths):
            raise RuffLspWorkerError("sources must contain the exact configured paths")

        deadline = time.monotonic() + self._timeout_seconds
        expected_versions: dict[str, int] = {}
        for path in self._paths:
            version = self._versions[path] + 1
            self._versions[path] = version
            uri = self._uris[path]
            expected_versions[uri] = version
            if version == 1:
                self._notify(
                    "textDocument/didOpen",
                    {
                        "textDocument": {
                            "uri": uri,
                            "languageId": "python",
                            "version": version,
                            "text": sources[path],
                        },
                    },
                    deadline,
                )
            else:
                self._notify(
                    "textDocument/didChange",
                    {
                        "textDocument": {"uri": uri, "version": version},
                        "contentChanges": [{"text": sources[path]}],
                    },
                    deadline,
                )

        received: dict[str, _PublishDiagnostics] = {}
        while len(received) < len(expected_versions):
            message = self._read_message(deadline)
            if message.method != "textDocument/publishDiagnostics":
                detail = self._poison("unexpected Ruff LSP message")
                raise RuffLspWorkerError(detail)
            if (
                not _is_unset(message.id)
                or _is_unset(message.params)
                or not _is_unset(message.result)
                or not _is_unset(message.error)
            ):
                detail = self._poison("malformed Ruff diagnostics envelope")
                raise RuffLspWorkerError(detail)
            try:
                published = msgspec.convert(
                    message.params,
                    type=_PublishDiagnostics,
                )
            except (TypeError, msgspec.ValidationError) as exc:
                detail = self._poison("malformed Ruff diagnostics notification")
                raise RuffLspWorkerError(detail) from exc
            expected_version = expected_versions.get(published.uri)
            if expected_version is None:
                detail = self._poison(
                    f"diagnostics for unknown URI: {published.uri}",
                )
                raise RuffLspWorkerError(detail)
            if published.version is None or published.version > expected_version:
                detail = self._poison(
                    f"unexpected diagnostics version for {published.uri}: "
                    f"{published.version!r}",
                )
                raise RuffLspWorkerError(detail)
            if published.version < expected_version:
                continue
            received[published.uri] = published

        findings: list[dict[str, object]] = []
        for uri, published in received.items():
            relative_path = self._paths_by_uri[uri]
            filename = str(self._root / relative_path)
            for diagnostic in published.diagnostics:
                if diagnostic.code is None:
                    detail = self._poison(
                        f"Ruff diagnostic lacked a code for {relative_path}",
                    )
                    raise RuffLspWorkerError(detail)
                findings.append({
                    "filename": filename,
                    "code": str(diagnostic.code),
                })
        return tuple(findings)

    def _initialize(self) -> None:
        deadline = time.monotonic() + self._timeout_seconds
        root_uri = self._root.as_uri()
        request_id = self._send_request(
            "initialize",
            {
                "processId": None,
                "rootUri": root_uri,
                "capabilities": {},
                "workspaceFolders": [{"uri": root_uri, "name": "generated-world"}],
            },
            deadline,
        )
        response = self._wait_for_response(request_id, deadline)
        if not _is_unset(response.error):
            detail = self._poison(f"Ruff initialize failed: {response.error!r}")
            raise RuffLspWorkerError(detail)
        self._notify("initialized", {}, deadline)

    def _send_request(self, method: str, params: object, deadline: float) -> int:
        request_id = self._request_id
        self._request_id += 1
        self._send(
            _LspRequest(id=request_id, method=method, params=params),
            deadline,
        )
        return request_id

    def _notify(self, method: str, params: object, deadline: float) -> None:
        self._send(_LspNotification(method=method, params=params), deadline)

    def _send(
        self,
        message: _LspRequest | _LspNotification,
        deadline: float,
    ) -> None:
        stdin = self._process.stdin
        if stdin is None or self._closed:
            raise RuffLspWorkerError("Ruff server is not running")
        if self._process.poll() is not None:
            detail = self._poison("Ruff server exited before a write")
            raise RuffLspWorkerError(detail)
        body = msgspec.json.encode(message)
        frame = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body
        if len(body) > _MAX_FRAME_BYTES:
            detail = self._poison("Ruff LSP request exceeded size limit")
            raise RuffLspWorkerError(detail)
        stdin_fd = stdin.fileno()
        remaining_frame = memoryview(frame)
        with selectors.DefaultSelector() as selector:
            selector.register(stdin_fd, selectors.EVENT_WRITE)
            while remaining_frame:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    detail = self._poison(
                        f"Ruff LSP request timed out after "
                        f"{self._timeout_seconds:g}s",
                    )
                    raise RuffLspWorkerError(detail)
                if self._process.poll() is not None:
                    detail = self._poison("Ruff server exited during a write")
                    raise RuffLspWorkerError(detail)
                try:
                    written = os.write(stdin_fd, remaining_frame)
                except BlockingIOError:
                    continue
                except (BrokenPipeError, OSError) as exc:
                    detail = self._poison("Ruff server closed stdin")
                    raise RuffLspWorkerError(detail) from exc
                if written < 1:
                    detail = self._poison("Ruff server accepted zero request bytes")
                    raise RuffLspWorkerError(detail)
                remaining_frame = remaining_frame[written:]

    def _wait_for_response(
        self,
        request_id: int,
        deadline: float,
    ) -> _LspEnvelope:
        while True:
            message = self._read_message(deadline)
            if message.id == request_id:
                if (
                    not _is_unset(message.method)
                    or not _is_unset(message.params)
                    or (
                        _is_unset(message.result)
                        == _is_unset(message.error)
                    )
                ):
                    detail = self._poison(
                        f"malformed Ruff response {request_id}",
                    )
                    raise RuffLspWorkerError(detail)
                return message
            if not _is_unset(message.id):
                detail = self._poison(
                    f"Ruff response id {message.id} did not match {request_id}",
                )
                raise RuffLspWorkerError(detail)
            if (
                _is_unset(message.method)
                or _is_unset(message.params)
                or not _is_unset(message.result)
                or not _is_unset(message.error)
            ):
                detail = self._poison("malformed Ruff notification envelope")
                raise RuffLspWorkerError(detail)

    def _read_message(self, deadline: float) -> _LspEnvelope:
        header = self._read_until(b"\r\n\r\n", deadline, _MAX_HEADER_BYTES)
        lengths: list[int] = []
        try:
            for line in header.split(b"\r\n"):
                name, separator, value = line.partition(b":")
                if separator and name.lower() == b"content-length":
                    lengths.append(int(value.strip()))
        except ValueError as exc:
            detail = self._poison("invalid Ruff LSP Content-Length")
            raise RuffLspWorkerError(detail) from exc
        if len(lengths) != 1 or not 0 <= lengths[0] <= _MAX_FRAME_BYTES:
            detail = self._poison("invalid Ruff LSP Content-Length")
            raise RuffLspWorkerError(detail)
        body = self._read_exact(lengths[0], deadline)
        try:
            return msgspec.json.decode(body, type=_LspEnvelope)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            detail = self._poison("malformed Ruff LSP response")
            raise RuffLspWorkerError(detail) from exc

    def _read_until(self, marker: bytes, deadline: float, limit: int) -> bytes:
        while True:
            index = self._buffer.find(marker)
            if index >= 0:
                result = bytes(self._buffer[:index])
                del self._buffer[: index + len(marker)]
                return result
            if len(self._buffer) > limit:
                detail = self._poison("Ruff LSP header exceeded size limit")
                raise RuffLspWorkerError(detail)
            self._read_available(deadline)

    def _read_exact(self, length: int, deadline: float) -> bytes:
        while len(self._buffer) < length:
            self._read_available(deadline)
        result = bytes(self._buffer[:length])
        del self._buffer[:length]
        return result

    def _read_available(self, deadline: float) -> None:
        stdout = self._process.stdout
        assert stdout is not None
        remaining = deadline - time.monotonic()
        with selectors.DefaultSelector() as selector:
            selector.register(stdout.fileno(), selectors.EVENT_READ)
            if remaining <= 0 or not selector.select(remaining):
                detail = self._poison(
                    f"Ruff LSP response timed out after {self._timeout_seconds:g}s",
                )
                raise RuffLspWorkerError(detail)
        try:
            chunk = os.read(stdout.fileno(), 64 * 1024)
        except OSError as exc:
            detail = self._poison("failed reading Ruff LSP response")
            raise RuffLspWorkerError(detail) from exc
        if not chunk:
            detail = self._poison("Ruff server exited before a complete response")
            raise RuffLspWorkerError(detail)
        self._buffer.extend(chunk)

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

    def _stderr(self) -> str:
        with self._stderr_lock:
            tail = bytes(self._stderr_tail)
        return tail.decode("utf-8", errors="replace").strip()[-2000:]

    def _poison(self, reason: str) -> str:
        self._kill()
        self._join_stderr_thread()
        stderr = self._stderr()
        self._close_pipes()
        self._cleanup_temporary()
        self._failure = f"{reason}{f'; stderr: {stderr}' if stderr else ''}"
        return self._failure

    def _cleanup_temporary(self) -> None:
        if self._temporary_cleaned:
            return
        self._temporary.cleanup()
        self._temporary_cleaned = True

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

    def _kill(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._process.poll() is None:
            self._process.kill()
            self._process.wait()

    def _join_stderr_thread(self) -> None:
        thread = self._stderr_thread
        if thread is None or thread.ident is None:
            return
        thread.join(timeout=0.1)

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
            self._cleanup_temporary()
            return
        try:
            deadline = time.monotonic() + self._timeout_seconds
            request_id = self._send_request("shutdown", None, deadline)
            self._wait_for_response(request_id, deadline)
            self._notify("exit", None, deadline)
        except RuffLspWorkerError:
            pass
        stdin = self._process.stdin
        if stdin is not None and not stdin.closed:
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
        self._cleanup_temporary()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
