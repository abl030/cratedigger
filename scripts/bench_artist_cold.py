"""Measure cold large-artist browse routes with the production request graph.

Start a cacheless local route server first. The route benchmark includes
library overlays, so it requires the same read-only Postgres tunnel plus Beets
database and library root described in ``docs/web-dev-server.md``. Keep Redis
disabled: otherwise metadata would not be cold and ``--mirror-counts`` refuses
to run.

    ssh -N -L 15432:10.20.0.11:5432 doc2
    PIPELINE_DB_DSN=postgresql://cratedigger@127.0.0.1:15432/cratedigger \\
    nix-shell --run "python3 scripts/web_dev_server.py --data live-db --measure-mirror-requests \\
      --beets-db /mnt/virtio/cratedigger/beets-db/beets-library.db \\
      --beets-directory /mnt/virtio/Music/Beets \\
      --mb-api http://192.168.1.43:5200/ws/2 \\
      --discogs-api http://192.168.1.44:8086"
    nix-shell --run "python3 scripts/bench_artist_cold.py --api-base http://127.0.0.1:8096 --mirror-counts"

It drives the same initial artist/library/compare request graph as
``loadArtistPage``. Pass ``--browser`` to also launch headless
``playwright-mcp`` and measure the real browser ``loadArtistPage`` →
``renderUnified`` path, including settled DOM/text size and actual API request
count. Timings deliberately have no pass/fail threshold: mirrors and the
library are live systems.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from typing import Protocol

import msgspec


@dataclass(frozen=True)
class Artist:
    name: str
    mbid: str


ARTISTS = (
    Artist("Taylor Swift", "20244d07-534f-4eff-b4d4-930878889970"),
    Artist("The Rolling Stones", "b071f9fa-14b0-4217-8e97-eb41da73f598"),
    Artist("The Beatles", "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"),
)


@dataclass(frozen=True)
class ResponseMeasurement:
    route: str
    elapsed_s: float
    bytes: int
    rows: int


@dataclass(frozen=True)
class ArtistMeasurement:
    artist: Artist
    useful_s: float
    settled_s: float
    responses: tuple[ResponseMeasurement, ...]
    mb_mirror_requests: int | None
    discogs_mirror_requests: int | None


@dataclass(frozen=True)
class BrowserMeasurement:
    artist: Artist
    useful_s: float
    settled_s: float
    html_chars: int
    text_chars: int
    api_requests: int
    mb_mirror_requests: int | None
    discogs_mirror_requests: int | None


def _wire_objects() -> list[object]:
    return []


class _RouteResponseWire(msgspec.Struct, forbid_unknown_fields=False):
    release_groups: list[object] = msgspec.field(default_factory=_wire_objects)
    ungrouped_releases: list[object] = msgspec.field(default_factory=_wire_objects)
    albums: list[object] = msgspec.field(default_factory=_wire_objects)
    both: list[object] = msgspec.field(default_factory=_wire_objects)
    mb_unpaired: list[object] = msgspec.field(default_factory=_wire_objects)
    discogs_unpaired: list[object] = msgspec.field(default_factory=_wire_objects)
    discogs_ungrouped_releases: list[object] = msgspec.field(default_factory=_wire_objects)


class _MirrorMeasurementWire(msgspec.Struct, forbid_unknown_fields=False):
    data: str | None = None
    redis_enabled: bool | None = None
    token: str | None = None
    mb_mirror_requests: int | None = None
    discogs_mirror_requests: int | None = None


class _MCPContentWire(msgspec.Struct, forbid_unknown_fields=False):
    type: str
    text: str | None = None


def _mcp_contents() -> list[_MCPContentWire]:
    return []


class _MCPToolResultWire(msgspec.Struct, forbid_unknown_fields=False):
    content: list[_MCPContentWire] = msgspec.field(default_factory=_mcp_contents)


class _MCPResponseWire(msgspec.Struct, forbid_unknown_fields=False):
    id: int | None = None
    result: _MCPToolResultWire | None = None


class _BrowserMeasurementWire(msgspec.Struct, forbid_unknown_fields=False):
    useful_s: float
    settled_s: float
    html_chars: int
    text_chars: int
    api_requests: int
    settled_complete: bool


def require_complete_browser_measurement(payload: _BrowserMeasurementWire) -> None:
    """Refuse a browser result unless both post-useful decorations landed."""
    if not payload.settled_complete:
        raise RuntimeError(
            "browser benchmark did not settle: compare or disambiguation did not complete",
        )


def _fetch_json(url: str, route: str) -> ResponseMeasurement:
    started = time.perf_counter()
    request = urllib.request.Request(url, headers={"Cache-Control": "no-store"})
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read()
    data = msgspec.json.decode(raw, type=_RouteResponseWire)
    rows = sum(len(value) for value in (
        data.release_groups, data.ungrouped_releases, data.albums, data.both,
        data.mb_unpaired, data.discogs_unpaired, data.discogs_ungrouped_releases,
    ))
    return ResponseMeasurement(
        route=route,
        elapsed_s=time.perf_counter() - started,
        bytes=len(raw),
        rows=rows,
    )


def _mirror_measurement_request(
    api_base: str, path: str, *, token: str | None = None,
) -> _MirrorMeasurementWire:
    separator = "?" if token is not None else ""
    query = urllib.parse.urlencode({"token": token}) if token is not None else ""
    request = urllib.request.Request(
        f"{api_base}/__dev/mirror-counts{path}{separator}{query}",
        method="POST" if path in ("/start", "/finish") else "GET",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return msgspec.json.decode(response.read(), type=_MirrorMeasurementWire)


def _start_mirror_measurement(api_base: str) -> str:
    capability = _mirror_measurement_request(api_base, "/capability")
    if capability.data != "live-db" or capability.redis_enabled is not False:
        raise RuntimeError(
            "mirror counts require a live-db dev server with Redis disabled",
        )
    payload = _mirror_measurement_request(api_base, "/start")
    token = payload.token
    if not isinstance(token, str) or not token:
        raise TypeError("mirror counter start returned no lease token")
    return token


def _finish_mirror_measurement(api_base: str, token: str) -> tuple[int, int]:
    payload = _mirror_measurement_request(api_base, "/finish", token=token)
    mb_count = payload.mb_mirror_requests
    discogs_count = payload.discogs_mirror_requests
    if not isinstance(mb_count, int) or not isinstance(discogs_count, int):
        raise TypeError("mirror counter endpoint returned invalid counts")
    return mb_count, discogs_count


def measure_artist(
    api_base: str, artist: Artist, *, mirror_counts: bool = False,
) -> ArtistMeasurement:
    quoted_name = urllib.parse.quote(artist.name)
    quoted_id = urllib.parse.quote(artist.mbid)
    urls = {
        "artist": f"{api_base}/api/artist/{quoted_id}?name={quoted_name}",
        "library": f"{api_base}/api/library/artist?name={quoted_name}&mbid={quoted_id}",
        "compare": f"{api_base}/api/artist/compare?name={quoted_name}&mbid={quoted_id}",
    }
    lease_token = _start_mirror_measurement(api_base) if mirror_counts else None
    started = time.perf_counter()
    # Match the cold browser request graph: compare starts immediately but
    # useful content waits only for selected-source catalogue plus library.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    try:
        try:
            futures = {
                route: executor.submit(_fetch_json, url, route)
                for route, url in urls.items()
            }
            artist_response = futures["artist"].result()
            library_response = futures["library"].result()
            useful_s = time.perf_counter() - started
            # This is exactly where loadArtistPage starts analysis: after useful
            # content has rendered, while compare may still be traversing mirrors.
            disambiguate_future = executor.submit(
                _fetch_json,
                f"{api_base}/api/artist/{quoted_id}/disambiguate",
                "disambiguate",
            )
            compare_response = futures["compare"].result()
            disambiguate = disambiguate_future.result()
        finally:
            executor.shutdown(wait=True)
    finally:
        mb_count, discogs_count = (
            _finish_mirror_measurement(api_base, lease_token)
            if lease_token is not None else (None, None)
        )
    return ArtistMeasurement(
        artist=artist,
        useful_s=useful_s,
        settled_s=time.perf_counter() - started,
        responses=(artist_response, library_response, compare_response, disambiguate),
        mb_mirror_requests=mb_count,
        discogs_mirror_requests=discogs_count,
    )


def _mcp_request(
    process: subprocess.Popen[str], request_id: int, method: str, params: dict[str, object],
) -> _MCPToolResultWire:
    assert process.stdin is not None and process.stdout is not None
    process.stdin.write(json.dumps({
        "jsonrpc": "2.0", "id": request_id, "method": method, "params": params,
    }) + "\n")
    process.stdin.flush()
    while line := process.stdout.readline():
        message = msgspec.json.decode(line.encode(), type=_MCPResponseWire)
        if message.id == request_id:
            if message.result is not None:
                return message.result
            raise RuntimeError("Playwright MCP response had no result")
    raise RuntimeError("Playwright MCP exited before responding")


class _StoppablePlaywrightProcess(Protocol):
    def terminate(self) -> None: ...
    def kill(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


def _stop_playwright_process(process: _StoppablePlaywrightProcess) -> None:
    """Terminate MCP, escalating to kill so repeats cannot orphan Chromium."""
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


def measure_browser(
    api_base: str, artist: Artist, command: str, *, mirror_counts: bool = False,
) -> BrowserMeasurement:
    """Drive the SPA's real loadArtistPage/renderUnified path in headless Chromium."""
    lease_token = _start_mirror_measurement(api_base) if mirror_counts else None
    process: subprocess.Popen[str] | None = None
    try:
        process = subprocess.Popen(
            [command, "--headless", "--isolated", "--allowed-hosts", "*"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        _mcp_request(process, 1, "initialize", {
            "protocolVersion": "2025-03-26", "capabilities": {},
            "clientInfo": {"name": "bench-artist-cold", "version": "1"},
        })
        assert process.stdin is not None
        process.stdin.write(json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {},
        }) + "\n")
        process.stdin.flush()
        _mcp_request(process, 2, "tools/call", {
            "name": "browser_navigate", "arguments": {"url": api_base},
        })
        code = """async (page) => await page.evaluate(async () => {
          const artist = ARTIST_JSON;
          const browse = await import('/js/browse.js');
          const { state } = await import('/js/state.js');
          performance.clearResourceTimings(); const start = performance.now();
          await browse.loadArtistPage(artist.mbid, artist.name);
          const useful = performance.now() - start; const deadline = performance.now() + 180000;
          let settledComplete = false;
          while (performance.now() < deadline) {
            const cached = state.browseCache[artist.mbid];
            if (cached?.compare && cached?.disamb) { settledComplete = true; break; }
            await new Promise(resolve => setTimeout(resolve, 25));
          }
          const body = document.getElementById('browse-artist-body');
          return { useful_s: useful / 1000, settled_s: (performance.now() - start) / 1000,
            html_chars: body?.innerHTML.length || 0, text_chars: body?.textContent.length || 0,
            api_requests: performance.getEntriesByType('resource').filter(x => x.name.includes('/api/')).length,
            settled_complete: settledComplete };
        })""".replace("ARTIST_JSON", json.dumps(asdict(artist)))
        result = _mcp_request(process, 3, "tools/call", {
            "name": "browser_run_code_unsafe", "arguments": {"code": code},
        })
        if not result.content:
            raise RuntimeError("Playwright MCP returned no measurement")
        text = result.content[0].text
        if not isinstance(text, str):
            raise TypeError("Playwright MCP measurement was not text")
        # MCP wraps an unsafe-code return value in a human-readable transcript:
        # ``### Result`` followed by JSON, then the executed source.  Do not
        # accidentally benchmark the route graph successfully and then reject
        # its measurement because of that envelope.
        result_prefix = "### Result\n"
        if not text.startswith(result_prefix):
            raise RuntimeError(f"Playwright MCP returned no result: {text[:200]!r}")
        result_json = text[len(result_prefix):].split("\n### ", 1)[0]
        payload = msgspec.json.decode(result_json.encode(), type=_BrowserMeasurementWire)
        require_complete_browser_measurement(payload)
        mb_count, discogs_count = (
            _finish_mirror_measurement(api_base, lease_token)
            if lease_token is not None else (None, None)
        )
        lease_token = None
        return BrowserMeasurement(
            artist=artist,
            mb_mirror_requests=mb_count,
            discogs_mirror_requests=discogs_count,
            useful_s=payload.useful_s,
            settled_s=payload.settled_s,
            html_chars=payload.html_chars,
            text_chars=payload.text_chars,
            api_requests=payload.api_requests,
        )
    finally:
        if process is not None:
            _stop_playwright_process(process)
        if lease_token is not None:
            _finish_mirror_measurement(api_base, lease_token)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-base", required=True,
        help="live-db dev server with Redis disabled; see docs/web-dev-server.md",
    )
    parser.add_argument("--runs", type=int, default=3, help="cold runs per fixed artist")
    parser.add_argument("--json", action="store_true", help="emit machine-readable measurements")
    parser.add_argument("--browser", action="store_true", help="also measure real headless browser rendering")
    parser.add_argument(
        "--mirror-counts", action="store_true",
        help="require exact outbound counts from a dev server started with --measure-mirror-requests",
    )
    parser.add_argument("--playwright-command", default="playwright-mcp")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    api_base = args.api_base.rstrip("/")
    all_runs = [
        measure_artist(api_base, artist, mirror_counts=args.mirror_counts)
        for artist in ARTISTS
        for _ in range(args.runs)
    ]
    browser_runs = [
        measure_browser(api_base, artist, args.playwright_command, mirror_counts=args.mirror_counts)
        for artist in ARTISTS
        for _ in range(args.runs)
    ] if args.browser else []
    if args.json:
        print(json.dumps({
            "route": [asdict(run) for run in all_runs],
            "browser": [asdict(run) for run in browser_runs],
        }, indent=2))
        return
    for run in all_runs:
        print(f"{run.artist.name}: useful {run.useful_s:.3f}s, settled {run.settled_s:.3f}s")
        if run.mb_mirror_requests is not None:
            print(
                f"  outbound mirrors: MB {run.mb_mirror_requests}, "
                f"Discogs {run.discogs_mirror_requests}",
            )
        for response in run.responses:
            print(
                f"  {response.route:12} {response.elapsed_s:.3f}s "
                f"{response.bytes:>9} B {response.rows:>6} rows",
            )
    for run in browser_runs:
        print(
            f"{run.artist.name} browser: useful {run.useful_s:.3f}s, "
            f"settled {run.settled_s:.3f}s, {run.html_chars} HTML chars, "
            f"{run.text_chars} text chars, {run.api_requests} API requests, complete; "
            f"outbound mirrors: MB {run.mb_mirror_requests}, "
            f"Discogs {run.discogs_mirror_requests}",
        )


if __name__ == "__main__":
    main()
