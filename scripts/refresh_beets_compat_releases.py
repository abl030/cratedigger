#!/usr/bin/env python3
"""Refresh the reviewed rolling Beets release compatibility manifest.

Nix evaluation consumes the resulting immutable JSON only.  This command is
the deliberately separate, networked operator step that prepares a reviewed
manifest change; it never updates the lock file or production package.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
from collections.abc import Callable, Iterable
from pathlib import Path

import msgspec

WINDOW_DAYS = 730
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "nix" / "beets-compat-releases.json"
_SRI_SHA256 = re.compile(r"sha256-[A-Za-z0-9+/]{43}=")


class GithubRelease(msgspec.Struct, forbid_unknown_fields=True):
    tag_name: str
    published_at: str | None
    draft: bool
    prerelease: bool


class ManifestEntry(msgspec.Struct, forbid_unknown_fields=True):
    version: str
    tag: str
    publishedAt: str
    rev: str
    narHash: str
    buildBackend: str


class GithubCommit(msgspec.Struct, forbid_unknown_fields=True):
    sha: str


class NixPrefetch(msgspec.Struct, forbid_unknown_fields=True):
    hash: str


def parse_utc_timestamp(value: str) -> dt.datetime:
    """Return an aware UTC timestamp or reject a non-canonical API value."""
    if not value.endswith("Z"):
        raise ValueError(f"timestamp must be UTC Zulu time: {value!r}")
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value!r}") from exc
    if parsed.tzinfo != dt.UTC:
        raise ValueError(f"timestamp must resolve to UTC: {value!r}")
    return parsed


def parse_as_of(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"--as-of must be YYYY-MM-DD: {value!r}") from exc


def select_final_releases(
    releases: Iterable[GithubRelease], *, as_of: dt.date,
) -> list[GithubRelease]:
    """Select final releases in the inclusive rolling UTC-day window."""
    start = as_of - dt.timedelta(days=WINDOW_DAYS)
    selected: list[GithubRelease] = []
    seen_tags: set[str] = set()
    for release in releases:
        tag = release.tag_name
        published_at = release.published_at
        if release.draft or release.prerelease or published_at is None:
            continue
        published = parse_utc_timestamp(published_at).date()
        if not start <= published <= as_of:
            continue
        if tag in seen_tags:
            raise ValueError(f"duplicate qualifying release tag: {tag}")
        seen_tags.add(tag)
        selected.append(release)
    return sorted(selected, key=lambda item: (item.published_at or "", item.tag_name))


def build_backend_for(tag: str) -> str:
    """Describe source layout, not a runtime capability decision."""
    # Beets switched packaging at 2.13.  The tag remains in generated,
    # reviewable manifest data so Nix never has to parse release versions.
    parts = tag.removeprefix("v").split(".")
    if len(parts) < 2 or not all(part.isdigit() for part in parts):
        raise ValueError(f"release tag is not a final numeric Beets tag: {tag!r}")
    return "hatchling" if tuple(int(part) for part in parts[:2]) >= (2, 13) else "poetry-core"


def render_manifest(entries: Iterable[ManifestEntry]) -> str:
    ordered = sorted(entries, key=lambda item: (item.publishedAt, item.tag))
    rendered = [
        json.dumps(msgspec.to_builtins(entry), sort_keys=True, separators=(",", ":"))
        for entry in ordered
    ]
    return "[\n" + ",\n".join(f"  {entry}" for entry in rendered) + "\n]\n"


def decode_manifest(raw: bytes | str) -> list[ManifestEntry]:
    """Decode the checked-in manifest through its exact wire schema."""
    try:
        return msgspec.json.decode(raw, type=list[ManifestEntry], strict=True)
    except msgspec.DecodeError as exc:
        raise TypeError(f"manifest is not a valid compatibility wire payload: {exc}") from exc


def validate_manifest(entries: Iterable[ManifestEntry]) -> None:
    """Fail closed on a manifest that is not immutable generated data."""
    prior_key: tuple[str, str] | None = None
    seen_tags: set[str] = set()
    for entry in entries:
        tag = entry.tag
        version = entry.version
        published_at = entry.publishedAt
        rev = entry.rev
        nar_hash = entry.narHash
        backend = entry.buildBackend
        if tag != f"v{version}" or build_backend_for(tag) != backend:
            raise ValueError(f"manifest tag/version/backend disagree: {entry!r}")
        parse_utc_timestamp(published_at)
        if not re.fullmatch(r"[0-9a-f]{40}", rev):
            raise ValueError(f"manifest revision is not immutable SHA-1: {rev!r}")
        if not _SRI_SHA256.fullmatch(nar_hash):
            raise ValueError(f"manifest NAR hash is not sha256 SRI: {nar_hash!r}")
        key = (published_at, tag)
        if tag in seen_tags or (prior_key is not None and key <= prior_key):
            raise ValueError("manifest entries must have unique tags in deterministic order")
        seen_tags.add(tag)
        prior_key = key


def resolve_entries(
    releases: Iterable[GithubRelease], *, as_of: dt.date,
    resolve_revision: Callable[[str], str], prefetch: Callable[[str], str],
) -> list[ManifestEntry]:
    entries: list[ManifestEntry] = []
    for release in select_final_releases(releases, as_of=as_of):
        tag = release.tag_name
        published_at = release.published_at
        assert published_at is not None
        rev = resolve_revision(tag)
        entry = ManifestEntry(
            version=tag.removeprefix("v"), tag=tag, publishedAt=published_at,
            rev=rev, narHash=prefetch(rev), buildBackend=build_backend_for(tag),
        )
        entries.append(entry)
    validate_manifest(entries)
    return entries


def _run_json(*argv: str) -> str:
    result = subprocess.run(argv, check=True, capture_output=True, text=True)
    return result.stdout


def _github_releases() -> list[GithubRelease]:
    raw = _run_json(
        "gh", "api", "--paginate", "--slurp",
        "repos/beetbox/beets/releases?per_page=100",
    )
    try:
        return flatten_release_pages(msgspec.json.decode(raw, type=list[list[GithubRelease]]))
    except msgspec.DecodeError as exc:
        raise TypeError(f"GitHub paginated releases response has an invalid shape: {exc}") from exc


def flatten_release_pages(pages: list[list[GithubRelease]]) -> list[GithubRelease]:
    """Decode the paginated/slurped GitHub release payload without truncation."""
    return [release for page in pages for release in page]


def _resolve_revision(tag: str) -> str:
    try:
        return msgspec.json.decode(
            _run_json("gh", "api", f"repos/beetbox/beets/commits/{tag}"),
            type=GithubCommit,
        ).sha
    except msgspec.DecodeError as exc:
        raise TypeError(f"GitHub did not resolve immutable revision for {tag}: {exc}") from exc


def _prefetch(rev: str) -> str:
    try:
        return msgspec.json.decode(
            _run_json("nix", "flake", "prefetch", "--json", f"github:beetbox/beets/{rev}"),
            type=NixPrefetch,
        ).hash
    except msgspec.DecodeError as exc:
        raise TypeError(f"nix prefetch did not return a hash for {rev}: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", help="UTC date for deterministic refresh (YYYY-MM-DD)")
    parser.add_argument("--check", action="store_true", help="fail unless the manifest is already current")
    parser.add_argument("--dry-run", action="store_true", help="print instead of writing the manifest")
    parser.add_argument("--output", type=Path, default=MANIFEST_PATH)
    args = parser.parse_args()
    as_of = parse_as_of(args.as_of) if args.as_of else dt.datetime.now(dt.UTC).date()
    rendered = render_manifest(resolve_entries(
        _github_releases(), as_of=as_of,
        resolve_revision=_resolve_revision, prefetch=_prefetch,
    ))
    existing = args.output.read_text(encoding="utf-8") if args.output.exists() else ""
    if args.check and existing != rendered:
        raise SystemExit(f"{args.output} is not current for --as-of {as_of}")
    if args.dry_run:
        print(rendered, end="")
    elif existing != rendered:
        args.output.write_text(rendered, encoding="utf-8")


if __name__ == "__main__":
    main()
