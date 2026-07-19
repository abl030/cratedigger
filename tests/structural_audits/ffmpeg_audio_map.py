"""Bounded AST contract for literal production ffmpeg command sequences."""

from __future__ import annotations

import ast
from dataclasses import dataclass


@dataclass(frozen=True)
class FFmpegCommandSite:
    """One canonical literal ffmpeg command construction."""

    filename: str
    lineno: int


def _string_literal(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _is_ffmpeg_token(node: ast.AST) -> bool:
    value = _string_literal(node)
    return value == "ffmpeg" or bool(value and value.endswith("/ffmpeg"))


def _has_literal_audio_map(elements: list[ast.expr]) -> bool:
    return any(
        _string_literal(current) == "-map"
        and _string_literal(following) == "0:a"
        for current, following in zip(elements, elements[1:])
    )


def assert_ffmpeg_audio_mapping(
    source: str,
    *,
    filename: str = "<unknown>",
) -> tuple[FFmpegCommandSite, ...]:
    """Require every literal ffmpeg token to head an audio-mapped sequence.

    The accepted grammar is deliberately small: a list or tuple whose first
    element is a literal ``ffmpeg`` binary token and which contains the
    adjacent literal pair ``"-map", "0:a"``. Incremental construction,
    aliases, and shared argument splats fail closed instead of asking this
    audit to infer runtime values.
    """

    tree = ast.parse(source, filename=filename)
    parents = {
        id(child): parent
        for parent in ast.walk(tree)
        for child in ast.iter_child_nodes(parent)
    }
    sites: list[FFmpegCommandSite] = []
    violations: list[str] = []

    for node in ast.walk(tree):
        if not _is_ffmpeg_token(node):
            continue
        parent = parents.get(id(node))
        if (
            not isinstance(parent, (ast.List, ast.Tuple))
            or not parent.elts
            or parent.elts[0] is not node
        ):
            lineno = getattr(node, "lineno", 0)
            violations.append(
                f"{filename}:{lineno}: non-canonical ffmpeg token; "
                "use one literal command list or tuple"
            )
            continue

        sites.append(FFmpegCommandSite(filename=filename, lineno=parent.lineno))
        if not _has_literal_audio_map(parent.elts):
            violations.append(
                f"{filename}:{parent.lineno}: ffmpeg command is missing "
                "literal -map 0:a"
            )

    if violations:
        raise AssertionError("\n".join(violations))
    return tuple(sorted(sites, key=lambda site: (site.filename, site.lineno)))
