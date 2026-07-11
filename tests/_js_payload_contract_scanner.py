"""Structural scanner for JS fixtures that seed download-history payloads."""

from __future__ import annotations

import glob
import os
import re
from collections.abc import Mapping


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

_SURFACE_CALLS = {
    "pipeline_log": ("renderRecentsItems",),
    "download_history": ("renderDownloadHistoryItem", "renderEvidenceStrip"),
}


def _mask_strings_and_comments(source: str) -> str:
    """Blank non-code text while preserving offsets for call discovery."""
    chars = list(source)
    quote: str | None = None
    escaped = False
    line_comment = False
    block_comment = False
    i = 0
    while i < len(chars):
        ch = chars[i]
        nxt = chars[i + 1] if i + 1 < len(chars) else ""
        if line_comment:
            if ch == "\n":
                line_comment = False
            else:
                chars[i] = " "
            i += 1
            continue
        if block_comment:
            chars[i] = " "
            if ch == "*" and nxt == "/":
                chars[i + 1] = " "
                block_comment = False
                i += 2
            else:
                i += 1
            continue
        if quote is not None:
            chars[i] = " "
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            i += 1
            continue
        if ch == "/" and nxt == "/":
            chars[i] = chars[i + 1] = " "
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            chars[i] = chars[i + 1] = " "
            block_comment = True
            i += 2
            continue
        if ch in "'\"`":
            chars[i] = " "
            quote = ch
        i += 1
    return "".join(chars)


def _matching_delimiter(source: str, start: int, opening: str, closing: str) -> int:
    """Return the matching delimiter while ignoring strings and comments."""
    depth = 0
    quote: str | None = None
    escaped = False
    line_comment = False
    block_comment = False
    i = start
    while i < len(source):
        ch = source[i]
        nxt = source[i + 1] if i + 1 < len(source) else ""
        if line_comment:
            if ch == "\n":
                line_comment = False
            i += 1
            continue
        if block_comment:
            if ch == "*" and nxt == "/":
                block_comment = False
                i += 2
            else:
                i += 1
            continue
        if quote is not None:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            i += 1
            continue
        if ch == "/" and nxt == "/":
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            block_comment = True
            i += 2
            continue
        if ch in "'\"`":
            quote = ch
            i += 1
            continue
        if ch == opening:
            depth += 1
        elif ch == closing:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise ValueError(f"unclosed {opening!r} at offset {start}")


def _direct_object_keys(object_source: str) -> set[str]:
    """Extract direct identifier keys from one JavaScript object literal."""
    if not object_source.startswith("{") or not object_source.endswith("}"):
        raise ValueError("expected a complete object literal")
    chars = list(object_source[1:-1])
    nested = 0
    quote: str | None = None
    escaped = False
    line_comment = False
    block_comment = False
    i = 0
    while i < len(chars):
        ch = chars[i]
        nxt = chars[i + 1] if i + 1 < len(chars) else ""
        if line_comment:
            chars[i] = " "
            if ch == "\n":
                line_comment = False
            i += 1
            continue
        if block_comment:
            chars[i] = " "
            if ch == "*" and nxt == "/":
                chars[i + 1] = " "
                block_comment = False
                i += 2
            else:
                i += 1
            continue
        if quote is not None:
            chars[i] = " "
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            i += 1
            continue
        if ch == "/" and nxt == "/":
            chars[i] = chars[i + 1] = " "
            line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            chars[i] = chars[i + 1] = " "
            block_comment = True
            i += 2
            continue
        if ch in "'\"`":
            chars[i] = " "
            quote = ch
            i += 1
            continue
        if ch in "{[(":
            nested += 1
            chars[i] = " "
        elif ch in "}])":
            chars[i] = " "
            nested -= 1
        elif nested:
            chars[i] = " "
        i += 1
    flattened = "".join(chars)
    if "..." in flattened:
        raise ValueError(
            "spread properties hide seeded fixture fields; use an explicit "
            "literal in audited download-payload fixtures"
        )
    keyed = set(re.findall(
        r"(?:^|,)\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*:", flattened,
    ))
    shorthand = set(re.findall(
        r"(?:^|,)\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*(?=,|$)", flattened,
    ))
    return keyed | shorthand


def fixture_fields_for_call(source: str, call_name: str) -> set[str]:
    """Return direct fields seeded in literal first args to ``call_name``."""
    fields: set[str] = set()
    call_re = re.compile(rf"(?:\b__test__\s*\.\s*)?\b{re.escape(call_name)}\s*\(")
    code = _mask_strings_and_comments(source)
    for match in call_re.finditer(code):
        i = match.end()
        while i < len(source) and source[i].isspace():
            i += 1
        if i >= len(source):
            continue
        if source[i] == "{":
            end = _matching_delimiter(source, i, "{", "}")
            fields.update(_direct_object_keys(source[i:end + 1]))
            continue
        if source[i] != "[":
            raise ValueError(
                f"{call_name} fixture must use a direct object/array literal; "
                "indirection hides seeded payload fields"
            )
        array_end = _matching_delimiter(source, i, "[", "]")
        j = i + 1
        while j < array_end:
            ch = code[j]
            if ch.isspace() or ch == ",":
                j += 1
                continue
            if ch == "{":
                end = _matching_delimiter(source, j, "{", "}")
                fields.update(_direct_object_keys(source[j:end + 1]))
                j = end + 1
                continue
            raise ValueError(
                f"{call_name} array fixtures must contain direct object "
                "literals only; spread/indirect elements hide seeded fields"
            )
    return fields


def scan_js_payload_fixture_fields(
    tests_dir: str = TESTS_DIR,
) -> dict[str, set[str]]:
    """Scan every JS test module for direct download-payload fixtures."""
    result = {surface: set() for surface in _SURFACE_CALLS}
    for path in sorted(glob.glob(os.path.join(tests_dir, "test_js_*.mjs"))):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        for surface, call_names in _SURFACE_CALLS.items():
            for call_name in call_names:
                result[surface].update(fixture_fields_for_call(source, call_name))
    return result


def assert_fixture_fields_have_server_contract(
    fixture_fields: Mapping[str, set[str]],
    allowed_fields: Mapping[str, set[str]],
) -> None:
    """Reject client-only fixture fields absent from their server contract."""
    violations: list[str] = []
    for surface, fields in fixture_fields.items():
        missing = fields - allowed_fields.get(surface, set())
        if missing:
            violations.append(f"{surface}: {', '.join(sorted(missing))}")
    if violations:
        raise AssertionError(
            "JS payload fixtures seed fields absent from the corresponding "
            "server contract:\n" + "\n".join(violations)
        )
