"""Structural scanner for JS fixtures that seed download-history payloads."""

from __future__ import annotations

import ast
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
        if ch == "`":
            chars[i] = " "
            i += 1
            while i < len(chars):
                ch = chars[i]
                nxt = chars[i + 1] if i + 1 < len(chars) else ""
                if ch == "\\":
                    chars[i] = " "
                    if i + 1 < len(chars):
                        chars[i + 1] = " "
                    i += 2
                    continue
                if ch == "`":
                    chars[i] = " "
                    i += 1
                    break
                if ch == "$" and nxt == "{":
                    end = _matching_delimiter(source, i + 1, "{", "}")
                    chars[i] = chars[i + 1] = " "
                    chars[i + 2:end] = _mask_strings_and_comments(
                        source[i + 2:end]
                    )
                    chars[end] = " "
                    i = end + 1
                    continue
                chars[i] = " "
                i += 1
            else:
                raise ValueError("unclosed template literal")
            continue
        if ch in "'\"":
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


def _direct_segments(delimited_source: str) -> list[str]:
    """Split one delimited literal at direct commas, blanking comments."""
    if len(delimited_source) < 2:
        raise ValueError("expected a complete delimited literal")
    chars = list(delimited_source[1:-1])
    depths = {"{": 0, "[": 0, "(": 0}
    closing_to_opening = {"}": "{", "]": "[", ")": "("}
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
            quote = ch
            i += 1
            continue
        if ch in "{[(":
            depths[ch] += 1
        elif ch in "}])":
            opening = closing_to_opening[ch]
            depths[opening] -= 1
            if depths[opening] < 0:
                raise ValueError(f"unexpected {ch!r} in literal fixture")
        i += 1
    if quote is not None or block_comment or any(depths.values()):
        raise ValueError("unclosed syntax in literal fixture")

    cleaned = "".join(chars)
    segments: list[str] = []
    start = 0
    depths = {"{": 0, "[": 0, "(": 0}
    quote = None
    escaped = False
    for i, ch in enumerate(cleaned):
        if quote is not None:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = None
            continue
        if ch in "'\"`":
            quote = ch
            continue
        if ch in "{[(":
            depths[ch] += 1
            continue
        if ch in "}])":
            depths[closing_to_opening[ch]] -= 1
            continue
        if ch == "," and not any(depths.values()):
            segments.append(cleaned[start:i].strip())
            start = i + 1
    tail = cleaned[start:].strip()
    if tail:
        segments.append(tail)
    return segments


def _static_quoted_key(source: str) -> tuple[str, int]:
    """Resolve a single/double-quoted key at the start of ``source``."""
    if not source or source[0] not in "'\"":
        raise ValueError("expected a static quoted fixture key")
    quote = source[0]
    escaped = False
    for i in range(1, len(source)):
        ch = source[i]
        if escaped:
            escaped = False
        elif ch == "\\":
            escaped = True
        elif ch == quote:
            try:
                value = ast.literal_eval(source[:i + 1])
            except (SyntaxError, ValueError) as exc:
                raise ValueError("unsupported quoted fixture key") from exc
            if not isinstance(value, str):
                raise ValueError("fixture key did not resolve to a string")
            return value, i + 1
    raise ValueError("unclosed quoted fixture key")


def _require_property_colon(remainder: str) -> None:
    if not remainder.lstrip().startswith(":"):
        raise ValueError("fixture object key must be followed by a colon")


def _direct_object_keys(object_source: str) -> set[str]:
    """Extract statically-known direct keys from one JS object literal."""
    if not object_source.startswith("{") or not object_source.endswith("}"):
        raise ValueError("expected a complete object literal")
    keys: set[str] = set()
    for prop in _direct_segments(object_source):
        if not prop:
            raise ValueError("empty property in literal fixture")
        if prop.startswith("..."):
            raise ValueError(
                "spread properties hide seeded fixture fields; use an explicit "
                "literal in audited download-payload fixtures"
            )
        if prop[0] in "'\"":
            key, end = _static_quoted_key(prop)
            _require_property_colon(prop[end:])
            keys.add(key)
            continue
        if prop[0] == "[":
            end = _matching_delimiter(prop, 0, "[", "]")
            expression = prop[1:end].strip()
            if not expression or expression[0] not in "'\"":
                raise ValueError(
                    "computed fixture keys must be statically quoted strings"
                )
            key, consumed = _static_quoted_key(expression)
            if expression[consumed:].strip():
                raise ValueError(
                    "computed fixture keys must be statically quoted strings"
                )
            _require_property_colon(prop[end + 1:])
            keys.add(key)
            continue
        match = re.match(r"[A-Za-z_$][A-Za-z0-9_$]*", prop)
        if match is None:
            raise ValueError("unsupported fixture object key syntax")
        key = match.group(0)
        remainder = prop[match.end():]
        if remainder.strip():
            _require_property_colon(remainder)
        keys.add(key)
    return keys


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
        for element in _direct_segments(source[i:array_end + 1]):
            if not element or element[0] != "{":
                raise ValueError(
                    f"{call_name} array fixtures must contain direct object "
                    "literals only; spread/indirect elements hide seeded fields"
                )
            end = _matching_delimiter(element, 0, "{", "}")
            if element[end + 1:].strip():
                raise ValueError(
                    f"{call_name} array fixtures must contain direct object "
                    "literals only; spread/indirect elements hide seeded fields"
                )
            fields.update(_direct_object_keys(element))
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
