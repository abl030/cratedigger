"""Shared tree-sitter foundation for repository JavaScript audits."""

from __future__ import annotations

import glob
import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

from tree_sitter import Language, Node, Parser, Tree
import tree_sitter_javascript


TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_JAVASCRIPT_LANGUAGE = Language(tree_sitter_javascript.language())

_PAYLOAD_SURFACE_CALLS = {
    "pipeline_log": ("renderRecentsItems",),
    "download_history": ("renderDownloadHistoryItem", "renderEvidenceStrip"),
}

_RESERVED_NATIVE_WINDOW_CALLS = {
    "alert",
    "blur",
    "cancelAnimationFrame",
    "clearInterval",
    "clearTimeout",
    "close",
    "confirm",
    "fetch",
    "focus",
    "open",
    "print",
    "prompt",
    "requestAnimationFrame",
    "scroll",
    "scrollBy",
    "scrollTo",
    "setInterval",
    "setTimeout",
}
_NON_HANDLER_BARE_CALLS = _RESERVED_NATIVE_WINDOW_CALLS | {
    "catch",
    "for",
    "if",
    "parseFloat",
    "parseInt",
    "switch",
    "while",
}

_STATIC_WINDOW_CALL_RE = re.compile(r"\bwindow\.([A-Za-z_$][\w$]*)\s*\(")
_COMPUTED_WINDOW_RE = re.compile(r"\bwindow\s*\[")
_WINDOW_BEFORE_SUBSTITUTION_RE = re.compile(r"\bwindow\s*\.\s*$")
_ONCLICK_RE = re.compile(
    r"\bonclick\s*=\s*([\"'])([\s\S]*?)\1", re.IGNORECASE
)
_BARE_CALL_RE = re.compile(r"(?<![.\w$])([A-Za-z_$][\w$]*)\s*\(")


@dataclass(frozen=True)
class EmittedWindowHandlers:
    """Static handler discovery plus unsupported dynamic surfaces."""

    handlers: set[str]
    dynamic_callees: tuple[str, ...]


@dataclass(frozen=True)
class WindowBindingAudit:
    """Comparison between statically emitted and explicitly exposed handlers."""

    required: set[str]
    exposed: set[str]
    missing: set[str]
    dynamic_callees: tuple[str, ...]
    native_collisions: set[str]


def parse_javascript(source: str, *, origin: str = "<javascript>") -> Tree:
    """Parse a complete JS module, rejecting every ERROR or missing node."""
    source_bytes = source.encode("utf-8")
    tree = Parser(_JAVASCRIPT_LANGUAGE).parse(source_bytes)
    problem = _first_parse_problem(tree.root_node)
    if problem is not None:
        row, column = problem.start_point
        kind = "missing node" if problem.is_missing else "ERROR node"
        raise ValueError(
            f"{origin}: JavaScript parse error ({kind} at "
            f"{row + 1}:{column + 1})"
        )
    return tree


def _first_parse_problem(node: Node) -> Node | None:
    if node.is_error or node.is_missing:
        return node
    if not node.has_error:
        return None
    for child in node.children:
        problem = _first_parse_problem(child)
        if problem is not None:
            return problem
    return node


def _walk(node: Node) -> Iterator[Node]:
    yield node
    for child in node.children:
        yield from _walk(child)


def _semantic_named_children(node: Node) -> list[Node]:
    """Return grammar-bearing children; comments are extras in this grammar."""
    return [child for child in node.named_children if child.type != "comment"]


def _node_text(node: Node, source_bytes: bytes) -> str:
    """Slice by tree-sitter byte offsets before decoding Unicode."""
    return source_bytes[node.start_byte:node.end_byte].decode("utf-8")


def _decode_js_identifier_text(raw: str) -> str:
    """Decode Unicode escapes in an identifier as ECMAScript spells them."""
    result: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i] != "\\":
            result.append(raw[i])
            i += 1
            continue
        if i + 1 >= len(raw) or raw[i + 1] != "u":
            raise ValueError("unsupported JavaScript identifier escape")
        i += 2
        if i < len(raw) and raw[i] == "{":
            end = raw.find("}", i + 1)
            digits = raw[i + 1:end] if end >= 0 else ""
            if (
                end < 0
                or not re.fullmatch(r"[0-9A-Fa-f]{1,6}", digits)
                or int(digits, 16) > 0x10FFFF
                or 0xD800 <= int(digits, 16) <= 0xDFFF
            ):
                raise ValueError("unsupported JavaScript identifier escape")
            result.append(chr(int(digits, 16)))
            i = end + 1
            continue
        digits = raw[i:i + 4]
        if len(digits) != 4 or not re.fullmatch(r"[0-9A-Fa-f]{4}", digits):
            raise ValueError("unsupported JavaScript identifier escape")
        codepoint = int(digits, 16)
        if 0xD800 <= codepoint <= 0xDFFF:
            raise ValueError("unsupported JavaScript identifier escape")
        result.append(chr(codepoint))
        i += 4
    return "".join(result)


def _identifier_value(node: Node | None, source_bytes: bytes) -> str | None:
    if node is None or node.type not in {
        "identifier",
        "property_identifier",
        "shorthand_property_identifier",
    }:
        return None
    return _decode_js_identifier_text(_node_text(node, source_bytes))


def _identifier_is(
    node: Node | None,
    source_bytes: bytes,
    name: str,
    *,
    exact_spelling: bool = False,
) -> bool:
    value = _identifier_value(node, source_bytes)
    return (
        value == name
        and (
            not exact_spelling
            or (node is not None and _node_text(node, source_bytes) == name)
        )
    )


def _direct_member_is(
    node: Node | None,
    source_bytes: bytes,
    object_name: str,
    property_name: str,
    *,
    exact_spelling: bool = False,
) -> bool:
    if node is None or node.type != "member_expression":
        return False
    return _identifier_is(
        node.child_by_field_name("object"),
        source_bytes,
        object_name,
        exact_spelling=exact_spelling,
    ) and _identifier_is(
        node.child_by_field_name("property"),
        source_bytes,
        property_name,
        exact_spelling=exact_spelling,
    )


def _same_node(left: Node | None, right: Node | None) -> bool:
    return (
        left is not None
        and right is not None
        and left.type == right.type
        and left.start_byte == right.start_byte
        and left.end_byte == right.end_byte
    )


def _node_key(node: Node) -> tuple[str, int, int]:
    return (node.type, node.start_byte, node.end_byte)


def _walk_with_parent(
    node: Node, parent: Node | None = None
) -> Iterator[tuple[Node, Node | None]]:
    yield node, parent
    for child in node.children:
        yield from _walk_with_parent(child, node)


def _callee_renderer_references(
    node: Node, source_bytes: bytes, call_name: str
) -> list[Node]:
    references: list[Node] = []
    for child, parent in _walk_with_parent(node):
        if _identifier_is(child, source_bytes, call_name):
            references.append(child)
            continue
        if (
            child.type == "string"
            and parent is not None
            and parent.type == "subscript_expression"
            and _decode_js_string(child, source_bytes) == call_name
        ):
            references.append(child)
    return references


def _payload_call_reference(
    node: Node, source_bytes: bytes, call_name: str
) -> Node | None:
    function = node.child_by_field_name("function")
    references = (
        _callee_renderer_references(function, source_bytes, call_name)
        if function is not None
        else []
    )
    if not references:
        return None
    if any(child.type == "optional_chain" for child in node.children):
        raise ValueError(f"unsupported optional audited renderer call: {call_name}")
    if _identifier_is(function, source_bytes, call_name):
        return function
    if (
        function is not None
        and function.type == "member_expression"
        and _identifier_is(
            function.child_by_field_name("object"), source_bytes, "__test__"
        )
        and _identifier_is(
            function.child_by_field_name("property"), source_bytes, call_name
        )
    ):
        return function.child_by_field_name("property")
    raise ValueError(f"unsupported audited renderer callee form: {call_name}")


def _validate_renderer_references(
    root: Node, source_bytes: bytes, call_name: str
) -> set[tuple[str, int, int]]:
    """Allow only direct imports and direct renderer call references."""
    allowed: set[tuple[str, int, int]] = set()
    for node, parent in _walk_with_parent(root):
        if node.type == "subscript_expression":
            object_node = node.child_by_field_name("object")
            index_node = node.child_by_field_name("index")
            if object_node is None or index_node is None:
                children = _semantic_named_children(node)
                if len(children) == 2:
                    object_node, index_node = children
            if (
                object_node is not None
                and index_node is not None
                and (
                    _identifier_is(object_node, source_bytes, "__test__")
                    or _identifier_is(object_node, source_bytes, "globalThis")
                )
                and index_node.type != "string"
            ):
                raise ValueError(
                    f"unsupported dynamic audited renderer access: {call_name}"
                )
        if (
            node.type == "string"
            and _decode_js_string(node, source_bytes) == call_name
            and parent is not None
            and parent.type == "variable_declarator"
            and _same_node(parent.child_by_field_name("value"), node)
        ):
            raise ValueError(
                f"unsupported string alias for audited renderer: {call_name}"
            )

    for node in _walk(root):
        if node.type == "call_expression":
            reference = _payload_call_reference(node, source_bytes, call_name)
            if reference is not None:
                allowed.add(_node_key(reference))
        elif node.type == "import_specifier":
            identifiers = [
                child
                for child in _semantic_named_children(node)
                if _identifier_value(child, source_bytes) is not None
            ]
            matching = [
                child
                for child in identifiers
                if _identifier_is(child, source_bytes, call_name)
            ]
            if not matching:
                continue
            if len(identifiers) != 1:
                raise ValueError(
                    f"unsupported aliased audited renderer import: {call_name}"
                )
            allowed.add(_node_key(matching[0]))

    for node, parent in _walk_with_parent(root):
        if _identifier_is(node, source_bytes, call_name):
            if _node_key(node) in allowed:
                continue
            if (
                parent is not None
                and parent.type == "pair"
                and _same_node(parent.child_by_field_name("key"), node)
            ):
                continue
            raise ValueError(
                f"unsupported indirect/aliased audited renderer reference: {call_name}"
            )
        if (
            node.type == "string"
            and parent is not None
            and parent.type == "subscript_expression"
            and _decode_js_string(node, source_bytes) == call_name
        ):
            raise ValueError(
                f"unsupported computed audited renderer reference: {call_name}"
            )
    return allowed


def _decode_js_escapes(content: str) -> str:
    """Decode the escape forms shared by JS strings and template chunks."""
    result: list[str] = []
    i = 0
    simple = {
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "v": "\v",
        "'": "'",
        '"': '"',
        "\\": "\\",
    }
    while i < len(content):
        if content[i] != "\\":
            result.append(content[i])
            i += 1
            continue
        i += 1
        if i >= len(content):
            raise ValueError("unterminated JavaScript string escape")
        escaped = content[i]
        i += 1
        if escaped in {"\n", "\r", "\u2028", "\u2029"}:
            if escaped == "\r" and i < len(content) and content[i] == "\n":
                i += 1
            continue
        if escaped in simple:
            result.append(simple[escaped])
            continue
        if escaped == "0":
            if i < len(content) and content[i].isdigit():
                raise ValueError(
                    "legacy octal JavaScript string escapes are unsupported"
                )
            result.append("\0")
            continue
        if escaped in "1234567":
            raise ValueError(
                "legacy octal JavaScript string escapes are unsupported"
            )
        if escaped == "x":
            digits = content[i:i + 2]
            if len(digits) != 2 or not re.fullmatch(r"[0-9A-Fa-f]{2}", digits):
                raise ValueError("unsupported hexadecimal JavaScript string escape")
            result.append(chr(int(digits, 16)))
            i += 2
            continue
        if escaped == "u":
            if i < len(content) and content[i] == "{":
                end = content.find("}", i + 1)
                digits = content[i + 1:end] if end >= 0 else ""
                if (
                    end < 0
                    or not re.fullmatch(r"[0-9A-Fa-f]{1,6}", digits)
                    or int(digits, 16) > 0x10FFFF
                ):
                    raise ValueError("unsupported Unicode JavaScript string escape")
                result.append(chr(int(digits, 16)))
                i = end + 1
                continue
            digits = content[i:i + 4]
            if len(digits) != 4 or not re.fullmatch(r"[0-9A-Fa-f]{4}", digits):
                raise ValueError("unsupported Unicode JavaScript string escape")
            codepoint = int(digits, 16)
            i += 4
            if 0xD800 <= codepoint <= 0xDBFF:
                low_match = re.match(r"\\u([0-9A-Fa-f]{4})", content[i:])
                if low_match is not None:
                    low = int(low_match.group(1), 16)
                    if 0xDC00 <= low <= 0xDFFF:
                        codepoint = (
                            0x10000
                            + ((codepoint - 0xD800) << 10)
                            + (low - 0xDC00)
                        )
                        i += 6
            result.append(chr(codepoint))
            continue
        # JavaScript identity escapes resolve to the escaped character.
        result.append(escaped)
    return "".join(result)


def _decode_js_string(node: Node, source_bytes: bytes) -> str:
    raw = _node_text(node, source_bytes)
    if len(raw) < 2 or raw[0] not in {"'", '"'} or raw[-1] != raw[0]:
        raise ValueError("expected a static quoted JavaScript string")
    return _decode_js_escapes(raw[1:-1])


def _static_object_key(node: Node, source_bytes: bytes) -> str:
    if node.type in {"property_identifier", "identifier"}:
        value = _identifier_value(node, source_bytes)
        if value is None:
            raise ValueError("fixture object key is not an identifier")
        return value
    if node.type == "string":
        return _decode_js_string(node, source_bytes)
    if node.type == "computed_property_name":
        children = _semantic_named_children(node)
        if len(children) != 1 or children[0].type != "string":
            raise ValueError(
                "computed fixture keys must be statically quoted strings"
            )
        return _decode_js_string(children[0], source_bytes)
    raise ValueError("unsupported fixture object key syntax")


def _direct_object_keys(node: Node, source_bytes: bytes) -> set[str]:
    if node.type != "object":
        raise ValueError("expected a direct object literal")
    keys: set[str] = set()
    for child in _semantic_named_children(node):
        if child.type == "shorthand_property_identifier":
            value = _identifier_value(child, source_bytes)
            if value is None:
                raise ValueError("fixture shorthand key is not an identifier")
            keys.add(value)
            continue
        if child.type == "pair":
            key = child.child_by_field_name("key")
            if key is None:
                raise ValueError("fixture object property has no key")
            keys.add(_static_object_key(key, source_bytes))
            continue
        if child.type == "spread_element":
            raise ValueError(
                "spread properties hide seeded fixture fields; use an explicit "
                "literal in audited download-payload fixtures"
            )
        raise ValueError(
            f"unsupported fixture object property form: {child.type}"
        )
    return keys


def _direct_array_elements(node: Node) -> list[Node]:
    """Return direct elements, rejecting elisions such as ``[, value]``."""
    elements: list[Node] = []
    expect_element = True
    saw_element = False
    for child in node.children:
        if child.type in {"[", "]", "comment"}:
            continue
        if child.type == ",":
            if expect_element:
                raise ValueError(
                    "array fixture elisions hide the direct element shape"
                )
            expect_element = True
            continue
        if not expect_element:
            raise ValueError("array fixture elements must be comma-separated")
        elements.append(child)
        saw_element = True
        expect_element = False
    if not saw_element:
        return []
    return elements


def fixture_fields_for_call(
    source: str, call_name: str, *, origin: str = "<javascript>"
) -> set[str]:
    """Return direct fields seeded in literal first args to ``call_name``."""
    source_bytes = source.encode("utf-8")
    tree = parse_javascript(source, origin=origin)
    _validate_renderer_references(tree.root_node, source_bytes, call_name)
    fields: set[str] = set()
    for node in _walk(tree.root_node):
        if node.type != "call_expression":
            continue
        if _payload_call_reference(node, source_bytes, call_name) is None:
            continue
        arguments = node.child_by_field_name("arguments")
        args = _semantic_named_children(arguments) if arguments is not None else []
        first = args[0] if args else None
        if first is None:
            raise ValueError(f"{call_name} fixture has no first argument")
        if first.type == "object":
            fields.update(_direct_object_keys(first, source_bytes))
            continue
        if first.type != "array":
            raise ValueError(
                f"{call_name} fixture must use a direct object/array literal; "
                "indirection hides seeded payload fields"
            )
        for element in _direct_array_elements(first):
            if element.type != "object":
                raise ValueError(
                    f"{call_name} array fixtures must contain direct object "
                    "literals only; spread/indirect elements hide seeded fields"
                )
            fields.update(_direct_object_keys(element, source_bytes))
    return fields


def scan_js_payload_fixture_fields(
    tests_dir: str = TESTS_DIR,
) -> dict[str, set[str]]:
    """Scan every JS test module for direct download-payload fixtures."""
    result = {surface: set() for surface in _PAYLOAD_SURFACE_CALLS}
    for path in sorted(glob.glob(os.path.join(tests_dir, "test_js_*.mjs"))):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        for surface, call_names in _PAYLOAD_SURFACE_CALLS.items():
            for call_name in call_names:
                result[surface].update(
                    fixture_fields_for_call(source, call_name, origin=path)
                )
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


def _literal_surfaces(
    source: str, *, origin: str
) -> tuple[list[str], list[str]]:
    source_bytes = source.encode("utf-8")
    tree = parse_javascript(source, origin=origin)
    surfaces: list[str] = []
    dynamic: list[str] = []

    def append_raw_and_cooked(raw: str, *, allow_invalid: bool = False) -> None:
        if not raw:
            return
        surfaces.append(raw)
        try:
            cooked = _decode_js_escapes(raw)
        except ValueError:
            if not allow_invalid:
                raise
            # Invalid escapes are legal in tagged templates; raw scanning
            # still preserves the conservative audit for those surfaces.
            return
        if cooked != raw:
            surfaces.append(cooked)

    for node in _walk(tree.root_node):
        if node.type == "string":
            raw = _node_text(node, source_bytes)
            append_raw_and_cooked(raw[1:-1])
            continue
        if node.type != "template_string":
            continue
        fragment = ""
        for child in _semantic_named_children(node):
            if child.type == "template_substitution":
                dynamic_prefixes = [fragment]
                try:
                    cooked_prefix = _decode_js_escapes(fragment)
                except ValueError:
                    pass
                else:
                    dynamic_prefixes.append(cooked_prefix)
                if any(
                    _WINDOW_BEFORE_SUBSTITUTION_RE.search(prefix)
                    for prefix in dynamic_prefixes
                ):
                    dynamic.append(f"{origin}: {fragment.strip()}${{...}}")
                append_raw_and_cooked(fragment, allow_invalid=True)
                fragment = ""
            elif child.type in {"string_fragment", "escape_sequence"}:
                fragment += _node_text(child, source_bytes)
        append_raw_and_cooked(fragment, allow_invalid=True)
    return surfaces, dynamic


def _collect_window_surface(
    surface: str,
    *,
    origin: str,
    handlers: set[str],
    dynamic_callees: list[str],
    include_bare: bool,
) -> None:
    if _COMPUTED_WINDOW_RE.search(surface):
        dynamic_callees.append(f"{origin}: {surface.strip()}")
    for match in _STATIC_WINDOW_CALL_RE.finditer(surface):
        if match.group(1) not in _RESERVED_NATIVE_WINDOW_CALLS:
            handlers.add(match.group(1))
    if include_bare:
        for match in _BARE_CALL_RE.finditer(surface):
            if match.group(1) not in _NON_HANDLER_BARE_CALLS:
                handlers.add(match.group(1))


def emitted_window_handlers(
    js_sources: Mapping[str, str], index_html: str
) -> EmittedWindowHandlers:
    """Find conservative handler names in JS literals and HTML onclick bodies."""
    handlers: set[str] = set()
    dynamic_callees: list[str] = []
    for name, source in js_sources.items():
        surfaces, template_dynamic = _literal_surfaces(source, origin=name)
        dynamic_callees.extend(template_dynamic)
        for surface in surfaces:
            _collect_window_surface(
                surface,
                origin=name,
                handlers=handlers,
                dynamic_callees=dynamic_callees,
                include_bare=False,
            )
    for match in _ONCLICK_RE.finditer(index_html):
        _collect_window_surface(
            match.group(2),
            origin="web/index.html",
            handlers=handlers,
            dynamic_callees=dynamic_callees,
            include_bare=True,
        )
    return EmittedWindowHandlers(
        handlers=handlers,
        dynamic_callees=tuple(sorted(set(dynamic_callees))),
    )


def _subtree_has_identifier(node: Node, source_bytes: bytes, name: str) -> bool:
    return any(_identifier_is(child, source_bytes, name) for child in _walk(node))


def _semantic_subscript_property_is(
    node: Node | None, source_bytes: bytes, object_name: str, property_name: str
) -> bool:
    if node is None or node.type != "subscript_expression":
        return False
    object_node = node.child_by_field_name("object")
    index_node = node.child_by_field_name("index")
    if object_node is None or index_node is None:
        children = _semantic_named_children(node)
        if len(children) != 2:
            return False
        object_node, index_node = children
    return (
        _identifier_is(object_node, source_bytes, object_name)
        and index_node.type == "string"
        and _decode_js_string(index_node, source_bytes) == property_name
    )


def _semantic_object_assign_reference(node: Node, source_bytes: bytes) -> bool:
    return _direct_member_is(
        node, source_bytes, "Object", "assign"
    ) or _semantic_subscript_property_is(
        node, source_bytes, "Object", "assign"
    )


def _window_member_target(node: Node | None, source_bytes: bytes) -> bool:
    if node is None or node.type not in {"member_expression", "subscript_expression"}:
        return False
    object_node = node.child_by_field_name("object")
    return _identifier_is(object_node, source_bytes, "window") or (
        object_node is not None
        and object_node.type == "parenthesized_expression"
        and _subtree_has_identifier(object_node, source_bytes, "window")
    )


def _validated_window_assign_calls(
    root: Node, source_bytes: bytes
) -> list[Node]:
    """Return exact binding calls after rejecting every bypassing reference."""
    calls: list[Node] = []
    allowed_assign_refs: set[tuple[str, int, int]] = set()

    for node in _walk(root):
        if node.type in {"assignment_expression", "augmented_assignment_expression"}:
            left = node.child_by_field_name("left")
            right = node.child_by_field_name("right")
            if _window_member_target(left, source_bytes):
                raise ValueError("direct window mutations bypass Object.assign audit")
            if right is not None and (
                any(
                    _semantic_object_assign_reference(child, source_bytes)
                    for child in _walk(right)
                )
                or _identifier_is(right, source_bytes, "window")
                or (
                    _identifier_is(right, source_bytes, "Object")
                    and left is not None
                    and _subtree_has_identifier(left, source_bytes, "assign")
                )
            ):
                raise ValueError("aliases for Object.assign/window are unsupported")
        if node.type == "update_expression":
            argument = node.child_by_field_name("argument")
            if argument is None:
                children = _semantic_named_children(node)
                argument = children[0] if children else None
            if _window_member_target(argument, source_bytes):
                raise ValueError("direct window mutations bypass Object.assign audit")
        if node.type == "variable_declarator":
            name = node.child_by_field_name("name")
            value = node.child_by_field_name("value")
            if value is not None and (
                any(
                    _semantic_object_assign_reference(child, source_bytes)
                    for child in _walk(value)
                )
                or _identifier_is(value, source_bytes, "window")
                or (
                    _identifier_is(value, source_bytes, "Object")
                    and name is not None
                    and _subtree_has_identifier(name, source_bytes, "assign")
                )
            ):
                raise ValueError("aliases for Object.assign/window are unsupported")

        if node.type != "call_expression":
            continue
        function = node.child_by_field_name("function")
        if function is None:
            continue
        assign_refs = [
            child
            for child in _walk(function)
            if _semantic_object_assign_reference(child, source_bytes)
        ]
        if not assign_refs:
            continue
        if any(child.type == "optional_chain" for child in node.children):
            raise ValueError("optional Object.assign calls are unsupported")
        arguments = node.child_by_field_name("arguments")
        args = _semantic_named_children(arguments) if arguments is not None else []
        targets_window = bool(args) and _subtree_has_identifier(
            args[0], source_bytes, "window"
        )
        if not _semantic_object_assign_reference(function, source_bytes):
            raise ValueError("unsupported indirect Object.assign call")
        if not _direct_member_is(
            function,
            source_bytes,
            "Object",
            "assign",
            exact_spelling=True,
        ):
            raise ValueError("computed/escaped Object.assign calls are unsupported")
        allowed_assign_refs.add(_node_key(function))
        if not targets_window:
            continue
        if not args or not _identifier_is(
            args[0], source_bytes, "window", exact_spelling=True
        ):
            raise ValueError("Object.assign window target must be the direct identifier")
        calls.append(node)

    for node in _walk(root):
        if (
            _semantic_object_assign_reference(node, source_bytes)
            and _node_key(node) not in allowed_assign_refs
        ):
            raise ValueError("unsupported aliased Object.assign reference")
    return calls


def _window_binding_keys(object_node: Node, source_bytes: bytes) -> set[str]:
    bindings: set[str] = set()
    for child in _semantic_named_children(object_node):
        if child.type == "shorthand_property_identifier":
            value = _identifier_value(child, source_bytes)
            if value is None:
                raise ValueError("unsupported window shorthand binding")
            bindings.add(value)
            continue
        if child.type == "pair":
            key = child.child_by_field_name("key")
            if key is None or key.type != "property_identifier":
                raise ValueError("unsupported Object.assign window binding key")
            value = _identifier_value(key, source_bytes)
            if value is None:
                raise ValueError("unsupported Object.assign window binding key")
            bindings.add(value)
            continue
        raise ValueError(
            f"unsupported Object.assign window binding entry: {child.type}"
        )
    return bindings


def exposed_window_bindings(main_source: str) -> set[str]:
    """Return direct public keys from every Object.assign(window, {...})."""
    source_bytes = main_source.encode("utf-8")
    tree = parse_javascript(main_source, origin="main.js")
    bindings: set[str] = set()
    calls = _validated_window_assign_calls(tree.root_node, source_bytes)
    for node in calls:
        arguments = node.child_by_field_name("arguments")
        args = _semantic_named_children(arguments) if arguments is not None else []
        if len(args) != 2 or args[1].type != "object":
            raise ValueError(
                "Object.assign(window, ...) bindings require exactly one direct "
                "object literal source"
            )
        bindings.update(_window_binding_keys(args[1], source_bytes))
    if not calls:
        raise ValueError("main.js has no Object.assign(window, {...}) binding block")
    return bindings


def audit_window_bindings(
    js_sources: Mapping[str, str], index_html: str, main_source: str
) -> WindowBindingAudit:
    """Compare emitted handler names with direct main.js window bindings."""
    emitted = emitted_window_handlers(js_sources, index_html)
    exposed = exposed_window_bindings(main_source)
    return WindowBindingAudit(
        required=emitted.handlers,
        exposed=exposed,
        missing=emitted.handlers - exposed,
        dynamic_callees=emitted.dynamic_callees,
        native_collisions=exposed & _RESERVED_NATIVE_WINDOW_CALLS,
    )


def assert_window_bindings(
    js_sources: Mapping[str, str], index_html: str, main_source: str
) -> WindowBindingAudit:
    """Fail when a conservative handler is dynamic, missing, or native-shadowing."""
    audit = audit_window_bindings(js_sources, index_html, main_source)
    if audit.dynamic_callees:
        raise ValueError(
            "dynamic window callee forms are unsupported:\n"
            + "\n".join(audit.dynamic_callees)
        )
    if audit.native_collisions:
        raise ValueError(
            "app bindings collide with reserved native window names: "
            + ", ".join(sorted(audit.native_collisions))
        )
    if audit.missing:
        raise ValueError(
            "static window handlers missing from bindings: "
            + ", ".join(sorted(audit.missing))
        )
    return audit
