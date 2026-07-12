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
    "pipeline_log": ("renderRecentsFixture",),
    "download_history": (
        "renderDownloadHistoryFixture",
        "renderEvidenceFixture",
    ),
}
_PAYLOAD_FIXTURE_RENDERERS = {
    "renderRecentsFixture": "renderRecentsItems",
    "renderDownloadHistoryFixture": "renderDownloadHistoryItem",
    "renderEvidenceFixture": "renderEvidenceStrip",
}
_PAYLOAD_FIXTURE_MODULES = {
    "renderRecentsFixture": "../web/js/recents.js",
    "renderDownloadHistoryFixture": "../web/js/history.js",
    "renderEvidenceFixture": "../web/js/history.js",
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
_BROWSER_GLOBALS = frozenset({"window", "globalThis", "self"})

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


def _subscript_parts(node: Node) -> tuple[Node | None, Node | None]:
    object_node = node.child_by_field_name("object")
    index_node = node.child_by_field_name("index")
    if object_node is None or index_node is None:
        children = _semantic_named_children(node)
        if len(children) == 2:
            return children[0], children[1]
    return object_node, index_node


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
    if any(child.type == "optional_chain" for child in node.children) or (
        function is not None
        and any(child.type == "optional_chain" for child in _walk(function))
    ):
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


def _fixture_registration_nodes(
    root: Node,
    source_bytes: bytes,
    fixture_name: str,
    renderer_name: str,
    renderer_module: str,
) -> set[tuple[str, int, int]] | None:
    """Validate one exact local alias from a production renderer to a fixture."""
    allowed: set[tuple[str, int, int]] = set()
    renderer_nodes: set[tuple[str, int, int]] = set()
    parents = {
        _node_key(node): parent
        for node, parent in _walk_with_parent(root)
        if parent is not None
    }
    module_imports: list[Node] = []
    for node in _walk(root):
        if node.type != "import_statement":
            continue
        module_strings = [
            child for child in _semantic_named_children(node) if child.type == "string"
        ]
        if len(module_strings) == 1 and (
            _decode_js_string(module_strings[0], source_bytes) == renderer_module
        ):
            module_imports.append(node)

    has_opaque_module_import = any(
        any(child.type == "namespace_import" for child in _walk(module_import))
        or any(
            child.type == "import_clause"
            and any(
                direct.type == "identifier"
                for direct in _semantic_named_children(child)
            )
            for child in _walk(module_import)
        )
        for module_import in module_imports
    )
    has_recents_test_import = renderer_name == "renderRecentsItems" and any(
        _identifier_is(child, source_bytes, "__test__")
        for module_import in module_imports
        for child in _walk(module_import)
    )
    mentions_boundary = has_opaque_module_import or has_recents_test_import or any(
        _identifier_is(node, source_bytes, fixture_name)
        or _identifier_is(node, source_bytes, renderer_name)
        for node in _walk(root)
    )
    if not mentions_boundary:
        return None

    target_import_keys = {_node_key(node) for node in module_imports}

    def inside_target_import(node: Node) -> bool:
        parent = parents.get(_node_key(node))
        while parent is not None:
            if _node_key(parent) in target_import_keys:
                return True
            parent = parents.get(_node_key(parent))
        return False

    imported_test_namespace = False

    def top_level_declarator(node: Node) -> bool:
        declaration = parents.get(_node_key(node))
        return (
            declaration is not None
            and declaration.type == "lexical_declaration"
            and (parent := parents.get(_node_key(declaration))) is not None
            and parent.type == "program"
        )

    for node in _walk(root):
        if node.type == "import_specifier":
            identifiers = [
                child
                for child in _semantic_named_children(node)
                if _identifier_value(child, source_bytes) is not None
            ]
            if (
                len(identifiers) == 2
                and inside_target_import(node)
                and _identifier_is(
                    identifiers[0], source_bytes, renderer_name, exact_spelling=True
                )
                and _identifier_is(
                    identifiers[1], source_bytes, fixture_name, exact_spelling=True
                )
            ):
                renderer_nodes.add(_node_key(identifiers[0]))
                allowed.add(_node_key(identifiers[1]))
            if (
                len(identifiers) == 1
                and inside_target_import(node)
                and _identifier_is(
                    identifiers[0], source_bytes, "__test__", exact_spelling=True
                )
            ):
                imported_test_namespace = True
        if (
            node.type != "variable_declarator"
            or not top_level_declarator(node)
            or not _identifier_is(
                node.child_by_field_name("value"), source_bytes, "__test__"
            )
        ):
            continue
        pattern = node.child_by_field_name("name")
        if pattern is None or pattern.type != "object_pattern":
            continue
        for entry in _semantic_named_children(pattern):
            if entry.type != "pair_pattern":
                continue
            key = entry.child_by_field_name("key")
            value = entry.child_by_field_name("value")
            if _identifier_is(
                key, source_bytes, renderer_name, exact_spelling=True
            ) and _identifier_is(
                value, source_bytes, fixture_name, exact_spelling=True
            ):
                if key is not None:
                    renderer_nodes.add(_node_key(key))
                if value is not None:
                    allowed.add(_node_key(value))

    registration_is_import = any(
        parents.get(key) is not None
        and parents[key].type == "import_specifier"
        for key in allowed
    )
    module_registration_valid = (
        len(module_imports) == 1
        and not has_opaque_module_import
        and (
            registration_is_import
            or (imported_test_namespace and renderer_name == "renderRecentsItems")
        )
    )
    if (
        len(allowed) != 1
        or len(renderer_nodes) != 1
        or not module_registration_valid
    ):
        raise ValueError(
            f"{fixture_name} requires exactly one explicit registration from "
            f"{renderer_name}"
        )

    for node in _walk(root):
        if _identifier_is(node, source_bytes, renderer_name):
            if _node_key(node) not in renderer_nodes:
                raise ValueError(
                    f"raw renderer reference outside {fixture_name} registration: "
                    f"{renderer_name}"
                )
    return allowed


def _validate_renderer_references(
    root: Node,
    source_bytes: bytes,
    call_name: str,
    *,
    declaration_nodes: set[tuple[str, int, int]] | None = None,
) -> set[tuple[str, int, int]]:
    """Enforce the explicit direct-callee fixture-registration boundary."""
    allowed = set(declaration_nodes or ())
    for node in _walk(root):
        if node.type != "call_expression":
            continue
        function = node.child_by_field_name("function")
        if function is None or function.type != "subscript_expression":
            continue
        namespace, _ = _subscript_parts(function)
        if _identifier_is(namespace, source_bytes, "__test__"):
            raise ValueError(
                "computed audited renderer fixture namespace calls are unsupported"
            )

    for node in _walk(root):
        if node.type == "call_expression":
            reference = _payload_call_reference(node, source_bytes, call_name)
            if reference is not None:
                if declaration_nodes and not _identifier_is(
                    node.child_by_field_name("function"), source_bytes, call_name
                ):
                    raise ValueError(
                        f"registered fixture {call_name} must be called through "
                        "its direct local alias"
                    )
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
            if all(_node_key(child) in allowed for child in matching):
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
    source: str,
    call_name: str,
    *,
    origin: str = "<javascript>",
    registered_renderer: str | None = None,
    registered_module: str | None = None,
) -> set[str]:
    """Return direct fields seeded in literal first args to ``call_name``."""
    source_bytes = source.encode("utf-8")
    tree = parse_javascript(source, origin=origin)
    try:
        declaration_nodes = (
            _fixture_registration_nodes(
                tree.root_node,
                source_bytes,
                call_name,
                registered_renderer,
                registered_module or "",
            )
            if registered_renderer is not None and registered_module is not None
            else set()
        )
        if declaration_nodes is None:
            return set()
        _validate_renderer_references(
            tree.root_node,
            source_bytes,
            call_name,
            declaration_nodes=declaration_nodes,
        )
    except ValueError as exc:
        raise ValueError(f"{origin}: {exc}") from None
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
                    fixture_fields_for_call(
                        source,
                        call_name,
                        origin=path,
                        registered_renderer=_PAYLOAD_FIXTURE_RENDERERS[call_name],
                        registered_module=_PAYLOAD_FIXTURE_MODULES[call_name],
                    )
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


def _browser_global_identifier(node: Node | None, source_bytes: bytes) -> bool:
    return _identifier_value(node, source_bytes) in _BROWSER_GLOBALS


def _direct_browser_global_reference(
    node: Node | None, source_bytes: bytes
) -> bool:
    if node is None:
        return False
    if _browser_global_identifier(node, source_bytes):
        return True
    if node.type == "parenthesized_expression":
        children = _semantic_named_children(node)
        return len(children) == 1 and _direct_browser_global_reference(
            children[0], source_bytes
        )
    return False


def _browser_global_rooted(node: Node | None, source_bytes: bytes) -> bool:
    if node is None:
        return False
    if _browser_global_identifier(node, source_bytes):
        return True
    if node.type == "parenthesized_expression":
        children = _semantic_named_children(node)
        return len(children) == 1 and _browser_global_rooted(
            children[0], source_bytes
        )
    if node.type in {"member_expression", "subscript_expression"}:
        return _browser_global_rooted(
            node.child_by_field_name("object"), source_bytes
        )
    return False


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


def _browser_global_member_target(node: Node | None, source_bytes: bytes) -> bool:
    if node is None or node.type not in {"member_expression", "subscript_expression"}:
        return False
    return _browser_global_rooted(
        node.child_by_field_name("object"), source_bytes
    )


def _computed_object_callee(
    node: Node | None, source_bytes: bytes
) -> bool:
    if node is None or node.type != "subscript_expression":
        return False
    object_node, _ = _subscript_parts(node)
    return _semantic_object_reference(object_node, source_bytes)


def _semantic_object_reference(node: Node | None, source_bytes: bytes) -> bool:
    if node is None:
        return False
    if node.type == "parenthesized_expression":
        children = _semantic_named_children(node)
        return len(children) == 1 and _semantic_object_reference(
            children[0], source_bytes
        )
    if _identifier_is(node, source_bytes, "Object"):
        return True
    if node.type == "subscript_expression":
        object_node, property_node = _subscript_parts(node)
        return (
            _direct_browser_global_reference(object_node, source_bytes)
            and property_node is not None
            and property_node.type == "string"
            and _decode_js_string(property_node, source_bytes) == "Object"
        )
    return (
        node.type == "member_expression"
        and _direct_browser_global_reference(
            node.child_by_field_name("object"), source_bytes
        )
        and _identifier_is(
            node.child_by_field_name("property"), source_bytes, "Object"
        )
    )


def _exact_window_binding_call(node: Node, source_bytes: bytes) -> bool:
    if node.type != "call_expression" or not _direct_member_is(
        node.child_by_field_name("function"),
        source_bytes,
        "Object",
        "assign",
        exact_spelling=True,
    ):
        return False
    arguments = node.child_by_field_name("arguments")
    args = _semantic_named_children(arguments) if arguments is not None else []
    return bool(args) and _identifier_is(
        args[0], source_bytes, "window", exact_spelling=True
    )


def _validated_window_assign_calls(
    root: Node, source_bytes: bytes
) -> list[Node]:
    """Return exact binding calls after rejecting every bypassing reference."""
    calls: list[Node] = []
    allowed_assign_refs: set[tuple[str, int, int]] = set()
    owns_window_binding = any(
        _exact_window_binding_call(node, source_bytes) for node in _walk(root)
    )

    for node in _walk(root):
        if node.type in {"assignment_expression", "augmented_assignment_expression"}:
            left = node.child_by_field_name("left")
            right = node.child_by_field_name("right")
            if _browser_global_member_target(left, source_bytes):
                raise ValueError(
                    "direct browser-global mutations bypass Object.assign audit"
                )
            if right is not None and (
                any(
                    _semantic_object_assign_reference(child, source_bytes)
                    for child in _walk(right)
                )
                or _browser_global_rooted(right, source_bytes)
                or (
                    _identifier_is(right, source_bytes, "Object")
                    and left is not None
                    and _subtree_has_identifier(left, source_bytes, "assign")
                )
            ):
                raise ValueError(
                    "aliases for Object.assign/browser globals are unsupported"
                )
        if node.type == "update_expression":
            argument = node.child_by_field_name("argument")
            if argument is None:
                children = _semantic_named_children(node)
                argument = children[0] if children else None
            if _browser_global_member_target(argument, source_bytes):
                raise ValueError(
                    "direct browser-global mutations bypass Object.assign audit"
                )
        if node.type == "variable_declarator":
            name = node.child_by_field_name("name")
            value = node.child_by_field_name("value")
            if value is not None and (
                any(
                    _semantic_object_assign_reference(child, source_bytes)
                    for child in _walk(value)
                )
                or _browser_global_rooted(value, source_bytes)
                or (
                    _identifier_is(value, source_bytes, "Object")
                    and name is not None
                    and _subtree_has_identifier(name, source_bytes, "assign")
                )
            ):
                raise ValueError(
                    "aliases for Object.assign/browser globals are unsupported"
                )

        if node.type != "call_expression":
            continue
        function = node.child_by_field_name("function")
        if function is None:
            continue
        arguments = node.child_by_field_name("arguments")
        args = _semantic_named_children(arguments) if arguments is not None else []
        if owns_window_binding and _computed_object_callee(function, source_bytes):
            raise ValueError(
                "computed Object calls in a window-binding owner are unsupported"
            )
        targets_browser_global = bool(args) and _browser_global_rooted(
            args[0], source_bytes
        )
        if not _semantic_object_assign_reference(function, source_bytes):
            continue
        if any(child.type == "optional_chain" for child in node.children) or any(
            child.type == "optional_chain" for child in _walk(function)
        ):
            raise ValueError("optional Object.assign calls are unsupported")
        if not _direct_member_is(
            function,
            source_bytes,
            "Object",
            "assign",
            exact_spelling=True,
        ):
            raise ValueError("computed/escaped Object.assign calls are unsupported")
        allowed_assign_refs.add(_node_key(function))
        if not targets_browser_global:
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
