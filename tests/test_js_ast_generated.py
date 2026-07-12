"""Generated grammar qualification for the shared JavaScript AST audits."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable
import re
import unittest

from hypothesis import given, strategies as st

import tests._hypothesis_profiles  # noqa: F401  # profile registration side effect
from tests.structural_audits.js_ast import (
    WindowBindingAudit,
    assert_window_bindings,
    audit_window_bindings,
    emitted_window_handlers,
    exposed_window_bindings,
    fixture_fields_for_call,
)


_CALL_NAME = "renderDownloadHistoryItem"
_FIXTURE_NAME = "renderDownloadHistoryFixture"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_$][A-Za-z0-9_$]*$")
_KEYS = (
    "outcome",
    "comparison_basis",
    "bad_extensions",
    "request_id",
    "$private",
    "_leading",
    "quoted-key",
    "apostrophe's",
    'double"quote',
    "back\\slash",
    "café",
    "music🎵",
    "line\nbreak",
)


@dataclass(frozen=True)
class PayloadLiteralWorld:
    source: str
    expected_fields: frozenset[str]


@dataclass(frozen=True)
class BoundaryWorld:
    payload_source: str
    payload_rejected: bool
    payload_fixture: str
    payload_renderer: str
    payload_module: str
    window_source: str
    window_rejected: bool


def assert_exact_fixture_fields(
    world: PayloadLiteralWorld, actual_fields: set[str]
) -> None:
    """Independent oracle: every authored direct key appears, and only those."""
    if actual_fields != set(world.expected_fields):
        raise AssertionError(
            f"fixture fields {sorted(actual_fields)!r} != "
            f"oracle {sorted(world.expected_fields)!r}"
        )


def assert_window_world(
    audit: WindowBindingAudit,
    *,
    required: set[str],
    exposed: set[str],
) -> None:
    """Independent oracle for required, exposed, and missing handler sets."""
    expected_missing = required - exposed
    if (
        audit.required != required
        or audit.exposed != exposed
        or audit.missing != expected_missing
    ):
        raise AssertionError(
            "window audit diverged from authored handler/binding oracle"
        )


def assert_boundary_world(
    world: BoundaryWorld,
    *,
    payload_rejected: bool,
    window_rejected: bool,
) -> None:
    """Independent oracle for the two explicit supported-callee grammars."""
    if (
        payload_rejected != world.payload_rejected
        or window_rejected != world.window_rejected
    ):
        raise AssertionError("explicit fixture/window boundary diverged from oracle")


def assert_unsupported_renderer_reference_is_rejected(
    source: str,
    extractor: Callable[[str, str], set[str]] = fixture_fields_for_call,
) -> None:
    """An executable renderer reference must be audited or rejected, never hidden."""
    try:
        fields = extractor(source, _CALL_NAME)
    except ValueError:
        return
    raise AssertionError(
        f"unsupported audited renderer reference returned fields {fields!r}"
    )


def assert_unsupported_window_binding_is_rejected(
    source: str,
    extractor: Callable[[str], set[str]] = exposed_window_bindings,
) -> None:
    """Unsupported binding syntax must not disappear behind a valid block."""
    try:
        bindings = extractor(source)
    except ValueError:
        return
    raise AssertionError(
        f"unsupported window binding reference returned {bindings!r}"
    )


def _encode_js_key(key: str, quote: str, escape_mode: str) -> str:
    encoded: list[str] = []
    for char in key:
        codepoint = ord(char)
        if char == "\\":
            encoded.append("\\\\")
        elif char == quote:
            encoded.append("\\" + char)
        elif char == "\n":
            encoded.append("\\n")
        elif escape_mode == "hex" and char == "_":
            encoded.append("\\x5f")
        elif escape_mode == "unicode" and codepoint > 0x7F:
            encoded.append(
                f"\\u{codepoint:04x}"
                if codepoint <= 0xFFFF
                else f"\\u{{{codepoint:x}}}"
            )
        else:
            encoded.append(char)
    return "".join(encoded)


def _encode_js_identifier(key: str) -> str:
    for index, char in enumerate(key):
        if char.isascii() and char.isalpha():
            return key[:index] + f"\\u{ord(char):04x}" + key[index + 1:]
    return key


@st.composite
def payload_literal_worlds(draw: st.DrawFn) -> PayloadLiteralWorld:
    keys = draw(
        st.lists(
            st.sampled_from(_KEYS),
            min_size=1,
            max_size=7,
            unique=True,
        )
    )
    entries: list[str] = []
    value_forms = (
        "1",
        "'renderDownloadHistoryItem({ fake_string_key: 1 })'",
        "{ nested_only: 1, deeper: { hidden: 2 } }",
        "[{ array_nested_only: 1 }]",
        "`text ${templateValue}`",
    )
    for key in keys:
        forms = ["quoted", "computed"]
        if _IDENTIFIER_RE.fullmatch(key):
            forms.extend(("identifier", "shorthand"))
        form = draw(st.sampled_from(forms))
        value = draw(st.sampled_from(value_forms))
        if form == "shorthand":
            authored_key = _encode_js_identifier(key) if draw(st.booleans()) else key
            entry = authored_key
        elif form == "identifier":
            authored_key = _encode_js_identifier(key) if draw(st.booleans()) else key
            entry = f"{authored_key}: {value}"
        else:
            quote = draw(st.sampled_from(("'", '"')))
            escape_mode = draw(st.sampled_from(("plain", "hex", "unicode")))
            literal = quote + _encode_js_key(key, quote, escape_mode) + quote
            entry = f"{literal}: {value}"
            if form == "computed":
                entry = f"[{literal}]: {value}"
        entries.append(entry)

    entries = list(draw(st.permutations(entries)))
    comment = draw(
        st.sampled_from(("", "/* direct-key separator */", "// separator\n"))
    )
    separator = f",{comment}"
    if draw(st.booleans()) and len(entries) > 1:
        split = draw(st.integers(min_value=1, max_value=len(entries) - 1))
        first = separator.join(entries[:split])
        second = separator.join(entries[split:])
        argument = f"[{{{first}}}, /* array gap */ {{{second}}}]"
    elif draw(st.booleans()):
        argument = f"[{{{separator.join(entries)}}}]"
    else:
        argument = f"{{{separator.join(entries)}}}"

    callee = draw(st.sampled_from((_CALL_NAME, f"__test__.{_CALL_NAME}")))
    call = f"{callee}({argument})"
    wrapper = draw(st.sampled_from(("direct", "template")))
    statement = (
        f"const rendered = {call};"
        if wrapper == "direct"
        else f"const rendered = `before ${{{call}}} after`;"
    )
    prefix = (
        "const templateValue = 1; const unicodePrefix = 'é🎵';\n"
        f"// {_CALL_NAME}({{ fake_comment_key: 1 }});\n"
        f"const bait = '{_CALL_NAME}({{ fake_string_key: 1 }})';\n"
    )
    return PayloadLiteralWorld(
        source=prefix + statement,
        expected_fields=frozenset(keys),
    )


@st.composite
def boundary_worlds(draw: st.DrawFn) -> BoundaryWorld:
    declaration = draw(st.sampled_from(("let", "const")))
    shape = draw(st.sampled_from(("scalar", "array", "object", "duplicate")))
    initial = draw(st.sampled_from(("target", "unrelated")))
    mutation = draw(st.sampled_from(("none", "before_target", "before_other", "after")))
    shadow = draw(st.booleans())
    unknown = draw(st.booleans())
    registration_variant = draw(
        st.sampled_from(("valid", "namespace", "default_alias", "shadowed_test"))
    )
    object_root = draw(
        st.sampled_from(
            (
                "Object",
                "(Object)",
                "globalThis.Object",
                'globalThis["Object"]',
                "self['Object']",
                "globalThis.window.Object",
                "globalThis['window']['Object']",
            )
        )
    )
    target_expression = draw(
        st.sampled_from(
            (
                "window",
                "true ? window : {}",
                "window || {}",
                "target.nested",
                "{nested: window}",
            )
        )
    )

    target = _CALL_NAME
    initial_value = target if initial == "target" else "unrelated"
    final_value = target if mutation == "before_target" else "unrelated"
    if unknown:
        setup = (
            f"{declaration} selector = JSON.parse('\"{initial_value}\"');"
        )
        selector = "selector"
        before = ""
        after = ""
        shadow_name = "selector"
    elif shape == "scalar":
        setup = f'{declaration} selector = "{initial_value}";'
        selector = "selector"
        before = (
            f'selector = "{final_value}";'
            if mutation.startswith("before") and declaration == "let"
            else ""
        )
        after = (
            'selector = "unrelated";'
            if mutation == "after" and declaration == "let"
            else ""
        )
        shadow_name = "selector"
    else:
        opener = "[" if shape == "array" else "{current: "
        closer = "]" if shape == "array" else "}"
        if shape == "duplicate":
            literal = f'{{current: "unrelated", current: "{initial_value}"}}'
        else:
            literal = f'{opener}"{initial_value}"{closer}'
        setup = f"{declaration} selectors = {literal};"
        selector = "selectors[0]" if shape == "array" else "selectors.current"
        before = (
            f'{selector} = "{final_value}";'
            if mutation.startswith("before")
            else ""
        )
        after = (
            f'{selector} = "unrelated";' if mutation == "after" else ""
        )
        shadow_name = "selectors"

    shadow_block = (
        f'{{ const {shadow_name} = "shadow-only"; void {shadow_name}; }}'
        if shadow
        else ""
    )
    payload_fixture = _FIXTURE_NAME
    payload_renderer = _CALL_NAME
    payload_module = "./fixture.js"
    if registration_variant == "namespace":
        registration = "import * as historyModule from './fixture.js';"
        payload_call = (
            f"historyModule[{selector}]({{invented_client_only: 1}});"
        )
    elif registration_variant == "default_alias":
        registration = (
            f"import historyDefault, {{ {_CALL_NAME} as {_FIXTURE_NAME} }} "
            "from './fixture.js';"
        )
        payload_call = f"{_FIXTURE_NAME}({{invented_client_only: 1}});"
    elif registration_variant == "shadowed_test":
        payload_fixture = "renderRecentsFixture"
        payload_renderer = "renderRecentsItems"
        payload_module = "./recents.js"
        registration = "import { __test__ } from './recents.js';"
        payload_call = (
            "function renderWithShadow(__test__) {"
            "const { renderRecentsItems: renderRecentsFixture } = __test__;"
            "return renderRecentsFixture([{invented_client_only: 1}]);}"
        )
    else:
        registration = (
            f"import {{ {_CALL_NAME} as {_FIXTURE_NAME} }} from './fixture.js';"
        )
        payload_call = f"helpers[{selector}]({{invented_client_only: 1}});"
    payload_source = (
        f"{registration}{setup}{shadow_block}{before}{payload_call}{after}"
    )

    assign_initial = "assign" if initial == "target" else "unrelated"
    window_setup = setup.replace(target, "assign").replace(
        initial_value, assign_initial
    )
    window_before = before.replace(target, "assign")
    window_after = after.replace(target, "assign")
    target_setup = (
        "const target = {nested: window};"
        if "target." in target_expression
        else ""
    )
    window_source = (
        f"{window_setup}{shadow_block}{window_before}{target_setup}"
        "Object.assign(window, { supported });"
        f"{object_root}[{selector}]({target_expression}, {{ fetch }});{window_after}"
    )
    return BoundaryWorld(
        payload_source=payload_source,
        payload_rejected=registration_variant != "valid",
        payload_fixture=payload_fixture,
        payload_renderer=payload_renderer,
        payload_module=payload_module,
        window_source=window_source,
        window_rejected=True,
    )


_UNSUPPORTED_PAYLOADS = st.sampled_from(
    (
        f"{_CALL_NAME}({{[fieldName]: 1}});",
        f"{_CALL_NAME}({{...base, outcome: 'success'}});",
        f"{_CALL_NAME}(fixture);",
        f"{_CALL_NAME}([fixture]);",
        f"{_CALL_NAME}([...rows]);",
        f"{_CALL_NAME}([{{outcome: 'success'}}, ...rows]);",
        f"{_CALL_NAME}([, {{outcome: 'success'}}]);",
        f"{_CALL_NAME}({{method() {{ return 1; }}}});",
    )
)

_UNSUPPORTED_RENDERER_REFERENCES = st.sampled_from(
    (
        f'globalThis["{_CALL_NAME}"]({{invented_client_only: 1}});',
        f'globalThis["renderDownloadHistory\\u0049tem"]({{invented_client_only: 1}});',
        (
            f'const name = "{_CALL_NAME}"; '
            "__test__[name]({invented_client_only: 1});"
        ),
        f"__test__?.{_CALL_NAME}({{invented_client_only: 1}});",
        f"(0, {_CALL_NAME})({{invented_client_only: 1}});",
        f"{_CALL_NAME}.call(null, {{invented_client_only: 1}});",
        f"{_CALL_NAME}?.({{invented_client_only: 1}});",
        f"const alias = {_CALL_NAME}; alias({{invented_client_only: 1}});",
        (
            f"let alias; alias = {_CALL_NAME}; "
            "alias({invented_client_only: 1});"
        ),
        (
            f"const {{{_CALL_NAME}: alias}} = helpers; "
            "alias({invented_client_only: 1});"
        ),
        (
            f"import {{{_CALL_NAME} as alias}} from './fixture.js'; "
            "alias({invented_client_only: 1});"
        ),
    )
)

_UNSUPPORTED_WINDOW_BINDINGS = st.sampled_from(
    (
        "Object.assign(window, { supported }); Object['assign'](window, { fetch });",
        "Object.assign(window, { supported }); Object.assign((window), { fetch });",
        r"Object.assign(window, { supported }); Object.\u0061ssign(window, { fetch });",
        "Object.assign(window, { supported }); Object.assign?.(window, { fetch });",
        (
            'const method = "assign"; '
            "Object.assign(window, { supported }); Object[method](window, { fetch });"
        ),
        "Object.assign(window, { supported }); Object.assign(globalThis, { fetch });",
        "Object.assign(window, { supported }); Object.assign(self, { fetch });",
        "Object.assign(window, { supported }); globalThis.fetch = localFetch;",
        "Object.assign(window, { supported }); globalThis.window.fetch = localFetch;",
        "Object.assign(window, { supported }); Object?.assign(window, { fetch });",
        (
            'let method = "assign"; Object.assign(window, { supported }); '
            "Object[method](window, { fetch });"
        ),
        (
            'const methods = {current: "unrelated"}; methods.current = "assign"; '
            "Object.assign(window, { supported }); "
            "Object[methods.current](window, { fetch });"
        ),
        (
            "const alias = Object.assign; "
            "Object.assign(window, { supported }); alias(window, { fetch });"
        ),
        (
            "const {assign: alias} = Object; "
            "Object.assign(window, { supported }); alias(window, { fetch });"
        ),
        (
            "const target = window; Object.assign(window, { supported }); "
            "Object.assign(target, { fetch });"
        ),
        "Object.assign(window, { supported }); (window).fetch = localFetch;",
    )
)

_SUPPORTED_UNRELATED_COMPUTED_CALLS = st.sampled_from(
    (
        'const key = "unrelated"; globalThis[key]();',
        'const label = "renderDownloadHistoryItem"; console.log(label);',
        "const key = getRuntimeName(); unrelatedNamespace[key]();",
    )
)


_HANDLER_NAMES = tuple(f"generatedHandler{i:02d}" for i in range(16))


class TestJsAstGenerated(unittest.TestCase):
    @given(payload_literal_worlds())
    def test_supported_payload_world_matches_independent_field_oracle(
        self, world: PayloadLiteralWorld
    ) -> None:
        assert_exact_fixture_fields(
            world, fixture_fields_for_call(world.source, _CALL_NAME)
        )

    @given(_UNSUPPORTED_PAYLOADS)
    def test_unsupported_payload_worlds_fail_closed(self, source: str) -> None:
        with self.assertRaises(ValueError):
            fixture_fields_for_call(source, _CALL_NAME)

    @given(_UNSUPPORTED_RENDERER_REFERENCES)
    def test_unsupported_renderer_references_fail_closed(self, source: str) -> None:
        assert_unsupported_renderer_reference_is_rejected(source)

    @given(_UNSUPPORTED_WINDOW_BINDINGS)
    def test_unsupported_window_binding_references_fail_closed(
        self, source: str
    ) -> None:
        assert_unsupported_window_binding_is_rejected(source)

    @given(_SUPPORTED_UNRELATED_COMPUTED_CALLS)
    def test_unrelated_computed_calls_and_inert_strings_remain_supported(
        self, source: str
    ) -> None:
        if "Object.assign" in source:
            self.assertEqual(exposed_window_bindings(source), {"supported"})
        else:
            self.assertEqual(fixture_fields_for_call(source, _CALL_NAME), set())

    @given(boundary_worlds())
    def test_explicit_boundaries_match_independent_state_world_oracle(
        self, world: BoundaryWorld
    ) -> None:
        try:
            fields = fixture_fields_for_call(
                world.payload_source,
                world.payload_fixture,
                registered_renderer=world.payload_renderer,
                registered_module=world.payload_module,
            )
        except ValueError:
            payload_rejected = True
        else:
            payload_rejected = False
            self.assertEqual(fields, set())
        try:
            exposed_window_bindings(world.window_source)
        except ValueError:
            window_rejected = True
        else:
            window_rejected = False
        assert_boundary_world(
            world,
            payload_rejected=payload_rejected,
            window_rejected=window_rejected,
        )

    @given(
        st.sampled_from(("'", "`")),
        st.sampled_from(("\u2028", "\u2029")),
    )
    def test_unicode_line_continuations_preserve_static_window_handler(
        self, delimiter: str, separator: str
    ) -> None:
        literal = (
            f"{delimiter}window.generated\\{separator}Handler()"
            f"{delimiter}"
        )
        audit = audit_window_bindings(
            {"generated.js": f"const value = {literal};"},
            "",
            "Object.assign(window, { generatedHandler });",
        )
        assert_window_world(
            audit,
            required={"generatedHandler"},
            exposed={"generatedHandler"},
        )

    @given(
        st.sampled_from(("'", "`")),
        st.sampled_from((r"\uD800", r"\uDC00", r"\u{D800}", r"\u{DC00}")),
    )
    def test_valid_surrogate_escape_worlds_parse(
        self, delimiter: str, escape: str
    ) -> None:
        source = f"const value = {delimiter}{escape}{delimiter};"
        self.assertEqual(
            emitted_window_handlers({"surrogate.js": source}, "").handlers,
            set(),
        )

    @given(
        st.sampled_from(("fetch", "alert", "open", "setTimeout")),
        st.integers(min_value=0, max_value=15),
    )
    def test_escaped_native_binding_keys_still_collide(
        self, name: str, selector: int
    ) -> None:
        index = selector % len(name)
        escaped = name[:index] + f"\\u{ord(name[index]):04x}" + name[index + 1:]
        source = f"Object.assign(window, {{ {escaped} }});"
        audit = audit_window_bindings({}, "", source)
        self.assertEqual(audit.native_collisions, {name})
        with self.assertRaisesRegex(ValueError, "reserved native window names"):
            assert_window_bindings({}, "", source)

    @given(
        st.lists(
            st.sampled_from(_HANDLER_NAMES),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        st.integers(min_value=0, max_value=15),
        st.booleans(),
    )
    def test_window_handler_world_matches_independent_binding_oracle(
        self, handlers: list[str], missing_selector: int, escape_names: bool
    ) -> None:
        required = set(handlers)
        missing = handlers[missing_selector % len(handlers)]
        exposed = required - {missing}
        def authored_name(name: str, index: int) -> str:
            if not escape_names or index % 2:
                return name
            return f"\\u{ord(name[0]):04x}{name[1:]}"

        literals = "\n".join(
            f"const html{i} = '<button onclick=\"window.{authored_name(name, i)}()\">x</button>';"
            for i, name in enumerate(handlers)
        )
        sorted_exposed = sorted(exposed)
        midpoint = len(sorted_exposed) // 2
        blocks = (
            "Object.assign(window, {"
            + ", ".join(sorted_exposed[:midpoint])
            + "});\nObject.assign(window, {"
            + ", ".join(
                f"{name}: local_{name}" if i % 2 else name
                for i, name in enumerate(sorted_exposed[midpoint:])
            )
            + "});"
        )
        audit = audit_window_bindings(
            {"generated.js": literals}, "", blocks
        )
        assert_window_world(audit, required=required, exposed=exposed)

    @given(
        st.sampled_from(
            (
                'const html = `<button onclick="window.${handler}()">x</button>`;',
                r'const html = `<button onclick="window\u002e${handler}()">x</button>`;',
                "const html = '<button onclick=\"window[handler]()\">x</button>';",
            )
        )
    )
    def test_dynamic_window_callee_worlds_fail_closed(self, source: str) -> None:
        audit = audit_window_bindings(
            {"generated.js": source}, "", "Object.assign(window, {});"
        )
        self.assertTrue(audit.dynamic_callees)
        with self.assertRaisesRegex(ValueError, "dynamic window callee"):
            assert_window_bindings(
                {"generated.js": source}, "", "Object.assign(window, {});"
            )

    def test_known_bad_quoted_key_escape_mutant_trips_oracle(self) -> None:
        world = PayloadLiteralWorld(
            source=f'{_CALL_NAME}({{"invented\\x5fclient": 1, outcome: 2}});',
            expected_fields=frozenset({"invented_client", "outcome"}),
        )
        real_fields = fixture_fields_for_call(world.source, _CALL_NAME)
        quoted_key_escape_mutant = real_fields - {"invented_client"}
        with self.assertRaisesRegex(AssertionError, "invented_client"):
            assert_exact_fixture_fields(world, quoted_key_escape_mutant)

    def test_known_bad_template_interpolation_mutant_trips_oracle(self) -> None:
        world = PayloadLiteralWorld(
            source=(
                "const html = `before ${"
                f"{_CALL_NAME}({{inside_template: 1}})"
                "} after`;"
            ),
            expected_fields=frozenset({"inside_template"}),
        )
        call_inside_template_interpolation_mutant: set[str] = set()
        with self.assertRaisesRegex(AssertionError, "inside_template"):
            assert_exact_fixture_fields(
                world, call_inside_template_interpolation_mutant
            )

    def test_known_bad_missing_window_binding_trips_oracle(self) -> None:
        audit = WindowBindingAudit(
            required={"needed"},
            exposed=set(),
            missing=set(),
            dynamic_callees=(),
            native_collisions=set(),
        )
        with self.assertRaisesRegex(AssertionError, "diverged"):
            assert_window_world(audit, required={"needed"}, exposed=set())

    def test_known_bad_fail_open_renderer_reference_trips_checker(self) -> None:
        world = BoundaryWorld(
            payload_source=(
                "import * as historyModule from './fixture.js';"
                f'const names = {{current: "{_CALL_NAME}"}}; '
                'names.current = "unrelated"; '
                "historyModule[names.current]({invented_client_only: 1});"
            ),
            payload_rejected=True,
            payload_fixture=_FIXTURE_NAME,
            payload_renderer=_CALL_NAME,
            payload_module="./fixture.js",
            window_source=(
                'const methods = {current: "unrelated"}; '
                'methods.current = "assign"; '
                "Object.assign(window, {}); "
                "Object[methods.current]({nested: window}, {fetch});"
            ),
            window_rejected=True,
        )
        with self.assertRaisesRegex(AssertionError, "boundary diverged"):
            assert_boundary_world(
                world,
                payload_rejected=False,
                window_rejected=False,
            )

    def test_known_bad_shadowed_registration_mutant_trips_checker(self) -> None:
        world = BoundaryWorld(
            payload_source=(
                "import { __test__ } from './recents.js';"
                "function run(__test__) {"
                "const {renderRecentsItems: renderRecentsFixture} = __test__;"
                "return renderRecentsFixture([{invented_client_only: 1}]);}"
            ),
            payload_rejected=True,
            payload_fixture="renderRecentsFixture",
            payload_renderer="renderRecentsItems",
            payload_module="./recents.js",
            window_source=(
                "Object.assign(window, {}); Object[method]({}, {fetch});"
            ),
            window_rejected=True,
        )
        with self.assertRaisesRegex(AssertionError, "boundary diverged"):
            assert_boundary_world(
                world,
                payload_rejected=False,
                window_rejected=True,
            )

    def test_known_bad_fail_open_window_binding_trips_checker(self) -> None:
        source = "Object.assign(window, {}); Object['assign'](window, {fetch});"
        with self.assertRaisesRegex(AssertionError, "unsupported window binding"):
            assert_unsupported_window_binding_is_rejected(
                source, extractor=lambda _source: set()
            )


if __name__ == "__main__":
    unittest.main()
