"""Generated patrol for the web process's runtime value (#1313).

``tests/web/test_runtime.py`` pins the exact worlds; this patrols the
space around them. Two invariant families, both production behaviour
that used to be spread across thirteen module globals and five module
functions in ``web/server.py``:

* **Resolution.** Which handle a runtime hands back is a function of two
  declared either/ors — a DSN versus an injected pipeline handle, an
  injected Beets handle versus the admitted library pair — and teardown
  never touches what the caller owns. ``scripts/web_dev_server.py``
  depends on both directions of the first one.
* **Installation.** ``install_runtime`` restores rather than clears, so
  an arbitrary nesting of installs unwinds to exactly the state it
  started from, and ``runtime()`` fails closed outside any install.

Both checkers accumulate violations rather than raising at the first
one, so a world that breaks several clauses reports all of them and no
clause can hide behind an earlier one's short-circuit.
"""

import dataclasses
import os
import sys
import unittest
from collections.abc import Callable, Generator
from contextlib import AbstractContextManager, contextmanager

from hypothesis import given
from hypothesis import strategies as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401 — registers the profiles
import web.runtime as runtime_module
from lib.beets_db import BeetsDB
from lib.pipeline_db import PipelineDB
from tests.fakes import FakeBeetsDB, FakePipelineDB
from tests.helpers import make_web_runtime
from web.runtime import WebRuntime, install_runtime, runtime

# A DSN string that is never dialled: every clause that could open a
# connection is gated on ``db_dsn`` being unset, so this only ever
# exercises the "a DSN is configured" branch of the resolution decision.
UNDIALLED_DSN = "postgresql://generated-never-connected/cratedigger"


def runtime_resolution_violations(rt: WebRuntime) -> list[str]:
    """Every way a runtime's handle resolution can be wrong."""
    violations: list[str] = []

    expected_available = bool(rt.db_dsn) or rt.shared_db is not None
    if rt.db_available() != expected_available:
        violations.append(
            "db_available disagrees with the dsn-or-injected either/or",
        )

    if not rt.db_dsn:
        # Safe to actually resolve: without a DSN nothing dials out.
        if (rt.db_or_none() is not None) != rt.db_available():
            violations.append("db_or_none disagrees with db_available")
        if rt.shared_db is not None and rt.db() is not rt.shared_db:
            violations.append(
                "a DSN-less runtime did not return its injected pipeline handle",
            )

    if rt.shared_beets is not None and rt.beets_db() is not rt.shared_beets:
        violations.append(
            "an injected Beets handle did not win over the admitted pair",
        )

    if (
        rt.shared_beets is None
        and rt.beets_db_path is None
        and rt.beets_db() is not None
    ):
        violations.append("an unconfigured runtime produced a Beets handle")

    # Every generated path is absent from disk: an unopenable library
    # degrades to "not configured" rather than raising into a route.
    if (
        rt.shared_beets is None
        and rt.beets_db_path is not None
        and rt.beets_db() is not None
    ):
        violations.append(
            "an unopenable Beets library did not degrade to None",
        )

    owned_before = (rt.shared_db, rt.shared_beets)
    rt.close_thread_handles()
    if (rt.shared_db, rt.shared_beets) != owned_before:
        violations.append("close_thread_handles disturbed an injected handle")

    return violations


def install_nesting_violations(
    labels: list[str],
    *,
    install: Callable[
        [WebRuntime], AbstractContextManager[WebRuntime],
    ] = install_runtime,
    current: Callable[[], WebRuntime] = runtime,
) -> list[str]:
    """Every way the install stack can fail to unwind cleanly.

    ``install``/``current`` are kwarg-DI seams defaulting to production,
    so each clause below has a world that makes exactly it true —
    otherwise four of the five would be unfalsifiable, since they assert
    properties of the production functions rather than of ``labels``.
    """
    violations: list[str] = []
    runtimes = [WebRuntime(canonical_origin=label) for label in labels]

    def descend(index: int) -> None:
        if index == len(runtimes):
            return
        with install(runtimes[index]) as installed:
            if installed is not runtimes[index]:
                violations.append("install_runtime yielded a different value")
            if current() is not runtimes[index]:
                violations.append("runtime() is not the innermost install")
            descend(index + 1)
            if current() is not runtimes[index]:
                violations.append(
                    "an inner install did not restore its enclosing runtime",
                )

    try:
        current()
    except RuntimeError:
        pass
    else:
        violations.append("runtime() resolved outside any install")

    descend(0)

    try:
        current()
    except RuntimeError:
        return violations
    violations.append("the install stack did not unwind to no runtime")
    return violations


absent_paths = st.sampled_from([
    None,
    "/nonexistent/generated/beets-library.db",
    "/nonexistent/generated/other.db",
])


@st.composite
def web_runtimes(draw: st.DrawFn) -> WebRuntime:
    """Every combination of the two declared handle either/ors."""
    base = WebRuntime(
        db_dsn=UNDIALLED_DSN if draw(st.booleans()) else None,
        beets_db_path=draw(absent_paths),
        beets_library_root=draw(st.sampled_from(["", "/nonexistent/library"])),
    )
    return make_web_runtime(
        base,
        db=FakePipelineDB() if draw(st.booleans()) else None,
        beets=FakeBeetsDB() if draw(st.booleans()) else None,
    )


class TestWebRuntimeGenerated(unittest.TestCase):
    @given(rt=web_runtimes())
    def test_handle_resolution_holds_over_every_configuration(
        self, rt: WebRuntime,
    ) -> None:
        self.assertEqual(runtime_resolution_violations(rt), [])

    @given(labels=st.lists(
        st.text(min_size=1, max_size=6), min_size=0, max_size=4,
    ))
    def test_installs_unwind_to_exactly_where_they_started(
        self, labels: list[str],
    ) -> None:
        self.assertEqual(install_nesting_violations(labels), [])

    @given(rt=web_runtimes(), root=st.sampled_from(["", "/a", "/b"]))
    def test_a_derived_runtime_never_shares_a_handle_store(
        self, rt: WebRuntime, root: str,
    ) -> None:
        """``replace`` mints a fresh ``_threads``; see the module docstring.

        Without it a derived runtime would serve handles the base opened
        on a thread it does not own — the reason ``_threads`` is
        ``init=False`` rather than an ordinary field.
        """
        marker = object()
        rt._threads.db = marker

        derived = dataclasses.replace(rt, beets_library_root=root)

        self.assertIsNot(derived._threads, rt._threads)
        self.assertIsNone(getattr(derived._threads, "db", None))
        self.assertIs(rt._threads.db, marker)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """One minimal world per clause, asserting that clause's own message.

    Each world makes exactly its clause's condition true while every
    earlier clause passes, so a message match proves that clause fired
    rather than an earlier one.
    """

    def test_db_available_clause(self) -> None:
        class Wrong(WebRuntime):
            def db_available(self) -> bool:
                return not (bool(self.db_dsn) or self.shared_db is not None)

        # An injected handle, so the later clauses this world also
        # reaches stay survivable and the message match is this clause's.
        wrong = make_web_runtime(Wrong(), db=FakePipelineDB())
        self.assertIn(
            "db_available disagrees with the dsn-or-injected either/or",
            runtime_resolution_violations(wrong),
        )

    def test_db_or_none_clause(self) -> None:
        class Wrong(WebRuntime):
            def db_or_none(self) -> None:
                return None

        wrong = make_web_runtime(Wrong(), db=FakePipelineDB())
        self.assertIn(
            "db_or_none disagrees with db_available",
            runtime_resolution_violations(wrong),
        )

    def test_injected_pipeline_handle_clause(self) -> None:
        class Wrong(WebRuntime):
            def db(self) -> PipelineDB:
                # A handle that is real enough for the clause and is
                # deliberately NOT the injected one.
                return FakePipelineDB()  # pyright: ignore[reportReturnType]

        wrong = make_web_runtime(Wrong(), db=FakePipelineDB())
        self.assertIn(
            "a DSN-less runtime did not return its injected pipeline handle",
            runtime_resolution_violations(wrong),
        )

    def test_injected_beets_handle_clause(self) -> None:
        class Wrong(WebRuntime):
            def beets_db(self) -> BeetsDB | None:
                return FakeBeetsDB()  # pyright: ignore[reportReturnType]

        wrong = make_web_runtime(Wrong(), beets=FakeBeetsDB())
        self.assertIn(
            "an injected Beets handle did not win over the admitted pair",
            runtime_resolution_violations(wrong),
        )

    def test_unconfigured_beets_clause(self) -> None:
        class Wrong(WebRuntime):
            def beets_db(self) -> BeetsDB | None:
                return FakeBeetsDB()  # pyright: ignore[reportReturnType]

        self.assertIn(
            "an unconfigured runtime produced a Beets handle",
            runtime_resolution_violations(Wrong()),
        )

    def test_unopenable_library_clause(self) -> None:
        class Wrong(WebRuntime):
            def beets_db(self) -> BeetsDB | None:
                return FakeBeetsDB()  # pyright: ignore[reportReturnType]

        wrong = Wrong(beets_db_path="/nonexistent/generated/beets-library.db")
        self.assertIn(
            "an unopenable Beets library did not degrade to None",
            runtime_resolution_violations(wrong),
        )

    def test_teardown_clause(self) -> None:
        class Wrong(WebRuntime):
            def close_thread_handles(self) -> None:
                object.__setattr__(self, "shared_db", None)

        wrong = make_web_runtime(Wrong(), db=FakePipelineDB())
        self.assertIn(
            "close_thread_handles disturbed an injected handle",
            runtime_resolution_violations(wrong),
        )

    def test_runtime_resolves_outside_any_install_clause(self) -> None:
        with install_runtime(WebRuntime()):
            self.assertIn(
                "runtime() resolved outside any install",
                install_nesting_violations([]),
            )

    def test_install_yields_a_different_value_clause(self) -> None:
        @contextmanager
        def yields_an_impostor(
            new: WebRuntime,
        ) -> Generator[WebRuntime]:
            with install_runtime(new):
                yield WebRuntime(canonical_origin="impostor")

        self.assertIn(
            "install_runtime yielded a different value",
            install_nesting_violations(["a"], install=yields_an_impostor),
        )

    def test_runtime_is_not_the_innermost_install_clause(self) -> None:
        impostor = WebRuntime(canonical_origin="impostor")

        def resolves_an_impostor() -> WebRuntime:
            if runtime_module._installed is None:
                raise RuntimeError("no WebRuntime installed")
            return impostor

        violations = install_nesting_violations(
            ["a"], current=resolves_an_impostor,
        )
        self.assertIn("runtime() is not the innermost install", violations)

    def test_inner_install_did_not_restore_its_enclosing_runtime_clause(
        self,
    ) -> None:
        stranger = WebRuntime(canonical_origin="stranger")

        @contextmanager
        def strands_the_innermost(
            new: WebRuntime,
        ) -> Generator[WebRuntime]:
            previous = runtime_module._installed
            runtime_module._installed = new
            try:
                yield new
            finally:
                # A nested exit restores a stranger instead of its
                # enclosing runtime; the outermost still unwinds to
                # nothing, so the "did not unwind" clause stays quiet and
                # the message match below is this clause's alone.
                runtime_module._installed = (
                    stranger if previous is not None else None
                )

        violations = install_nesting_violations(
            ["outer", "inner"], install=strands_the_innermost,
        )
        self.assertIn(
            "an inner install did not restore its enclosing runtime",
            violations,
        )

    def test_stack_did_not_unwind_clause(self) -> None:
        leaked = WebRuntime(canonical_origin="leaked")
        seen: list[int] = []

        def resolves_after_the_stack() -> WebRuntime:
            # Raises for the pre-descend probe (so the "resolved outside
            # any install" clause passes), then resolves normally, then
            # leaks a value on the final probe.
            seen.append(1)
            if len(seen) == 1:
                raise RuntimeError("no WebRuntime installed")
            installed = runtime_module._installed
            return installed if installed is not None else leaked

        violations = install_nesting_violations(
            [], current=resolves_after_the_stack,
        )
        self.assertEqual(
            violations, ["the install stack did not unwind to no runtime"],
        )


if __name__ == "__main__":
    unittest.main()
