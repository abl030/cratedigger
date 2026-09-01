"""Comment-stripped source reads for whole-file pins (issues #1172, #1186).

A test that pins source text by reading a whole file and asserting a substring
is satisfied by that substring appearing in a **comment**. So the single most
likely way a line gets disabled — putting a ``#`` in front of it — leaves the
pin green while the thing it guards is gone.

This is not hypothetical. #1172 proved it for ``nix/module.nix``: commenting
out the migrate unit's ``after = optional cfg.pipelineDb.createLocally
"postgresql-setup.service";`` left ``TestCreateLocallyContract`` passing, so
the guard that keeps a stranger's first boot from racing NixOS role/database
provisioning could be switched off with one character. #1186 found the same
shape across the rest of the tree, most seriously in the deploy runbook, where
every executable pin sits inside a fenced ``bash`` block and a leading ``#``
silently disables a real deploy-safety step.

Read pinned source through :func:`pinned_source` and the assertion sees only
lines that actually do something.

**Deliberately line-start only.** Nothing here parses inline comments, because
that needs quote tracking and would corrupt real code:
``sed 's#/cratedigger.py##'`` and ``grep -o '/nix/store/[^ ;]*/bin/cratedigger'``
both appear in the deploy runbook and both carry a literal ``#`` inside quotes.
The line-start rule needs no parser and is exactly the mutant class that
matters — a whole line switched off.
"""

from __future__ import annotations

from pathlib import Path

_MARKDOWN_FENCE = "```"

# Full-line comment prefixes per file suffix. An unlisted suffix raises rather
# than silently returning raw source, so a new kind of pinned artifact has to be
# considered rather than inheriting the very defect this module exists to
# remove.
#
# ``.json`` is JSONC deliberately, not an oversight: pyright accepts ``//``
# comments in ``pyrightconfig*.json`` (verified against the pinned pyright), so
# a config line there really can be commented out. Assuming "JSON has no
# comments" would have left exactly this file unguarded.
_LINE_COMMENT_PREFIXES: dict[str, tuple[str, ...]] = {
    ".bash": ("#",),
    ".js": ("//",),
    ".json": ("//",),
    # The JavaScript test suites and their shared harness (issue #1313
    # candidate 6). Same syntax as ``.js``; a separate entry because the
    # suffix is what dispatch keys on.
    ".mjs": ("//",),
    ".nix": ("#",),
    ".py": ("#",),
    ".sh": ("#",),
    ".sql": ("--",),
    ".yaml": ("#",),
    ".yml": ("#",),
}


class UnknownPinnedFormat(AssertionError):
    """Raised for a suffix with no declared comment syntax.

    Fail closed: returning the raw source instead would reinstate #1172 for
    whatever new artifact type someone starts pinning.
    """


def strip_line_comments(source: str, prefixes: tuple[str, ...]) -> str:
    """``source`` with every full-line comment removed.

    A trailing comment after code keeps its line — the code on it is real and
    must stay pinnable. A first-line ``#!`` shebang is kept too: it is a
    functional directive, not a disabled line, and removing it would be a lie
    about what the file does.

    Comment lines are **blanked, not deleted**, which matters twice. Deleting
    would splice the lines either side of a comment together and could satisfy
    a multi-line pin that the real file does not contain — turning one false
    green into another. It would also shift line numbers, and two pinning
    modules ``ast.parse`` the same source they pin, so a parse error should
    still name the real line. Several working pins span shell
    line-continuations (``exec ruff check \\`` + its arguments) and match only
    because the lines stay adjacent and in order; nothing here reflows.
    """
    lines = source.splitlines()
    kept = [
        line
        if ((index == 0 and line.startswith("#!"))
            or not line.lstrip().startswith(prefixes))
        else ""
        for index, line in enumerate(lines)
    ]
    return "\n".join(kept)


def strip_fenced_comments(source: str) -> str:
    """Markdown with full-line shell comments removed **inside fences only**.

    Headings are untouched. A Markdown ``#`` at the start of a prose line is a
    heading, and several pins depend on headings as section anchors
    (``## Database migrations`` bounds the strict-hold slice); stripping those
    would destroy the document's structure to fix a problem prose does not
    have. Inside a fenced block the same character is a shell comment, which is
    where the real exposure lives: #1186 measured 14 pinned deploy-runbook
    steps that a single ``# `` disables with the suite green.

    Blanks rather than deletes, for the same reasons as
    :func:`strip_line_comments`.
    """
    kept: list[str] = []
    inside_fence = False
    for line in source.splitlines():
        if line.lstrip().startswith(_MARKDOWN_FENCE):
            inside_fence = not inside_fence
            kept.append(line)
            continue
        kept.append("" if inside_fence and line.lstrip().startswith("#") else line)
    return "\n".join(kept)


def pinned_source(path: Path) -> str:
    """Read ``path`` for a source pin, with disabled lines removed.

    Dispatches on suffix and raises :class:`UnknownPinnedFormat` for anything
    undeclared.
    """
    source = path.read_text(encoding="utf-8")
    if path.suffix == ".md":
        return strip_fenced_comments(source)
    try:
        prefixes = _LINE_COMMENT_PREFIXES[path.suffix]
    except KeyError:
        raise UnknownPinnedFormat(
            f"no declared comment syntax for {path.suffix!r} ({path}). Add it "
            "to tests/_source_pins.py rather than pinning raw source — a raw "
            "pin is satisfied by commented-out text (#1172, #1186)."
        ) from None
    return strip_line_comments(source, prefixes)
