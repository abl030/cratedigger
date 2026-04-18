---
paths:
  - "harness/**"
  - "lib/beets.py"
  - "lib/quality.py"
---

# Beets Harness Rules

- The harness runs in the beets Python environment (Nix Home Manager on doc1), NOT in the dev shell
- `_serialize_album_candidate()` must capture EVERY field from AlbumMatch — never discard data
- **Harness-emitted types are `msgspec.Struct`, not `@dataclass`** — `HarnessItem`, `HarnessTrackInfo`, `TrackMapping`, `CandidateSummary`, `ChooseMatchMessage`. Decoded at the wire boundary in `lib/beets.py::beets_validate` via `msgspec.convert(msg, type=ChooseMatchMessage)`. The strict-typed decoder is what catches int-vs-str drift (PR #98 bug, issue #99). See the `Wire-boundary types` section in `.claude/rules/code-quality.md` for the full policy.
- **Never normalise on the consumer side.** If beets emits an int where our Struct says str, fix `_id_str` in `harness/beets_harness.py`. Defensive coercion downstream defeats the boundary.
- import_one.py emits ImportResult as a `__IMPORT_RESULT__` sentinel JSON line on stdout, human logging on stderr
- Use `beets-docs` skill (`.claude/commands/beets-docs.md`) to look up beets internals
