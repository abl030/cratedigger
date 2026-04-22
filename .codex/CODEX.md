# Codex Local Rules

## Commit Message Style

When Codex creates commits in this repo, match the existing Claude-style
history more closely.

### Target style

- Prefer specific one-line subjects in the style already visible in `git log`.
- Name the behavior, invariant, or boundary that changed, not just the area.
- Use the repo's existing prefixes when they fit: `refactor:`, `test:`,
  `docs:`, `fix:`.
- When a prefix is not the clearest fit, use a short imperative subject such
  as `Tighten dual-driver refactor skills`.
- Review-fix commits should say what finding they addressed, not just
  `address review`.

### Good examples from this repo

- `refactor: preserve library rank fallback semantics`
- `test: pin library album row fallback behavior`
- `docs: harden refactor review workflow`
- `refactor: own the library artist album row contract`
- `Tighten dual-driver refactor skills`

### Avoid

- vague subjects like `fix tests`, `refactor delete route`, or `cleanup`
- subjects that name only a file or module without the behavior change
- stacked generic verbs like `update`, `adjust`, or `tweak` when the real
  invariant can be named directly

### Preferred rewrite pattern

- Instead of: `extract beets delete workflow into a typed service`
- Prefer: `refactor: move beets delete semantics into a library service`

- Instead of: `split library delete failures into concrete result types`
- Prefer: `refactor: make beets delete failures explicit in the service seam`
