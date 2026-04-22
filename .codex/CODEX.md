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
- For non-trivial commits, add a short summary body in Claude's style.

### Commit summaries / bodies

- If the commit carries real structural or behavioral context, do not stop at
  the subject line.
- Add a short body that explains the key change in plain language.
- The body should usually answer one or two of:
  - what seam or invariant changed
  - what bug or review finding this addresses
  - why the new shape is safer or clearer
- Keep it short. One compact paragraph is usually enough.
- Review-fix commits should say what finding they closed, not just that they
  were follow-up work.

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

### Preferred body shape

```text
refactor: move beets delete semantics into a library service

Extract the /api/beets/delete workflow out of the route so preflight checks,
pipeline purge ordering, and partial-success classification all live behind
one typed service seam.
```

```text
refactor: share library delete request resolution

Route album-detail and delete through the same pipeline lookup helper so the
exact-release fallback chain cannot drift between the two call sites.
```
