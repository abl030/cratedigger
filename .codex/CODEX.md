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
- For ordinary feature commits, one compact paragraph is usually enough.
- For review-fix commits, be willing to write a fuller audit-trail body when
  the point is to close a specific review finding, document equivalence, or
  record live-shape evidence.
- Review-fix commits should say what finding they closed, not just that they
  were follow-up work.

### Shared review-fix format

- When following the shared `refactor` or `fix-bug` workflows, keep the
  cross-engine review-fix subject pattern from the shared skill instead of
  inventing a Codex-only variant.
- Prefer subjects such as
  `review(r3 p2): document equivalence + pin observed prod shapes (codex)` or
  `review(r2 adversarial): stale docs + pre-48914ca mbid pin test`.
- The subject should preserve the review round and reviewer, while the body
  carries the actual reasoning, evidence, and verification.
- Codex-specific customization belongs in attribution and trailers, not in a
  separate subject convention that would drift from Claude's history.

### Agent attribution

- When Codex writes a commit body in this repo, explicitly name `Codex` so the
  full history can distinguish Codex-authored commits from Claude-authored
  ones.
- Prefer natural body text such as `Codex: restore the original pipeline-first
  read order...` or `Codex review caught ...` rather than polluting the subject
  line.
- If the commit would otherwise be subject-only but attribution matters, add a
  compact body so the authorship is still visible in `git log` output.
- End Codex-authored commits with this exact trailer so GitHub renders Codex as
  a separate co-author:
  `Co-Authored-By: Codex <noreply@openai.com>`
- Put the trailer after the summary body / verification bullets, separated by a
  blank line, matching the repo's existing Claude commit shape.
- Leave the git author metadata alone unless the user explicitly asks to change
  commit authorship, name, or email.

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

```text
fix: distinguish library detail cancel from delete

Codex: split the detail-page cancel path from the actual delete action so the
UI only reports deletes when the service actually removed something.

Co-Authored-By: Codex <noreply@openai.com>
```
