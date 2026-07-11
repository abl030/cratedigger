---
name: beets-docs
description: Look up the pinned beets documentation and implementation used by Cratedigger.
---

Before doing anything, run `date` to get the current date and time.

# Beets Documentation Lookup

Read beets reference documentation from the local nix store.

## Instructions

The beets source docs (RST format) are available in the nix store. First, resolve the store path:

```bash
nix build nixpkgs#beets.src --no-link --print-out-paths
```

This returns a path like `/nix/store/<hash>-source`. The docs live at `${BEETS_SRC}/docs/`.

### Doc Tree

| Doc | Path | Lines | Purpose |
|-----|------|-------|---------|
| Config reference | `docs/reference/config.rst` | ~1181 | All config.yaml options |
| CLI commands | `docs/reference/cli.rst` | ~538 | CLI commands reference |
| Path templates | `docs/reference/pathformat.rst` | ~292 | Path format templates |
| Query syntax | `docs/reference/query.rst` | ~443 | Query syntax |
| Plugin overview | `docs/plugins/index.rst` | ~706 | Plugin overview & list |
| Plugin docs | `docs/plugins/<name>.rst` | varies | One file per plugin |
| Autotagger guide | `docs/guides/tagger.rst` | varies | How the autotagger works |
| Advanced guide | `docs/guides/advanced.rst` | varies | Advanced usage |
| FAQ | `docs/faq.rst` | varies | Common questions |

### How to Use

1. **Resolve the path** using the nix build command above
2. **Read docs** with `sed -n '1,220p' "${BEETS_SRC}/docs/reference/config.rst"`
3. **Search docs** with `rg -n "import" "${BEETS_SRC}/docs/reference"`
4. **Find plugin docs** with `sed` or `rg`, for example
   `${BEETS_SRC}/docs/plugins/chroma.rst`

### Quick Lookups

- **Config option**: search `docs/reference/config.rst` for the option name
- **Plugin config**: read `docs/plugins/<plugin-name>.rst`
- **Path template variables**: read `docs/reference/pathformat.rst`
- **Import behaviour**: search config.rst for `import`
- **Matching/autotagger**: read `docs/guides/tagger.rst` and search config.rst for `match`

### Current Beets Packaging

Cratedigger owns its pinned beets build and module integration in:

- `nix/beets.nix` for the patched beets package
- `nix/module.nix` for service options and the rendered beets configuration

Read those first for the deployed shape before diving into upstream RST docs.
After changes, run the relevant Cratedigger tests and use the `deploy` skill; do
not rebuild a hard-coded NixOS target from this workflow.
