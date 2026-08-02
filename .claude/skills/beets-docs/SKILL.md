---
name: beets-docs
description: Look up Beets documentation and the deployment-owned runtime boundary consumed by Cratedigger.
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

### Current Beets Deployment Boundary

The deployment owns the Beets package, immutable effective configuration,
catalog, library, state, secrets, and plain operator `beet`. Cratedigger
consumes and validates those authorities; it does not render or own them.

- `nix/beets.nix` is an optional compatible package factory. A NixOS consumer
  instantiates it with `pkgs = config.services.cratedigger.packageSet;` so its
  Python interpreter matches the application closure.
- `nix/module.nix` consumes the package through
  `services.cratedigger.beets.runtime.package`, points every Beets consumer at
  the external immutable `BEETSDIR`, and validates the supplied runtime
  contract at application startup.
- `docs/beets-primer.md` owns the complete authority and safe-operation
  contract, including use of the deployment-provided plain operator `beet`.

Read those first for the deployed shape before diving into upstream RST docs.
After changes, run the relevant Cratedigger tests and use the `deploy` skill; do
not rebuild a hard-coded NixOS target from this workflow.
