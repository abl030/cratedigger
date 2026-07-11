# AI Portability

Cratedigger keeps project knowledge independent of the client driving a session.
Claude-flavoured Markdown is the authored form because it is already the larger
surface; Codex consumes symlinks or generated TOML adapters rather than a second
copy.

| Concept | Authored source | Adapter |
| --- | --- | --- |
| Repository instructions | `CLAUDE.md` | `AGENTS.md` symlink |
| Reusable skills | `.claude/skills/*/SKILL.md` | `.agents/skills` symlink |
| Specialist agents | `.claude/agents/*.md` | `.codex/agents/*.toml` |
| Project MCP servers | `.mcp.json` | `.codex/config.toml` |
| Durable learning | `.claude/memory/`, `docs/`, issues and PRs | none |

`tools/generate-ai-adapters.py` owns the format conversions. Run it through
`nix-shell` after an agent or MCP edit, then run it with `--check`. The unit suite
runs the same check to catch stale or manually edited adapters.

Claude auto-memory and Codex native memory are useful recall caches, but neither
is canonical. A discovery that must survive a change of client belongs in the
shared memory index, a subsystem document, or an issue/PR. Keep detailed
rationale in docs and use `.claude/memory/MEMORY.md` as the routing surface.

Compound Engineering and the retired refactor/review orchestration are not part
of this model. Both clients use their native capabilities and the repository's
shared rules.
