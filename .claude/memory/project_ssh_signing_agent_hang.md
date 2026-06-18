---
name: ssh-signing-agent-hang
description: "git commit can hang forever on SSH signing when a dead forwarded ssh-agent socket is wedged — check ssh-add -l, fix with SSH_AUTH_SOCK="
metadata: 
  node_type: memory
  type: project
  originSessionId: 97658132-fa2a-4e94-805b-032f01177072
---

On doc1, `git commit` (SSH-signed) can hang indefinitely in `ssh-keygen -Y sign`. Root cause (verified 2026-06-11): `ssh-keygen -Y sign` consults the ssh-agent at `SSH_AUTH_SOCK` *before* falling back to the private-key file — even though the signing key (`~/.ssh/id_ed25519_git_sign`) is an unencrypted file that needs no agent. The session's `SSH_AUTH_SOCK` points at an sshd-forwarded per-login socket under `~/.ssh/agent/`; when the user's SSH connection drops uncleanly, the socket lingers, accepts connections, and never replies — ssh-keygen blocks forever. Once sshd cleans the socket (ENOENT), connect fails fast and signing falls back to the file instantly.

**Why:** A 13-minute "stuck commit" during the #410 session looked like a background-task or pyright-hook problem; it was neither — coincidental timing with the operator's SSH session dying mid-commit.

**How to apply:** If `git commit` hangs, check `ps` for `ssh-keygen -Y sign` and run `timeout 5 ssh-add -l` — a hang (not an error) means a wedged agent socket. Fix: kill the stuck commit and retry with `SSH_AUTH_SOCK= git commit ...` (signing never needs the agent here), or just retry later after sshd cleans the socket. Note `~/.ssh/agent/` accumulates stale per-login sockets (months old); they are harmless once dead — only the wedged-but-alive state blocks.
