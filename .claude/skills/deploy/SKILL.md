---
name: deploy
description: Deploy merged Cratedigger main to doc2 now with one command, then check the change yourself. Merged main otherwise ships on the nightly rolling update.
---

# Deploy to doc2

Merged `main` ships without you: doc1's nightly rolling flake update pins the
branch tip into nixosconfig at 23:00 and doc2 applies it at 04:00, migrations
included. Deploy by hand only when a user-facing change needs to be live now,
or when you need to see a fix working before you close its issue.

Run it from the shared checkout on doc1 (`hostname` = `proxmox-vm`) after the
PR is merged. A worktree-isolated session must leave its worktree first,
because the script drives git in `~/nixosconfig`:

```bash
scripts/deploy.sh
```

That is the whole runbook. The script pins nixosconfig's `cratedigger-src` to
`origin/main` (pass a 40-hex SHA on `origin/main` to pin an older one), commits
the one-file change SSH-signed, pushes it to Forgejo through nixosconfig's
token boundary, triggers doc2's verified rebuild over the locked-sibling fleet
trigger, waits for that run to finish, and checks doc2's trust anchor equals
the commit it pushed. It prints one line per stage. On failure it prints the
upgrade journal and exits nonzero; read that, fix the cause, run it again. Do
not narrate, poll, or re-verify its stages yourself.

Then check your change, not the deploy. The workers (`cratedigger-web`,
`-importer`, `-import-preview-worker`, `-youtube-ingest`) restarted on the
switch, and `cratedigger.service` (`restartIfChanged = false`) loads the new
code on its next timer cycle, a few minutes at most. Exercise the real CLI,
API, or UI path the change touched. If the change was not user-facing there is
nothing to check, and no reason to have deployed.

After a non-trivial series has shipped, run the post-ship reflection in
`.claude/rules/deploy.md`.
