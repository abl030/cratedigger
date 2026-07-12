# Security Audit — 2026-07-12

Point-in-time application-security review of cratedigger. Orchestrated as a
multi-agent audit: 10 attack-surface dimensions, each finding traced to the
real code path and then adversarially re-verified (refute-by-default) by an
independent pass. This document is the full rundown; the tracking issue links
back here.

- **Scope:** the whole runtime — HTTP transport + routes, SQL/DB layer,
  subprocess/command construction, filesystem/destructive ops, web-UI XSS,
  secrets/credentials, SSRF/deserialization, dependency/supply-chain, and the
  NixOS module/systemd hardening.
- **Method:** a sequential fable finder per dimension produced structured
  findings; each finding was handed to an independent skeptic that read the
  real code and returned CONFIRMED / PLAUSIBLE / REFUTED with severity adjusted
  to reality. Six dimensions ran through that workflow; the remaining four
  (secrets, SSRF/deserialization, dependency, nix-module) were completed
  in-session by the orchestrator after the fable credit pool was exhausted, at
  the same rigor.
- **Nothing in this audit was auto-fixed.** It is a report only.

## Threat model (load-bearing)

cratedigger ingests data from **untrusted Soulseek P2P peers** — filenames,
directory names, usernames, and file metadata all originate from arbitrary
remote strangers and flow into filesystem paths, subprocess argv, the pipeline
DB, and the web UI. MusicBrainz/Discogs/YouTube data is semi-trusted but
attacker-influenceable. The one legitimate operator model means classic
multi-tenant IDOR matters little, while any RCE / arbitrary file delete /
secret leak / stored-XSS via peer or API data matters a lot.

### Deployment reality (why several severities are lower than they first look)

The verification pass established that the web UI is **not internet-reachable**:
`music.ablz.au` is a Cloudflare **DNS-only** record resolving to a private LAN
IP, doc2's host firewall opens only 80/443, and the nginx reverse proxy in
front of the app adds no authentication. So the practical attacker position is
**"a device on the LAN/tailnet, or the operator's own browser being used as a
CSRF relay"** — not an anonymous internet client. The findings below are rated
against that reality, not against a hypothetical public exposure.

## Severity summary

| ID | Severity | Title | Primary location |
|----|----------|-------|------------------|
| CD-SEC-01 | High | Cleartext Meelo credential committed to a public repo | `docs/meelo-primer.md` |
| CD-SEC-02 | High | No auth + wildcard CORS on file-destructive endpoints | `web/server.py` |
| CD-SEC-03 | Medium | Manual-import / import-preview accept an arbitrary absolute path (+ in-place `mp3val -f`) | `web/routes/imports.py`, `lib/util.py` |
| CD-SEC-04 | Medium | No systemd sandboxing on services that process attacker-controlled bytes | `nix/module.nix` |
| CD-SEC-05 | Low | Internal exception strings reflected in HTTP 500 bodies | `web/server.py` |
| CD-SEC-06 | Low | TLS verification disabled on the Plex/Jellyfin fallback path | `lib/util.py` |
| CD-SEC-07 | Low | Unbounded request body read + JSON parse (memory-exhaustion DoS) | `web/server.py` |
| CD-SEC-08 | Low | Unvalidated MB `id` interpolated into the mirror URL (request-shaping) | `web/routes/browse.py`, `web/mb.py` |
| CD-SEC-09 | Low | Latent identifier-interpolation SQLi footguns (hardcoded today) | `lib/pipeline_db/requests.py`, `lib/pipeline_db/dashboard.py` |
| CD-SEC-10 | Low | Unescaped controlled-vocabulary metadata in a few JS rows | `web/js/pipeline.js`, `web/js/library.js`, `web/js/wrong-matches.js` |
| CD-SEC-11 | Info | Symlink/containment asymmetries in delete/move paths | `lib/processing_paths.py`, `lib/download_materialization.py` |
| CD-SEC-12 | Info | Dependency currency (yt-dlp/ffmpeg) + CI gates only on GitGuardian | `flake.nix`, `flake.lock` |
| CD-SEC-13 | Info | Plex XML parsed with stdlib ElementTree under an unverified-TLS fallback | `lib/util.py` |

**Clean (no exploitable issue found):** SQL injection (every attacker-influenced
value is `%s`-parameterized), command/subprocess injection (all argv is
list-form, no shell; the yt-dlp URL is `--`-separated; peer filenames are
absolutised before reaching ffmpeg/mp3val/sox/flac), static-file serving
(basename reduction + a `/js/` prefix/suffix allowlist), stored XSS of
free-text peer fields (a single consistent escape helper is applied to every
free-text string), config secret handling (`*_file` indirection, peer-auth DB,
no plaintext in the rendered config, VM-test-enforced), and request-time SSRF to
arbitrary hosts (every outbound base URL is fixed config, not request input).

---

## Priority 1

### CD-SEC-01 — Cleartext Meelo credential in a public repo (High)

`docs/meelo-primer.md` contained a real login (`username abl030`, a plaintext
password) in four places, and it has been present in git **history** since the
Meelo-scan feature commit. The cratedigger repo remote is public GitHub, so the
credential is world-readable and is in history — removing the lines from the
working tree does not un-publish it.

- **Impact:** anyone reading the public repo obtains the operator's Meelo login;
  the dominant real risk is password reuse across other services.
- **Why CI missed it:** the only CI gate is GitGuardian, whose detectors key on
  high-entropy tokens; a low-entropy dictionary-style password does not trip it.
- **Remediation:** treat the credential as compromised and **rotate it** (and
  anywhere it was reused). Scrub the plaintext from the docs (done in the same
  change that adds this audit). History rewrite is optional for a single-operator
  repo once the credential is rotated. (Operator note: Meelo is no longer in
  active use, which caps the blast radius — rotation is still the correct close.)

### CD-SEC-02 — No auth + wildcard CORS on file-destructive endpoints (High)

`web/server.py`'s `do_GET`/`do_POST` dispatch every route with **no** identity,
session, token, or origin check, and every JSON response sends
`Access-Control-Allow-Origin: *` with a permissive `do_OPTIONS`. `do_POST` also
parses the body with `json.loads` regardless of `Content-Type`, so even a
preflight-free `text/plain` "simple request" reaches handlers. Destructive sinks
reachable with no credential include `/api/beets/delete` (removes library
files), `/api/pipeline/ban-source` (routes to `beet remove -d` via
`lib/release_cleanup.py`), `/api/pipeline/delete`, and the `/api/wrong-matches/*`
family. The `confirm: "DELETE"` fields are input validation, not authorization —
an attacker simply supplies the constant.

- **Attack path (given the LAN/tailnet deployment):** any device on the trusted
  network issues the POST directly with `curl`; or the operator visits any
  malicious web page whose JavaScript `fetch()`es the internal API and
  permanently deletes archival albums — the exact irreversible action the
  archivist invariants reserve for the operator.
- **Verification note:** the raw auditor framing ("any internet client") was
  refuted — the service is LAN/tailnet-only. The surviving, confirmed path is
  the CSRF/drive-by from the operator's browser plus any device already on the
  trusted network.
- **Remediation:** drop `Access-Control-Allow-Origin: *` entirely (this is a
  same-origin SPA — it does not need CORS), validate `Origin`/`Referer` on all
  mutating POSTs, and add an application-layer auth check (a shared secret the
  reverse proxy injects and the handler verifies, or a session). Destructive
  routes should be treated as privileged operations, not confirm-string-gated
  ones. This is a design decision (auth mechanism) and is intentionally left for
  the operator rather than auto-fixed.

## Priority 2

### CD-SEC-03 — Arbitrary absolute path in manual-import / import-preview (Medium)

`web/routes/imports.py` (`post_manual_import`, `post_import_preview`) forwards an
operator-supplied `path` to the resolver in `lib/util.py`, which returns
`os.path.abspath` of **any** existing directory with no staging-root
confinement (the slskd root is only a fallback base for *relative* inputs). The
resolved path is enqueued as an import job or measured directly. `import-preview`
additionally runs `mp3val -f` **in place** on any `.mp3` under the supplied
directory (an integrity mutation of arbitrary on-host files) and reads audio
tags from arbitrary directories (info disclosure / existence oracle).
`post_import_preview` also reads the raw body instead of going through the
pydantic `parse_body` seam. The sibling streaming route in
`web/wrong_match_file_service.py` already enforces a within-root containment
check — these two endpoints simply omit it.

- **Remediation:** confine both endpoints to the configured Incoming /
  `failed_imports` roots with the same within-root check the wrong-match service
  uses, and route import-preview through `parse_body`.

### CD-SEC-04 — No systemd sandboxing on untrusted-input services (Medium)

Every long-running unit rendered by `nix/module.nix` (web, importer,
import-preview-worker, youtube-ingest) sets only `User`/`Group`/`ExecStart`/
`WorkingDirectory`/`Restart`. There is **no** `ProtectSystem=strict`,
`ReadWritePaths`, `PrivateTmp`, `ProtectHome`, `NoNewPrivileges`,
`RestrictAddressFamilies`, or `SystemCallFilter`. These services drive yt-dlp,
ffmpeg, beets, mp3val, and sox over attacker-controlled bytes. Running as a
non-root system user is the only current mitigation — a memory-safety bug in any
of those tools yields the full service-user capability set: write access to the
entire music library plus read access to every secret the user can read.

- **Remediation:** add a hardening block to the untrusted-input units (start
  with `NoNewPrivileges`, `ProtectSystem=strict` + an explicit `ReadWritePaths`
  allowlist for the state/library/staging dirs, `PrivateTmp`, `ProtectHome`,
  `RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX`). Gate the change with the
  existing module VM check, since over-tight confinement would break real file
  access. This is a module change and is deferred to its own PR.

## Priority 3 — low / hardening

### CD-SEC-05 — Exception strings reflected in 500 bodies (Low)

`web/server.py`'s `do_GET`/`do_POST` catch-alls return the raw exception text to
the client (`self._error(str(e), 500)`). A malformed body, wrong-typed field, or
DB/filesystem error becomes a reflected error oracle. In this deployment nothing
secret can leak (the production DSN is passwordless and the internal paths it
could reveal are already in the public repo), so severity is low.

- **Remediation:** return a generic `{"error": "internal error"}` on the 500 path
  and keep the full trace in the server log.

### CD-SEC-06 — TLS verification disabled on the Plex/Jellyfin fallback (Low)

The verify-then-unverified helper in `lib/util.py` retries with
`check_hostname = False` and `verify_mode = CERT_NONE` on `ssl.SSLError`. It is
used by the Plex XML/PUT and Jellyfin JSON calls, which carry the
`X-Plex-Token` / `X-Emby-Token`. An active LAN MITM can present an invalid cert
to force the fallback and then harvest the token and inject responses. The first
attempt still verifies, and these are homelab media-server tokens, so severity
is low — but a "fall back to no verification" downgrade is worth removing.

- **Remediation:** trust the homelab CA (or pin the expected self-signed cert)
  instead of disabling verification.

### CD-SEC-07 — Unbounded request body / JSON parse DoS (Low)

`web/server.py`'s POST paths do `self.rfile.read(int(Content-Length))` then
`json.loads(...)` with no size cap, under a thread-per-connection server. Several
concurrent large bodies can drive the web process to OOM. It self-restarts in a
few seconds and the pipeline/importer/DB are separate processes, so severity is
low.

- **Remediation:** enforce a central maximum body size (reject over ~1 MB with
  413) before reading, and validate `Content-Length` parses to a non-negative int.

### CD-SEC-08 — Unvalidated MB `id` interpolated into the mirror URL (Low)

`web/routes/browse.py` validates `raw_id.isdigit()` only for the Discogs source;
for the MusicBrainz source the id is passed straight into the URL builder in
`web/mb.py`, which interpolates it into the release path with no `quote` and no
UUID check. A crafted id can reshape the path/query sent to the internal MB
mirror. There is no arbitrary-host SSRF (the origin is fixed config) and urllib
rejects control chars, so severity is low.

- **Remediation:** UUID-validate MB ids before dispatch (mirroring the Discogs
  `isdigit` gate) and `urllib.parse.quote` path segments in the MB URL builder.

### CD-SEC-09 — Latent identifier-interpolation SQLi footguns (Low)

Two DB helpers interpolate an identifier into SQL rather than a `%s` value:
`record_attempt` in `lib/pipeline_db/requests.py` (builds a column name from
`attempt_type`) and the private dashboard cycle-rows helper in
`lib/pipeline_db/dashboard.py` (takes `order_by`/`where` fragments). Every caller
today passes a hardcoded literal, so neither is exploitable — but they are the
only caller-supplied-identifier interpolations in the DB layer, and one future
web/CLI wiring that threads a parameter through them would create first-order
SQLi that `%s` cannot express.

- **Remediation:** add a module-level allowlist / enum mapping at each site and
  raise on anything else — one line closes each seam locally instead of relying
  on auditing every future caller.

### CD-SEC-10 — Unescaped controlled-vocabulary metadata in JS rows (Low)

A small, consistent class of enum-shaped metadata fields (country, format,
release type, year) is interpolated into `innerHTML` **without** the escape
helper in `web/js/pipeline.js`, `web/js/library.js`, and `web/js/wrong-matches.js`,
unlike every adjacent free-text field. These values come from MB/Discogs
metadata that the threat model treats as attacker-influenceable. Practical
exploitability is low because the fields are server-side controlled vocabularies
(ISO country codes, fixed format/type enums, integer year), but the gap violates
the codebase's own escaping discipline.

- **Remediation:** wrap the interpolations in the existing escape helper and add
  a JS lint/test rule that flags un-escaped interpolations inside `innerHTML`
  template literals.

## Info / defense-in-depth

### CD-SEC-11 — Symlink/containment asymmetries in delete/move paths (Info)

The containment guard in `lib/processing_paths.py` normalizes `..` textually but
does not resolve symlinks (`normpath`, not `realpath`), and the materialize move
in `lib/download_materialization.py` trusts the slskd-stamped source path with no
within-root check, whereas the delete/reap paths in `lib/slskd_transfers.py` do
guard it. No peer-reachable vector exists today (slskd sanitizes remote filenames
and writes only regular files into its own tree; the reaper additionally requires
ledger ownership and does not descend symlinked dirs), so these are hardening
asymmetries rather than live bugs.

- **Remediation:** unify on `realpath`-based containment and add the missing
  within-root check to the move source, matching the stronger guard used on the
  delete side.

### CD-SEC-12 — Dependency currency and CI scope (Info)

Supply-chain hygiene is otherwise good: `flake.lock` pins nixpkgs to a specific
rev, the runtime source is content-addressed, and there are no unpinned code or
binary fetches. The residual risk is **version currency of yt-dlp and ffmpeg** —
both parse attacker-controlled input/streams and are the highest-value targets in
the closure, so the `nix flake update` cadence (already coupled to a beets-drift
gate in the deploy rules) matters most for them. Separately, CI runs only
GitGuardian; the test suite, pyright, and dead-code checks are enforced by the
local pre-push hook rather than in CI, so a green PR check is not a green suite.

- **Remediation:** keep a regular flake-update cadence with an eye on yt-dlp/ffmpeg
  advisories; optionally add the suite to CI so the gate does not depend solely on
  the local hook.

### CD-SEC-13 — Plex XML parsed with stdlib ElementTree (Info)

Plex responses are parsed with `xml.etree.ElementTree` in `lib/util.py`. Stdlib
ElementTree does not resolve external entities (no classic XXE), but combined
with the unverified-TLS fallback (CD-SEC-06) a MITM could feed crafted XML.

- **Remediation:** use `defusedxml` for the Plex XML parse as defense-in-depth,
  and/or fix CD-SEC-06 so the response source is authenticated.

## Considered and dismissed (refuted)

- **`pipeline-cli query` read-only guard "bypass".** The session-scoped
  read-only guard in `scripts/pipeline_cli/query.py` can be overridden by the SQL
  the operator themselves supplies. This is not an injection: there is no
  untrusted taint source (the operator provides the whole statement) and reaching
  the command already requires a shell on doc2 holding the full-privilege DSN,
  which permits unrestricted writes via `psql` anyway. It is a footgun label, not
  a privilege boundary.
- **`failed_imports` rmtree fallback.** The fallback that approves a directory
  with a `failed_imports` ancestor is deliberate (force/manual quarantine folders
  live outside the strict slskd-root branch), every path reaching the delete
  originates from a cratedigger-written DB value, and no route/CLI accepts a path.
  Removing the fallback would regress legitimate cleanup.

## Remediation checklist

Operator actions (not code):

- [ ] **CD-SEC-01** — rotate the Meelo password (and any reuse); confirm the
      plaintext is scrubbed from the docs.
- [ ] **CD-SEC-02** — decide the web-UI auth mechanism (proxy-injected shared
      secret vs session) before wiring it.

Safe code fixes (candidate single hardening PR):

- [ ] CD-SEC-03 — within-root confinement + `parse_body` on import endpoints.
- [ ] CD-SEC-05 — generic 500 body.
- [ ] CD-SEC-07 — request-body size cap.
- [ ] CD-SEC-08 — UUID-validate MB id + `quote` path segments.
- [ ] CD-SEC-09 — allowlist the two identifier-interpolation sites.
- [ ] CD-SEC-10 — escape enum metadata in the three JS rows + lint rule.
- [ ] CD-SEC-11 — `realpath` containment + move-source within-root check.
- [ ] CD-SEC-13 — `defusedxml` for the Plex XML parse.

Larger / separate PRs:

- [ ] CD-SEC-02 — drop wildcard CORS + add auth layer.
- [ ] CD-SEC-04 — systemd hardening block (gated by the module VM check).
- [ ] CD-SEC-06 — replace `CERT_NONE` fallback with CA trust / cert pin.

## Appendix — audit method

The audit ran as a deterministic multi-agent workflow: one finder per dimension
(sequential, to stay within rate limits), each finding then handed to an
independent adversarial verifier that read the real code and returned a
CONFIRMED / PLAUSIBLE / REFUTED verdict with severity re-based on actual
reachability and deployment. Six dimensions (HTTP transport, route input
validation, SQL injection, command/subprocess injection, path traversal/file
ops, untrusted-data XSS) completed through the workflow; the fable credit pool
was exhausted mid-run, so the remaining four (secrets/credentials,
SSRF/deserialization, dependency/supply-chain, nix-module/infra) were completed
in-session by the orchestrator at the same rigor, reading the same files. Raw
finding counts before de-duplication: 3 CONFIRMED, 12 PLAUSIBLE, 2 REFUTED across
the workflow half, consolidated with the four inline dimensions into the 13
findings above.
