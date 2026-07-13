# Security Audit — 2026-07-12

Point-in-time application-security review of cratedigger. Orchestrated as a
multi-agent audit: 10 attack-surface dimensions, each finding traced to the
real code path and then adversarially re-verified (refute-by-default) by an
independent pass. This document is the full rundown; the tracking issue links
back here.

After PR #662 merged the original report, an independent Codex pass reviewed
the same runtime plus the request-state machine, import outcome atomicity,
strict wire boundaries, live doc2 perimeter, full Python closure, and complete
git history. The additions below are de-duplicated by root cause: overlaps keep
their original `CD-SEC-*` ID, while genuinely distinct defects start at
`CD-SEC-14`. Companion code-quality findings use `CD-QUAL-*` so they do not get
misrepresented as remotely exploitable vulnerabilities.

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
| CD-SEC-01 | High | Historical credentials committed to a public repo | deleted notifier docs, git history |
| CD-SEC-02 | High | No auth + wildcard CORS on file-destructive endpoints | `web/server.py` |
| CD-SEC-03 | Medium | Manual-import / import-preview accept an arbitrary absolute path (+ in-place `mp3val -f`) | `web/routes/imports.py`, `lib/util.py` |
| CD-SEC-04 | Medium | No systemd sandboxing on services that process attacker-controlled bytes | `nix/module.nix` |
| CD-SEC-05 | Low | Internal exception strings reflected in HTTP 500 bodies | `web/server.py` |
| CD-SEC-06 | Medium | TLS verification disabled on the Plex/Jellyfin fallback path | `lib/util.py` |
| CD-SEC-07 | Low | Unbounded request body read + JSON parse (memory-exhaustion DoS) | `web/server.py` |
| CD-SEC-08 | Low | Unvalidated MB `id` interpolated into the mirror URL (request-shaping) | `web/routes/browse.py`, `web/mb.py` |
| CD-SEC-09 | Low | Latent identifier-interpolation SQLi footguns (hardcoded today) | `lib/pipeline_db/requests.py`, `lib/pipeline_db/dashboard.py` |
| CD-SEC-10 | Low | Unescaped controlled-vocabulary metadata in a few JS rows | `web/js/pipeline.js`, `web/js/library.js`, `web/js/wrong-matches.js` |
| CD-SEC-11 | Medium | Symlink escape in wrong-match streaming + containment asymmetries | `lib/processing_paths.py`, `web/wrong_match_file_service.py` |
| CD-SEC-12 | Low | Vulnerable-version matches in the locked closure + narrow CI scope | `flake.lock`, `nix/package.nix` |
| CD-SEC-13 | Info | Plex XML parsed with stdlib ElementTree under an unverified-TLS fallback | `lib/util.py` |
| CD-SEC-14 | Critical | Remediated: destructive identifiers bind to one server-owned release | `lib/destructive_release_service.py` |
| CD-SEC-15 | High | Request transition engine fails open and can resurrect `replaced` rows | `lib/transitions.py`, `lib/pipeline_db/requests.py` |
| CD-SEC-16 | High | Remediated: destructive operations share importer locks | `lib/destructive_release_service.py` |
| CD-SEC-17 | High | Terminal import outcomes persist as multiple autocommit statements | `lib/dispatch/outcome_actions.py`, `scripts/import_preview_worker.py` |
| CD-SEC-18 | Medium | Track replacement is non-atomic | `lib/pipeline_db/misc.py` |
| CD-SEC-19 | Medium | Import-job JSONB payloads bypass the strict wire-boundary policy | `lib/import_queue.py`, `scripts/importer.py` |

**Clean (no exploitable issue found):** SQL injection (every attacker-influenced
value is `%s`-parameterized), command/subprocess injection (all argv is
list-form, no shell; the yt-dlp URL is `--`-separated; peer filenames are
absolutised before reaching ffmpeg/mp3val/sox/flac), static-file serving
(basename reduction + a `/js/` prefix/suffix allowlist), stored XSS of
free-text peer fields (a single consistent escape helper is applied to every
free-text string), runtime config secret handling (`*_file` indirection,
peer-auth DB, no secret material in the Nix store or process argv,
VM-test-enforced), and request-time SSRF to
arbitrary hosts (every outbound base URL is fixed config, not request input).

---

## Priority 1

### CD-SEC-01 — Historical credentials in a public repo (High)

A now-deleted media-server primer contained a real login (`username abl030`, a
plaintext password) in four places, and it has been present in git **history**
since the retired notifier was added. The cratedigger repo remote is public
GitHub, so the credential is world-readable and is in history — removing the
lines from the working tree does not un-publish it.

A full-history Gitleaks scan of 2,135 commits found one additional credible
credential-shaped value: the original imported `soularr.py` history contains
the same 32-character hexadecimal predecessor API key in two early commits.
The associated host is a private-LAN address and the integration is obsolete,
so the key may already be dead; it is nevertheless public and must be treated
as compromised. The other 16 raw Gitleaks matches were de-duplicated false
positives: a deliberately synthetic redaction-test token and expiring GitHub
private-image URL parameters. A tracked-HEAD scan found only the synthetic test
token, not a current credential.

- **Impact:** anyone reading the public repo obtains the retired service login;
  the dominant real risk is password reuse across other services.
- **Why CI missed it:** the only CI gate is GitGuardian, whose detectors key on
  high-entropy tokens; a low-entropy dictionary-style password does not trip it.
- **Disposition (2026-07-13):** both integrations are retired and absent from
  production. Their notifier/config/documentation surfaces were deleted from
  the current tree, leaving no live service or API target for either historical
  credential. History rewrite is optional for this single-operator repo.

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

### CD-SEC-14 — Destructive routes do not bind identifiers to one release (Critical)

**Remediated in the issue #663 destructive-authority workstream.** Ban-source
roots authority in `album_requests.id`; library-delete roots it in the beets
album primary key. Release/pipeline IDs supplied by a client are optional
confirmation values only. The service rejects a mismatch before mutation and
the HTTP/CLI adapters map that semantic conflict to 422/exit 3.

Two destructive workflows accept multiple independently trusted identifiers
without proving they describe the same release:

- `/api/pipeline/ban-source` accepts `request_id` and `mb_release_id`. It uses
  `request_id` for uploader lookup, bad-hash ownership, denylisting, state
  transition and audit logging, but uses `mb_release_id` for beets lookup,
  hashing and `beet remove -d`.
- `/api/beets/delete` accepts a beets `album_id`, optional `pipeline_id`, and
  `release_id`. `resolve_pipeline_request` deliberately prefers the explicit
  `pipeline_id`, while the filesystem/beets deletion independently targets the
  supplied `album_id`.

A mismatched payload can therefore delete release A's files while purging or
requeueing request B. The ban-source variant additionally records A's good
audio hashes as bad under B, poisoning future acquisition decisions. The
library-delete test suite currently pins the unsafe preference for explicit
`pipeline_id`, so the green suite certifies the wrong contract.

- **Remediation:** choose one server-resolved identity root per operation. Load
  the request/beets album server-side, derive every other identifier from it,
  and reject redundant mismatched identifiers with zero DB, beets or filesystem
  mutation. Ship a deterministic A/B mismatch pin plus a generated cross-product
  property for the no-mutation invariant.

### CD-SEC-15 — Request transitions fail open and can resurrect `replaced` rows (High)

`lib/transitions.py::apply_transition` logs an invalid transition and then
continues. Most target states ultimately call `update_status`,
`reset_to_wanted`, or `mark_imported_with_rescue`, whose SQL does not compare
the row's current status with the caller's `from_status`. The web
`/api/pipeline/update` route and CLI status actions can therefore move a frozen
`replaced` audit row back to `wanted`, `manual`, or `imported`; a stale valid
transition can also race Replace and overwrite the newly frozen status.

The follow-up audit reproduced the real seam with a stateful fake:
`replaced -> wanted` logged "proceeding anyway", returned `True`, and changed
the row to `wanted`. The existing deterministic test explicitly preserves this
fail-open behavior "for backward compatibility", contrary to the current
single-operator, forward-only contract.

- **Remediation:** reject invalid transitions with a typed conflict and make
  every transition a SQL compare-and-set against the expected source status.
  Expand the generated lifecycle model to drive every operator entry point from
  every status, with `replaced` frozen under all worlds.

### CD-SEC-16 — Destructive beets operations race the importer (High)

**Remediated in the issue #663 destructive-authority workstream.** Both
destructive services take the importer's session advisory locks in canonical
IMPORT then RELEASE order (RELEASE only when a library album has no pipeline
row), re-read identity and active-job state under lock, and hold the lock
through every destructive effect. Contention/active jobs map to 409/exit 4.
Ban-source additionally requires the server-validated literal confirmation
`BAN`; the browser dialog remains only the first UI affordance.

Ban-source checks for an active import job, then releases that observation and
hashes/removes the release without acquiring the importer's per-release
advisory lock. An importer can claim the job immediately after the check.
`/api/beets/delete` has neither an active-job check nor a release lock. Both
paths can therefore mutate the beets DB and album files concurrently with
`dispatch_import_core`, even though the importer deliberately holds the release
lock for exactly this data-loss boundary.

- **Remediation:** acquire the same per-release advisory lock before the final
  active-job recheck and hold it across the complete destructive operation.
  Return 409 on contention. Test with two real DB sessions and a
  barrier-controlled beets mutation. Add a server-validated confirmation to
  ban-source; its current browser `confirm()` is only a UI affordance.

### CD-SEC-17 — Terminal import outcomes are non-atomic (High)

Successful and rejected outcomes persist request state, attempts, download
audit, denylist state and import-job state as separate autocommit statements.
The preview worker first marks a job terminal, then catches and suppresses any
failure while requeueing/logging its parent; its own comment documents the
result as a terminal job whose request remains `downloading` forever. The
success path similarly marks a request `imported` before writing the mandatory
download audit.

- **Remediation:** introduce one DB-layer transaction per terminal domain
  outcome. Existing helpers commit internally, so merely wrapping their current
  calls is insufficient. Add failure injection at every write boundary and
  assert all-or-none persisted state.

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

### CD-SEC-18 — Track replacement is non-atomic (Medium)

`PipelineDB.set_tracks` deletes the current release tracklist, then inserts the
replacement one row at a time under autocommit. A malformed later track,
connection failure, or concurrent reader can observe or leave zero/partial
tracks. Those rows feed exact-pressing matching and persisted search plans, so
partial replacement corrupts identity evidence rather than merely degrading a
display. `FakePipelineDB` replaces its list atomically, while the real-PG write
audit explicitly leaves `set_tracks` on a TODO exemption and covers only the
successful path.

- **Remediation:** perform delete + bulk insert in one DB transaction. Seed an
  old tracklist, force a later-row constraint failure, and prove the old set
  survives. Pair the deterministic pin with an all-or-nothing generated
  property.

### CD-SEC-19 — Import-job JSONB payloads bypass strict boundary validation (Medium)

`ImportJob` is a dataclass containing `dict[str, Any]` payload/result fields,
and `from_row` manually coerces outer scalars rather than decoding job-specific
wire contracts. Force/manual consumers then use permissive `.get()` checks. A
drifted `download_log_id: "42"`, for example, silently becomes `None`; the job
continues without its wrong-match/audit ancestor instead of failing at the DB
boundary. This violates the repository's `msgspec.Struct` policy for JSONB and
subprocess wire data.

- **Remediation:** define strict payload Structs discriminated by job type and
  decode once when reading the DB row. Reject wrong-type IDs with a boundary
  test, then remove legacy preview-status compatibility after confirming the
  migration-swept values are absent live.

## Additional hardening findings

### CD-SEC-05 — Exception strings reflected in 500 bodies (Low)

`web/server.py`'s `do_GET`/`do_POST` catch-alls return the raw exception text to
the client (`self._error(str(e), 500)`). A malformed body, wrong-typed field, or
DB/filesystem error becomes a reflected error oracle. In this deployment nothing
secret can leak (the production DSN is passwordless and the internal paths it
could reveal are already in the public repo), so severity is low.

- **Remediation:** return a generic `{"error": "internal error"}` on the 500 path
  and keep the full trace in the server log.

### CD-SEC-06 — TLS verification disabled on the Plex/Jellyfin fallback (Medium)

The verify-then-unverified helper in `lib/util.py` retries with
`check_hostname = False` and `verify_mode = CERT_NONE` on `ssl.SSLError`. It is
used by the Plex XML/PUT and Jellyfin JSON calls, which carry the
`X-Plex-Token` / `X-Emby-Token`. An active LAN MITM can present an invalid cert
to force the fallback and then harvest the token and inject responses. Because
certificate failure itself deterministically activates the insecure retry,
there is no authenticated first-attempt protection once an attacker is on path.
The credentials authorize media-server reads/writes, making this a confirmed
credential-confidentiality and response-integrity failure rather than an
informational self-signed-certificate accommodation.

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

## Containment, dependency and defense-in-depth

### CD-SEC-11 — Symlink escape in wrong-match streaming + containment asymmetries (Medium)

The containment guard in `lib/processing_paths.py` normalizes `..` textually but
does not resolve symlinks (`abspath`/`normpath`, not `realpath`). The wrong-match
audio route validates a lexically contained candidate path and extension, then
opens it for streaming. A real-function reproduction planted
`failed_imports/Album/track.mp3` as a symlink to a file outside the candidate
root; the guard accepted it and the route would stream the target bytes.

The prerequisite is the ability to create a symlink in a candidate tree — not a
plain Soulseek filename — so this is not anonymous peer-to-file-read. A
compromised downloader, co-resident process, or writer sharing the staging group
is sufficient, and CD-SEC-04 documents how broad the live service-user access
currently is. Separately, the materialize move in
`lib/download_materialization.py` trusts the slskd-stamped source path without
the within-root check used by delete/reap paths; that half remains
defense-in-depth rather than a reproduced escape.

- **Remediation:** canonicalize both root and candidate with `realpath`, reject
  symlink components, and open the audio file with `O_NOFOLLOW`/fd-based
  validation to close the check/open race. Add the missing move-source
  within-root check, matching the stronger delete-side guard.

### CD-SEC-12 — Vulnerable-version matches in the locked closure + narrow CI scope (Low)

Supply-chain structure is good: `flake.lock` pins nixpkgs to a specific rev, the
runtime source is content-addressed, and there are no unpinned code or binary
fetches. The pin was last updated from a 2026-06-29 nixpkgs revision, so the
point-in-time OSV scan also checked the actual Nix Python environment rather
than inferring dependencies from a nonexistent requirements file.

The scan covered 111 installed distributions and returned 25 raw advisory
records. After alias de-duplication and Nix/call-path verification, six packages
genuinely match affected version ranges: Flask 3.1.2, idna 3.13, lxml 6.0.2,
msgpack 1.1.2, soupsieve 2.8.3, and urllib3 2.6.3. No current Cratedigger path
was found using the vulnerable Flask session behavior, lxml parser,
`msgpack.Unpacker`, attacker-controlled SoupSieve selector/IDNA hostname, or the
two affected urllib3 low-level streaming/proxy patterns. urllib3 is nevertheless
reachable through the YouTube/requests code and should be the first upgrade;
2.7.0 fixes GHSA-mf9v-mfxr-j63j and GHSA-qccp-gfcp-xxvc.

Two scanner hits were verified false positives rather than silently waived:
the cryptography advisory applies to OpenSSL bundled in wheels, while the Nix
extension dynamically links Nix OpenSSL 3.6.2; and broken Flask-CORS metadata
reports 0.0.1 even though the pinned Nix derivation/source is fixed 6.0.2.
yt-dlp and ffmpeg still deserve particular attention because both parse
attacker-controlled input/streams.

Separately, CI runs only GitGuardian; the suite, pyright, dead-code and flake
checks are enforced by the local pre-push hook rather than CI, so a green PR
check is not a green suite.

- **Remediation:** update the flake and re-run the real-beets/full-suite gates;
  confirm urllib3 >= 2.7.0, idna >= 3.15, lxml >= 6.1.0, msgpack >= 1.2.1,
  soupsieve >= 2.8.4, and Flask >= 3.1.3 in the realized closure. Keep a regular
  update cadence for yt-dlp/ffmpeg advisories; optionally add the suite to CI so
  the gate does not depend solely on the local hook.

### CD-SEC-13 — Plex XML parsed with stdlib ElementTree (Info)

Plex responses are parsed with `xml.etree.ElementTree` in `lib/util.py`. Stdlib
ElementTree does not resolve external entities (no classic XXE), but combined
with the unverified-TLS fallback (CD-SEC-06) a MITM could feed crafted XML.

- **Remediation:** use `defusedxml` for the Plex XML parse as defense-in-depth,
  and/or fix CD-SEC-06 so the response source is authenticated.

## Companion code-quality findings

These are durable correctness/maintenance gaps found by the follow-up pass.
They belong in the covering issue because they affect the same remediation
surfaces, but they are not counted as remotely exploitable security findings.

### CD-QUAL-01 — Seven operator actions lack CLI/API symmetry

Dynamic route/CLI comparison found web-only POST actions for beets delete,
ban-source, pipeline delete, set-quality, upgrade, wrong-match converge, and
release-group resolution. This violates the repository rule that both adapters
wrap one shared service method. The drift is already visible in ban-source: a
large destructive handler owns identity resolution, hashing, denylisting,
cleanup, transition and audit logic directly in the route.

- **Remediation:** as each affected security finding is fixed, move authority
  into a typed service result and add the missing thin CLI/API twin with matched
  exit/status mappings. Do not create seven independent rewrites; group by the
  shared destructive or identity service seam.

### CD-QUAL-02 — Forward-only cleanup and documentation drift

`scripts/populate_tracks.py` is an obsolete committed one-shot that passes an
old SQLite path to the PostgreSQL `PipelineDB` and can no longer run. The import
queue retains `would_import`/`uncertain` compatibility even though migration 018
is documented as having swept those values. The pipeline DB rule still lists
four statuses and omits terminal `replaced`. These are three instances of the
same forward-only hygiene failure: one-shot/compatibility residue outliving its
deployment window and prose no longer matching the authoritative schema.

- **Remediation:** delete the dead one-shot, confirm the legacy preview-status
  cohort is empty before removing compatibility, and update the status docs in
  the same PR that fixes CD-SEC-15/CD-SEC-19.

### CD-QUAL-03 — The real-PostgreSQL write audit overstates its coverage

The write-contract audit describes universal coverage but still allowlists
several writers with `TODO` rationales, including `set_tracks`. A green suite
therefore does not establish the repository's documented rule that every DB
write preserves every field and failure boundary through real PostgreSQL.

- **Remediation:** replace TODO exemptions with named real-PG round trips or
  narrowly document why a method is structurally inapplicable. CD-SEC-18 should
  remove the `set_tracks` exemption first and qualify the failure path with a
  planted later-row violation.

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
  originates from a cratedigger-written DB value, and no *delete* route/CLI
  accepts a free path. (CD-SEC-03 is a separate import/preview path-input bug.)
  Removing the fallback would regress legitimate cleanup.

## Remediation checklist

Operator actions (not code):

- [x] **CD-SEC-01** — retire the unused credential-bearing integrations and
      remove their runtime, configuration, test, and documentation surfaces.
- [ ] **CD-SEC-02** — decide the web-UI auth mechanism (proxy-injected shared
      secret vs session) before wiring it.

Priority data-loss / audit-integrity work:

- [ ] **CD-SEC-14** — bind every destructive identifier to one server-resolved
      release; mismatch must produce zero mutation.
- [ ] **CD-SEC-15** — fail closed on invalid transitions and compare-and-set the
      expected source status, including frozen `replaced` rows.
- [ ] **CD-SEC-16** — hold the importer release lock across ban/delete beets
      mutations and return 409 on contention.
- [ ] **CD-SEC-17** — persist each terminal import outcome in one DB transaction.

Priority containment / integrity work:

- [ ] CD-SEC-03 — within-root confinement + `parse_body` on import endpoints.
- [ ] CD-SEC-04 — systemd hardening block (gated by the module VM check).
- [ ] CD-SEC-06 — replace `CERT_NONE` fallback with CA trust / cert pin.
- [ ] CD-SEC-11 — symlink-safe stream open + `realpath` containment +
      move-source within-root check.
- [ ] CD-SEC-18 — transactional track replacement + real-PG failure pin/property.
- [ ] CD-SEC-19 — strict job-specific JSONB payload Structs at the DB boundary.

Safe hardening fixes (candidate single PR):

- [ ] CD-SEC-05 — generic 500 body.
- [ ] CD-SEC-07 — request-body size cap.
- [ ] CD-SEC-08 — UUID-validate MB id + `quote` path segments.
- [ ] CD-SEC-09 — allowlist the two identifier-interpolation sites.
- [ ] CD-SEC-10 — escape enum metadata in the three JS rows + lint rule.
- [ ] CD-SEC-13 — `defusedxml` for the Plex XML parse.

Auth, dependency and quality follow-through:

- [ ] CD-SEC-02 — drop wildcard CORS + add auth layer.
- [ ] CD-SEC-12 — update the flake, verify fixed Python closure versions, and
      consider moving local-only gates into CI.
- [ ] CD-QUAL-01 — add missing CLI/API twins while extracting the shared
      destructive/identity services for CD-SEC-14/CD-SEC-16.
- [ ] CD-QUAL-02 — delete the dead one-shot, retire proven-empty compatibility,
      and correct status docs alongside CD-SEC-15/CD-SEC-19.
- [ ] CD-QUAL-03 — eliminate TODO write-audit exemptions, starting with
      `set_tracks` in CD-SEC-18.

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
original findings.

The follow-up pass ran Bandit over 64,065 production lines, Gitleaks over tracked
HEAD and all 2,135 commits, OSV against 111 realized Python distributions,
warning-level ShellCheck, pyright, `nix flake check`, dead-code/dict-access/JS
gates, and the full 5,625-test suite. It also inspected the live doc2 listener,
route index/CORS behavior and systemd exposure without invoking a destructive
endpoint. After root-cause de-duplication it re-verified eight original items,
added six distinct security/integrity findings (`CD-SEC-14..19`), and added three
companion quality findings (`CD-QUAL-01..03`).
