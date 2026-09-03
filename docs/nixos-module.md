# NixOS Module

The upstream module lives in this repo at `nix/module.nix`, exposed via
`nixosModules.default` in `flake.nix`. It is generic and homelab-agnostic:
every secret is a `*File` path, the DB is a `dsn` string, and there are no
sops/nspawn or site-specific public-proxy assumptions. The module does own its
nginx authentication gateway, which defaults to loopback listeners; the
consumer supplies the HTTPS edge that forwards to it.

The flake export pins the module's package set to **Cratedigger's own
flake.lock**. The application environment is built from the nixpkgs revision
the suite ran against; the deployment-supplied Beets package must use that
package set's `python3`. This costs a second nixpkgs evaluation on the consumer
host. The escape hatch is `services.cratedigger.packageSet = pkgs;` (or another
package set), which also changes the Python identity the supplied Beets package
must use and forfeits the tested-closure guarantee.

> **Breaking interface:** the public module consumes an externally owned Beets
> runtime. A deployment using the removed `beets.package` or `beets.config`
> interface must supply the new runtime capability in the same change. There
> are no compatibility aliases.

The homelab deployment composes this upstream module with two downstream
modules. `~/nixosconfig/modules/nixos/services/beets.nix` owns the system Beets
package, config, state, secret include, storage readiness, and plain operator
command. `~/nixosconfig/modules/nixos/services/cratedigger.nix` imports the
upstream module and adds:

- the exact runtime capability exported by the sibling Beets owner
- sops-nix per-key secret materialization (`cratedigger-secrets-split` oneshot — see below)
- the nspawn PostgreSQL container for the pipeline DB
- the `homelab.localProxy.hosts` entry for `music.ablz.au`
- systemd `after`/`wants`/`restartTriggers` splicing in `container@cratedigger-db.service`

## Key options (full set in `nix/module.nix`)

| Option | Default | Purpose |
|---|---|---|
| `enable` | `false` | Master switch |
| `user` / `group` | `"cratedigger"` | Dedicated non-root service identity. Configure its supplementary access to slskd, external Beets storage, and runtime secrets. Root user/group identities are rejected. |
| `src` | `../.` | Path to cratedigger source tree. Defaults to this flake's repo root. |
| `packageSet` | cratedigger's own locked nixpkgs (via the flake export) | Package set for the runtime closure. Override = escape hatch, forfeits the tested-closure guarantee. |
| `beets.runtime.package` | required | Deployment-owned compatible Beets Python package. It must use `packageSet.python3`; every Cratedigger Beets consumer resolves this exact package. |
| `beets.runtime.configDir` | required | Absolute external immutable `BEETSDIR` containing the effective non-secret configuration. Cratedigger never renders or writes it. |
| `beets.runtime.expectedLibrary` | required | Absolute canonical external Beets SQLite database path. Must equal Beets' resolved `library`. |
| `beets.runtime.expectedDirectory` | required | Absolute canonical external Beets library root. Must equal Beets' resolved `directory`. |
| `beets.runtime.expectedStateFile` | required | Absolute, externally provisioned, persistent host-local Beets state file. Importer gets exact write capability; main, preview, and web get read-only access. |
| `beets.runtime.expectedSecretInclude` | required | Absolute designated runtime include. Its only admitted content is a non-empty `discogs.user_token` scalar. |
| `beets.runtime.readinessUnits` | `[]` | Deployment-owned units that must complete before guarded applications start, such as storage/state/secret provisioning. Long-running workers use `After=` + `Requires=`; the timer-owned main cycle uses `After=` + `Wants=` so a readiness restart cannot kill an active cycle. |
| `musicbrainz.apiBase` | `https://musicbrainz.org` | Cratedigger's MB origin for web, CLI, and pipeline lookups. The external Beets owner must configure its own corresponding `musicbrainz` policy. Public default is functional but ~1 req/s. |
| `discogs.apiBase` | `null` | Discogs mirror origin. Mirror-REQUIRED: unset ⇒ Discogs browse off with a 503 mirror-required message (public api.discogs.com does not serve this API shape). |
| `stateDir` | `/var/lib/cratedigger` | Mutable runtime state (lock, denylists, processing metadata). Application config is an immutable store file. |
| `processingDir` | `${stateDir}/processing` | Private `0700` Cratedigger-owned root: canonical albums and their same-filesystem failure quarantine live in `albums/`, bounded preview scratch in `preview/`. Must be an absolute normalized path (no trailing slash, no `.`/`..` components, no doubled slashes) and lexically disjoint from slskd's download tree. |
| `localImport.enable` | `false` | Enable the manual local-import lane (issue #1176): "here's a request ID, here's a folder on disk, import it." Off by default. `pipeline-cli import-local <request_id> <path>` / `POST /api/pipeline/import-local`. |
| `localImport.dir` | `null` | Absolute root the local-import lane may read from. Deliberately has NO working default even when enabled — see "Manual local-import lane" below. |
| `slskd.apiKeyFile` | (required) | Path to a file containing the raw slskd API key (one line). |
| `slskd.downloadDir` | (required) | Where slskd downloads land. When set, must be an absolute normalized path (no trailing slash, no `.`/`..` components, no doubled slashes). |
| `slskd.hostUrl` | `http://localhost:5030` | slskd HTTP base URL. |
| `pipelineDb.dsn` | `null` | PostgreSQL DSN. Required unless `createLocally`. |
| `pipelineDb.createLocally` | `false` | Provision local PostgreSQL: role + database named after `cfg.user`, unix-socket peer auth (no password material anywhere), socket DSN default, migrate unit ordered after `postgresql-setup.service` has provisioned the role and database. doc2 keeps `false` + its nspawn DSN. |
| `redis.{enable,host,port,maxmemory}` | enabled, `127.0.0.1:6379`, `2gb` | App-owned local Redis server for the pipeline peer cache and web metadata cache. Uses `allkeys-lru`. |
| `peerCache.{ttlSeconds,speedTtlSeconds,redisConnectTimeoutMs,redisOperationTimeoutMs}` | 7d, 24h, 200ms, 100ms | Redis TTL and timeout settings rendered into `[Peer Cache]`. |
| `beets.validation.{enable,distanceThreshold,stagingDir,trackingFile,verifiedLosslessTarget}` | sensible defaults | Beets validation config. |
| `web.enable` | `false` | Enable the Unix-only web backend and module-owned nginx gateway. The gateway defaults to host loopback only. Exactly one of `basicAuthFile`, `enableInsecure = true`, or `externalAuth = true` is then required. |
| `web.hostName` | `null` | Lowercase canonical public DNS hostname. Required when web is enabled; it defines the fixed `https://` browser origin and exact gateway vhost. IP literals are rejected. |
| `web.gatewayPort` | `8086` | Module-owned nginx gateway port. The HTTPS reverse proxy forwards here; this is not a Python application listener. |
| `web.gatewayAddresses` | IPv4 loopback plus IPv6 loopback when enabled | Non-empty nginx listener-address set for `web.gatewayPort`. Keep the default for a same-host proxy. A deployment may explicitly add a private bridge address for a sandboxed proxy, but then owns the firewall and network-perimeter restriction for that address. |
| `web.accessGroup` | `"cratedigger-web"` | Dedicated group authorized to connect to the web backend Unix socket. This grants complete HTTP/API authority, not Basic-password-file access or unrelated CLI authority. Known privileged or overlapping authority groups are rejected. |
| `web.basicAuthFile` | `null` | Absolute runtime `htpasswd` file outside `/nix/store`. Basic mode requires a root-owned, non-empty `root:<nginx-group>` `0440` target readable by nginx and denied to the application/non-nginx socket users. |
| `web.enableInsecure` | `false` | Explicitly disable browser authentication while retaining the gateway, Unix socket, canonical-origin checks, and all other request-security controls. Mutually exclusive with `basicAuthFile` and `externalAuth`. |
| `web.externalAuth` | `false` | Declare that a component you run in front of the gateway owns browser authorization as a whole-site allow/deny decision. Cratedigger performs no authorization and contacts no authorizer, so fail-closed behaviour is a property of your proxy. Mutually exclusive with `basicAuthFile` and `enableInsecure`. |
| `web.redis.{host,port}` | shared app Redis | Web metadata-cache connection; follows `services.cratedigger.redis` unless explicitly overridden. |
| `notifiers.plex.{enable,url,tokenFile,librarySectionId,pathMap}` | disabled | Plex notifier. |
| `notifiers.jellyfin.{enable,url,tokenFile,pathMap}` | disabled | Jellyfin notifier. Every import reports only its mapped final album path through `POST /Library/Media/Updated`; `pathMap` supplies Jellyfin's view of that path and enables the upgrade DateCreated pin. Cratedigger never calls a Jellyfin refresh endpoint (issue #1221, `docs/jellyfin-primer.md`). |
| `healthCheck.{enable,onFailureCommand}` | enabled, no recovery | Pre-cycle slskd healthcheck. `onFailureCommand` runs to recover (e.g. `systemctl restart slskd.service`). |
| `releaseSettings.*` / `searchSettings.*` / `downloadSettings.*` | application defaults | Pipeline tunables. See "Search loop tunables" below for the trio that caps the slskd search window. |
| `qualityRanks.*` | mirror of `QualityRankConfig.defaults()` | See docs/quality-ranks.md § "Tuning reference (Nix options)". |
| `timer.{enable,onBootSec,onUnitInactiveSec}` | 1s after exit | Cycle frequency. |
| `importer.enable` | `true` | Enable both long-lived queue workers: async preview and the serial importer. Disabled queues remain non-runnable. |
| `importer.previewWorkers` | `2` | Async preview worker concurrency when `importer.enable = true`. Must be at least 1. |
| `logging.{level,format,datefmt}` | INFO | Python logging config. |

## Search loop tunables

Three options under `services.cratedigger.searchSettings.*` control the slskd search window and the persisted-search-plans escalation ladder. Listed together here because they're easy to forget when triaging stuck releases.

| Option | Default | Maps to | Effect |
|--------|---------|---------|--------|
| `searchResponseLimit` | `1000` | slskd `responseLimit` | Caps peer responses per search. The slskd-api default is 100; popular albums returning more than 100 peers had their results truncated. 1000 covers ~99% of observed searches without triggering the cap. |
| `searchFileLimit` | `50000` | slskd `fileLimit` | Caps total files across all peer responses. The slskd-api default is 10000; popular multi-disc/OST/compilation searches (peers each holding 50+ tracks) fill 10000 in ~3 seconds and terminate the search early — sometimes before the right peer responds. 50000 lets the buffer run to the search timeout for these. |
| `searchEscalationThreshold` | `5` | cratedigger only | Number of repeated default slots the persisted-search-plans generator (`lib/search.py`, `SEARCH_PLAN_GENERATOR_ID`) emits at the head of each plan before stepping into `unwild`, optional `unwild_year`, and up to three track slots. The legacy `select_variant`/`search_attempts` ladder is gone — see [`docs/pipeline-db-schema.md`](pipeline-db-schema.md#persisted-search-plans-migration-014) for the new schema and [`docs/parallel-search.md`](parallel-search.md#plan-driven-execution-post-2026-05-cutover) for execution flow. |

**The 30s cycle floor is upstream.** `cfg.search_timeout` exists but slskd caps it at 30000ms; values above that are silently ignored. With response/file limits high enough that they rarely cap, every search runs the full 30s. The path to shorter cycles is changing the client (issue #196), not tuning these options.

## What the module does

1. Consumes `beets.runtime.package` and builds the Cratedigger Python
   environment around that exact deployment-owned Beets package. The package,
   operator CLI, effective config, catalog, library, state, and secrets remain
   outside the module's ownership. `nix/beets.nix` is an optional compatible
   package factory for consumers, not an implicit module owner.
2. Wraps `cratedigger.py` / `pipeline_cli.py` / `migrate_db.py` / `scripts/importer.py` / `scripts/import_preview_worker.py` / `web/server.py` in shell scripts with ffmpeg, sox, mp3val, flac in PATH. The installed `pipeline-cli` wrapper fixes the twelve API-backed mutation commands to the permissioned web Unix socket; it exposes no production `--api-base` override. Direct commands such as `youtube-album` retain their database/mirror boundary and do not depend on `web.enable`. **A `web.enable = false` deployment has no execution path for those twelve commands**: the wrapper always passes the socket, and issue #1063 forbids a direct-DB fallback because the invoking operator identity cannot traverse the private `0700` processing tree those commands read and destroy. Such an installation must run them through a web instance or not at all.
3. Builds one immutable Nix-store application config containing the six
   external Beets authority fields, then pins its path in every wrapper. There
   is no mutable config renderer or fallback. The module does not render Beets
   config/secrets or provision the Beets catalog, root, or state file.
4. Adds deployment-owned `beets.runtime.readinessUnits` before each guarded
   application, exports the external immutable `BEETSDIR`, and grants role-
   specific state capability. Each top-level application performs the same
   intrinsic exactly-once contract admission; no systemd-only preflight exists.
5. Enables `redis-cratedigger.service` by default with bounded memory and
   `allkeys-lru`, then starts the ordinary pipeline units after their existing
   health, migration, and application-config boundaries.

The complete Beets authority, hard-versus-warning validation behavior, token
schema, mutation lanes, and trusted plain-`beet` operating boundary live in
[`docs/beets-primer.md`](beets-primer.md). The full external NixOS composition
is in [`examples/cratedigger.nix`](../examples/cratedigger.nix).

## Web authentication perimeter

The production web path has three separate listeners/authorities:

```text
browser
  -> https://music.example.net (operator-owned DNS, ACME, and TLS proxy)
  -> <web.gatewayAddresses>:<web.gatewayPort> (module-owned nginx auth gateway)
  -> /run/cratedigger-web/web.sock (systemd-owned, root:<web.accessGroup> 0660)
  -> web/server.py (one inherited Unix listener; no production TCP listener)

installed pipeline-cli API-backed mutations
  -> /run/cratedigger-web/web.sock (web access group authority)

installed pipeline-cli youtube-album
  -> shared resolver service -> PostgreSQL + configured mirrors
     (independent of web.enable and the web socket)
```

This split is deliberate: the CLI and browser route share the resolver service
and its outcome vocabulary, not one transport authority. The browser POST
retains the authenticated, same-origin web perimeter; the local CLI retains its
database and mirror authority. Cratedigger does not add an always-running
control socket merely to unify those adapters. With the exported module's
default `web.enable = false` composition, `pipeline-cli` is still installed
while `cratedigger-web.service` and Cratedigger-prefixed systemd sockets are
absent. A seeded durable YouTube mapping therefore remains usable headlessly
without the web API. Only a non-refresh lookup by an already-cached
MusicBrainz release-group identifier can also avoid mirror and YouTube
transport. A Discogs lookup must consult the configured mirror first because
release IDs and master IDs share the integer namespace; after the mirror
establishes the master, the normal post-widen durable-cache read may return
from cache.

The outer HTTPS proxy and the default loopback gateway may be server blocks in
the same nginx process, but they remain distinct listeners. With the default
`web.gatewayAddresses`, configure the outer proxy to forward only to
`127.0.0.1:<web.gatewayPort>`. A sandboxed proxy may instead require an
explicit private bridge address; restrict that listener to the bridge at the
deployment firewall and do not add a routable host address. Never publish the
gateway port, expose the Unix socket, or recreate the retired Python port
`8085`.
`web.hostName` is the canonical public hostname in both modes; the application
uses exactly `https://<web.hostName>` for mutation provenance rather than
trusting `Host` or any forwarded-host header.

### The two current modes

When `web.enable = true`, module evaluation accepts exactly one mode:

1. **Basic:** set `web.basicAuthFile` and leave `enableInsecure = false`.
   nginx challenges the complete SPA, static assets, read APIs, route
   discovery, and mutation APIs.
2. **Explicit insecure:** set `web.enableInsecure = true` and leave
   `basicAuthFile = null`. This is a deliberate test/development escape hatch,
   not a default inferred from localhost or missing configuration.

3. **External authorization:** set `web.externalAuth = true` and leave both
   other settings unset. Use this when a reverse proxy you operate — an OIDC
   provider fronted by forward authentication, for example — authorizes every
   request before forwarding it to `gatewayPort`.

Missing mode, more than one mode, a missing/invalid canonical hostname,
inactive-mode residue, a store-backed Basic path, or overlapping authority
groups fail closed. Basic additionally requires a non-root application identity
distinct from nginx. No mode ever falls back to another.

### External authorization mode

Cratedigger performs no authorization in this mode. It sends no sub-request to
your authorizer and does not probe whether one is reachable, so selecting the
mode is an assertion you make about your own deployment rather than one the
module verifies. If the component in front fails open, the gateway is served
anonymously and Cratedigger cannot detect it. Fail-closed behaviour is a
property of your proxy configuration.

What the mode changes is honesty: the UI drops the insecure-authentication
footer and the service records that an external component owns authorization
instead of logging a `[CRITICAL]` warning that authentication is absent.

The deployment contract you are asserting:

- With default `web.gatewayAddresses`, your proxy runs on the same host and
  nothing can reach the loopback gateway from another machine. A sandboxed
  same-host proxy may instead use an explicitly configured private bridge
  listener; the deployment firewall must admit only that bridge.
- Your proxy forwards the canonical `web.hostName` as the `Host` header. A
  non-canonical Host is rejected by the module's own default vhost.
- You decide whether `/healthz` stays anonymous at your layer. The module's
  anonymous health exception applies to its own gateway; your proxy sees the
  same path and may authorize or exempt it as you choose.

Provider identity, roles, cookies, and tokens never reach the application: the
gateway rebuilds a reviewed header set, so an authorizer's `Remote-User`,
`Remote-Groups`, `Cookie`, and `Authorization` headers are dropped at the
boundary. This holds in every mode and is proven in the module VM against a
front proxy that injects them.

A worked forward-auth example is in
[`examples/external-auth-nginx.nix`](../examples/external-auth-nginx.nix).

One browser-visible consequence is worth knowing: when a session expires, your
authorizer answers the in-flight request. The UI detects that answer and shows
an expired-session prompt rather than a generic load failure.

### Basic mode and the runtime credential

A safe sops-nix shape is:

```nix
sops.secrets."cratedigger/htpasswd" = {
  # The encrypted source is repository data; the decrypted target is runtime
  # state. Never construct this value with pkgs.writeText/builtins.toFile.
  sopsFile = ./secrets.yaml;
  owner = "root";
  group = config.services.nginx.group;
  mode = "0440";
  # Same-path rotations must enter nginx's validation/HUP path without
  # stopping unrelated virtual hosts.
  reloadUnits = [ "nginx.service" ];
  restartUnits = [ ];
};

services.cratedigger = {
  user = "cratedigger";
  group = "users";
  web = {
    enable = true;
    hostName = "music.example.net";
    gatewayPort = 8086;
    basicAuthFile = config.sops.secrets."cratedigger/htpasswd".path;
  };
};

# Add only identities that need passwordless access to the complete local API.
users.users.operator.extraGroups = [ "cratedigger-web" ];
```

Generate a modern bcrypt entry in a private temporary directory. `htpasswd`
prompts for the password, so it never appears in argv or shell history:

```bash
umask 077
auth_work="$(mktemp -d)"
htpasswd -cB "$auth_work/htpasswd" operator
```

Import the complete one-line file into the encrypted sops value, then remove
the temporary directory. Treat the bcrypt verifier as a secret. Never use
`htpasswd -b`, put a plaintext password or verifier in Nix source, use nginx's
inline `basicAuth` attribute, or make a Nix path/string derivation containing
the file. An encrypted sops source may enter the store; the decrypted
`basicAuthFile` must remain a runtime path outside it.

Before nginx starts or reloads, the module resolves and validates the file:
the target must be non-empty, root-owned, exactly `root:<nginx-group> 0440`,
have no extended ACL, and live beneath root-owned ancestors that are not
group/other writable. nginx must be able to read it. The application user and
every non-nginx web access group member must not. Web access group membership
therefore authorizes the socket, not the password file. The web unit repeats
the root validation before each start, then runs a separate unreadability
preflight under its final merged systemd `User`, `Group`, and supplementary
groups; a downstream identity override that gains credential access fails the
application start instead of creating a Basic-auth bypass.

The module defaults `services.nginx.enableReload = true` and requires both that
setting and `systemd.services.nginx.restartIfChanged = true`. The first
authenticated enable or a service-identity change can therefore restart nginx
to acquire the dedicated socket group. Authentication-policy and same-path
secret changes keep the rendered `nginx.service` unit stable and run through
nginx's reload unit instead. Reload preparation clears Cratedigger readiness,
strictly parses the module-owned policy descriptor, validates the runtime
credential, and writes a root-only receipt containing the exact descriptor and
credential fingerprints. Readiness is published after nginx's config test and
HUP only when the descriptor and credential remain byte-identical to that
receipt. A failed validation leaves the existing nginx master and unrelated
virtual hosts running, but every Cratedigger gateway route returns `503` until
a valid reload republishes readiness.

Rotate atomically; never edit the live runtime file in place:

1. Create a complete replacement bcrypt `htpasswd` file in a private temporary
   directory, again using the prompting `htpasswd -cB` form.
2. Replace only the encrypted sops value in one edit, commit the encrypted
   transaction, and deploy a signed NixOS generation that materializes the new
   runtime secret and reloads nginx. The module validation must pass before
   Cratedigger serves with it; the nginx master and unrelated virtual hosts
   remain running.
3. Without putting either password in argv, a URL, or logs, use interactive
   `curl --user operator` requests to prove the replacement receives the
   expected status and the old credential receives `401`. Retain the signed
   deployment and active secret-generation receipt.
4. Delete the private temporary material only after the new credential and
   denial of the old credential are proven.

If materialization, permissions, validation, or nginx activation fails, stop.
On a first authenticated start, nginx does not start. On a later policy or
credential reload, Cratedigger returns `503` while the existing nginx master
continues serving unrelated virtual hosts. Do not switch to insecure mode to
finish a Basic deployment.

### Browser, header, and response isolation

The gateway disables wholesale request-header forwarding and reconstructs the
small application contract. It overwrites the request channel as `browser`,
sets the canonical Host, and forwards only the reviewed content framing/type,
`Accept`, `Range`, `Origin`, and `Referer` values. It does not relay Basic
`Authorization`, cookies, bearer/session tokens, client connection/framing
headers, forwarded identity, usernames, groups, roles, or a client-supplied
internal request marker.

Unsafe browser methods must provide at least one valid `Origin` or `Referer`;
every supplied signal must match the fixed HTTPS canonical origin. This check
runs before body reads and route dispatch. There is no cross-origin API
contract: wildcard CORS is absent, documents deny framing, and application
resources are marked same-origin.

### Anonymous health monitoring

The only anonymous route is exact `GET` or `HEAD` `/healthz` with no query,
through the canonical hostname. It returns a bare `204` and performs no
database, Beets, mirror, cache, or configuration read. Query strings, other
methods, alternate paths, IP-literal/wrong Host requests, and non-canonical
target shapes do not inherit the exception.

```bash
curl --silent --show-error --output /dev/null \
  --write-out '%{http_code}\n' https://music.example.net/healthz
```

Use that exact target for liveness monitoring. It is not readiness: migration
and service dependencies may still prevent the web process from starting.

### Explicit insecure mode

Insecure mode removes only nginx's Basic challenge. It keeps the module-owned
gateway on the configured addresses, Unix backend, canonical Host/origin, header reconstruction,
same-origin mutation checks, CORS removal, response isolation, request-channel
classification, and destructive intent gates. Every application start logs
`Authentication is disabled for this Cratedigger instance.` at `CRITICAL`, and
every rendered page shows that exact persistent footer notice. Basic mode logs
and renders neither warning.

### Web access group versus other CLI authority

`web.accessGroup` grants the ability to connect to the complete Unix HTTP API
and assert the local `cli` request channel. The installed wrapper uses that
authority only for the API-backed mutations enumerated in
`docs/debugging-cli.md`; it carries no Basic
credential and has no TCP fallback. Group membership is therefore
security-sensitive and must be explicit.

It does **not** authorize the rest of `pipeline-cli`. Database-backed commands
still need their PostgreSQL connection, quarantine and library operations need
their filesystem/Beets permissions, and commands that consume secrets still
need the relevant secret group/file access. Conversely, making a Nix-store
program non-executable does not protect it: store objects are ordinarily
readable and can be invoked through an interpreter. Enforce authority at the
socket, database, filesystem, Beets, and secret resources.

The module rejects `root`, `wheel`, the Cratedigger service/media group,
nginx's primary group, `cratedigger-ops`, `users`, and the configured Discogs
operator group as `web.accessGroup`. It cannot infer that an arbitrary
differently named existing group carries unrelated authority. Keep the default
dedicated group or choose a newly dedicated group, then add each trusted
operator explicitly with `users.users.<name>.extraGroups`.

### Troubleshooting and rollback

- A module assertion about “exactly one authentication mode” means none or more
  than one of `basicAuthFile`, `enableInsecure`, and `externalAuth` were
  selected. A “mutually exclusive” assertion names the exact pair that clashed.
- `Cratedigger Basic authentication validation failed` in the nginx journal
  names a runtime path, ownership, ACL, ancestry, nginx-read, or
  non-nginx-denial failure. Fix the secret deployment; do not weaken the mode.
- `Permission denied` from an installed API-backed CLI command means the caller
  lacks membership in `web.accessGroup` (a new login/session is normally
  required after adding the group). Add the operator to the dedicated
  `web.accessGroup`; do not substitute `root`, `wheel`, `cratedigger-ops`,
  `users`, or another group that already carries unrelated authority. The
  module rejects its known authority groups, but an arbitrary local group name
  still requires operator review.
- A browser mutation rejected for provenance must use the configured HTTPS
  hostname and send same-origin `Origin` or `Referer`; do not derive trust from
  request headers or relax the canonical origin.

Rollback must preserve the perimeter. Close the public Cratedigger vhost before
rolling to code/configuration that does not implement this gateway, or roll
back to a generation that retains Basic mode and its runtime secret. Never
point the public proxy at legacy port `8085`, expose the Unix socket, or use
`enableInsecure` as a production rollback. Credential rollback is a separate
signed sops transaction: restore the prior encrypted verifier, deploy it
through the same validation/reload path, and prove the displaced credential
is denied.

## Service identity + filesystem permissions

The module defaults to the dedicated `cratedigger:cratedigger` system
identity and rejects root names or resolved UID/GID 0 for every guarded
application. Add the account to the deployment-owned groups that provide
slskd, external Beets, and secret access; this retains permission
configurability without granting host-root authority to the pipeline.

### Private processing boundary

`processingDir` is deliberately separate from `slskd.downloadDir`: slskd is a
source authority, not a safe destination. The module creates the root,
`albums/`, `albums/failed_imports/`, and `preview/` as `0700` for the
Cratedigger identity; tmpfiles may age-clean only `preview/` children. Put a
large processing root
directly beneath a root-owned, non-group-writable parent. Do not run slskd as
the Cratedigger user and do not put processing beneath a parent writable by
slskd. The module rejects a relative path, a `.`/`..` component, a doubled
slash, a trailing slash, and lexical overlap for both `processingDir` and
`slskd.downloadDir` at evaluation time — normalizing both before comparing
them closes a gap where a `..` component could make the lexical disjointness
check miss two options naming the same physical tree — while the runtime also
refuses symlinked/unsafe roots.

The descriptor-verified publish into `albums/` is the trust transition:
Cratedigger may repair or normalize that owned working copy in place before
measurement and import. slskd and quarantine bytes remain untrusted and are
never passed to mutating media tools directly. A force/quarantine preview keeps
one private normalized action copy through Beets; its original path remains the
job's audit and recovery authority.

### Manual local-import lane (issue #1176)

`localImport.enable` / `localImport.dir` configure a lane for the case
"here's a request ID, here's a folder already on disk (a CD rip, a friend's
copy, a completed download that failed at materialization) — import it."
The lane always **copies** the named folder into private processing
scratch before running the existing preview → importer → quality gate →
beets chain; it never imports in place and never mutates or deletes the
operator's folder (see issue #111: converting in place could destroy the
source on a `downgrade` verdict, before copy-first made the folder
strictly read-only input).

Both options are deliberately redundant — `enable` (bool) and `dir` (path)
rather than one nullable path — because it reads more obviously to a human
that this is a conscious, two-part act. `dir` has **no working default**
even when `enable = true`: an installation that never names a directory has
no local-import surface at all. A build that instead FAILED until a
directory was named would just push operators toward a placeholder like
`/tmp`, manufacturing the very surface the option exists to gate — so this
option stays off (and buildable) with nothing configured, unlike most of
this module's other required paths (`slskd.downloadDir`,
`beets.runtime.expectedDirectory`, …), which fail the build when unset
precisely because THEY have no safe "just don't use it" reading.

`dir` is the ALLOWLIST root, checked at config time for being absolute,
normalized (no trailing slash, no `.`/`..` components — `isAbsoluteNormalizedPath`,
the same helper this module's other path options, including `stateDir` and
every `beets.runtime.*` path, already use), and not `/`.
Execution-time authority (`lib.fs_authority.open_configured_local_import_directory`)
re-checks that the configured root is absolute and not degenerate (`/`,
`//`, …) itself — a preflight is not authority, so a hand-built
`CratediggerConfig` reaching that function is never trusted to have
already been validated by this module. Normalization issues within an
otherwise-absolute root (a stray `.`/`..` component, an internal doubled
slash) surface later, inside the no-follow open itself, rather than at
this upfront check. Execution-time authority additionally narrows the
allowlist further: it refuses any candidate that resolves inside a
Cratedigger-owned subtree even when nested beneath the configured
`localImport.dir`:

- the WHOLE `processingDir` (not narrowed to its `albums/` child — the
  private `preview/` scratch is equally off-limits; a candidate under it
  was an unprotected gap in this PR's own first review round);
- the Beets validation staging directory (`beets.validation.stagingDir`),
  which — unlike the rest of this list — really can be unset: it is
  `nullOr` and required only when `beets.validation.enable`;
- `slskd.downloadDir`;
- the Beets library root (`beets.runtime.expectedDirectory` /
  `CratediggerConfig.beets_directory`) — always populated for a working
  module deployment (this option is REQUIRED, not merely "when
  configured": asserted non-null, absolute, normalized, and non-`/`), so
  this entry never observes an empty value on a real deployment; the
  empty-string filter in `local_import_owned_subtrees` exists for the
  staging-dir case above, not this one;
- the directory holding the Beets SQLite library DB, journals, import
  log, and harness audit (`dirname(beets.runtime.expectedLibrary)` /
  `CratediggerConfig.beets_library_db`'s parent) — mirrors the module's
  own `beetsLibraryAuthorityRoots` (`nix/module.nix`), which is
  `expectedDirectory` PLUS `dirOf expectedLibrary`, not `expectedDirectory`
  alone.

Cratedigger's own mutable `stateDir` is deliberately NOT on that list. A
`dirname(cfg.lock_file_path)`-derived entry was tried and reverted: for
the production shape `read_runtime_config()` actually builds — the shape
every real consumer this lane will join uses — `lock_file_path` resolves
under the immutable store config's own directory (`/nix/store/…`), not
`stateDir`, so the entry would have protected the wrong path in
production while looking correct in a hand-built test fixture. `stateDir`
holds no audio and this lane only ever reads, so pointing it there is a
footgun that produces an `empty_fileset` rejection at import time, not a
security exposure — not worth a config field or a derivation that lies
about what it protects.

A broad root such as `/mnt/virtio` can legitimately contain those trees as
siblings of a genuine import source, and a config-time check would have to
reject the whole broad root outright — that is why the narrowing lives at
execution time rather than as a module assertion. Pointing this lane at
the live library or at Cratedigger's own processing tree is the realistic
operator typo, and it is also where file ownership stops being unambiguous
(good-citizen doctrine, issue #571).

Sandbox interaction: the web, importer, AND preview-worker units all run
under `ProtectHome = true` AND `PrivateTmp = true` (this module's shared
`untrustedInputSandbox`), so a `localImport.dir` under `/home`, `/tmp`, or
`/var/tmp` — natural choices for this lane, and `/tmp` is literally the
placeholder this option's own no-default design exists to discourage (see
above) — would otherwise resolve EVERY candidate as missing the moment ANY
of the three tries to read it. The web unit is the lane's actual front
door: `enqueue_local_import` (called directly from
`web/routes/pipeline_mutations.py`) opens the root and the candidate path
before a job is even enqueued, and `pipeline-cli import-local` is a pure
HTTP relay to that same route — it never opens the path itself. PR3 (issue
#1176) closes the gap for all three: every one of them read-only-binds the
exact configured `dir` (`localImportReadOnlyPaths` in `nix/module.nix`,
the same missing-tolerant `-`-prefixed shape every other external bind in
this module already uses — `beetsConfigReadOnlyPaths`,
`beetsObserverReadOnlyPaths`, …), so it is visible inside the sandbox
regardless of which of those three roots (or an ordinary path) it names.
The bind is always read-only — this lane's copy worker only ever reads
from the operator's folder, never writes into it. A PR3 review round found
the web unit missing from this bind set: only the importer and
preview-worker were bound, so the front door itself was sandbox-blind and
a `dir` under any of those roots failed at enqueue-time with a 422/503
about a folder that plainly exists.

This lane deliberately reintroduces a caller-named-path input vocabulary —
the same SHAPE as the removed `manual-import` HTTP endpoint (finding
CD-SEC-03, `docs/security-audit-2026-07-12.md`), whose teeth were that
`import-preview` ran `mp3val -f` **in place** on any `.mp3` under a
caller-supplied directory. It avoids CD-SEC-03's mistakes rather than
"fixing" that already-remediated finding: copy-first is now guaranteed by
architecture (this lane never operates on the operator's folder in place),
and the remaining job — confinement — is placed at execution authority
rather than skipped or left to the input vocabulary, mirroring
`lib/force_import_service.py::enqueue_force_import`'s own doctrine that a
preflight is not authority: the eventual copy worker (PR3) will call
`open_configured_local_import_directory` itself, inside its own held
session, rather than trusting an authorization computed earlier and
carried across a queue boundary as a bare path string — the same shape
`enqueue_force_import`'s own docstring names for the quarantine roots.

The module renders `[Local Import] enabled` / `dir` into `config.ini`;
`lib.fs_authority.open_configured_local_import_directory` (PR2) resolves an
operator-named path through it, and the local-import lane itself (PR3 —
`lib/local_import_service.py`, `pipeline-cli import-local`, `POST
/api/pipeline/import-local`, and the `local_import` preview/import worker
branches) is the reader that reaches Beets. See `docs/rejection-routing.md`
§ "Local-import outcomes" and `docs/pipeline-db-schema.md`'s `job_type`
entry for the full dispatch shape.

### Startup write-probe

The container-entrypoint pattern (issue #1085): a unit whose required
directory is missing, unwritable, or wrongly owned fails at switch time,
loudly, instead of discovering it later one operation at a time — exactly
the class of defect the #570/#578 root → `group-users` cutover found by
hand (a tmpfiles `z`-rule that could not survive the ownership transition,
a preStart that needed the Discogs token readable). `lib/startup_write_probe.py`
is a **list of required paths per unit**, not a capability model, probed
once immediately after strict runtime/Beets configuration admission and
before any queue recovery, claim, DB mutation, or filesystem mutation:

- **Read authority** — a no-follow descriptor open plus an enumerate.
- **Write authority** — a safely named create → write → fsync → rename →
  unlink probe, with the artifact always removed. Every probed path must
  already exist — each one is either externally provisioned (the slskd
  share, the beets staging root) or created once by this module's own
  tmpfiles rules (`stateDir`, `processingDir`'s `albums`/`preview`
  children, `youtubeIngest.tempDir`); the probe never creates its own
  required directory, and it never writes inside an album directory
  (`processing/albums/<album>` stays an exact media manifest — invariant 9
  / issues #853/#859).
- **The configured processing tree is probed through the SAME strict
  private-root primitives every action-time writer uses**
  (`lib.fs_authority.open_private_processing_root` /
  `open_private_child_directory` — root owned by the service identity,
  mode exactly `0700`, no group/other-writable ancestor), never a generic
  descriptor open. A generic open only proves "can I open and write" — it
  is green on a root that drifted to `0750` or a child a bad chown left at
  `0770`, exactly the #570/#578 shape, and every subsequent materialization
  / preview retention / force-action write then fails one album at a time,
  which is the outcome this probe exists to eliminate. Quarantine *reads*
  (`open_configured_quarantine_directory`) stay on the generic primitive,
  matching that function's own deliberately looser action-time behaviour.
- **`beets.validation.stagingDir` is probed only when configured
  (non-empty).** It is unset only on a deployment with NEITHER
  `beets.validation.enable` NOR `youtubeIngest.enable` turned on — each
  flag independently forces `stagingDir` non-null via its own module
  assertion (`nix/module.nix`), and the rendered config value is
  unconditional either way. When it genuinely is unset, production code
  never reaches the staging branch (`lib.download_processing`) and
  `open_configured_quarantine_directory` already skips a non-absolute
  root — so it is never probed, never reported "missing". Every other
  required path is unconditional.
- **On failure** — the unit exits non-zero with a message naming the unit,
  the path, the operation, and the errno class. `cratedigger-importer`,
  `cratedigger-import-preview-worker`, `cratedigger-web`, and
  `cratedigger-youtube-ingest` are `Restart=on-failure`, so systemd retries
  automatically; `cratedigger.service` is timer-driven and the timer's next
  cycle retries. No new status, flag, or `*_required` marker is ever
  written — nothing is parked (invariant 11).

`cratedigger-unfindable.service` is **deliberately never gated** by this
probe. It has its own systemd unit specifically so the never-stop-searching
invariant is enforceable at the systemd level; refusing it because storage
is unavailable would violate that invariant at the one place it is
currently guaranteed.

`cratedigger-retag-census.service` (#1142) and
`cratedigger-library-completeness-census.service` (#1149) are likewise **never gated** by
this probe — it never calls `probe_startup_paths`/`web_required_paths` at
all. They touch only `cfg.stateDir` (their own `WorkingDirectory`, always
provisioned by the same `systemd.tmpfiles.rules` entry every other unit
relies on) and the external Beets authority (already validated by
`enforce_beets_startup` before any scan runs); a write failure there
surfaces directly as the unit's own non-zero exit
(`scripts/run_retag_divergence_census.py`'s `EXIT_RUN_FAILED`), with no
separate startup gate needed.

This probe is an earlier, louder check, never the only one — every
existing action-time authority check (the private-root/quarantine
descriptor checks above, the Beets startup contract) still runs on every
mutation, because mounts, ACLs, ownership, and modes can all change after
a unit has started.

**Which unit requires which path** — the fact an operator actually needs
when a unit refuses at startup:

| Unit | Read | Write |
|---|---|---|
| `cratedigger` | `slskd.downloadDir` | `stateDir`; `slskd.downloadDir` (disk reaper); `processingDir/albums` (private primitives); validation staging root (if configured) |
| `cratedigger-importer` | `slskd.downloadDir`, `processingDir/albums`, validation staging root (if configured) | `processingDir/albums` (private primitives — force-action reclaim); validation staging root (if configured) |
| `cratedigger-import-preview-worker` | `slskd.downloadDir`, `processingDir/albums`, validation staging root (if configured) | `stateDir` (CD-rip cache/spool); `processingDir` root, `processingDir/albums`, `processingDir/preview` (all private primitives) |
| `cratedigger-web` | `slskd.downloadDir`, `processingDir/albums`, validation staging root (if configured) | `stateDir` (CD-rip cache/spool); `processingDir/albums`, validation staging root (if configured, generic — never the private primitives, matching Wrong Matches/Bad Rip's own `shutil.rmtree`-based delete); `processingDir` root, `processingDir/preview` (private primitives) |
| `cratedigger-youtube-ingest` | — | `youtubeIngest.tempDir`; validation staging root (unconditionally required whenever this unit is enabled — the module's own assertion guarantees it is set) |
| `cratedigger-unfindable` | never gated | never gated |
| `cratedigger-retag-census` | never gated | never gated |
| `cratedigger-library-completeness-census` | never gated | never gated |

`cratedigger-web` deliberately excludes `slskd.downloadDir` from write:
web only ever deletes from its own `wrong_matches`/`failed_imports`
quarantine children there (never required to exist) and never needs
write authority on the share itself. The governing rule is to probe
only the authority a unit actually uses, not a blanket ban on ever
writing into the slskd share — `cratedigger` DOES require write there,
correctly, because its disk reaper
(`lib.slskd_transfers.reap_disk_orphans`) genuinely unlinks proven-owned
files and prunes emptied directories back up to that same root on every
cycle.

**Known constraint, recorded rather than fixed:** the shared
`open_directory_path` primitive opens every path component `O_NOFOLLOW`. A
single symlinked component in `slskd.downloadDir` or the validation staging
root (plausible on another installation, e.g. a `/mnt/music ->
/pool/music` bind-mount substitute) now permanently blocks every unit that
requires the path, where it previously only degraded the one operation that
resolved it. No live impact on this deployment; changing the shared
primitive's symlink posture is out of this issue's remit.

### The `permissions` plugin + `fix_library_modes`

The deployment-owned Beets config must enable the built-in `permissions`
plugin with `file = "0664"` and `dir = "02775"` (setgid + group-writable).
Cratedigger's startup contract admits that policy before automation. The
plugin's `art_set` listener (`fix_art`) fixes fetched-art mode on both initial
import and a manual plain-`beet fetchart` re-fetch. This exists because Beets'
own `fetchart` writes art via `mkstemp` (which forces `0600` regardless of
umask) and nothing else chmods it afterward — without the plugin, art lands
`0600` and a media server reading it as a different user throws
`UnauthorizedAccessException` (issue #570 defect 1).

`lib/permissions.py::fix_library_modes` is the post-import belt-and-suspenders pass: `LIBRARY_DIR_MODE = 0o2775`, applied recursively to the imported album/artist dirs and everything the plugin's per-item listener doesn't reach (empty/intermediate dirs beets creates along the way). `reset_umask()` sets the process umask to `0o002` (group-writable) at every pipeline entry point, since a unit's `UMask=0000` alone doesn't reliably survive the subprocess chain down to beets.

**`dir = 02775` (setgid) is load-bearing, not cosmetic.** Plain `0775` strips the setgid bit, so every child album dir beets creates underneath would stop inheriting the library's group — the group-inheritance layout below silently breaks the moment this gets "simplified" to `0775`.

### Granting external authority

The module auto-declares the configured system user and group. A Cratedigger
identity needs supplementary group membership for:

1. **The slskd download directory's group** — the reaper (`reap_disk_orphans`) deletes/moves in-flight downloads via directory-write permission, not file ownership, so it needs write access to that directory's group (typically slskd's own service group).
2. **The group that owns its runtime secrets** (`/run/cratedigger-secrets/*` — the raw slskd API key, notifier creds) — whichever secrets backend materializes these needs to make them group-readable by cratedigger's group, or add cratedigger's user to the group that owns them.

The pgpass `EnvironmentFile` for `pipelineDb.createLocally` needs no special handling: systemd (PID1, root) reads `EnvironmentFile=` before dropping privileges to `cfg.user`, so a non-root service user never has to read that file itself.

### The group-`users` setgid library layout

Give the library tree a shared consumer group — `users` (gid 100) is the conventional choice, since that's commonly what Jellyfin/Plex containers run as — with setgid directories (`2775`). New album/artist dirs then inherit the group automatically (the setgid bit above), and any gid-100 media server can both READ fetched art and WRITE NFO/artwork alongside the media. This is the #570 "group twin" fix: `root:music-import 0775` dirs (no setgid, root-owned) previously blocked media-server writes outright.

Provision the library roots with a setgid tmpfiles rule:

```nix
systemd.tmpfiles.rules = [
  "d /srv/music/library 2775 cratedigger users -"
];
```

For a tree that already exists, fix it once — this is an operator action, not committed config (`.claude/rules/scope.md`):

```bash
chgrp -R users /srv/music/library
find /srv/music/library -type d -exec chmod 2775 {} +
find /srv/music/library -type f -exec chmod 0664 {} +
```

### Caveat: a root-owned secret under a non-root state dir

If the deployment-owned designated secret include lives beneath a directory
whose owner changes from root to `cfg.user`, systemd-tmpfiles may refuse to
manage it with "unsafe path transition". Provision it from a root-owned runtime
secret directory instead. The final file must be readable by the admitted
service/operator group, and its only YAML content may be the non-empty
`discogs.user_token` scalar.

Materialize the fixed-schema include atomically from the scalar secret as
`root:<operator-group> 0440` in a root-owned runtime directory. Add only the
trusted librarian and required service identity to that group; do not make the
application user the secret owner or place the include beneath `stateDir`.

### Health check still runs as root

`healthCheck`'s `ExecStartPre` (`slskdHealthCheck`) is `+`-prefixed, so it always runs as root regardless of `services.cratedigger.user` — this is what lets `onFailureCommand` (e.g. `systemctl restart slskd.service`) keep working under a non-root service user. `preStartScript` stays unprefixed and only clears the singleton lock as the service user; application configuration is an immutable Nix-store file, not a mutable render step.

### Minimal service-identity snippet

```nix
users.users.cratedigger = {
  isSystemUser = true;
  group = "users";
  extraGroups = [ "slskd" "cratedigger-ops" ];  # download-dir group + secrets group
};

services.cratedigger = {
  user = "cratedigger";
  group = "users";
  # ... slskd / beets.runtime / web options unchanged
};

systemd.tmpfiles.rules = [
  "d /srv/music/library 2775 cratedigger users -"
];
```

See [`examples/cratedigger.nix`](../examples/cratedigger.nix) for the full worked example.

## Systemd units

- `cratedigger-db-migrate.service` — oneshot, `restartIfChanged = true`, `stopIfChanged = false`, `RemainAfterExit = true`. Runs the schema migrator on every `nixos-rebuild switch`. **`stopIfChanged = false` is load-bearing — see "A switch must be able to re-run the migrator" below.** The long-running workers (`cratedigger-web`, `cratedigger-importer`, `cratedigger-import-preview-worker`, `cratedigger-youtube-ingest`) `requires` it, so they cannot start against a **failed** migration — though note a `Requires=` on a `RemainAfterExit` oneshot is satisfied by the unit merely being active and therefore cannot force a re-run. `cratedigger.service` and `cratedigger-unfindable.service` deliberately do NOT — both are timer-driven with `restartIfChanged = false`, and this unit's `ExecStart` store path changes on every deploy, so a `requires` edge would propagate its every-switch restart as a SIGTERM to a mid-flight cycle; they use `wants`+`after` instead and gate on schema currency themselves at startup (`lib/migrator.py::assert_schema_current`) so a behind/missing schema still aborts them before any work runs. `cratedigger-retag-census.service` (#1142, below) has no relationship to this unit at all — it never touches the pipeline DB, so it neither requires, wants, nor gates on migration state.
  `cratedigger-library-completeness-census.service` (#1149) has the same
  deliberate non-relationship: it has no pipeline-DB dependency, so it
  neither requires, wants, nor gates on migration state.

- `redis-cratedigger.service` — app-owned Redis server for peer cache and web metadata cache. `cratedigger.service` and `cratedigger-web.service` want/after it, but do not require it; runtime Redis failures degrade to cold-cache behavior.
- `cratedigger.service` — oneshot pipeline run. `restartIfChanged = false` (the timer picks up new code on the next cycle). It orders after and wants external Beets readiness, but deliberately does not require it: restarting readiness must not terminate an active timer-owned cycle.
- `cratedigger.timer` — starts the next cycle after the previous oneshot exits
  (configurable via `timer.onUnitInactiveSec`).
- `cratedigger-importer.service` — long-running serial beets import worker. It
  claims queued import jobs after async preview marks durable candidate
  evidence as `evidence_ready`; historical/raw `would_import` rows are
  non-runnable display/audit data and are not claimable. `restartIfChanged =
  true` (a deploy switch restarts it — see the 2026-05-16 incident recorded
  in the unit's own Nix comment). Since issue #1089 it catches that ordinary
  SIGTERM and drains gracefully: stops claiming new jobs and lets its
  in-flight job (beets child included) finish before exiting, bounded by
  `TimeoutStopSec = "10min"` before systemd falls back to SIGKILL. A large
  virtiofs import can still exceed that bound (the #1089 RCA's own 51-track
  box set ran 26 minutes) — recovery-side crash-debris removal
  (`lib.automation_recovery_debris`) is that residual world's safety net,
  cleaning up a killed child's committed-but-unmoved Beets catalog row on
  the next startup/liveness-reprobe sweep.
- `cratedigger-import-preview-worker.service` — long-running async preview
  worker enabled with `importer.enable`. It starts after DB migrations,
  defaults to two worker loops, and runs validation/spectral/measurement
  preview outside the beets mutation lane.
- `cratedigger-unfindable.service` — oneshot, `Type=oneshot`, `restartIfChanged = false`, `TimeoutStartSec=5h` (raised from `2h` alongside the `DEFAULT_BATCH_SIZE` bump below — issue #1112), runs as `cfg.user`. Wraps `scripts/run_unfindable_detection.py` via the `cratedigger-unfindable` wrapper bin. `wants = ["cratedigger-db-migrate.service"]` (not `requires` — see the migrate unit's entry above). Unlike `cratedigger.service`, its `ExecStartPre` wires ONLY the optional `slskdHealthCheck` (when `healthCheck.enable = true`) — it does NOT share `cratedigger.service`'s `preStartScript` (that clears the main pipeline's singleton lock, which this unit never owns; `nix/tests/module-vm.nix` asserts `cratedigger-pipeline-prestart` is absent from `cratedigger-unfindable.service`). A slskd outage that's already down BEFORE the run starts fails the health check and the unit fast, before any probe fires. An outage that starts MID-RUN is a different case (issue #1090): the oneshot's own bounded per-probe submit retry and circuit breaker handle it, and `scripts/run_unfindable_detection.py::_process_batch` returns a distinct exit code — `0` for a fully classified run, `EXIT_INCOMPLETE_RUN=3` when the circuit breaker tripped after a sustained run of slskd submit failures (`systemctl status` then shows the unit as failed; no operator action needed, the untouched candidate tail rolls into the next daily run via the normal cadence), `EXIT_CONFIG_ABORT=2` for the pre-existing missing-config/behind-schema abort before any work runs. Lives in its own systemd unit, NOT inline in the main `cratedigger.service` loop, because R20 ("the system never stops searching") forbids the regular search cadence from being throttled by detection state. Implements PR3 U13 (`docs/plans/2026-05-25-001-feat-search-plan-iteration-2-plan.md`); the #1090 submit-resilience hardening is documented in `docs/search-plan-iter2-deploy.md` § "Post-iteration-2 hardening — submit resilience (issue #1090)". The upstream module sets `Environment="PIPELINE_DB_DSN=..."` only; the downstream wrapper must augment `serviceConfig.EnvironmentFile` with the sops `cratedigger-pgpass` path (same pattern the wrapper uses for `cratedigger.service`) — see `docs/search-plan-iter2-deploy.md` § "PR3 — Detection + telemetry" for the exact incantation and the 2026-05-26 first-deploy gotcha.
- `cratedigger-unfindable.timer` — `OnCalendar=daily`, `Persistent=true`, `RandomizedDelaySec=30min`. The 30-min jitter is purely local cron-collision avoidance (logrotate, postgres autovacuum on doc2); the single-operator install has no fleet to spread across. The daily fire processes `DEFAULT_BATCH_SIZE` (`lib/unfindable_detection_service.py`) rows per run with a ~7-day `PROBE_INTERVAL_DAYS` per-request cadence target. Raised 100 -> 240 (issue #1112, 2026-08-12): the wanted cohort had grown to ~1,300 rows, needing ~186 probes/day just to hold the backlog flat, which K=100 structurally could not sustain (observed due-backlog growing 624 -> 639 -> 685 over three days, oldest probe 15 days stale) — K=240 clears a ~1,300-row cohort inside the 7-day cadence window itself (`ceil(1300/240) = 6` days) with headroom for further cohort growth. Run health (cohort size, due backlog, batch outcomes, circuit-breaker trips) is persisted per run to `unfindable_run_metrics` (migration 077) and surfaced on the Pipeline dashboard's "Unfindable detection" card.
- `cratedigger-retag-census.service` (#1142) — oneshot, `Type=oneshot`,
  `restartIfChanged = false`, `TimeoutStartSec=30min`, runs as `cfg.user`.
  Wraps `scripts/run_retag_divergence_census.py` via the
  `cratedigger-retag-census` wrapper bin, admitting the runtime config the
  same explicit `--config`/`--runtime-dir` way `cratedigger-web` does (not
  the env-var-only shape `cratedigger-unfindable`/`pipeline-cli` use) so
  its published snapshot lands in the exact `cfg.stateDir` the dashboard
  route reads from. Runs the UNBOUNDED whole-library retag ``-W``
  divergence census (`lib/retag_divergence_audit.py`) and atomically
  publishes a snapshot (`lib/retag_divergence_census_snapshot.py`) — see
  `docs/debugging-cli.md` § "Daily whole-library census snapshot +
  dashboard card" for the full read/write contract and exit-code mapping.
  Beets-only: `after`/`wants` name only `cfg.beets.runtime.readinessUnits`,
  with NO `cratedigger-db-migrate.service` edge at all (unlike every other
  unit in this list) — it never opens the pipeline DB.
- `cratedigger-retag-census.timer` — `OnCalendar=daily`, `Persistent=true`,
  `RandomizedDelaySec=30min`, mirroring `cratedigger-unfindable.timer`'s
  own cadence/jitter rationale above.
- `cratedigger-library-completeness-census.service` (#1149) — a separate
  read-only daily oneshot that compares each exact Beets pressing against raw
  MusicBrainz/Discogs components, Beets items, and physical audio files. It
  atomically writes `library-completeness.json` in `cfg.stateDir`; the Pipeline
  Dashboard only reads that snapshot. It never opens the pipeline DB or mutates
  Beets/files. `TimeoutStartSec=4h` keeps public MusicBrainz (1 req/s) a
  supported fallback; mirror clients and the census use bounded concurrency.
- `cratedigger-library-completeness-census.timer` — `OnCalendar=daily`,
  `Persistent=true`, `RandomizedDelaySec=30min`, independent of the five-minute
  pipeline and the retag census.
- `cratedigger-library-completeness-census.path` — on-demand trigger: starts
  the same-named oneshot when `library-completeness-census.trigger` exists in
  `cfg.stateDir`. The web service (same user) writes that file for the
  dashboard's "Run census now" button and `pipeline-cli
  library-census-refresh`; the service's `ExecStartPre` removes it, so a
  request landing mid-run re-triggers one follow-up run. No sudo/polkit
  surface — stateDir ownership is the authority.
- `cratedigger-web.socket` — systemd-owned AF_UNIX listener at
  `/run/cratedigger-web/web.sock`, node `root:<web.accessGroup> 0660` beneath a
  separately managed `root:<web.accessGroup> 0750` directory.
- `cratedigger-web.service` — long-running Unix-only web backend. It requires
  the socket and adopts exactly its one inherited fd; a direct start activates
  the same socket rather than creating a bypass listener.
- `nginx.service` — when web is enabled, the module adds an exact-host gateway
  on `web.gatewayAddresses` plus a default-reject vhost, joins nginx only to the
  dedicated web access group, and validates Basic runtime material before
  start/reload. The module requires nginx reload support so policy and
  credential changes validate before HUP without stopping unrelated vhosts.

### A switch must be able to re-run the migrator (issue #1161)

`cratedigger-db-migrate.service` ships `stopIfChanged = false`. That is not
stylistic — it closes a silent schema-skip that produced a live outage on
2026-08-14, and the same shape can bite anything you build around these units.

**The mechanism.** With NixOS's default (`stopIfChanged = true`),
`switch-to-configuration` puts a changed unit in its *stop* list and its
*start* list. The migrate unit's stop job is ordered behind every `Requires=`
dependent's stop, because `Requires=` + `After=` means dependents must stop
first. While a slow worker drains — `cratedigger-importer` has
`KillMode = "mixed"` and a graceful drain that routinely takes seconds — that
stop job is still sitting in the queue. Any concurrent `systemctl start`
touching the unit (directly, or via a `Requires=` on it) enqueues a start job
in the default `replace` job mode, which **replaces the queued stop job**.
`RemainAfterExit = true` means the unit never left `active (exited)`, so the
replacement start hits `unit_start()`'s `-EALREADY`: `ExecStart` never forks,
and systemd writes *no journal entry at all*. The switch log still lists the
unit under both "stopping" and "starting"; the migration simply never runs,
and `systemctl show` keeps reporting `active`/`exited`/`success` from the
previous deploy.

`stopIfChanged = false` routes the unit to switch-to-configuration's *restart*
list instead. systemd's job-merge table collapses `JOB_START` into
`JOB_RESTART`, so a concurrent start can no longer swallow the re-run. The
restart phase also runs after the stop phase and before the start phase, so
the migration completes before the `Requires=` workers come back up.

**What this means for your configuration.**

- Anything you add that starts cratedigger units on a timer or watchdog can
  land inside a switch window. The 2026-08-14 trigger was a 60-second
  reconciler calling `systemctl --no-block start` on a list of cratedigger
  units, which pulled in the migrate unit through their own edges to it. Note
  `Wants=` pulls in a start job just as `Requires=` does, so
  `cratedigger.service` — which only `wants` the migrate unit — is equally
  capable of triggering this.
  Guard such a reconciler against running during a rebuild, or accept that it
  will occasionally fight the switch over unit state. The module's own
  `stopIfChanged = false` protects the *migration*; it does not stop a
  reconciler from starting your other units on stale unit definitions if it
  fires before `switch-to-configuration`'s daemon-reload.
- If you write your own `RemainAfterExit` oneshot that other units
  `Requires=`, `Wants=`, or merely order themselves `After=`, give it
  `stopIfChanged = false` for the same reason. Ordering alone is enough to
  create the queue window: anything that must stop *after* other units makes
  its own stop job wait behind theirs. Whether that window is exploitable
  then depends only on whether something can start the unit concurrently.
- Do not rely on a `Requires=` edge to guarantee a dependency *ran*. It
  guarantees the dependency is **active**, which for a `RemainAfterExit`
  oneshot can mean "succeeded days ago". It blocks startup on a failed
  migration, never on a skipped one.
- When verifying a deploy, compare the migrate unit's `InvocationID` against a
  value captured before the switch. `ActiveState` / `SubState` / `Result` on a
  `RemainAfterExit` oneshot cannot distinguish a fresh run from a stale one.
  `scripts/verify_cratedigger_cycle.sh` implements exactly that check through
  its `capture-migrate` and `verify-migrate-ran <pre-switch-invocation>`
  subcommands.

`nix/tests/module-vm.nix` pins the rendered `X-StopIfChanged=false` and the
behaviour pair it depends on (a plain `start` is a silent no-op on the active
oneshot; a `restart` genuinely re-runs it).

### Untrusted-input service sandbox

Exactly four long-running units receive the shared systemd sandbox:
`cratedigger-web.service`, `cratedigger-importer.service`,
`cratedigger-import-preview-worker.service`, and
`cratedigger-youtube-ingest.service`. The timer-driven
`cratedigger.service`/`cratedigger-unfindable.service`/
`cratedigger-retag-census.service` (#1142)/
`cratedigger-library-completeness-census.service` (#1149) and the migration oneshot
deliberately remain outside this boundary.

Every sandboxed unit has `NoNewPrivileges=yes`, `PrivateTmp=yes`,
`ProtectSystem=strict`, `ProtectHome=yes`, and
`RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`. `SystemCallFilter` uses
systemd's portable `@system-service` allowlist. It intentionally includes the
ordinary `fchownat` operations used by owned file workflows; do not add a broad
negative syscall class without re-running
the module VM, because it can prevent an owned file mutation after the worker starts.

`ReadWritePaths` is derived from the configured authority roots and is exact:

| Unit | Writable paths |
|---|---|
| web | `stateDir`, `processingDir`, `slskd.downloadDir`, external Beets root/database parent for the explicit exact-delete lane, validation staging root |
| importer | web paths plus parent of `beets.validation.trackingFile` and the exact external Beets state file |
| import-preview-worker | `stateDir`, `processingDir`, `slskd.downloadDir` |
| youtube-ingest | `stateDir`, `youtubeIngest.tempDir`, validation staging root |

External Beets paths are emitted with systemd's `-` missing-path modifier, so
a temporarily absent state/config/library authority reaches the intrinsic
application admission check instead of failing during sandbox namespace setup.
The module rejects `/` for directory capabilities and a library database whose
parent is `/`, so no Beets mutation lane can gain a host-root write grant.

An optional path (`slskd.downloadDir`, validation staging/tracking) is omitted
when its option is `null`. `ReadWritePaths` only makes the named portion of the
otherwise read-only system namespace writable; it neither grants Unix
permissions nor makes a path visible in a downstream private mount namespace.
A downstream writable `BindPaths` entry is an independent mount grant and can
reopen its target even when the upstream `ReadWritePaths` list is narrower.
For a consumer using `TemporaryFileSystem=/mnt`, the recommended composition is
broad shared-tree visibility through `BindReadOnlyPaths`, followed by
`BindPaths` only for that unit's exact writable roots from the table above.
Those writable binds must be narrow and per-unit. The external Beets owner
provisions the database parent, library root, and state file; Cratedigger does
not create defaults for them. Do not bind an entire shared music or data parent
writable for every worker. Verify effective denial rather
than inferring confinement from the rendered property strings alone. The
upstream module VM proves the generic module boundary without downstream
writable binds.

All service phases inherit the sandbox, including downstream `ExecCondition`
and `ExecStartPre` commands, unless `+`-prefixed (which runs with full
privileges, bypassing the sandbox entirely -- see below). On doc2, all four
sandboxed units -- web,
importer, import-preview-worker, AND youtube-ingest -- run the metadata gate's
`ExecCondition` start-check and declare `/var/lib/cratedigger-metadata-gate`
in their `ReadWritePaths` (verified via `systemctl show <unit> -p
ReadWritePaths -p ExecCondition`). This root is separate from `stateDir`
(`/var/lib/cratedigger`) and is not part of the upstream module's own
writable-path derivation in the table above, so the downstream wrapper adds it
explicitly rather than relying on a generic grant. This is a descriptive fact
about what the units declare, not a proven causal necessity: the gate's own
`ExecCondition=+...` runs `+`-prefixed (full privileges, bypassing
`ReadWritePaths` and the rest of the sandbox), so this declaration has not
been shown to be what permits the gate's own writes.

## Sops + per-key secrets

sops-nix's `key = "..."` does NOT actually extract a single value from a multi-key dotenv file (it writes the whole `KEY=VALUE` envfile regardless — verified empirically; same gotcha is documented in `~/nixosconfig/modules/nixos/services/alerting.nix` for the gotify token). The upstream module wants raw values per file, so the homelab wrapper materializes them via a `cratedigger-secrets-split` oneshot at boot:

```nix
systemd.services.cratedigger-secrets-split = {
  before = ["cratedigger.service" "cratedigger-web.service" "cratedigger-db-migrate.service"];
  # Required, not cosmetic (#1172 item 4). A RemainAfterExit oneshot ordered
  # Before= the workers has its stop job queued behind theirs on a switch;
  # with the NixOS default it is in both the stop and start lists, so a
  # concurrent `systemctl start` replaces that queued stop, hits -EALREADY,
  # and skips ExecStart silently — leaving these files stale after a
  # rotation. See #1161 for the same shape on the migrate unit.
  stopIfChanged = false;
  serviceConfig.RemainAfterExit = true;
  serviceConfig.ExecStart = pkgs.writeShellScript "cratedigger-secrets-split" ''
    set -euo pipefail
    install -d -m 0700 /run/cratedigger-secrets
    for key in SOULARR_SLSKD_API_KEY PLEX_TOKEN JELLYFIN_TOKEN; do
      grep -m1 "^$key=" "${config.sops.secrets."soularr/env".path}" \
        | cut -d= -f2- | tr -d '\n' > "/run/cratedigger-secrets/$key"
      chmod 0400 "/run/cratedigger-secrets/$key"
    done
  '';
};
services.cratedigger.slskd.apiKeyFile = "/run/cratedigger-secrets/SOULARR_SLSKD_API_KEY";
# ... etc
```

If you don't use sops or have one key per encrypted file, skip the splitter and point `apiKeyFile` directly at the secret path.

## Flake outputs

```
github:abl030/cratedigger
├── packages.<system>.default          ← operator/automation CLI bundle (pipeline-cli, migrate, world-audit gate, Beets checker)
├── apps.<system>.pipeline-cli         ← `nix run github:abl030/cratedigger#pipeline-cli -- --help`
├── nixosModules.default              ← upstream NixOS module (pins packageSet to this flake's lock)
├── devShells.<system>.default         ← test/dev environment (same pinned nixpkgs)
├── checks.<system>.moduleVm           ← NixOS VM test (boots module against ephemeral postgres)
├── checks.<system>.jellyfinMetadataVm ← Jellyfin 10.11.11 tagged-metadata + DateCreated pin lifecycle VM
├── checks.<system>.packageSetPin      ← eval guard: default packageSet = own lock; override honoured
├── checks.<system>.runtimeSrcPin      ← eval guard: module uses the filtered runtime source
├── checks.<system>.moduleAssertions   ← eval guard: external Beets capability is required and compatible
├── checks.<system>.checkBeetsConfigPackageBoundary ← installed checker ignores hostile inherited PYTHONPATH
├── checks.<system>.beetsMirrorPatches ← beets mirror knobs patch/don't-patch as configured
└── checks.<system>.packageDefault     ← the CLI bundle builds (`nix run` stays green)
```

## Validating before deploy

The flake exposes separate NixOS VM checks for module wiring and the real
Jellyfin integration contract:

```bash
nix build .#checks.x86_64-linux.moduleVm
nix build .#checks.x86_64-linux.jellyfinMetadataVm
```

The module VM builds a guest-local Nix store image just in time for every run,
with an ephemeral writable overlay for its closure queries. This avoids routing
the test's Python- and Beets-heavy store reads through the host's 9p mount; the
image construction time is included in the check's wall clock.

This catches option-surface breakage, immutable runtime-capability wiring,
systemd dependency cycles, wrapper `PYTHONPATH` errors, and missing Python
dependencies. It does not exercise live slskd interaction or downloads. Run it
before any `nix/module.nix` change.

`jellyfinMetadataVm` boots the flake-pinned Jellyfin, invokes the production
targeted notifier against tagged FLAC fixtures, and proves metadata population,
scoped targeting, curated-field preservation, and the real PostgreSQL-backed
DateCreated capture/reconcile lifecycle. Run it for Jellyfin notifier, pin, or
flake-pinned Jellyfin changes.

For Redis peer-cache changes, verify with a paused timer and one manual cycle:

```bash
sudo systemctl stop cratedigger.timer
sudo nixos-rebuild switch --flake .#HOST
systemctl is-active redis-cratedigger.service
redis-cli -p 6379 CONFIG GET maxmemory-policy
systemctl show -p After -p Wants cratedigger.service cratedigger-web.service
wrapper=$(systemctl show -P ExecStart cratedigger.service | sed -n 's/.*path=\([^ ;]*\).*/\1/p')
runtime_config=$(grep -o '/nix/store/[^" ]*cratedigger-config.ini' "$wrapper")
grep -A8 '^\[Peer Cache\]' "$runtime_config"
sudo systemctl start cratedigger.service
journalctl -u cratedigger.service -n 80 --no-pager | grep 'Cratedigger cycle complete'
redis-cli -p 6379 --scan --pattern 'peer_*' | wc -l
sudo systemctl start cratedigger.timer
```

Expected output: Redis is `active`, `maxmemory-policy` is `allkeys-lru`,
both app units list `redis-cratedigger.service` in `After` and `Wants`, and
the immutable runtime config contains `[Peer Cache]` with the selected Redis
host, port, TTL, and timeout values. The first cycle may be cold; later cycle
summaries should show `cache_pos_hits`, `cache_neg_hits`, and `cache_misses`
moving while `cache_errors=0 cache_fuse_tripped=0 cache_write_errors=0`.

Stop and roll back if cache error counters are nonzero after Redis is active,
if Redis key growth is far above the number of browsed peer directories, or if
matching/download behavior regresses. The old
`/var/lib/cratedigger/cratedigger_cache.json` is no longer read or updated by
new code; keep it on disk only as a rollback aid after code rollback.

Rollback:

```bash
sudo systemctl stop cratedigger.timer cratedigger.service
sudo nixos-rebuild switch --flake .#HOST --rollback
# Optional when bad Redis writes are suspected:
redis-cli -p 6379 --scan --pattern 'peer_dir:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_dir_neg:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_speed:*' | xargs -r redis-cli -p 6379 DEL
redis-cli -p 6379 --scan --pattern 'peer_dir_count:*' | xargs -r redis-cli -p 6379 DEL
sudo systemctl start cratedigger.service
stat /var/lib/cratedigger/cratedigger_cache.json
sudo systemctl start cratedigger.timer
```

After deploy, verify the queue workers before assuming imports will drain:

```bash
systemctl status cratedigger-db-migrate cratedigger-import-preview-worker cratedigger-importer
journalctl -u cratedigger-import-preview-worker -u cratedigger-importer -n 100 --no-pager
```

Queued jobs should move from `preview_status='waiting'` to `evidence_ready` or
a terminal preview failure. The importer claims `evidence_ready` jobs, with
historical/raw `would_import` rows retained as non-runnable display/audit data.
If `importer.enable = false`, neither queue worker should exist; the operator
must restore the preview/evidence path before queueing beets-mutating work.

Rollback note: before starting pre-018 importer or preview-worker code, stop the
queue services and reset active `evidence_ready` rows to `waiting` so old code
re-previews them instead of treating a neutral readiness token as import
authority. Include `running` rows because a stopped new importer can leave a
claimed job with `preview_status='evidence_ready'`, which old startup recovery
would otherwise requeue without changing the preview token:

```sql
UPDATE import_jobs
SET status = 'queued',
    worker_id = NULL,
    started_at = NULL,
    heartbeat_at = NULL,
    preview_status = 'waiting',
    importable_at = NULL,
    updated_at = NOW()
WHERE status IN ('queued', 'running')
  AND preview_status = 'evidence_ready';
```
