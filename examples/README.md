# Examples — sample NixOS configs

These are **samples, not supported products** (cratedigger is tier-2:
reproducible and runnable by a competent NixOS stranger, with no
versioned-upgrade or support promises). Copy them into your own config
and adapt paths, hostnames, and secrets handling.

| File | What it stands up |
|---|---|
| [`cratedigger.nix`](cratedigger.nix) | Cratedigger itself — the minimal working consumer config. Start here; also shows the non-root + group-`users` setgid pattern for media-server integration. |
| [`musicbrainz-mirror.nix`](musicbrainz-mirror.nix) | A local MusicBrainz mirror (upstream musicbrainz-docker under podman). Optional — cratedigger works against public MB, just slower. |
| [`discogs-mirror.nix`](discogs-mirror.nix) | The Discogs mirror (Rust JSON API over PostgreSQL, loaded from the monthly CC0 dumps). Optional — without it, Discogs browse is off and MB browse carries the UI. |

What you always need besides cratedigger: **slskd** (`services.slskd`
exists in nixpkgs) and somewhere for music to live. PostgreSQL is
provisioned for you by `pipelineDb.createLocally = true`.

The sample selects the safe web default: whole-site Basic authentication. You
must provision its bcrypt `htpasswd` file as runtime secret material before the
first switch and put a public HTTPS reverse proxy in front of the module's
loopback gateway. Missing, invalid, or conflicting auth configuration fails
closed. Do not put plaintext or the bcrypt verifier in Nix source or a store
derivation. [`docs/nixos-module.md`](../docs/nixos-module.md#web-authentication-perimeter)
has the sops-backed creation/rotation procedure, exact file permissions,
anonymous `/healthz` contract, Unix-socket CLI authority, and rollback rules.

The only alternative currently exposed is deliberate
`web.enableInsecure = true`; it retains the gateway and request-security
envelope while emitting a `CRITICAL` startup warning and a persistent footer.
There is no external-auth/OIDC option yet—the required external-session
credential bridge is deferred, so the examples do not promise or configure
one.

The sample keeps `services.cratedigger.processingDir` under a root-owned
high-capacity parent and outside slskd's writable download tree. Run slskd and
Cratedigger under distinct identities; add Cratedigger only to the slskd
download-directory group.

The honest account of what each mirror buys you (and the degraded modes
without them) is in [`docs/mirrors.md`](../docs/mirrors.md).
