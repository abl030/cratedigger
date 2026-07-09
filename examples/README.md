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

The honest account of what each mirror buys you (and the degraded modes
without them) is in [`docs/mirrors.md`](../docs/mirrors.md).
