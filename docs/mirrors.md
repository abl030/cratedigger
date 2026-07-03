# Mirrors — system requirements and runbook

Cratedigger works out of the box against **public MusicBrainz** and with
**Discogs browse off**. The mirrors are what make it fast and complete;
this page is the honest account of what each one is, what you lose
without it, and how the operator's instances are stood up.

## TL;DR matrix

| Dependency | Without it | With it | Module option |
|---|---|---|---|
| MusicBrainz mirror | Works against public MB — functional but ~1 req/s | Production-speed matching (ratelimit 100) | `services.cratedigger.musicbrainz.apiBase` |
| Discogs mirror | Discogs **browse is off** (clear 503); MB browse unaffected | Discogs search/artist/master/release browse + numeric-ID validation | `services.cratedigger.discogs.apiBase` + `beets.discogsMirrorUrl` |
| LRCLIB instance | Lyrics fetched from public lrclib.net | Local lyrics lookups | `services.cratedigger.beets.lrclibUrl` |
| slskd | Nothing works — it's the Soulseek client | — | `services.slskd` exists in nixpkgs; point `slskd.hostUrl`/`apiKeyFile`/`downloadDir` at it |

## Public-MusicBrainz degraded mode (supported)

The stranger default (`musicbrainz.apiBase = "https://musicbrainz.org"`)
threads through all three consumers: `web/mb.py` (browse), `pipeline-cli`
lookups, and the rendered beets `musicbrainz.host/https/ratelimit`
(public ⇒ ratelimit 1).

The math to know: public MB allows ~1 request/second. A beets validation
of one album is a handful of requests; browsing is interactive-tolerable;
but anything that touches per-cycle album counts (large watch lists,
field resolution sweeps) multiplies against that 1 req/s. The pipeline's
oneshot has `TimeoutStartSec = 1h` — a large cycle against public MB can
brush against it. This mode is **supported-but-slow**: the search cadence
itself never changes (R20 — searching is slskd-side and unaffected), only
metadata lookups slow down.

## MusicBrainz mirror (the operator's setup)

The operator runs the official MusicBrainz mirror stack (musicbrainz-docker,
podman on doc2, serving `http://192.168.1.35:5200`):

- Follow <https://github.com/metabrainz/musicbrainz-docker>: postgres +
  the WS/2 web service + search indexes, plus live data replication.
- Replication needs a (free) MetaBrainz **replication token** —
  <https://metabrainz.org/supporters/account-type> — dropped into the
  stack's config; the mirror then stays hours-fresh.
- Size: plan ~100GB+ for the database + search indexes.
- Wire it up: `services.cratedigger.musicbrainz.apiBase = "http://<host>:5200"`.
  The module derives the beets side (host:port, plain http, ratelimit 100)
  from the same value.

**Endgame note:** the long-term plan is `mb-api` — a sibling Rust project
reimplementing the observed WS/2 subset (XML for musicbrainzngs/beets,
JSON for `web/mb.py`) against Discogs-style full-dump re-imports, with
golden-diff verification against the running mirror. When it lands it
replaces this, the hardest section of the runbook.

## Discogs mirror (mirror-required for browse)

`web/discogs.py` speaks the **Rust mirror's** endpoint shape
(`/api/search`, `/api/masters/<id>`, ...) and response schema. Public
api.discogs.com does not serve that API, so there is **no public
fallback**: without a mirror, Discogs browse returns a clear 503
mirror-required message and the web UI is MB-browse-only. This is a
deliberate stance, not a gap — a translation adapter for public Discogs
belongs to the mirror project, not cratedigger.

The mirror is the `discogs-api` repo (~19M releases, Rust JSON API over
PostgreSQL loaded from the monthly Discogs data dumps, running in an
nspawn container on doc2, served at `https://discogs.ablz.au`).
**Follow-up plan:** packaging it as a flake + generic NixOS module (the
same pattern as this repo's tier-2 work) lives in the discogs-api repo.

Wire it up: `services.cratedigger.discogs.apiBase` (browse) and
`services.cratedigger.beets.discogsMirrorUrl` (build-time patch of the
beets discogs plugin, so *imports* also hit the mirror). The beets
plugin additionally wants a Discogs user token via
`beets.discogsTokenFile` (issue #117 `*File` pattern); tokenless installs
get a placeholder that keeps plugin load clean — public-Discogs lookups
are then token-required.

## LRCLIB (optional)

The operator runs a local LRCLIB instance (`http://192.168.1.35:3300`).
`services.cratedigger.beets.lrclibUrl = "http://<host>:3300/api"`
build-time-patches the beets lyrics plugin at that base. Unset = public
lrclib.net (stock behaviour).

## slskd (required)

Not a mirror, but the other system requirement: cratedigger drives a
[slskd](https://github.com/slskd/slskd) instance. `services.slskd`
exists in nixpkgs; give cratedigger its URL, API key file, and download
directory (`services.cratedigger.slskd.{hostUrl,apiKeyFile,downloadDir}`).
