---
title: External Authorization Mode - Plan
type: feat
date: 2026-08-05
topic: external-authorization-mode
artifact_contract: ce-unified-plan/v1
artifact_readiness: requirements-only
product_contract_source: ce-brainstorm
execution: code
---

# External Authorization Mode - Plan

## Goal Capsule

- Objective: let an operator running their own identity provider front Cratedigger's web UI, and have the module describe that posture truthfully instead of calling it insecure.
- Product authority: issue #924, rescoped by the operator decision at https://github.com/abl030/cratedigger/issues/924#issuecomment-5186604597. Authority: "enforce is my end and not what I want ... i want cratedigger to be compatible with oidc providers."
- Open blockers: none. Every Outstanding Question below is deferred to planning.

---

## Product Contract

### Summary

Add a third mutually exclusive web gateway mode declaring that an established external component upstream owns browser authorization. The mode is behaviourally identical to `enableInsecure` except that it stops asserting authentication is disabled. Ship it with the operator deployment contract, a worked provider example, one module-VM scenario, and a fix for the expired-session failure mode.

### Problem Frame

Cratedigger already works behind an external authorizer. The module gateway listens on loopback only, so an operator's auth proxy is necessarily in front of it; the 401, the portal redirect, the session cookie, and the callback all resolve above Cratedigger, which never participates. The reviewed proxy-header allowlist already drops `Cookie`, `Authorization`, and provider identity headers such as `Remote-User` before the application, so provider identity cannot leak inward even by accident.

The only mode that expresses this posture is `web.enableInsecure`. Selecting it renders a non-dismissible footer reading "Authentication is disabled for this Cratedigger instance" and logs a `[CRITICAL]` warning at every start. Both statements are false in a deployment where an IdP sits in front. Three shipped documents — `docs/webui-primer.md`, `docs/nixos-module.md`, `docs/security-audit-2026-07-12.md` — additionally state that no external-auth story exists, so an operator evaluating Cratedigger reads "not supported" for a topology that works.

Separately, `web/js/` has no central fetch wrapper. Each module calls `fetch()` directly, and `fetch` follows the authorizer's redirect transparently, so an expired session arrives as `response.ok === true` carrying HTML. The subsequent `response.json()` throws into a local `catch` and the operator sees a generic load failure.

### Key Decisions

- KD1. Cratedigger does not enforce external authorization. (session-settled: user-directed — chosen over module-side `auth_request`: enforcement is the operator's responsibility at their own edge.) Governs R1, R2.
- KD2. No reachability or liveness probe of the authorizer. A probe that passes proves the authorizer answered, not that it authorized anything, so it would read as a guarantee the module cannot make. Governs R2.
- KD3. The credential-bridge design gate closes by not needing a bridge. The module makes no authorizer sub-request, so the browser session cookie never has to reach the gateway and its scope stays a property of the operator's IdP. Governs R2.
- KD4. The new mode is the existing insecure mode minus the false assertions, not a new perimeter. Governs R4, R6.
- KD5. Mode identity reuses the shipped gateway policy machinery rather than a parallel mechanism. Governs R5.

### Requirements

**Module option surface**

- R1. The module exposes a third web authorization mode declaring that an established external component upstream owns browser authorization.
- R2. External mode issues no request to the authorizer and carries no provider-specific configuration: no authorizer endpoint, no sign-in redirect template, no reserved path prefix, no token or JWT validation.
- R3. When the web UI is enabled, exactly one of Basic, insecure, and external mode is selectable, and no mode falls back to another.
- R4. External mode suppresses the insecure-authentication footer and the `[CRITICAL]` start warning, and records one start-time statement that an external component owns authorization.
- R5. External mode participates in the existing gateway policy descriptor, fingerprint marker, and reload receipt on the same terms as the other two modes.

**Preserved perimeter**

- R6. Every existing perimeter property holds unchanged in external mode: the loopback-only gateway listener, canonical-host reconstruction, the reviewed proxy-header allowlist, browser request-channel marking, Origin/Referer provenance, security headers, and the permissioned Unix socket.
- R7. `/healthz` remains the only anonymous exception at the module gateway with its exact bodyless `GET`/`HEAD` contract; whether the operator also exempts it at their own layer is their decision.
- R8. Provider identity, roles, cookies, and tokens never reach the Cratedigger application in any mode.

**Operator contract**

- R9. The module documents the deployment contract external mode depends on: the operator's auth proxy is on the same host as the gateway, forwards the canonical `Host`, and owns the anonymous-health decision.
- R10. A worked provider example ships under `examples/`, written against one concrete forward-auth provider without making the module aware of it.
- R11. The three shipped statements that no external-auth mode exists are corrected in the same change that adds the mode.

**Browser session expiry**

- R12. A request whose session expired at the operator's authorizer surfaces to the browser user as an expired session rather than as a generic data-load failure.

**Qualification**

- R13. The module VM proves external mode against a stub authorizer at the front proxy, covering both the authorized and the denied path.
- R14. The mode-selection invariant ships as a deterministic pin and a generated property over the mode domain, each with a known-bad self-test.

### Key Flows

- F1. Operator selects external mode
  - **Trigger:** Operator enables the web UI and selects external mode alongside their own front proxy.
  - **Steps:** Module asserts exactly one mode is selected; renders the gateway vhost with no Basic credential; writes the policy descriptor naming external mode; publishes the fingerprint marker; the application starts without the insecure warning and records that an external component owns authorization.
  - **Outcome:** Gateway serves on loopback with the full perimeter intact and no false authentication claim.
  - **Covers R1, R3, R4, R5.**

- F2. Authorized browser request
  - **Trigger:** A signed-in browser requests an application page or API route.
  - **Steps:** Operator's proxy authorizes the request and proxies it to the gateway with the canonical `Host`; the gateway rebuilds the reviewed header set, marks the browser channel, and forwards over the Unix socket; the application applies its provenance envelope and responds.
  - **Outcome:** The response is served and no provider identity, cookie, or token reached the application.
  - **Covers R6, R8, R9.**

- F3. Session expires mid-use
  - **Trigger:** The operator's session expires while the SPA is running.
  - **Steps:** The authorizer redirects the in-flight request to the portal; the browser follows it and receives HTML with a success status; the SPA detects that the response is not the application's own and surfaces an expired session.
  - **Outcome:** The operator is told to re-authenticate instead of seeing a data-load failure.
  - **Covers R12.**

- F4. Authorizer unavailable
  - **Trigger:** The operator's authorizer is down or misconfigured.
  - **Steps:** The operator's proxy decides the outcome. Cratedigger is not consulted and has no signal either way.
  - **Outcome:** A proxy that fails closed denies the request before Cratedigger is reached; a proxy that fails open serves the UI anonymously and Cratedigger does not detect it. This is the accepted consequence of KD1 and belongs in the operator contract, not in module behaviour.
  - **Covers R2, R9.**

### Acceptance Examples

- AE1. Mode exclusivity
  - **Covers R3.**
  - **Given** the web UI is enabled, **when** external mode is selected together with Basic or with explicit insecure mode, **then** evaluation fails with a message naming the conflict.
  - **Given** the web UI is enabled, **when** no mode is selected, **then** evaluation fails rather than defaulting to any mode.

- AE2. Inactive-mode residue
  - **Covers R3.**
  - **Given** the web UI is disabled, **when** external mode is left set, **then** evaluation fails on the same terms as leftover Basic or insecure settings.

- AE3. Honest assertions
  - **Covers R4.**
  - **Given** external mode is active, **when** the UI is served and the unit starts, **then** the served body contains no insecure footer and the journal contains no `[CRITICAL]` insecure-authentication warning, and exactly one start-time record states that an external component owns authorization.

- AE4. Anonymous health unchanged
  - **Covers R7.**
  - **Given** external mode is active, **when** an anonymous bodyless `GET /healthz` reaches the gateway, **then** it is served; **and** when any other method, any request body, or any other path attempts the exception, **then** it is refused on the existing terms.

- AE5. Identity never reaches the application
  - **Covers R8.**
  - **Given** external mode is active and the operator's proxy sets identity headers and a session cookie, **when** the request reaches the application, **then** no cookie, authorization header, or provider identity header is present.

- AE6. Composed authorization, both directions
  - **Covers R13.**
  - **Given** the VM runs the stub authorizer at the front proxy, **when** an authorized request is made, **then** the application and API respond; **when** an unauthorized request is made, **then** it is denied before reaching the gateway.

- AE7. Expired session
  - **Covers R12.**
  - **Given** the SPA is loaded and the session then expires, **when** a data fetch is redirected to the portal and resolves with a success status and an HTML body, **then** the UI reports an expired session rather than a load failure.

### Scope Boundaries

- Module-side `auth_request` or forward-auth, and any reachability or liveness probe of the authorizer.
- Any OIDC client, provider SDK, discovery document handling, or token/JWT validation inside Cratedigger.
- Authorizer endpoint, sign-in redirect template, or reserved path-prefix options.
- Identity, role, or group propagation into the application, and any per-user or per-route authorization.
- Bundling, packaging, or standing up an identity provider.
- The downstream doc2 cutover from explicit insecure mode. This work does not require it and does not block on it.

### Dependencies / Assumptions

- The operator's auth proxy runs on the same host as the gateway. This follows from the loopback-only listener and is already true of any reverse proxy fronting Cratedigger; R9 makes it explicit rather than implied.
- The operator's provider speaks a forward-auth-shaped topology, which Authelia, authentik, oauth2-proxy, and Pocket ID all do. The module takes no dependency on which one.
- Compatibility as described was established by reading `nix/module.nix`, `web/request_security.py`, `web/server.py`, `web/index_document.py`, and `nix/tests/module-vm.nix`. No live deployment behind an identity provider has been run, which is what R13 exists to establish.

### Outstanding Questions

**Deferred to Planning**

- Option shape: a dedicated boolean alongside `basicAuthFile` and `enableInsecure`, or a single mode enum replacing all three. The boolean composes with the shipped exclusivity assertion and changes no existing operator's configuration; the enum reads better but is a breaking rename of two published options.
- Whether R12 is fixed at each call site or behind one shared response helper. A shared helper is the only shape that makes the guarantee hold for call sites added later, but `web/js/` has no such seam today.
- Which provider the R10 example is written against. The example must not imply the module knows about it.
- Whether the R13 stub authorizer reuses the module VM's existing public-proxy vhost or adds a separate one.

### Sources / Research

- `nix/module.nix` — mode-exclusivity locals and assertions, gateway policy descriptor and fingerprint marker, reload receipt, the gateway vhost, the reviewed proxy-header allowlist, and the `/healthz` exception.
- `web/request_security.py`, `web/server.py` — request-channel contract, Origin/Referer provenance, Unix-socket adoption, and the insecure-mode warning.
- `web/index_document.py` — the insecure footer markers.
- `web/js/recents.js`, `web/js/browse.js` — representative direct `fetch()` call sites and their local `catch` behaviour, the shape R12 addresses.
- `nix/tests/module-vm.nix` — the TLS fixture, the public-proxy topology mirror, and the existing Basic and insecure scenarios the new scenario sits beside.
- `docs/plans/2026-07-28-002-fix-web-authentication-perimeter-plan.md` — the perimeter this extends, and the deferral language this plan supersedes.
- Issue #924 and its scope-correction comment; issues #663 and #921.
