---
name: feedback-no-skipped-tests
description: "Skipped/gated tests are an anti-pattern in this repo; the suite either runs a test or doesn't have it. No allowlist."
metadata: 
  node_type: memory
  type: feedback
  originSessionId: c8cfb77a-e80b-4c12-8e68-6924d9827a7e
---

In this repo, **never** write a test that can skip. No `@unittest.skipUnless`, no `@unittest.skipIf`, no `raise unittest.SkipTest`, no env-gated "only when CRATEDIGGER_REAL_X is set", no "fixtures must be generated first." If you write a test, it runs every `bash scripts/run_tests.sh` invocation in a freshly-cloned nix-shell. Period.

**Why:** On 2026-05-20 the suite was reporting `OK (skipped=56)`. Investigation showed all 56 had never run since the day they were written:
- 38 spectral-fixture tests gated on a directory created by `tests/fixtures/generate_fixtures.sh`, which itself hardcoded a source-FLAC path that didn't exist on any machine.
- 14 slskd-live tests gated on `SLSKD_HOST`, which is unset in the dev shell.
- A real-Redis slice gated on `CRATEDIGGER_REAL_REDIS_PORT` — nothing in the repo sets it.
- A `@unittest.skip("Obsolete after migration 021")` test that the codebase had moved past.

The user's framing: "I was obviously thinking of something when I asked you to build them, but we haven't used them." A test that has never run gates nothing; it's aspirational coverage that creates false confidence as the suite quietly shrinks by attrition while the headline number climbs. Pyright doesn't catch this — pyright passing only proves the test compiles.

**How to apply:**
- When tempted to gate a test on an env var, an external daemon, a file fixture, or "if sox is available" → STOP. nix-shell provides sox, ffmpeg, postgresql, slskd_api, music-tag. If the resource isn't in nix-shell, the test belongs as a manual procedure doc or a slice with a fake (`FakeSlskdAPI`, `FakePipelineDB`) — not as a `unittest.TestCase`.
- If a test legitimately needs a synthetic audio file, generate it in `setUpClass` with the nix-shell sox/ffmpeg binaries (see `tests/test_spectral_check.py::TestM4aFallback` and `TestArgvFlagConfusion` for the pattern). No external fixture directory.
- The audit test `tests/test_skip_audit.py` fails CI if any test file reintroduces a skip marker. No allowlist. If you genuinely need to remove a test, delete the method — don't decorate it.
- The codified rule lives in `CLAUDE.md` under "Skipped tests are an anti-pattern."

**Related drift fixed in the same pass:**
- `tests/conftest.py` is loaded twice (once as `conftest` via `tests/` on sys.path, once as `tests.conftest` via the package). Top-level side effects run twice, and the second run captures `slskd_api` AFTER other tests have mocked it. Import as `import conftest` everywhere (not `from tests import conftest`) to ensure a single module instance. See header comment in `test_slskd_http_pool.py::test_installed_slskd_client_shape_is_configurable_without_network`.
