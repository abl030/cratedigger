/**
 * The one in-repo harness fixture, and it is TRACKED (issue #1394 item 2).
 *
 * `tests/test_js_harness.mjs` runs this as a child process to prove the
 * harness names a module by its repository-relative path. That claim needs a
 * module that really lives in the repository, so the fixture cannot go in a
 * scratch directory with the rest of them — but it must not be created and
 * deleted per run either. It used to be: the JavaScript phase wrote a
 * transient fixture here while the Python phase ran concurrently, and a Nix
 * evaluation copying the working tree died mid-walk with
 * "path .../tests/_harness_fixtures does not exist". A tracked file is in
 * the tree before any phase starts and stays there, so no walker can catch
 * it half-existing, and the git snapshot every Nix eval under `tests/` now
 * loads carries it like any other tracked file.
 *
 * It is deliberately not named `test_js_*.mjs`, and it sits in a
 * subdirectory of `tests/` rather than in `tests/` itself:
 * `scripts/run_js_checks.sh` and the audits over the JS suites all glob
 * `tests/test_js_*.mjs` non-recursively, so this is out of their reach on
 * two counts, and stays so if either is ever widened to one of them.
 *
 * Its body is fixed. Every case that varies the body runs from a scratch
 * directory outside the repository; the only thing this one is here to
 * exercise is the identity, so it emits one failing and one passing
 * assertion under a section and lets the caller read both markers.
 */

import { suite } from '../js_harness.mjs';

const t = suite(import.meta.url);

t.section('in repo');
t.equal(1, 2, 'reported from inside the repository');
t.ok(true, 'and a passing one for the tally');
t.done();
