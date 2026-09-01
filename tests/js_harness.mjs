/**
 * The one JavaScript test harness (issue #1313, strong candidate 6).
 *
 * Before this module every `tests/test_js_*.mjs` file hand-rolled its own
 * harness: 23 files, two incompatible idioms (21 counting into
 * `passed`/`failed`, 2 throwing through `node:assert/strict`), ~500
 * duplicated lines of near-identical `assertEqual`/`assertContains`
 * definitions, and failure reporting at *file* granularity — the suite
 * coordinator learned only that `node tests/test_js_x.mjs` exited nonzero,
 * while the Python phase gets a per-test entry in the failure index.
 *
 * What this module owns:
 *
 *   - `suite(import.meta.url)` returns one checker with the whole
 *     assertion vocabulary. Every file uses the same names.
 *   - One exit path (`checker.done()`), which emits a
 *     `CRATEDIGGER_JS_FAILURE` marker per FAILED ASSERTION —
 *     `scripts/run_test_suite.py::_parse_failures` decodes each into its
 *     own indexed failure, owner and rerun command derived from the file
 *     half of the identity.
 *   - `stubGlobals()` / `domStub()` / `element()`, so the recurring
 *     "save the old global, install a stub, restore it" dance is written
 *     once instead of at hundreds of sites.
 *
 * Fail-closed by construction: a file that never reaches `done()` — it
 * threw, or the author simply forgot the call — is reported as a failure
 * by the `process.on('exit')` guard below. A test that does not run does
 * not exist (`.claude/rules/code-quality.md`).
 *
 * This is test infrastructure, so its own tests are deterministic only:
 * `tests/test_js_harness.mjs` (behaviour) and
 * `tests/test_js_suite_audit.py` (every JS suite really uses it).
 */

import { fileURLToPath } from 'node:url';
import path from 'node:path';

/** The marker prefix `scripts/run_test_suite.py::_parse_failures` decodes. */
export const FAILURE_MARKER = 'CRATEDIGGER_JS_FAILURE';

/**
 * Printed by `done()` once the single exit path has been reached.
 *
 * `scripts/run_js_checks.sh` uses its ABSENCE, not the failure markers,
 * to decide whether to add a file-level "this suite died" marker: a file
 * that failed three assertions and then crashed owes both reports, and
 * keying the fallback on the markers would swallow the crash.
 */
export const DONE_MARKER = 'CRATEDIGGER_JS_DONE';

/** Longest detail we put on one marker line; HTML haystacks get huge. */
const MAX_DETAIL = 400;

const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  '..',
);

/**
 * Stream writers captured at module load.
 *
 * Suites legitimately stub `console.error` (and could stub `console.log`) to
 * assert what production logs — `tests/test_js_search_plan.mjs` did exactly
 * that, and had to capture `console.error.bind(console)` at module load for
 * its own harness output to survive. Writing through the captured stream
 * methods instead means no suite can swallow a failure marker by stubbing a
 * global, whatever it replaces.
 */
const writeOut = process.stdout.write.bind(process.stdout);
const writeErr = process.stderr.write.bind(process.stderr);

/** Collapse tabs/newlines and cap length — one marker is one line. */
function oneLine(value, limit = MAX_DETAIL) {
  const flat = String(value).replace(/[\t\r\n]+/g, ' ');
  return flat.length > limit ? `${flat.slice(0, limit - 1)}…` : flat;
}

function show(value) {
  if (typeof value === 'string') return JSON.stringify(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

/**
 * One suite's checker.
 *
 * @param {string} moduleUrl - the calling file's `import.meta.url`.
 * @returns {any} the checker; call `done()` exactly once, last.
 */
export function suite(moduleUrl) {
  const file = path.relative(REPO_ROOT, fileURLToPath(moduleUrl));
  let passed = 0;
  let failed = 0;
  let section = '';
  let finished = false;

  function identityFor(message) {
    return section
      ? `${file}::${section}::${message}`
      : `${file}::${message}`;
  }

  function record(succeeded, message, detail) {
    if (succeeded) {
      passed += 1;
      return true;
    }
    failed += 1;
    const identity = oneLine(identityFor(message), 240);
    writeOut(`${FAILURE_MARKER}\t${identity}\t${oneLine(detail)}\n`);
    writeErr(`  FAIL: ${message} — ${oneLine(detail)}\n`);
    return false;
  }

  const checker = {
    /** Label the assertions that follow; also printed as a header. */
    section(label) {
      section = String(label);
      writeOut(`${section}\n`);
      return checker;
    },
    /** Drop back to unlabelled assertions. */
    endSection() {
      section = '';
      return checker;
    },
    ok(condition, message) {
      return record(Boolean(condition), message, `expected truthy, got ${show(condition)}`);
    },
    notOk(condition, message) {
      return record(!condition, message, `expected falsy, got ${show(condition)}`);
    },
    equal(actual, expected, message) {
      return record(
        actual === expected,
        message,
        `expected ${show(expected)}, got ${show(actual)}`,
      );
    },
    notEqual(actual, expected, message) {
      return record(
        actual !== expected,
        message,
        `expected anything but ${show(expected)}`,
      );
    },
    /** Structural equality by JSON shape — the historical `assertDeepEqual`. */
    deepEqual(actual, expected, message) {
      return record(
        JSON.stringify(actual) === JSON.stringify(expected),
        message,
        `expected ${show(expected)}, got ${show(actual)}`,
      );
    },
    contains(haystack, needle, message) {
      return record(
        String(haystack).includes(needle),
        message,
        `${show(needle)} not in ${show(haystack)}`,
      );
    },
    excludes(haystack, needle, message) {
      return record(
        !String(haystack).includes(needle),
        message,
        `${show(needle)} unexpectedly present in ${show(haystack)}`,
      );
    },
    match(value, pattern, message) {
      return record(
        pattern.test(String(value)),
        message,
        `${String(pattern)} did not match ${show(value)}`,
      );
    },
    notMatch(value, pattern, message) {
      return record(
        !pattern.test(String(value)),
        message,
        `${String(pattern)} unexpectedly matched ${show(value)}`,
      );
    },
    /**
     * Assert `fn()` throws. `errorClass` is optional; when given the thrown
     * value must be an instance of it.
     */
    throws(fn, message, errorClass) {
      let thrown;
      let didThrow = false;
      try {
        fn();
      } catch (error) {
        didThrow = true;
        thrown = error;
      }
      if (!didThrow) return record(false, message, 'expected a throw, none happened');
      if (errorClass && !(thrown instanceof errorClass)) {
        return record(
          false,
          message,
          `expected ${errorClass.name}, got ${thrown && thrown.constructor && thrown.constructor.name}`,
        );
      }
      passed += 1;
      return true;
    },
    /** Await `promiseOrFn` and assert it rejects. */
    async rejects(promiseOrFn, message, errorClass) {
      let thrown;
      let didThrow = false;
      try {
        await (typeof promiseOrFn === 'function' ? promiseOrFn() : promiseOrFn);
      } catch (error) {
        didThrow = true;
        thrown = error;
      }
      if (!didThrow) return record(false, message, 'expected a rejection, none happened');
      if (errorClass && !(thrown instanceof errorClass)) {
        return record(
          false,
          message,
          `expected ${errorClass.name}, got ${thrown && thrown.constructor && thrown.constructor.name}`,
        );
      }
      passed += 1;
      return true;
    },
    /** Record a failure directly — for a local helper doing its own check. */
    fail(message, detail = 'explicit failure') {
      return record(false, message, detail);
    },
    /** Record a pass directly — the counterpart of `fail()`. */
    pass() {
      passed += 1;
      return true;
    },
    get passed() {
      return passed;
    },
    get failed() {
      return failed;
    },
    /**
     * The single exit path. Prints the tally, emits `DONE_MARKER`, and sets
     * the process exit code — deliberately `process.exitCode` rather than
     * `process.exit()`, which discards stdout still queued on a pipe and
     * would drop the very markers this harness exists to emit.
     */
    done() {
      finished = true;
      writeOut(`\n${passed} passed, ${failed} failed\n`);
      writeOut(`${DONE_MARKER}\t${file}\t${passed}\t${failed}\n`);
      if (failed > 0) process.exitCode = 1;
      return checker;
    },
  };

  process.on('exit', () => {
    if (finished) return;
    writeOut(
      `${FAILURE_MARKER}\t${file}::suite never reached done()\t`
        + `the suite exited before checker.done(); ${passed} assertions had `
        + 'passed and any later ones never ran\n',
    );
    process.exitCode = 1;
  });

  return checker;
}

/**
 * Install global stubs and hand back a restorer.
 *
 * Replaces the hand-written dance this repository had at hundreds of
 * sites — save `globalThis.document`, assign a stub, remember to put the
 * old value back — which was also silently unbalanced in places, leaking
 * one test's DOM into the next.
 *
 * A key absent from `globalThis` before the call is DELETED on restore,
 * not set to `undefined`, so `typeof globalThis.x === 'undefined'` and
 * `'x' in globalThis` both read the same after as before.
 *
 * @param {Record<string, any>} values
 * @returns {{restore: () => void}}
 */
export function stubGlobals(values) {
  const saved = new Map();
  for (const key of Object.keys(values)) {
    saved.set(key, Object.prototype.hasOwnProperty.call(globalThis, key)
      ? { present: true, value: globalThis[key] }
      : { present: false, value: undefined });
    globalThis[key] = values[key];
  }
  return {
    restore() {
      for (const [key, previous] of saved) {
        if (previous.present) globalThis[key] = previous.value;
        else delete globalThis[key];
      }
      saved.clear();
    },
  };
}

/**
 * A minimal stand-in DOM element: the fields production render code
 * touches, plus whatever the caller seeds.
 *
 * @param {Object} [initial]
 * @returns {any}
 */
export function element(initial = {}) {
  return {
    textContent: '',
    innerHTML: '',
    className: '',
    disabled: false,
    style: {},
    dataset: {},
    removed: false,
    remove() { this.removed = true; },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    setAttribute(name, value) { this[name] = value; },
    getAttribute(name) { return Object.prototype.hasOwnProperty.call(this, name) ? this[name] : null; },
    addEventListener() {},
    insertAdjacentHTML() {},
    ...initial,
  };
}

/**
 * A `document` stub resolving ids from a plain map.
 *
 * `extra` is spread last, so any test needing a bespoke `querySelector`,
 * `createElement`, or `body` overrides the default without abandoning the
 * factory.
 *
 * @param {Record<string, any>} [elements]
 * @param {Object} [extra]
 * @returns {any}
 */
export function domStub(elements = {}, extra = {}) {
  return {
    getElementById(id) {
      return Object.prototype.hasOwnProperty.call(elements, id)
        ? elements[id]
        : null;
    },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    createElement() { return element(); },
    addEventListener() {},
    ...extra,
  };
}
