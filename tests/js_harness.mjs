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
 *     `scripts/phase_parsers/js_checks.py` decodes each into its
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

/** The marker prefix `scripts/phase_parsers/js_checks.py` decodes. */
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

/**
 * Every character that would split one marker into two on the reading
 * side.
 *
 * The set is PYTHON's, not JavaScript's: the reader is
 * `scripts/phase_parsers/js_checks.py`, which iterates
 * `str.splitlines()` — and that also breaks on VT, FF, FS, GS, RS,
 * U+0085, U+2028 and U+2029, not just CR and LF. `JSON.stringify`
 * escapes the ones below 0x20 but not U+0085/U+2028/U+2029, and an
 * assertion MESSAGE never passes through `show()` at all, so it reaches
 * the marker verbatim. A message carrying one of these would otherwise
 * yield a two-field line and trip the reader's malformed-marker
 * refusal — replacing the real failure with a parser error. The tab is
 * here for the same reason one field down: it is the field separator.
 *
 * Written as escapes on purpose: U+2028 and U+2029 are JavaScript line
 * terminators too, so a literal one here ends the regex mid-source.
 */
const MARKER_LINE_BREAKERS = /[\t\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029]+/g;

/** Collapse anything line-breaking and cap length — one marker is one line. */
function oneLine(value, limit = MAX_DETAIL) {
  const flat = String(value).replace(MARKER_LINE_BREAKERS, ' ');
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
 * Structural equality, key-order independent.
 *
 * Deliberately NOT `JSON.stringify(a) === JSON.stringify(b)`, which the
 * pre-harness `test_js_wrong_matches.mjs::assertDeepEqual` used: that
 * compares key ORDER as well as content, so reordering two lines in a
 * production object literal — a semantically null edit — reds the test.
 * `browse` and `convergence` came from real `node:assert` `deepEqual` and
 * would have been newly coupled to production key order at five sites.
 * It also silently drops `undefined`-valued keys, folds `NaN`/`Infinity`
 * to `null`, and flattens `Set`/`Map` to `{}`.
 *
 * `Object.is` at the leaves, so `NaN` equals `NaN` and `-0` does not equal
 * `0` — the same strictness `assert.deepStrictEqual` applies.
 *
 * CYCLES are handled, because `assert.deepStrictEqual` — the idiom
 * `browse` and `convergence` came from — handles them, and the first cut
 * of this function did not: it overflowed the stack (measured:
 * `RangeError`). `pending` holds the pairs currently being compared
 * further UP the stack, and a pair already on it is assumed equal; the
 * entry is removed once that comparison finishes, so the assumption never
 * leaks sideways into a sibling. Two structures that differ only in where
 * they close their loop still differ at a leaf, so the assumption cannot
 * manufacture an equality on its own.
 */
function deepMatches(actual, expected, pending = new Map()) {
  if (Object.is(actual, expected)) return true;
  if (
    typeof actual !== 'object' || actual === null
    || typeof expected !== 'object' || expected === null
  ) return false;
  if (Array.isArray(actual) !== Array.isArray(expected)) return false;

  const against = pending.get(actual);
  if (against) {
    if (against.has(expected)) return true;
    against.add(expected);
  } else {
    pending.set(actual, new Set([expected]));
  }
  try {
    if (Array.isArray(actual)) {
      return actual.length === expected.length
        && actual.every(
          (item, index) => deepMatches(item, expected[index], pending),
        );
    }
    const actualKeys = Object.keys(actual);
    if (actualKeys.length !== Object.keys(expected).length) return false;
    return actualKeys.every(
      key => Object.prototype.hasOwnProperty.call(expected, key)
        && deepMatches(actual[key], expected[key], pending),
    );
  } finally {
    pending.get(actual).delete(expected);
  }
}

/**
 * Does `thrown` match `expected`?
 *
 * `expected` is an error class OR a `RegExp` tested against the message.
 * The regex form exists because the audit now forbids `node:assert`, whose
 * `assert.throws(fn, /re/)` was how this repository asserted a thrown
 * MESSAGE — without it the next author needing one hand-rolls a try/catch,
 * which is exactly what `tests/test_js_browse.mjs` had to do.
 */
function errorMatches(thrown, expected) {
  if (expected instanceof RegExp) {
    return expected.test(String(thrown && thrown.message));
  }
  return thrown instanceof expected;
}

/**
 * Refuse a non-string haystack for `contains`/`excludes`, or return `''`.
 *
 * These two used to read `String(haystack).includes(needle)`, which is
 * silently WRONG for an array: `['ab', 'cd'].includes('a')` is `false`,
 * while `String(['ab', 'cd']).includes('a')` is `true`. The sweep of the
 * remaining `t.ok(x.includes(y))` sites, and of the negated
 * `t.ok(!x.includes(y))` ones that map to `excludes`, would have flipped
 * every array one from correct-failing to passing with no test anywhere to
 * notice (issue #1319's residual 1, which is why that sweep waited for
 * this guard).
 *
 * Refusing the type makes such a site fail loudly instead, and it did.
 * Running the sweep converted 397 sites; this guard caught the five whose
 * haystack turned out to be an array, each one a recorded list of fetch
 * URLs in `test_js_pipeline.mjs` or `test_js_wrong_matches.mjs`. Those
 * five keep `t.ok(!x.includes(y), …)`. There is deliberately no
 * `t.includes` for collections: every one of them is negative, nothing
 * calls a positive one, and a harness method with no callers is how this
 * file grows shapes nobody reads.
 */
function nonStringHaystack(method, haystack) {
  if (typeof haystack === 'string') return '';
  const kind = Array.isArray(haystack) ? 'array' : typeof haystack;
  return `t.${method} needs a string haystack, got ${kind}: ${show(haystack)}`;
}

function errorMismatchDetail(thrown, expected) {
  const got = thrown && thrown.constructor && thrown.constructor.name;
  return expected instanceof RegExp
    ? `expected a message matching ${String(expected)}, got ${show(thrown && thrown.message)}`
    : `expected ${expected.name}, got ${got}`;
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
    // An assertion with no message has no identity, and identity is the
    // whole point of this harness: an unnamed failure lands in the suite
    // index as `<file>::undefined`, indistinguishable from every other
    // unnamed one in the same file. Fail closed rather than emit that.
    // The pre-harness idiom permitted it — `assertEqual(a, b)` with no
    // third argument — and the conversion of all 23 suites had to name
    // every such site (measured: 0 remain).
    if (typeof message !== 'string' || message === '') {
      failed += 1;
      const detailText = `assertion has no message; detail was ${oneLine(detail, 200)}`;
      writeOut(
        `${FAILURE_MARKER}\t${oneLine(identityFor('<unnamed assertion>'), 240)}`
          + `\t${detailText}\n`,
      );
      writeErr(`  FAIL: <unnamed assertion> — ${detailText}\n`);
      return false;
    }
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
    /**
     * Label the assertions that follow; also printed as a header.
     *
     * Also the boundary that hands back every global stubbed since the
     * previous one — see `stubGlobals`.
     */
    section(label) {
      releaseSectionStubs();
      sectionsHaveStarted = true;
      section = String(label);
      writeOut(`${section}\n`);
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
    /** Structural equality, key-order independent — see `deepMatches`. */
    deepEqual(actual, expected, message) {
      return record(
        deepMatches(actual, expected),
        message,
        `expected ${show(expected)}, got ${show(actual)}`,
      );
    },
    /**
     * Substring containment. The haystack must be a STRING — see
     * `nonStringHaystack`; an array is refused rather than stringified.
     */
    contains(haystack, needle, message) {
      const refusal = nonStringHaystack('contains', haystack);
      if (refusal) return record(false, message, refusal);
      return record(
        haystack.includes(needle),
        message,
        `${show(needle)} not in ${show(haystack)}`,
      );
    },
    /** The negation of `contains`, with the same string-only haystack rule. */
    excludes(haystack, needle, message) {
      const refusal = nonStringHaystack('excludes', haystack);
      if (refusal) return record(false, message, refusal);
      return record(
        !haystack.includes(needle),
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
     * Assert `fn()` throws. `expected` is optional and is either an error
     * class the thrown value must be an instance of, or a `RegExp` its
     * message must match.
     */
    throws(fn, message, expected) {
      let thrown;
      let didThrow = false;
      try {
        fn();
      } catch (error) {
        didThrow = true;
        thrown = error;
      }
      if (!didThrow) return record(false, message, 'expected a throw, none happened');
      if (expected && !errorMatches(thrown, expected)) {
        return record(false, message, errorMismatchDetail(thrown, expected));
      }
      return record(true, message, '');
    },
    /**
     * Await `promiseOrFn` and assert it rejects. `expected` takes the same
     * error class or `RegExp` as `throws`.
     *
     * The ONLY async method here. `await` it: an un-awaited call records
     * after `done()` has already set the exit code, so the marker is
     * emitted but the process still exits 0. The suite coordinator catches
     * that shape ("phase emitted failure markers but exited zero" ->
     * infrastructure-failure), so it is mislabelled rather than silent —
     * but it is still a failure reported as a broken tool.
     */
    async rejects(promiseOrFn, message, expected) {
      let thrown;
      let didThrow = false;
      try {
        await (typeof promiseOrFn === 'function' ? promiseOrFn() : promiseOrFn);
      } catch (error) {
        didThrow = true;
        thrown = error;
      }
      if (!didThrow) return record(false, message, 'expected a rejection, none happened');
      if (expected && !errorMatches(thrown, expected)) {
        return record(false, message, errorMismatchDetail(thrown, expected));
      }
      return record(true, message, '');
    },
    /** Record a failure directly — for a local helper doing its own check. */
    fail(message, detail = 'explicit failure') {
      return record(false, message, detail);
    },
    /**
     * Record a pass directly — the counterpart of `fail()`. Used where the
     * claim is "this call completed at all"; it still takes a name, so the
     * suite index can say which assertion it was.
     */
    pass(message) {
      return record(true, message, '');
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
      releaseSectionStubs();
      releaseModuleStubs();
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
 * Stubs installed since the last `section()`, newest last.
 *
 * One suite runs per process — `scripts/run_js_checks.sh` invokes `node`
 * once per file, and the harness's own fixtures spawn children rather than
 * building a second suite in-process — so one registry per module load is
 * one registry per suite.
 */
const sectionStubs = [];

/** Stubs installed before the first `section()`: the file's baseline world. */
const moduleStubs = [];

let sectionsHaveStarted = false;

function releaseStubs(handles) {
  // Newest first: a key stubbed twice unwinds through its intermediate
  // value back to the one that was there before the block started.
  for (let i = handles.length - 1; i >= 0; i -= 1) handles[i].restore();
  handles.length = 0;
}

function releaseSectionStubs() {
  releaseStubs(sectionStubs);
}

function releaseModuleStubs() {
  releaseStubs(moduleStubs);
}

/**
 * Install global stubs for the rest of this section.
 *
 * Replaces the hand-written dance this repository had at hundreds of
 * sites — save `globalThis.document`, assign a stub, remember to put the
 * old value back — which was also silently unbalanced in places, leaking
 * one test's DOM into the next.
 *
 * **The harness owns the restore, and the section is the scope.** Every
 * stub installed after a `section()` is handed back at the next
 * `section()`, and the last section's at `done()`. Stubs installed BEFORE
 * the first `section()` are the file's baseline world — the `document` a
 * module needs at evaluation time, say — and live until `done()`. The
 * returned `restore()` is still there for a block that wants its world
 * back earlier, and calling it twice is a no-op, so an explicit restore
 * and the automatic one cannot fight.
 *
 * Section scope is the point rather than a convenience. 104 sites across
 * six suites assigned `globalThis.fetch` bare and restored nothing, so a
 * section that installed no `fetch` of its own silently answered from the
 * previous section's mock (issue #1346). Making the boundary the harness's
 * job means a block cannot inherit by forgetting: it either installs its
 * own or reads whatever was there before the file started, which fails
 * visibly.
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
  const handle = {
    restore() {
      for (const [key, previous] of saved) {
        if (previous.present) globalThis[key] = previous.value;
        else delete globalThis[key];
      }
      saved.clear();
    },
  };
  (sectionsHaveStarted ? sectionStubs : moduleStubs).push(handle);
  return handle;
}

/**
 * A minimal stand-in DOM element: the fields production render code
 * touches, plus whatever the caller seeds.
 *
 * The field list is not a guess. It is what the suites hand-rolled in common
 * before adopting this factory (`test_js_release_actions.mjs`'s
 * `fakeDomElement`, `test_js_discography.mjs`'s local `element(tag)`, the
 * processing-lock button in `test_js_analysis.mjs` /
 * `test_js_long_tail_console.mjs`, `test_js_wrong_matches.mjs`'s
 * `fakeElement`, `test_js_util.mjs`'s session-overlay node and
 * `test_js_convergence.mjs`'s button), which is why `isConnected`,
 * `children`, `focused` and the attribute map live here rather than in seven
 * files. It is deliberately not their UNION: `insertAdjacentElement` stays
 * hand-rolled at 12 sites because each closes over that test's own
 * `inserted` array, and `append`, `tag`, `type`, `listeners` and `closest`
 * have one caller each. Seed them through `initial`.
 *
 * A fresh node is `isConnected: false`, as in a real DOM: it is connected
 * by `appendChild`, by a caller's `insertAdjacentElement`, or by seeding
 * the field. `remove()` reverses that and flags `removed`.
 *
 * @param {Object} [initial]
 * @returns {any}
 */
export function element(initial = {}) {
  // Attributes live in their own map, NOT as fields on the element. The
  // earlier version assigned `this[name] = value`, so
  // `setAttribute('remove', …)` clobbered the element's own method and
  // `getAttribute('textContent')` answered `''` instead of `null` — less
  // faithful than the hand-rolled `attributes`-Map stubs this factory
  // exists to replace, which is a fake diverging from the real edge
  // (`.claude/rules/test-fidelity.md` Rule B in spirit).
  const attributes = new Map();
  return {
    id: '',
    textContent: '',
    innerHTML: '',
    className: '',
    disabled: false,
    style: {},
    dataset: {},
    children: [],
    isConnected: false,
    focused: 0,
    removed: false,
    focus() { this.focused += 1; },
    remove() {
      this.removed = true;
      this.isConnected = false;
    },
    appendChild(child) {
      child.isConnected = true;
      this.children.push(child);
      return child;
    },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    setAttribute(name, value) { attributes.set(name, String(value)); },
    getAttribute(name) { return attributes.has(name) ? attributes.get(name) : null; },
    removeAttribute(name) { attributes.delete(name); },
    hasAttribute(name) { return attributes.has(name); },
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
