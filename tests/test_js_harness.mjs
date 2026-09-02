/**
 * Tests for the shared JavaScript test harness (issue #1313, candidate 6).
 *
 * Every behaviour that only shows up in a REAL process — the exit code, the
 * `process.on('exit')` guard for a suite that never reaches `done()`, the
 * repo-relative identity in a marker — is driven through an actual `node`
 * child running an actual fixture file, never simulated in-process. Running
 * a second suite inside this one would register a second exit guard and
 * corrupt this file's own exit code, which is the same reason the fixtures
 * are files rather than closures.
 *
 * Test infrastructure, so deterministic only (`.claude/rules/code-quality.md`
 * § "Never property-test the test machinery").
 */

import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, rmdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  DONE_MARKER,
  FAILURE_MARKER,
  domStub,
  element,
  stubGlobals,
  suite,
} from './js_harness.mjs';

const t = suite(import.meta.url);

const TESTS_DIR = path.dirname(fileURLToPath(import.meta.url));
const HARNESS = path.join(TESTS_DIR, 'js_harness.mjs');

/**
 * Where an in-repo fixture goes: a subdirectory, not `tests/` itself.
 *
 * A fixture has to live inside the repository for the repo-relative identity
 * assertions to mean anything, and it is removed in a `finally` — but a
 * SIGKILL in that window leaks the file, and nothing reaps it. Under
 * `tests/` directly that leak is an untracked `.mjs` sitting among the real
 * suites (#1319's residual 6); in its own subdirectory it is one line of
 * `git status` naming a directory whose whole purpose is scratch.
 */
const FIXTURE_DIR = path.join(TESTS_DIR, '_harness_fixtures');

/**
 * Run `body` as a suite in a child process and return its result.
 *
 * `inRepo` places the fixture inside the repository so the repo-relative
 * identity is exercised; otherwise it goes to a scratch directory.
 */
function runFixture(body, { inRepo = false } = {}) {
  let dir;
  if (inRepo) {
    dir = FIXTURE_DIR;
    mkdirSync(dir, { recursive: true });
  } else {
    dir = mkdtempSync(path.join(tmpdir(), 'js-harness-'));
  }
  // `_`-prefixed as well: `run_js_checks.sh` globs `test_js_*.mjs`
  // non-recursively, so an in-repo fixture is already out of its reach on
  // two counts, and stays so if the glob is ever widened to one of them.
  const file = path.join(dir, `_js_harness_fixture_${process.pid}_${Math.random().toString(36).slice(2)}.mjs`);
  const source = `import { suite, stubGlobals, domStub, element } from ${JSON.stringify(HARNESS)};\n`
    + `const t = suite(import.meta.url);\n${body}\n`;
  writeFileSync(file, source);
  try {
    const child = spawnSync(process.execPath, [file], { encoding: 'utf8' });
    return {
      status: child.status,
      stdout: child.stdout,
      stderr: child.stderr,
      lines: child.stdout.split('\n'),
      markers: child.stdout.split('\n').filter(line => line.startsWith(`${FAILURE_MARKER}\t`)),
      file: path.relative(path.resolve(TESTS_DIR, '..'), file),
    };
  } finally {
    rmSync(file, { force: true });
    if (inRepo) {
      // Non-recursive on purpose: it succeeds once this run's last fixture
      // is gone and fails harmlessly (ENOTEMPTY) while a leaked one remains,
      // so the leak stays visible instead of being swept up by its own test.
      try { rmdirSync(dir); } catch { /* another fixture is still there */ }
    } else {
      rmSync(dir, { force: true, recursive: true });
    }
  }
}

// The assertions below are made THROUGH the harness, so a harness whose
// `record()` returned true unconditionally would report every one of them
// as passing — including the ones checking that a failure is reported.
// This one raw check, owing nothing to the harness, is what makes the rest
// falsifiable (independent review F2).
{
  const probe = runFixture("t.equal(1, 2, 'always fails');\nt.done();");
  if (probe.status !== 1 || probe.markers.length !== 1) {
    throw new Error(
      'js_harness is inert: a failing fixture exited '
        + `${probe.status} with ${probe.markers.length} markers, `
        + 'so every assertion in this file is unfalsifiable',
    );
  }
}

t.section('a green suite exits 0 and reports its tally');
{
  const run = runFixture("t.equal(1, 1, 'one'); t.ok(true, 'two'); t.done();");
  t.equal(run.status, 0, 'a suite with no failures exits 0');
  t.contains(run.stdout, '2 passed, 0 failed', 'the tally counts both assertions');
  t.contains(run.stdout, `${DONE_MARKER}\t`, 'the done marker is emitted');
  t.equal(run.markers.length, 0, 'no failure marker on a green suite');
}

t.section('a failing suite emits one marker per failed assertion');
{
  const run = runFixture(
    "t.section('sec');\n"
    + "t.equal(1, 2, 'first');\n"
    + "t.contains('abc', 'z', 'second');\n"
    + "t.ok(true, 'third');\n"
    + 't.done();',
    { inRepo: true },
  );
  t.equal(run.status, 1, 'any failed assertion makes the process exit 1');
  t.equal(run.markers.length, 2, 'exactly one marker per FAILED assertion, not one per file');
  t.equal(
    run.markers[0].split('\t')[1],
    `${run.file}::sec::first`,
    'identity is <repo-relative file>::<section>::<message>',
  );
  t.equal(run.markers[0].split('\t')[2], 'expected 2, got 1', 'detail carries the diff');
  t.equal(
    run.markers[1].split('\t')[1],
    `${run.file}::sec::second`,
    'the second failure gets its own identity',
  );
  t.contains(run.stdout, `${DONE_MARKER}\t${run.file}\t1\t2`, 'the done marker carries the tally');
}

t.section('every marker is exactly three tab-separated fields on one line');
{
  const run = runFixture(
    "t.equal('a\\tb\\nc', 'x\\ty\\nz', 'msg\\twith\\ttabs\\nand newline');\nt.done();",
  );
  t.equal(run.markers.length, 1, 'the embedded newline did not split the marker into two lines');
  t.equal(run.markers[0].split('\t').length, 3, 'tabs in message and detail are collapsed');
  t.contains(run.markers[0], 'msg with tabs and newline', 'the message survives, flattened');
}

t.section('a marker survives every character PYTHON calls a line break');
{
  // The reader is `scripts/phase_parsers/js_checks.py` iterating
  // `str.splitlines()`, which breaks on more than CR/LF. An assertion
  // message never passes through `show()`, so one of these reaches the
  // marker verbatim and yields a two-field line -- which the reader
  // REFUSES as malformed, replacing the real failure with a parser error
  // (independent review F4). JSON.stringify does not escape the three
  // above 0x20, so the JS-side test cannot catch itself.
  const breakers = '\\t\\n\\r\\v\\f\\x1c\\x1d\\x1e\\x85\\u2028\\u2029';
  const run = runFixture(
    `t.equal('a${breakers}b', 'x', 'msg${breakers}with breakers');\nt.done();`,
  );
  t.equal(run.markers.length, 1, 'the marker is still exactly one line');
  t.equal(run.markers[0].split('\t').length, 3, 'and still exactly three fields');
  t.contains(run.markers[0], 'msg with breakers', 'the message survives, flattened');
}

t.section('a long detail is truncated so one marker stays one readable line');
{
  const run = runFixture(
    `t.contains('${'x'.repeat(5000)}', 'needle', 'huge haystack');\nt.done();`,
  );
  const detail = run.markers[0].split('\t')[2];
  t.ok(detail.length <= 400, `detail is capped (was ${detail.length})`);
  t.contains(detail, '…', 'truncation is marked with an ellipsis');
}

t.section('a long identity is capped too, not just the detail');
{
  // The identity has its own, tighter cap; only the detail's was pinned, so
  // the identity's could be widened unnoticed (independent review, mutant
  // A16). Both halves of "one marker stays one readable line" are covered.
  const run = runFixture(
    `t.ok(false, '${'m'.repeat(1000)}');\nt.done();`,
  );
  const identity = run.markers[0].split('\t')[1];
  t.ok(identity.length <= 240, `identity is capped (was ${identity.length})`);
  t.contains(identity, '…', 'the identity truncation is marked too');
}

t.section('a suite that never reaches done() fails closed');
{
  const run = runFixture("t.equal(1, 1, 'ran'); // no done()", { inRepo: true });
  t.equal(run.status, 1, 'a forgotten done() cannot pass silently');
  t.equal(run.markers.length, 1, 'the exit guard emits exactly one marker');
  t.contains(
    run.markers[0],
    `${run.file}::suite never reached done()`,
    'the marker names the file and the reason',
  );
  t.excludes(run.stdout, DONE_MARKER, 'no done marker, so run_js_checks.sh adds its file-level finding');
}

t.section('a suite that throws before done() leaves no done marker');
{
  const run = runFixture("t.equal(1, 1, 'ran');\nthrow new Error('boom');\nt.done();");
  t.equal(run.status, 1, 'the throw propagates as a nonzero exit');
  t.excludes(run.stdout, DONE_MARKER, 'the crash is distinguishable from a clean finish');
  t.contains(run.stderr, 'boom', 'the real error still reaches stderr');
}

t.section('checker vocabulary — the passing side of every method');
{
  const run = runFixture(
    "t.ok(1, 'ok'); t.notOk(0, 'notOk');\n"
    + "t.equal('a', 'a', 'equal'); t.notEqual('a', 'b', 'notEqual');\n"
    + "t.deepEqual({x: [1]}, {x: [1]}, 'deepEqual');\n"
    + "t.contains('abc', 'b', 'contains'); t.excludes('abc', 'z', 'excludes');\n"
    + "t.match('abc', /b/, 'match'); t.notMatch('abc', /z/, 'notMatch');\n"
    + "t.throws(() => { throw new TypeError('x'); }, 'throws', TypeError);\n"
    + "t.pass('pass');\n"
    + "await t.rejects(Promise.reject(new RangeError('x')), 'rejects', RangeError);\n"
    + 't.done();',
  );
  t.equal(run.status, 0, 'every passing-side assertion passes');
  t.contains(run.stdout, '12 passed, 0 failed', 'all twelve counted exactly once');
}

t.section('checker vocabulary — the failing side of every method');
{
  const run = runFixture(
    "t.ok(0, 'ok'); t.notOk(1, 'notOk');\n"
    + "t.equal('a', 'b', 'equal'); t.notEqual('a', 'a', 'notEqual');\n"
    + "t.deepEqual({x: [1]}, {x: [2]}, 'deepEqual');\n"
    + "t.contains('abc', 'z', 'contains'); t.excludes('abc', 'b', 'excludes');\n"
    + "t.match('abc', /z/, 'match'); t.notMatch('abc', /b/, 'notMatch');\n"
    + "t.throws(() => {}, 'throwsNothing');\n"
    + "t.throws(() => { throw new TypeError('x'); }, 'throwsWrongClass', RangeError);\n"
    + "t.fail('explicit');\n"
    + "await t.rejects(Promise.resolve(1), 'rejectsNothing');\n"
    + "await t.rejects(Promise.reject(new TypeError('x')), 'rejectsWrongClass', RangeError);\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the failing side exits 1');
  t.equal(run.markers.length, 14, 'each of the fourteen failing assertions reports once');
  t.contains(run.stdout, '0 passed, 14 failed', 'none of them was miscounted as a pass');
  t.contains(run.stdout, 'expected a throw, none happened', 'throws() explains a missing throw');
  t.contains(run.stdout, 'expected a rejection, none happened', 'rejects() explains a missing rejection');
  t.contains(run.stdout, 'expected RangeError, got TypeError', 'the wrong error class is named');
}

t.section('contains and excludes refuse a non-string haystack');
{
  // Both used to coerce with String(haystack), which INVERTS an array:
  // measured, ['ab','cd'].includes('a') is false while
  // String(['ab','cd']).includes('a') is true. A mechanical sweep of the
  // remaining t.ok(x.includes(y)) sites would have turned array ones into
  // the opposite assertion, silently, which is why #1319 held that sweep
  // back until this guard existed. Running it then found five real array
  // sites and this refusal is what stopped them. One needle, both
  // directions of the inversion: with the old
  // coercion `contains` passed where the array says no, and `excludes`
  // failed where the array says yes. Verified by disabling the guard: the
  // fixture then reports 1 passed, 1 failed instead of two refusals.
  const run = runFixture(
    "t.contains(['ab', 'cd'], 'a', 'the array does not contain it');\n"
    + "t.excludes(['ab', 'cd'], 'a', 'the array really does exclude it');\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'an array haystack fails rather than coercing');
  t.equal(run.markers.length, 2, 'both methods refuse it');
  t.contains(run.stdout, 't.contains needs a string haystack, got array',
    'the refusal names the method and the type');
  t.contains(run.stdout, 't.excludes needs a string haystack, got array',
    'excludes refuses it too, rather than accusing a correct assertion');

  // Must still work: a string haystack, which is what every one of the
  // other suites' 1,485 call sites passes.
  const ok = runFixture(
    "t.contains('abc', 'b', 'a string haystack still matches');\n"
    + "t.excludes('abc', 'z', 'and still excludes');\n"
    + 't.done();',
  );
  t.equal(ok.status, 0, 'a string haystack is unaffected');

  // Anything else is refused by TYPE, not by a list of known-bad ones.
  const other = runFixture(
    "t.contains(12345, '234', 'a number is not a string either');\n"
    + 't.done();',
  );
  t.equal(other.status, 1, 'a number haystack is refused, not stringified');
  t.contains(other.stdout, 'got number', 'the refusal names the real type');
}

t.section('equal and notEqual are STRICT');
{
  // Nothing else distinguished `===` from `==`: degrading both to loose
  // equality left all 23 suites green (independent review, mutants A7 and
  // A17). `t.equal` is now the most-used assertion in the tree, replacing a
  // mix of strict and loose helpers, so this is the pin that fixes which
  // one it is.
  const run = runFixture(
    "t.equal(1, '1', 'a number is not its string');\n"
    + "t.equal('', 0, 'an empty string is not zero');\n"
    + "t.equal(null, undefined, 'null is not undefined');\n"
    + "t.notEqual(1, '1', 'notEqual is strict the same way');\n"
    + "t.notEqual(0, false, 'zero is not false');\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the three loose-equal pairs are refused');
  t.equal(run.markers.length, 3, 'exactly the three equal() cases fail');
  t.contains(run.stdout, '2 passed, 3 failed', 'both notEqual cases pass under strictness');
  t.contains(run.markers[0], 'expected "1", got 1', 'the detail distinguishes the string from the number');
}

t.section('deepEqual is structural, not a JSON string comparison');
{
  const run = runFixture(
    "t.deepEqual({ a: 1, b: 2 }, { b: 2, a: 1 }, 'key order does not matter');\n"
    + "t.deepEqual([1, [2, { c: 3 }]], [1, [2, { c: 3 }]], 'nested shapes compare');\n"
    + "t.deepEqual({ a: undefined }, {}, 'an undefined-valued key is NOT dropped');\n"
    + "t.deepEqual({ a: NaN }, { a: NaN }, 'NaN equals NaN');\n"
    + "t.deepEqual({ a: 1 }, { a: 1, b: 2 }, 'a missing key is a difference');\n"
    + "t.deepEqual([1, 2], { 0: 1, 1: 2 }, 'an array is not a plain object');\n"
    // Each of the four below was an unconstrained guard: dropping it left
    // every suite green (round-2 review, mutants M14/M15/M17/M19).
    + "t.deepEqual({ 0: 1, 1: 2 }, [1, 2], 'nor is a plain object an array');\n"
    + "t.deepEqual([1, 2], [1, 2, 3], 'a shorter array is a difference');\n"
    + "t.deepEqual([1, 2, 3], [1, 2], 'and so is a longer one');\n"
    + "t.deepEqual({ a: undefined }, { b: undefined }, 'same key COUNT is not the same keys');\n"
    + "t.deepEqual(null, { a: 1 }, 'null is not an object');\n"
    + "t.deepEqual({ a: 1 }, null, 'nor is an object null');\n"
    + 't.done();',
  );
  t.equal(run.markers.length, 9, 'exactly the nine genuine differences fail');
  t.contains(run.stdout, '3 passed, 9 failed', 'order-independence and NaN pass');
  t.equal(run.status, 1, 'and none of the nine crashed the suite instead of failing');
  t.contains(
    run.markers[0],
    'an undefined-valued key is NOT dropped',
    'JSON.stringify would have called these two equal',
  );
}

t.section('deepEqual compares cyclic structures instead of overflowing');
{
  // `assert.deepStrictEqual` -- the idiom browse and convergence came from
  // -- handles cycles; the first cut of `deepMatches` blew the stack on
  // one (round-2 review). A `RangeError` here would abort the whole suite,
  // so the third case proves the guard does not simply answer "equal" to
  // everything cyclic.
  const run = runFixture(
    'const a = { name: 1 }; a.self = a;\n'
    + 'const b = { name: 1 }; b.self = b;\n'
    + 'const c = { name: 2 }; c.self = c;\n'
    + "t.deepEqual(a, b, 'two identically-shaped cycles are equal');\n"
    + "t.deepEqual(a, c, 'cycles differing at a leaf are not');\n"
    + 'const d = [1]; d.push(d);\n'
    + 'const e = [1]; e.push(e);\n'
    + "t.deepEqual(d, e, 'cyclic arrays too');\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the one genuine difference fails');
  t.equal(run.markers.length, 1, 'and only it — no stack overflow, no false equality');
  t.contains(run.stdout, '2 passed, 1 failed', 'both matching cycles compare equal');
  t.excludes(run.stderr, 'RangeError', 'nothing overflowed the stack');
}

t.section('throws and rejects accept an error class OR a message regexp');
{
  const run = runFixture(
    "t.throws(() => { throw new TypeError('bad shape'); }, 'by class', TypeError);\n"
    + "t.throws(() => { throw new Error('bad shape'); }, 'by message', /bad shape/);\n"
    + "t.throws(() => { throw new Error('other'); }, 'wrong message', /bad shape/);\n"
    + "await t.rejects(Promise.reject(new Error('nope')), 'rejects by message', /nope/);\n"
    + "await t.rejects(Promise.reject(new Error('nope')), 'rejects wrong message', /yes/);\n"
    + 't.done();',
  );
  t.equal(run.markers.length, 2, 'only the two mismatching regexps fail');
  t.contains(run.stdout, '3 passed, 2 failed', 'class and matching-regexp forms pass');
  t.contains(
    run.markers[0],
    'expected a message matching /bad shape/, got "other"',
    'the detail names the pattern and the actual message',
  );
}

t.section('an assertion with no message fails closed, even when it would pass');
{
  // The pre-harness idiom allowed `assertEqual(a, b)` with no message.
  // An unnamed assertion has no identity, and identity is the whole point
  // of the per-assertion marker, so the harness refuses it — including on
  // the success side, where nothing else would ever notice.
  const run = runFixture(
    "t.equal(1, 1, 'named');\n"
    + 't.equal(1, 1);\n'
    + "t.ok(true, '');\n"
    + 't.pass();\n'
    + "t.throws(() => { throw new Error('x'); });\n"
    + 't.done();',
    { inRepo: true },
  );
  t.equal(run.status, 1, 'an unnamed assertion reds the suite even though its check passed');
  t.equal(run.markers.length, 4, 'each unnamed site is reported once');
  t.equal(
    run.markers[0].split('\t')[1],
    `${run.file}::<unnamed assertion>`,
    'the identity says what is wrong instead of naming `undefined`',
  );
  t.contains(run.markers[0], 'assertion has no message', 'the detail explains the refusal');
  t.contains(run.stdout, '1 passed, 4 failed', 'only the named assertion counted as a pass');
}

t.section('a suite that stubs console cannot swallow its own markers');
{
  // Suites legitimately stub console to assert what production logs;
  // tests/test_js_search_plan.mjs did, and had to hand-capture
  // console.error at module load to keep its own output.
  const run = runFixture(
    "console.log = () => {}; console.error = () => {};\n"
    + "globalThis.console = { log() {}, error() {}, warn() {} };\n"
    + "t.section('stubbed');\n"
    + "t.equal(1, 2, 'still reported');\n"
    + 't.done();',
  );
  t.equal(run.status, 1, 'the failure still fails the process');
  t.equal(run.markers.length, 1, 'the marker survived a fully replaced console');
  t.contains(run.markers[0], 'stubbed::still reported', 'the section header was tracked too');
  t.contains(run.stdout, DONE_MARKER, 'the done marker survived as well');
  t.contains(run.stderr, 'FAIL: still reported', 'the human-readable line survived');
}

t.section('an unlabelled assertion has no section segment');
{
  const run = runFixture("t.equal(1, 2, 'bare');\nt.done();", { inRepo: true });
  t.equal(
    run.markers[0].split('\t')[1],
    `${run.file}::bare`,
    'without section() the identity is <file>::<message>',
  );
}

t.section('stubGlobals restores exactly what was there before');
{
  const sentinel = { marker: 'original' };
  globalThis.__harnessPresent = sentinel;
  t.excludes(Object.keys(globalThis).join(','), '__harnessAbsent', 'the absent key really is absent first');

  const stubs = stubGlobals({ __harnessPresent: 'stub', __harnessAbsent: 'stub' });
  t.equal(globalThis.__harnessPresent, 'stub', 'an existing global is replaced');
  t.equal(globalThis.__harnessAbsent, 'stub', 'a new global is installed');

  stubs.restore();
  t.equal(globalThis.__harnessPresent, sentinel, 'the original value is restored by identity');
  t.equal(
    Object.prototype.hasOwnProperty.call(globalThis, '__harnessAbsent'),
    false,
    'a key that was absent is DELETED, not left as undefined',
  );
  delete globalThis.__harnessPresent;
}

t.section('stubGlobals restore is idempotent');
{
  globalThis.__harnessTwice = 'before';
  const stubs = stubGlobals({ __harnessTwice: 'after' });
  stubs.restore();
  globalThis.__harnessTwice = 'changed since';
  stubs.restore();
  t.equal(globalThis.__harnessTwice, 'changed since', 'a second restore does not re-apply a stale value');
  delete globalThis.__harnessTwice;
}

// Section scoping only exists in a real suite with real `section()` calls,
// and asserting it from inside THIS suite would mean stubbing a global here
// and reading it back after our own section boundary — which is the very
// boundary under test. So each of the four below runs a child fixture and
// reads what that child reported (issue #1346).

t.section('a stub installed in a section is released at the next section');
{
  const run = runFixture([
    "globalThis.__probe = 'baseline';",
    "t.section('one');",
    "stubGlobals({ __probe: 'section one' });",
    "t.equal(globalThis.__probe, 'section one', 'installed');",
    "t.section('two');",
    "t.equal(globalThis.__probe, 'baseline', 'released at the boundary');",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('a key absent before its section is deleted at the boundary');
{
  const run = runFixture([
    "t.section('one');",
    "stubGlobals({ __absentInSection: 'x' });",
    "t.section('two');",
    "t.equal(Object.prototype.hasOwnProperty.call(globalThis, '__absentInSection'),",
    "  false, 'an absent key is deleted, not left as undefined');",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('two stubs of one key in a section unwind to the pre-section value');
{
  const run = runFixture([
    "globalThis.__stack = 'base';",
    "t.section('one');",
    "stubGlobals({ __stack: 'first' });",
    "stubGlobals({ __stack: 'second' });",
    "t.equal(globalThis.__stack, 'second', 'the later stub wins');",
    "t.section('two');",
    "t.equal(globalThis.__stack, 'base', 'both unwind, newest first');",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('a stub installed before the first section lives until done()');
{
  // The release happens INSIDE `done()`, after the last assertion the
  // fixture can make, so the fixture checks it from an exit handler. The
  // harness registers its own guard first, at `suite()` time, so this one
  // runs second and sees a finished suite.
  // The LAST section's stub is released by `done()` too, and that half went
  // untested until PR #1352's mutant runner removed `releaseSectionStubs()`
  // from `done()` and watched the suite stay green. Both are checked here.
  const run = runFixture([
    "globalThis.__baseline = 'before';",
    "globalThis.__lastSection = 'before';",
    "stubGlobals({ __baseline: 'module scope' });",
    "t.section('one');",
    "t.equal(globalThis.__baseline, 'module scope', 'survives a boundary');",
    "t.section('two');",
    "t.equal(globalThis.__baseline, 'module scope', 'and the next one');",
    "stubGlobals({ __lastSection: 'last section' });",
    "t.equal(globalThis.__lastSection, 'last section', 'the last section stubs too');",
    'process.on(\'exit\', () => {',
    "  const released = globalThis.__baseline === 'before'",
    "    && globalThis.__lastSection === 'before';",
    "  process.stdout.write(released ? 'ALL_RELEASED\\n'",
    "    : `LEAKED baseline=${globalThis.__baseline} last=${globalThis.__lastSection}\\n`);",
    '});',
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.contains(run.stdout, 'ALL_RELEASED',
    'done() hands back the module-scope stub AND the last section\'s');
}

t.section('an explicit restore is not undone by the section boundary');
{
  const run = runFixture([
    "globalThis.__early = 'base';",
    "t.section('one');",
    "const g = stubGlobals({ __early: 'stub' });",
    'g.restore();',
    "globalThis.__early = 'set after restore';",
    "t.section('two');",
    "t.equal(globalThis.__early, 'set after restore',",
    "  'the boundary does not re-apply an already-restored stub');",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('restoring an outer stub hands back the one nested inside it');
{
  // The world the section above cannot reach, and the one that was
  // genuinely broken: a section stubs the same key twice, then restores the
  // OLDER handle. The younger handle's saved value is now stale, and a
  // boundary that faithfully re-installed it brought the first mock back
  // from the dead — after the block had explicitly disposed of it.
  // `test_js_release_actions.mjs` has this shape twice, and the leak was
  // found by PR #1352's reader, not by the section above.
  const run = runFixture([
    "globalThis.__nested = 'original';",
    "t.section('one');",
    "const outer = stubGlobals({ __nested: 'outer' });",
    "stubGlobals({ __nested: 'inner' });",
    "t.equal(globalThis.__nested, 'inner', 'the inner stub is installed');",
    'outer.restore();',
    "t.equal(globalThis.__nested, 'original',",
    "  'restoring the outer handle unwinds the inner one too');",
    "t.section('two');",
    "t.equal(globalThis.__nested, 'original',",
    "  'and the boundary does not resurrect either of them');",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('a second suite in one process is refused, not silently allowed');
{
  // The registries are module state, so two suites in one process share
  // them: the second one's `done()` would release the first's module-scope
  // stubs. The audit checks only that a file CALLS `suite(...)`.
  const run = runFixture([
    "import { suite as secondSuite } from " + JSON.stringify(HARNESS) + ";",
    'let refused = null;',
    'try {',
    '  secondSuite(import.meta.url);',
    '} catch (e) { refused = e.message; }',
    "t.ok(refused && refused.includes('called twice in one process'),",
    "  `a second suite() throws: ${refused}`);",
    't.done();',
  ].join('\n'));
  t.equal(run.markers.length, 0, 'the fixture reports no failure');
  t.equal(run.status, 0, 'the fixture exits clean');
}

t.section('domStub resolves seeded ids and nothing else');
{
  const button = element({ textContent: 'Go' });
  const doc = domStub({ 'my-btn': button });
  t.equal(doc.getElementById('my-btn'), button, 'a seeded id resolves to its element');
  t.equal(doc.getElementById('missing'), null, 'an unseeded id is null, not undefined');
  t.equal(doc.getElementById('toString'), null, 'an inherited Object key is not mistaken for an element');
  t.equal(doc.querySelector('.anything'), null, 'querySelector defaults to null');
  t.deepEqual(doc.querySelectorAll('.anything'), [], 'querySelectorAll defaults to empty');
}

t.section('domStub extras override the defaults');
{
  const doc = domStub({}, { querySelector: () => 'overridden', body: 'the body' });
  t.equal(doc.querySelector('x'), 'overridden', 'a bespoke querySelector wins');
  t.equal(doc.body, 'the body', 'an extra field is added');
  t.equal(doc.getElementById('x'), null, 'the id lookup still works');
}

t.section('element() gives the fields render code touches');
{
  const el = element();
  t.equal(el.textContent, '', 'textContent defaults to empty');
  t.equal(el.disabled, false, 'disabled defaults to false');
  t.equal(el.removed, false, 'removed starts false');
  el.remove();
  t.equal(el.removed, true, 'remove() flags the element');
  el.setAttribute('aria-disabled', 'true');
  t.equal(el.getAttribute('aria-disabled'), 'true', 'setAttribute round-trips through getAttribute');
  t.equal(el.getAttribute('never-set'), null, 'an unset attribute reads null');

  const seeded = element({ textContent: 'seed', extra: 1 });
  t.equal(seeded.textContent, 'seed', 'a seeded field overrides the default');
  t.equal(seeded.extra, 1, 'an unknown seeded field is kept');
}

t.section('element() tracks connection, children and focus');
{
  // `isConnected`, `children` and `focused` are three of the four fields the
  // factory was missing (`id` is the fourth), and their absence is why it had
  // no callers: every suite that needed a button hand-rolled them beside the
  // attribute map. `removed` was already here and is asserted alongside them
  // because `remove()` now writes both.
  const parent = element({ isConnected: true });
  const child = element();
  t.equal(child.isConnected, false, 'a fresh node is disconnected, as in a real DOM');
  t.equal(parent.appendChild(child), child, 'appendChild hands the child back');
  t.equal(child.isConnected, true, 'appendChild connects the child');
  t.deepEqual(parent.children, [child], 'and records it under the parent');

  child.focus();
  child.focus();
  t.equal(child.focused, 2, 'focus() counts its calls');

  child.remove();
  t.equal(child.removed, true, 'remove() flags the child');
  t.equal(child.isConnected, false, 'and disconnects it');
  t.deepEqual(parent.children, [child],
    'remove() flags the child rather than splicing it out of its parent');
}

t.section('element() attributes live beside the element, never on it');
{
  // The first version assigned `this[name] = value`, which is less faithful
  // than the hand-rolled `attributes`-Map stubs this factory replaces
  // (independent review F10): an attribute could clobber the element's own
  // method, and getAttribute answered a seeded FIELD instead of null.
  const el = element({ textContent: 'hello' });
  el.setAttribute('remove', 'not a function any more?');
  t.equal(typeof el.remove, 'function', 'an attribute cannot clobber a method');
  el.remove();
  t.equal(el.removed, true, 'and the method still works');

  t.equal(el.getAttribute('textContent'), null, 'a seeded FIELD is not an attribute');
  t.equal(el.textContent, 'hello', 'and the field itself is untouched');

  el.setAttribute('aria-busy', true);
  t.equal(el.getAttribute('aria-busy'), 'true', 'attribute values are strings, as in the DOM');
  t.equal(el.hasAttribute('aria-busy'), true, 'hasAttribute sees it');
  el.removeAttribute('aria-busy');
  t.equal(el.getAttribute('aria-busy'), null, 'removeAttribute clears it');
  t.equal(el.hasAttribute('aria-busy'), false, 'and hasAttribute agrees');
}

t.done();
