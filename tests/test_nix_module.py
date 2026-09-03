"""Contract tests for nix/module.nix.

The Nix wrappers in ``nix/module.nix`` decide what environment
subprocesses (``beet``, ``import_one.py``, etc.) inherit. Historically,
leaks here have caused pipeline-wide failures that were hard to trace:

* 2026-04-21 ``cratedigger-web`` force-import path crashed on every
  post-import ``beet remove`` with ``ModuleNotFoundError: No module
  named 'msgspec'``. Root cause: the wrapper exported
  ``PYTHONPATH=${src}:${src}/lib:${src}/web:...`` which put
  ``lib/beets.py`` at sys.path top level as a bare ``beets`` module,
  shadowing the real beets PyPI package. The ``beet`` subprocess did
  ``from beets.ui import main`` → loaded our ``lib/beets.py`` → hit
  ``import msgspec`` (line 11) → ``ModuleNotFoundError`` because the
  beet-wrapped Python doesn't carry msgspec. The accumulated effect
  was three split-brain rows for one MBID (Unter Null "Sick Fuck"
  request 1748).

These grep-based contracts are cheap to write and catch the whole
class of "an export in module.nix leaked into a subprocess and broke
something five layers away". They run inside the Python suite because
most invariants only need a source-level check. The state-directory authority
boundary is the exception: it has a known-bad ``nix eval`` pin because the
failure depends on Nix option assertion evaluation.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import unittest
from pathlib import Path
from typing import ClassVar

from tests._source_pins import strip_line_comments

REPO_ROOT = Path(__file__).resolve().parent.parent
MODULE_NIX = REPO_ROOT / "nix" / "module.nix"
FLAKE_NIX = REPO_ROOT / "flake.nix"
WRAPPERS_NIX = REPO_ROOT / "nix" / "wrappers.nix"
PACKAGE_NIX = REPO_ROOT / "nix" / "package.nix"
BEETS_NIX = REPO_ROOT / "nix" / "beets.nix"
SHELL_NIX = REPO_ROOT / "nix" / "shell.nix"
MODULE_VM_NIX = REPO_ROOT / "nix" / "tests" / "module-vm.nix"


def _strip_comment_lines(source: str) -> str:
    """``source`` with every full-line ``#`` comment removed.

    Thin alias for the shared reader so this module keeps one local name for
    the operation; the implementation is shared with every other pinning
    module (#1186) rather than duplicated here.
    """
    return strip_line_comments(source, ("#",))


def _nix_source(path: Path) -> str:
    """A Nix file's source with full-line comments removed.

    Every source pin in this module reads through here. Issue #1172: pinning
    against raw text means the single most likely way an attribute gets
    disabled — commenting it out — leaves the pin green, because the attribute
    is still present as comment text. That was confirmed with a planted mutant:
    ``# after = optional cfg.pipelineDb.createLocally "postgresql-setup.service";``
    left ``TestCreateLocallyContract`` passing, so the guard that keeps a
    stranger's first boot from racing NixOS role/database provisioning could be
    disabled with one ``#`` and nothing noticed.

    #1161 fixed this shape for the migrate-unit pins by stripping comments
    inside :func:`_attrset_block`; the whole-file pins were out of that PR's
    scope. Stripping once at read time covers both, so a pin cannot opt out.
    """
    return _strip_comment_lines(path.read_text(encoding="utf-8"))


# The only two places allowed to read a Nix file's raw source: the helper that
# does the stripping, and the mutant fixture below, which needs raw text in
# order to comment a line out in the first place.
_RAW_NIX_READ_ALLOWED = frozenset({
    "_nix_source",
    "test_commenting_out_a_real_attribute_defeats_its_pin",
})


class TestSourcePinsCannotBeSatisfiedByCommentText(unittest.TestCase):
    """#1172 item 1. Every pin in this module reads through
    :func:`_nix_source`, so a commented-out attribute cannot satisfy one."""

    def test_commenting_out_a_real_attribute_defeats_its_pin(self) -> None:
        """The exact planted mutant from the issue, driven over the real file.

        ``TestCreateLocallyContract.test_migrate_ordered_after_local_postgres_setup``
        asserts this attribute. Commenting it out left that test green: the
        module was equivalent to having no ordering at all, so a stranger's
        first boot could race NixOS role/database provisioning.
        """
        attribute = (
            'after = optional cfg.pipelineDb.createLocally '
            '"postgresql-setup.service";'
        )
        raw = MODULE_NIX.read_text(encoding="utf-8")
        self.assertIn(f"      {attribute}", raw)

        commented_out = raw.replace(f"      {attribute}", f"      # {attribute}", 1)
        # The defect: the attribute is still there, as comment text.
        self.assertIn(attribute, commented_out)
        # The fix: it is not there in what the pins actually read.
        self.assertNotIn(attribute, _strip_comment_lines(commented_out))

    def test_a_trailing_comment_after_code_keeps_its_code(self) -> None:
        """Only whole comment lines go — the code before a trailing ``#`` is
        real and must stay pinnable."""
        source = "NoNewPrivileges = true; # install/grep/chmod as root\n"
        self.assertIn("NoNewPrivileges = true;", _strip_comment_lines(source))

    def test_indented_comment_lines_are_stripped(self) -> None:
        """Nix attributes are nested, so a commented-out one is almost always
        indented rather than at column zero."""
        self.assertEqual(_strip_comment_lines("      # stopIfChanged = false;"), "")

    def test_every_nix_read_in_this_module_is_comment_stripped(self) -> None:
        """A deliberately narrow syntactic audit: no ``<NAME>_NIX.read_text``
        call outside the two allowed functions.

        Converting the existing call sites is only worth doing if the next one
        cannot quietly reintroduce the defect. This checks one local fact — a
        ``read_text`` call on a module-level ``*_NIX`` constant — and infers
        nothing about the source being read.
        """
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        offenders: list[str] = []
        for func in ast.walk(tree):
            if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if func.name in _RAW_NIX_READ_ALLOWED:
                continue
            offenders.extend(
                f"{func.name} (line {node.lineno})"
                for node in ast.walk(func)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "read_text"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id.endswith("_NIX")
            )
        self.assertEqual(
            offenders,
            [],
            "read Nix source through _nix_source(): a raw read lets a "
            "commented-out attribute satisfy the pin (#1172)",
        )


class TestPythonPathCarriesOnlyRepoRoot(unittest.TestCase):
    """No wrapper in ``nix/module.nix`` may export PYTHONPATH that includes
    ``${src}/lib`` or ``${src}/web``.

    All internal imports use the qualified form ``from lib.X import Y`` /
    ``from web.X import Y``, so the repo root on PYTHONPATH is sufficient.
    Adding the sub-directories promotes our internal modules (``lib/beets.py``,
    ``web/discogs.py``, ``web/classify.py``) to top-level names, where they
    shadow the real ``beets``, ``discogs_client`` and anything else a
    subprocess might import. The beet subprocess has historically been
    the first victim because its wrapper does ``from beets.ui import main``.
    """

    # Matches any ``export PYTHONPATH=...${src}/<subdir>...``
    # The test looks for the forbidden sub-paths specifically rather than
    # trying to parse the full expression — that keeps the pattern simple
    # and catches any future ``${src}/foo`` that would cause the same class
    # of shadowing.
    FORBIDDEN = re.compile(r'PYTHONPATH=.*\$\{src\}/(lib|web)')

    def test_no_wrapper_leaks_subdir(self) -> None:
        text = _nix_source(MODULE_NIX)
        hits: list[tuple[int, str]] = []
        for lineno, line in enumerate(text.splitlines(), start=1):
            # Skip comments — comments are explanation, not code.
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue
            if self.FORBIDDEN.search(line):
                hits.append((lineno, line.strip()))
        self.assertEqual(
            hits, [],
            f"{MODULE_NIX} exports PYTHONPATH with ${{src}}/lib or "
            f"${{src}}/web — these shadow PyPI packages (beets, "
            f"discogs_client, ...) in any subprocess that inherits "
            f"PYTHONPATH. Use ${{src}} only; internal imports are "
            f"qualified (from lib.X import Y). Offending lines:\n"
            + "\n".join(f"  {n}: {s}" for n, s in hits)
        )


class TestPipelineCliWrapperContract(unittest.TestCase):
    """API-backed CLI commands use the module-owned Unix listener."""

    def _wrapper(self) -> str:
        text = _nix_source(MODULE_NIX)
        wrapper_start = text.index('writeShellScriptBin "pipeline-cli"')
        wrapper_end = text.index('writeShellScriptBin "pipeline-migrate"')
        return text[wrapper_start:wrapper_end]

    def test_wrapper_selects_non_overridable_unix_socket(self) -> None:
        wrapper = self._wrapper()
        self.assertIn('main(api_socket="${webSocketPath}")', wrapper)
        self.assertNotIn("--api-base", wrapper)
        self.assertNotIn("127.0.0.1", wrapper)

    def test_wrapper_uses_safe_path_with_trusted_source_first(self) -> None:
        wrapper = self._wrapper()
        trusted_path = (
            'export PYTHONPATH="${src}\'\'${PYTHONPATH:+:$PYTHONPATH}"'
        )
        safe_exec = "exec ${pythonEnv}/bin/python -P -c"
        self.assertIn(trusted_path, wrapper)
        self.assertIn(safe_exec, wrapper)
        self.assertLess(wrapper.index(trusted_path), wrapper.index(safe_exec))


class TestDecisionDifferentialWrapperContract(unittest.TestCase):
    """The read-only differential always runs deployed source and Python."""

    def test_wrapper_uses_the_module_source_and_safe_python(self) -> None:
        text = _nix_source(MODULE_NIX)
        start = text.index('writeShellScriptBin "decision-differential"')
        end = text.index('writeShellScriptBin "cratedigger-importer"', start)
        wrapper = text[start:end]
        self.assertIn(
            'unset PYTHONPATH PYTHONHOME PYTHONSTARTUP', wrapper,
        )
        self.assertIn('export PYTHONNOUSERSITE=1', wrapper)
        self.assertIn(
            'exec ${pythonEnv}/bin/python -I ${src}/scripts/decision_differential.py "$@"',
            wrapper,
        )
        self.assertNotIn('PYTHONPATH:+', wrapper)
        self.assertIn("decisionDifferential", text[text.index("environment.systemPackages"):])


#: Issue #1131 review round 2: exception-memoizing cache for the two
#: cost-grouped nix evaluations below, keyed by the EXPRESSION itself
#: (never a hand-written tag — a stale or mismatched tag would silently
#: return one expression's cached value for a different one).
#: ``functools.cache`` does not memoize a raised exception — on a real
#: module regression every consumer of a failing evaluation would
#: independently re-pay the full ``nix eval`` just to report the same
#: failure. Memoizing the exception too means a real regression costs one
#: nix eval to detect, not one per consumer.
_NIX_EVAL_CACHE: dict[str, dict[str, object] | Exception] = {}


def _cached_nix_eval_json(expression: str) -> dict[str, object]:
    """Run one ``nix eval --json`` at most once per process per expression."""
    cached = _NIX_EVAL_CACHE.get(expression)
    if cached is not None:
        if isinstance(cached, Exception):
            raise cached
        return cached
    try:
        result = subprocess.run(
            ["nix", "eval", "--impure", "--json", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        if result.returncode != 0:
            raise AssertionError(result.stderr)
        value = json.loads(result.stdout)
        if not isinstance(value, dict):
            raise TypeError(value)
    except Exception as exc:
        # Not `except BaseException`: a Ctrl-C or SystemExit must propagate
        # normally, never get memoized and instantly re-raised at every
        # later consumer for the rest of the process.
        _NIX_EVAL_CACHE[expression] = exc
        raise
    _NIX_EVAL_CACHE[expression] = value
    return value


def _shared_module_worlds_web_auth_matrix_part1() -> dict[str, object]:
    """The first of two bounded ``webAuthMatrix`` nix evals (issue #1156 item 1).

    Issue #1226 (read this first — it supersedes the SIZES quoted below,
    not the reasoning): every ``lib.nixosSystem`` in this module's
    expressions now takes ``{ nixpkgs.pkgs = modulePkgs; }`` as its first
    module, so all of a given eval's worlds share the ONE nixpkgs instance
    the preamble already built for ``beetsPackage`` instead of each
    instantiating its own. ``flake.nix``'s own eval-level guards
    (``runtimeSrcPin``, ``packageSetPin``, ``moduleAssertions``) already
    used exactly this idiom; these expressions were the odd ones out. It
    changes nothing about what is evaluated or asserted -- the ``nix eval
    --json`` stdout is BYTE-IDENTICAL before and after for all three
    expressions, which is how the change was qualified -- and it is
    fail-closed rather than silent if that ever stops being true, because
    nixpkgs' own module errors out when ``nixpkgs.pkgs`` is set alongside a
    ``nixpkgs.config``/``overlays``/``hostPlatform`` definition. Measured
    solo on a quiet 30-core host, base vs candidate: this half 31.4s ->
    13.9s (-55.7%), the other half 26.6s -> 12.2s (-54.3%),
    :func:`_shared_module_worlds_rest` 44.0s -> 16.3s (-63.0%). The cost
    removed was never the shared preamble (measured at 0.19s standalone --
    see the paragraph below, which over-credited it); it was ~20 redundant
    nixpkgs instantiations, one per world. A bare ``lib.nixosSystem``
    reading only ``.config.assertions`` costs 0.21s marginal, so what each
    world pays now is close to that floor.

    Splits the independent, roughly-balanced worlds this module's single
    heaviest target used to force in one ``nix eval`` subprocess (measured
    57.2-61.0s solo across three quiet-host runs, mean ~58.5s; the prior
    #1131 docstring's 65.44s was this same target measured on an earlier
    host state) into two halves so the target's own solo floor drops
    without changing what is evaluated or asserted. This half carries
    ``missing`` through ``rootAccessGroup`` in declaration order;
    :func:`_shared_module_worlds_web_auth_matrix_part2` carries
    ``wheelAccessGroup`` through ``nginxRestartDisabled``.

    Measured solo (quiet host): this half alone 31.9s, the other half alone
    28.3s -- 60.2s summed vs the original single target's ~58.5s mean,
    +1.7s (an earlier draft of this sentence reported a wrong sum here;
    corrected). Splitting pays exactly ONE additional shared preamble
    (``getFlake`` + ``import nixpkgs`` + ``import ./nix/beets.nix``,
    measured well under 1s standalone) -- one before the split, two after
    -- not one per half, so the predicted overhead is ~1s; the measured
    +1.7s is close to that (independent review's own measurement
    corroborates the same shape: 59.75s base vs 33.85s + 28.13s = 61.98s
    split, +2.23s/+3.7%). Measured concurrently (both halves launched
    together, no other suite load): 33.2s wall -- close to the per-half
    floor, confirming the two subprocesses barely interfere with each
    other in isolation.

    That isolation number was NOT what decided this split -- three
    interleaved (baseline, candidate) pairs of full ``run_tests.sh``
    invocations were, run back to back on an otherwise-shared 30-core host
    (ambient contention from concurrent sibling work acknowledged and
    visible in the spread below). The ORIGINAL unsplit target -- frontloaded
    (``AUDITED_FRONTLOAD_MODULES``) and so starting alongside roughly twenty
    other concurrent workers, several of them CPU-heavy generated-test
    targets, not merely its own ``_rest`` sibling -- ran 100.1s / 114.3s /
    103.3s across the three baseline runs (mean 105.9s, 1.7-2.0x its solo
    floor). Splitting it into two raised this module's own
    concurrently-schedulable heavy-nix-eval target count from two
    (``webAuthMatrix`` + ``_rest``) to three (this half + the other half +
    ``_rest``) -- exactly the axis #1131's own worker-count sweep (88.0s at
    8 workers, 122.7s at 12, 147.7s at 16, 152.3s at 20) warned against
    raising carelessly. Measured anyway: the worst single target this
    module contributes to a run dropped to 95.0s / 96.4s / 85.7s (mean
    92.4s, -13% average, and every one of the three pairs improved,
    individually) with NO runaway blowup -- this half and the other half
    each stayed in the 46-62s range even competing against two heavy
    siblings instead of one. The suite-level python-phase wall time moved
    from a mean of 118.6s to 113.8s (-4%, and noisier than the per-target
    number: baseline's own three runs alone spanned 107.5-137.0s from
    ambient contention, before this change touches anything) -- a real,
    if modest, net win with no evidence of the feared regression, not the
    dramatic headline number the solo floor alone would suggest.
    Independent review's own controlled paired probes (real evals plus
    synthetic CPU spinners held at fixed concurrency, isolating the
    concurrency effect from ambient host noise) confirmed the same
    direction more sharply: -22.6%/-29.5% at ~21-22 busy (this suite's real
    worker count) and 72.3s -> 57.6s (-20.4%) at ~29-30 busy (at/over core
    count), four paired runs all favouring the split; ``_rest`` was NOT
    hurt by gaining a third competitor, and measured memory never dropped
    below 21 of 32 GB.

    The ``serviceGroupOverlap``/``nginxGroupOverlap``/``secretGroupOverlap``
    "must be dedicated" check and the ``rootAccessGroup``/
    ``wheelAccessGroup`` "forbidden authority group" check both originally
    lived in one ``for`` loop spanning the ``rootAccessGroup``/
    ``wheelAccessGroup`` boundary this split introduces. Every one of the
    original method's 39 ``worlds[...]`` references names exactly one
    world -- there is no cross-world comparison anywhere in the base
    method, so splitting a same-substring-check loop across that boundary
    cannot weaken anything; each check now runs once, in whichever half
    holds the world it names, on the identical message substring it
    always checked. Independent review planted three mutants directly
    against this claim; all three killed cleanly on both halves (record
    in the shipping commit's kill matrix): dropping the ``accessGroup``
    exclusion in ``nix/module.nix`` failed ``serviceGroupOverlap`` in this
    half and ``secretGroupOverlap`` in the other; removing "root"/"wheel"
    from the forbidden-group set failed ``rootAccessGroup`` here and
    ``wheelAccessGroup`` there, independently; deleting a world's own
    block from either half's expression failed that half with a
    ``KeyError`` on the deleted world's name.

    See ``HOTSPOT_ISOLATED_METHODS`` in ``scripts/run_python_tests.py`` for
    the scheduler half of this split: both this function's sole consumer
    and its sibling's must each keep their own singleton target for the
    same reason the original single-target carve-out did -- the whole
    point is defeated if either ever shares a worker process with a
    neighbour it was not measured against.

    The ~45-line Nix preamble (``getFlake``/``nixpkgs``/``beetsPackage``
    header plus the shared ``evaluate`` base-world definition) is
    byte-identical between this function and
    :func:`_shared_module_worlds_web_auth_matrix_part2` today, with
    nothing enforcing that beyond this claim -- the per-world assertions
    above are same-world substring checks and so cannot detect one half's
    base world silently drifting from the other's.
    ``TestWebAuthMatrixPreamblesStayIdentical`` below pins the two
    expressions' non-world regions equal so a future edit to one half's
    base world cannot pass unnoticed while every existing assertion still
    goes green.
    """
    expression = r'''
      let
        f = builtins.getFlake (toString ./.);
        lib = f.inputs.nixpkgs.lib;
        modulePkgs = import f.inputs.nixpkgs {
          system = builtins.currentSystem;
        };
        beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
      in {
        webAuthMatrixPart1 =
          let
            evaluate = extra:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    { nixpkgs.pkgs = modulePkgs; }
                    f.nixosModules.default
                    ({ ... }: {
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        web.enable = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                      };
                    })
                    extra
                  ];
                };
              in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix "services.cratedigger.web" assertion.message)
                  system.config.assertions);
          in {
            missing = evaluate {
              services.cratedigger.web.hostName = "music.example.test";
            };
            basic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
            };
            insecure = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
            };
            conflict = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                  enableInsecure = true;
                };
              };
            };
            storeBasic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/nix/store/fake-cratedigger.htpasswd";
                };
              };
            };
            badHost = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test;\nreturn 200";
                enableInsecure = true;
              };
            };
            uppercaseHost = evaluate {
              services.cratedigger.web = {
                hostName = "Music.example.test";
                enableInsecure = true;
              };
            };
            ipHost = evaluate {
              services.cratedigger.web = {
                hostName = "127.0.0.1";
                enableInsecure = true;
              };
            };
            injectedBasic = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile =
                    "/run/secrets/file; satisfy any; allow all; #";
                };
              };
            };
            disabled = evaluate {
              services.cratedigger.web.enable = lib.mkForce false;
            };
            disabledBasic = evaluate {
              services.cratedigger.web = {
                enable = lib.mkForce false;
                basicAuthFile = "/run/secrets/cratedigger.htpasswd";
              };
            };
            disabledInsecure = evaluate {
              services.cratedigger.web = {
                enable = lib.mkForce false;
                enableInsecure = true;
              };
            };
            external = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                externalAuth = true;
              };
            };
            externalBasicConflict = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                  externalAuth = true;
                };
              };
            };
            externalInsecureConflict = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
                externalAuth = true;
              };
            };
            allThreeConflict = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                  enableInsecure = true;
                  externalAuth = true;
                };
              };
            };
            disabledExternal = evaluate {
              services.cratedigger.web = {
                enable = lib.mkForce false;
                externalAuth = true;
              };
            };
            serviceGroupOverlap = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "cratedigger";
                };
              };
            };
            nginxGroupOverlap = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
                accessGroup = "nginx";
              };
            };
            rootAccessGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "root";
                };
              };
            };
          };
      }
    '''
    return _cached_nix_eval_json(expression)


def _shared_module_worlds_web_auth_matrix_part2() -> dict[str, object]:
    """The second of two bounded ``webAuthMatrix`` nix evals (issue #1156
    item 1). See :func:`_shared_module_worlds_web_auth_matrix_part1` for the
    full rationale, the measured solo/concurrent/under-load numbers, and why
    this split is a measured boundary decision, not merely half the file.
    This half carries the remaining worlds in declaration order
    (``wheelAccessGroup`` through ``nginxRestartDisabled``). Measured solo
    (quiet host): 28.3s. Under full-suite load across the same three
    interleaved pairs, this half itself measured 46.3s / 48.3s / 53.0s.

    Issue #1226 superseded those sizes: with the shared nixpkgs instance
    (see part1's own #1226 paragraph) this half is 12.2s solo and 24.8s
    under full-suite load, byte-identical output either way.
    """
    expression = r'''
      let
        f = builtins.getFlake (toString ./.);
        lib = f.inputs.nixpkgs.lib;
        modulePkgs = import f.inputs.nixpkgs {
          system = builtins.currentSystem;
        };
        beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
      in {
        webAuthMatrixPart2 =
          let
            evaluate = extra:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    { nixpkgs.pkgs = modulePkgs; }
                    f.nixosModules.default
                    ({ ... }: {
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        web.enable = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                      };
                    })
                    extra
                  ];
                };
              in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix "services.cratedigger.web" assertion.message)
                  system.config.assertions);
          in {
            wheelAccessGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "wheel";
                };
              };
            };
            explicitOperatorGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "music-operators";
                };
              };
              users.users.operator = {
                isNormalUser = true;
                extraGroups = [ "music-operators" ];
              };
            };
            secretGroupOverlap = evaluate {
              services.cratedigger = {
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                  accessGroup = "cratedigger-ops";
                };
              };
            };
            nginxAccountSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.users.nginx.extraGroups = [ "cratedigger-ops" ];
            };
            nginxReverseSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.cratedigger-ops.members = [ "nginx" ];
            };
            nginxAliasedReverseSecretGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.hiddenSecret = {
                name = "cratedigger-ops";
                members = [ "nginx" ];
              };
            };
            nginxServiceMediaGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.SupplementaryGroups = [
                "users"
              ];
            };
            nginxServiceNumericMediaGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                };
              };
              users.groups.cratedigger.gid = 4242;
              systemd.services.nginx.serviceConfig.SupplementaryGroups = [
                "4242"
              ];
            };
            nginxPrimaryServiceGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  enableInsecure = true;
                };
              };
              services.nginx.group = "cratedigger";
            };
            nginxServiceRootUserOverride = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.User =
                lib.mkForce "root";
            };
            nginxServiceRootGroupOverride = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.serviceConfig.Group =
                lib.mkForce "root";
            };
            nginxMissingAccessGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.users.nginx.extraGroups = lib.mkForce [];
              systemd.services.nginx.serviceConfig.SupplementaryGroups =
                lib.mkForce [];
            };
            nginxNumericAccessGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.cratedigger-web.gid = 4243;
              users.users.nginx.extraGroups = lib.mkForce [];
              systemd.services.nginx.serviceConfig.SupplementaryGroups =
                lib.mkForce [ "4243" ];
            };
            webServiceCredentialGroup = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.SupplementaryGroups = [
                "nginx"
              ];
            };
            webServiceRootOverride = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.User =
                lib.mkForce "root";
            };
            webServiceNginxGroupOverride = evaluate {
              services.cratedigger = {
                user = "cratedigger";
                group = "cratedigger";
                web = {
                  hostName = "music.example.test";
                  basicAuthFile = "/run/secrets/cratedigger.htpasswd";
                };
              };
              systemd.services.cratedigger-web.serviceConfig.Group =
                lib.mkForce "nginx";
            };
            nginxReverseUnrelatedGroup = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              users.groups.smokeping.members = [ "nginx" ];
            };
            nginxReloadDisabled = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              services.nginx.enableReload = lib.mkForce false;
            };
            nginxRestartDisabled = evaluate {
              services.cratedigger.web = {
                hostName = "music.example.test";
                enableInsecure = true;
              };
              systemd.services.nginx.restartIfChanged = lib.mkForce false;
            };
          };
      }
    '''
    return _cached_nix_eval_json(expression)


_WEB_AUTH_MATRIX_PART1_FIRST_WORLD_MARKER = "missing = evaluate {"
_WEB_AUTH_MATRIX_PART2_FIRST_WORLD_MARKER = "wheelAccessGroup = evaluate {"


def _web_auth_matrix_expression_source(function_name: str) -> str:
    """The literal ``expression = r'''...'''`` string inside ``function_name``.

    Static AST extraction of the source, not execution -- this module's own
    established idiom for source-level Nix content checks (``_nix_source``,
    :func:`strip_line_comments`). Reading the literal avoids paying for a
    ``nix eval`` just to compare two Python string constants.
    """
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            for stmt in node.body:
                if (
                    isinstance(stmt, ast.Assign)
                    and len(stmt.targets) == 1
                    and isinstance(stmt.targets[0], ast.Name)
                    and stmt.targets[0].id == "expression"
                    and isinstance(stmt.value, ast.Constant)
                    and isinstance(stmt.value.value, str)
                ):
                    return stmt.value.value
    raise AssertionError(
        f"no literal `expression = r'''...'''` assignment found in {function_name}"
    )


def _assert_web_auth_matrix_preambles_equal(
    part1_expression: str,
    part2_expression: str,
    *,
    part1_marker: str = _WEB_AUTH_MATRIX_PART1_FIRST_WORLD_MARKER,
    part2_marker: str = _WEB_AUTH_MATRIX_PART2_FIRST_WORLD_MARKER,
) -> None:
    """Issue #1156 item 1 review F3: the two split halves DUPLICATE (not
    share) the ~45-line Nix preamble (``getFlake``/``nixpkgs``/
    ``beetsPackage`` header plus the shared ``evaluate`` base-world
    definition) -- nothing else keeps the two copies equal, and every
    per-world assertion in both halves is a same-world substring check, so
    none of them can detect one copy's base world silently drifting from
    the other's. This is the guard that closes that gap.

    Pure function so the pin test can drive it against the real file AND a
    known-bad self-test can drive it against fabricated, controlled input
    without touching disk or paying for a fresh ``nix eval``.
    """
    part1_header, part1_sep, _ = part1_expression.partition(part1_marker)
    part2_header, part2_sep, _ = part2_expression.partition(part2_marker)
    if not part1_sep:
        raise AssertionError(f"part1 world-list marker not found: {part1_marker!r}")
    if not part2_sep:
        raise AssertionError(f"part2 world-list marker not found: {part2_marker!r}")
    normalized_part1 = part1_header.replace(
        "webAuthMatrixPart1 =", "webAuthMatrixPartN ="
    )
    normalized_part2 = part2_header.replace(
        "webAuthMatrixPart2 =", "webAuthMatrixPartN ="
    )
    if normalized_part1 != normalized_part2:
        raise AssertionError(
            "the two webAuthMatrix halves' shared Nix preamble/evaluate "
            "base world has drifted -- see "
            "_shared_module_worlds_web_auth_matrix_part1's docstring for "
            "why no per-world assertion can catch this"
        )


class TestWebAuthMatrixPreamblesStayIdentical(unittest.TestCase):
    """Issue #1156 item 1 review F3. See
    :func:`_assert_web_auth_matrix_preambles_equal`.
    """

    def test_real_halves_are_identical_today(self) -> None:
        _assert_web_auth_matrix_preambles_equal(
            _web_auth_matrix_expression_source(
                "_shared_module_worlds_web_auth_matrix_part1"
            ),
            _web_auth_matrix_expression_source(
                "_shared_module_worlds_web_auth_matrix_part2"
            ),
        )

    def test_a_diverged_base_world_is_caught(self) -> None:
        """Known-bad self-test: a base-world edit present in only one half
        must fail this guard, even though it would pass every per-world
        assertion in both halves (that is precisely the drift this guard
        exists to catch)."""
        part1 = (
            "        webAuthMatrixPart1 =\n"
            "          let\n"
            "            evaluate = extra: /* base world */ null;\n"
            "          in {\n"
            "            missing = evaluate {};\n"
        )
        part2_diverged = (
            "        webAuthMatrixPart2 =\n"
            "          let\n"
            "            evaluate = extra: /* DIFFERENT base world */ null;\n"
            "          in {\n"
            "            wheelAccessGroup = evaluate {};\n"
        )
        with self.assertRaisesRegex(AssertionError, "has drifted"):
            _assert_web_auth_matrix_preambles_equal(part1, part2_diverged)

    def test_identical_halves_pass(self) -> None:
        """Must-still-work control for the self-test above."""
        part1 = (
            "        webAuthMatrixPart1 =\n"
            "          let\n"
            "            evaluate = extra: /* base world */ null;\n"
            "          in {\n"
            "            missing = evaluate {};\n"
        )
        part2_same = (
            "        webAuthMatrixPart2 =\n"
            "          let\n"
            "            evaluate = extra: /* base world */ null;\n"
            "          in {\n"
            "            wheelAccessGroup = evaluate {};\n"
        )
        _assert_web_auth_matrix_preambles_equal(part1, part2_same)

    def test_missing_marker_fails_closed(self) -> None:
        """Known-bad self-test for the marker-not-found guard clauses."""
        with self.assertRaisesRegex(AssertionError, "part1 world-list marker not found"):
            _assert_web_auth_matrix_preambles_equal("no marker here", "wheelAccessGroup = evaluate {")
        with self.assertRaisesRegex(AssertionError, "part2 world-list marker not found"):
            _assert_web_auth_matrix_preambles_equal("missing = evaluate {", "no marker here")


def _shared_module_worlds_rest() -> dict[str, object]:
    """The other five nix-eval worlds this module's tests still merge.

    Issue #1131 review round 2: ``headlessComposition`` (0.80s),
    ``mergedGateway`` (18.67s), ``beetsCapability`` (38.28s), and
    ``beetsReadiness`` (3.46s) sum to ~61s solo — close enough to the
    original single ``webAuthMatrix`` target's 65.44s (see the historical
    #1131 measurement recorded in
    :func:`_shared_module_worlds_web_auth_matrix_part1`) that bundling them
    into one target kept the module's original two-target floor level with
    main's own worst-case bin-packed batch (~61.6s), not better. Issue
    #1156 item 1 later split that single ``webAuthMatrix`` target into
    ``_part1``/``_part2``, so this function is now one of THREE heavy
    targets the module contributes rather than two, AND — because it did
    not itself shrink while gaining a third competitor — this module's own
    pole. Measured across the same three interleaved baseline/candidate
    ``run_tests.sh`` pairs #1156 item 1's own shipping commit used: this
    target ran 88.3s / 107.1s / 92.7s (mean 96.0s) before the split and
    95.0s / 96.4s / 85.7s (mean 92.4s) after, competing against ONE extra
    heavy sibling (-3.8%, not hurt by the extra competitor). On the
    receipt bundle from the shipping commit's own ``check`` run, this
    target (87.5s) was the single SLOWEST of all 463 targets in the whole
    canonical suite, by 26s over the next entry — this module's own pole
    was then the suite's own tail. Issue #1226 closed that without
    splitting anything: sharing ONE nixpkgs instance across this
    expression's eight ``lib.nixosSystem`` calls (see
    :func:`_shared_module_worlds_web_auth_matrix_part1`'s own #1226
    paragraph for the mechanism and the byte-identical-output proof) took
    this eval from 44.0s to 16.3s solo, and this target from 95.0s to 38.9s
    in a real phase run. It is no longer the suite's tail and this module
    is no longer the phase's pole. Splitting this target further is now
    the wrong lever regardless: with the pole gone the phase is
    throughput-bound, so what costs wall time is total CPU and queue
    order, not any one target's length. See
    :func:`_shared_module_worlds_web_auth_matrix_part1` for the measured
    effect of the split on the OTHER two targets this module contributes.
    Merging these four
    also eliminates 3 of the 4 redundant ``getFlake`` + ``import nixpkgs``
    + ``import ./nix/beets.nix`` preambles they used to pay independently
    — each measured at well under 1s standalone, so the CPU this merge
    actually saves is on the order of a few seconds, not the difference
    between the four solo totals and one combined run (those totals
    double-count nixpkgs-import work every eval pays regardless of
    merging).

    Issue #1176 PR2 review round 1 finding 8 added a fifth world,
    ``localImportAssertions`` (seven small ``lib.nixosSystem`` evaluations
    with no ``web``/``beets.validation`` composition — the cheapest shape
    in this file). Measured this whole target's wall time before and
    after: ~34.9s solo before, ~41.8s solo after (+20%) — still well under
    the original single ``webAuthMatrix`` target's ~65s, so the
    (now-superseded, see above) two-target floor this function's own
    docstring argued for was unchanged at the time; no existing world here
    was weakened to fit it in.

    Every world here only ever reads ``.config.assertions`` or plain
    option/service values, never forces ``.system.build.toplevel``, so
    none of them can raise mid-evaluation and take the others down with
    it.

    ``test_injected_basic_path_cannot_render_toplevel`` DOES force
    ``.system.build.toplevel`` to observe the resulting Nix assertion
    failure — that failure is a catchable ``throw`` (``builtins.tryEval``
    returns ``{"success": false}`` in ~5.1s measured), not an uncatchable
    ``abort``, but ``tryEval`` discards the failure's message and the test
    asserts on the EXACT stderr text (``"nginx-token-safe segments"``).
    Merging it here would lose the one thing the test needs, so it keeps
    its own independent ``nix eval`` call regardless of catchability.
    """
    expression = r'''
      let
        f = builtins.getFlake (toString ./.);
        lib = f.inputs.nixpkgs.lib;
        modulePkgs = import f.inputs.nixpkgs {
          system = builtins.currentSystem;
        };
        beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
      in {
        headlessComposition =
          let
            system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                { nixpkgs.pkgs = modulePkgs; }
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                    };
                  };
                })
              ];
            };
          in {
            webEnabled = system.config.services.cratedigger.web.enable;
            systemPackages =
              map lib.getName system.config.environment.systemPackages;
            hasWebService =
              builtins.hasAttr
                "cratedigger-web"
                system.config.systemd.services;
            cratediggerSockets =
              builtins.filter
                (name: lib.hasPrefix "cratedigger" name)
                (builtins.attrNames system.config.systemd.sockets);
          };

        mergedGateway =
          let
            render = enableIPv6: basicAuthFile: externalAuth:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    { nixpkgs.pkgs = modulePkgs; }
                    f.nixosModules.default
                    ({ ... }: {
                      networking.enableIPv6 = enableIPv6;
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        user = "cratedigger";
                        group = "cratedigger";
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                        web = ({
                          enable = true;
                          hostName = "music.example.test";
                          enableInsecure =
                            basicAuthFile == null && !externalAuth;
                          inherit externalAuth;
                        } // lib.optionalAttrs (basicAuthFile != null) {
                          inherit basicAuthFile;
                        });
                      };
                      services.nginx.virtualHosts.cratedigger-auth-gateway
                        .locations."/merged-probe" = {
                          proxyPass =
                            "http://unix:/run/cratedigger-web/web.sock:";
                          recommendedProxySettings = false;
                        };
                    })
                  ];
                };
                gateway =
                  system.config.services.nginx.virtualHosts.cratedigger-auth-gateway;
                reject =
                  system.config.services.nginx.virtualHosts.cratedigger-auth-reject;
                socket = system.config.systemd.sockets.cratedigger-web;
                webService =
                  system.config.systemd.services.cratedigger-web;
                nginxService = system.config.systemd.services.nginx;
              in {
                failures = map (assertion: assertion.message)
                  (builtins.filter
                    (assertion:
                      !assertion.assertion
                      && lib.hasPrefix
                        "services.cratedigger.web"
                        assertion.message)
                    system.config.assertions);
                listen = map (item: {
                  inherit (item) addr port;
                }) gateway.listen;
                hostName = gateway.serverName;
                gatewayExtra = gateway.extraConfig;
                gatewayPolicy =
                  system.config.environment.etc
                    ."cratedigger/web-gateway-policy".text;
                basicAuthFile = gateway.basicAuthFile;
                rootBasicAuthFile = gateway.locations."/".basicAuthFile;
                mergedBasicAuthFile =
                  gateway.locations."/merged-probe".basicAuthFile;
                proxyPass = gateway.locations."/".proxyPass;
                healthProxy = gateway.locations."= /healthz".proxyPass;
                healthExtra = gateway.locations."= /healthz".extraConfig;
                rejectDefault = reject.default;
                rejectConfig = reject.locations."/".extraConfig;
                socketListen = socket.listenStreams;
                socketGroup = socket.socketConfig.SocketGroup;
                socketMode = socket.socketConfig.SocketMode;
                webAfter = webService.after;
                webRequires = webService.requires;
                webGroups = webService.serviceConfig.SupplementaryGroups;
                webUser = webService.serviceConfig.User;
                webGroup = webService.serviceConfig.Group;
                webStartPre = webService.serviceConfig.ExecStartPre;
                nginxEnableReload =
                  system.config.services.nginx.enableReload;
                nginxRestartIfChanged = nginxService.restartIfChanged;
                nginxAfter = nginxService.after;
                nginxWants = nginxService.wants;
                nginxRequires = nginxService.requires;
                nginxUnit =
                  system.config.systemd.units."nginx.service".text;
                nginxGroups = nginxService.serviceConfig.SupplementaryGroups;
                nginxUser = nginxService.serviceConfig.User;
                nginxGroup = nginxService.serviceConfig.Group;
                nginxUserGroups =
                  system.config.users.users.${system.config.services.nginx.user}.extraGroups;
                applicationUserGroups =
                  system.config.users.users.cratedigger.extraGroups;
                startPre = nginxService.serviceConfig.ExecStartPre;
                reload = nginxService.serviceConfig.ExecReload;
              };
          in {
            dualStack =
              render true "/run/secrets/cratedigger.htpasswd" false;
            ipv4Only =
              render false "/run/secrets/cratedigger.htpasswd" false;
            insecureRecovery = render false null false;
            alternateBasic =
              render false "/run/secrets/cratedigger-alternate.htpasswd" false;
            externalMode = render false null true;
          };

        beetsCapability =
          let
            ambientPkgs = modulePkgs // {
              python3 = modulePkgs.python311;
              python3Packages = modulePkgs.python311Packages;
            };
            ambientBeetsPackage = import ./nix/beets.nix {
              pkgs = ambientPkgs;
            };
            runtime = {
              package = beetsPackage;
              configDir = "/etc/beets";
              expectedLibrary = "/srv/beets/beets-library.db";
              expectedDirectory = "/srv/music";
              expectedStateFile = "/var/lib/beets/state.pickle";
              expectedSecretInclude = "/run/secrets/beets.yaml";
              readinessUnits = [];
            };
            failures = candidate:
              let system = lib.nixosSystem {
                system = builtins.currentSystem;
                modules = [
                  { nixpkgs.pkgs = modulePkgs; }
                  f.nixosModules.default
                  ({ ... }: {
                    services.cratedigger = {
                      enable = true;
                      src = ./.;
                      packageSet = modulePkgs;
                      slskd.apiKeyFile = "/run/secrets/slskd-key";
                      slskd.downloadDir = "/srv/slskd";
                      pipelineDb.createLocally = true;
                      beets.runtime = candidate;
                      beets.validation = {
                        stagingDir = "/srv/incoming";
                        trackingFile = "/srv/incoming/tracking.jsonl";
                      };
                    };
                  })
                ];
              }; in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix
                      "services.cratedigger.beets.runtime"
                      assertion.message)
                  system.config.assertions);
            disabled = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [ { nixpkgs.pkgs = modulePkgs; } f.nixosModules.default ];
            };
            identityFailures = user: group:
              let system = lib.nixosSystem {
                system = builtins.currentSystem;
                modules = [
                  { nixpkgs.pkgs = modulePkgs; }
                  f.nixosModules.default
                  ({ ... }: {
                    services.cratedigger = {
                      enable = true;
                      src = ./.;
                      packageSet = modulePkgs;
                      inherit user group;
                      slskd.apiKeyFile = "/run/secrets/slskd-key";
                      slskd.downloadDir = "/srv/slskd";
                      pipelineDb.createLocally = true;
                      beets.runtime = runtime;
                      beets.validation = {
                        stagingDir = "/srv/incoming";
                        trackingFile = "/srv/incoming/tracking.jsonl";
                      };
                    };
                  })
                ];
              }; in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix "services.cratedigger"
                      assertion.message)
                  system.config.assertions);
            defaultIdentity = let system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                { nixpkgs.pkgs = modulePkgs; }
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    packageSet = modulePkgs;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = runtime;
                    beets.validation = {
                      stagingDir = "/srv/incoming";
                      trackingFile = "/srv/incoming/tracking.jsonl";
                    };
                  };
                })
              ];
            }; in {
              user = system.config.services.cratedigger.user;
              group = system.config.services.cratedigger.group;
              serviceUser = system.config.systemd.services.cratedigger.serviceConfig.User;
              serviceGroup = system.config.systemd.services.cratedigger.serviceConfig.Group;
            };
          in {
            valid = failures runtime;
            missing = builtins.listToAttrs (map (field: {
              name = field;
              value = failures (builtins.removeAttrs runtime [ field ]);
            }) [
              "package" "configDir" "expectedLibrary" "expectedDirectory"
              "expectedStateFile" "expectedSecretInclude"
            ]);
            incompatiblePackage = failures (runtime // {
              package = beetsPackage // { pythonModule = null; };
            });
            ambientPackageMismatch = {
              distinct =
                ambientBeetsPackage.pythonModule != modulePkgs.python3;
              failures = failures (runtime // {
                package = ambientBeetsPackage;
              });
            };
            invalidPaths = {
              configDir = failures (runtime // { configDir = "etc/beets"; });
              expectedLibrary = failures (runtime // {
                expectedLibrary = "/srv/beets/../beets-library.db";
              });
              expectedDirectory = failures (runtime // {
                expectedDirectory = "/srv//music";
              });
              expectedStateFile = failures (runtime // {
                expectedStateFile = "var/lib/beets/state.pickle";
              });
              expectedSecretInclude = failures (runtime // {
                expectedSecretInclude = "/run/secrets/./beets.yaml";
              });
            };
            rootPaths = {
              configDir = failures (runtime // { configDir = "/"; });
              expectedDirectory = failures (runtime // {
                expectedDirectory = "/";
              });
              expectedLibrary = failures (runtime // {
                expectedLibrary = "/beets-library.db";
              });
            };
            rootIdentity = identityFailures "root" "root";
            numericIdentity = identityFailures "0" "0";
            inherit defaultIdentity;
            disabled = {
              assertions = builtins.filter
                (assertion:
                  !assertion.assertion
                  && lib.hasPrefix
                    "services.cratedigger"
                    assertion.message)
                disabled.config.assertions;
              services = builtins.filter
                (name: lib.hasPrefix "cratedigger" name)
                (builtins.attrNames disabled.config.systemd.services);
            };
          };

        beetsReadiness =
          let
            system = lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                { nixpkgs.pkgs = modulePkgs; }
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    packageSet = modulePkgs;
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    web = {
                      enable = true;
                      hostName = "music.example.test";
                      enableInsecure = true;
                    };
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                      readinessUnits = [
                        "beets-config-ready.service"
                        "beets-secret-ready.service"
                      ];
                    };
                    beets.validation = {
                      stagingDir = "/srv/incoming";
                      trackingFile = "/srv/incoming/tracking.jsonl";
                    };
                  };
                })
              ];
            };
            unit = name: let value = system.config.systemd.services.${name}; in {
              after = value.after;
              wants = value.wants;
              requires = value.requires;
              bindReadOnlyPaths = value.serviceConfig.BindReadOnlyPaths or [];
              bindPaths = value.serviceConfig.BindPaths or [];
              readWritePaths = value.serviceConfig.ReadWritePaths or [];
            };
          in {
            main = unit "cratedigger";
            importer = unit "cratedigger-importer";
            preview = unit "cratedigger-import-preview-worker";
            web = unit "cratedigger-web";
            census = unit "cratedigger-retag-census";
          };

        # Issue #1176 PR2 review finding 8: the localImport.{enable,dir}
        # assertion never had evaluated-world coverage. Reads only
        # .config.assertions (never forces .system.build.toplevel), so it
        # is safe to merge into this shared eval per this function's own
        # documented invariant.
        localImportAssertions =
          let
            evaluate = extra:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    { nixpkgs.pkgs = modulePkgs; }
                    f.nixosModules.default
                    ({ ... }: {
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                      };
                    })
                    extra
                  ];
                };
              in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.hasPrefix
                      "services.cratedigger.localImport"
                      assertion.message)
                  system.config.assertions);
          in {
            missingDir = evaluate {
              services.cratedigger.localImport.enable = true;
            };
            relativeDir = evaluate {
              services.cratedigger.localImport = {
                enable = true;
                dir = "srv/imports";
              };
            };
            emptyDir = evaluate {
              services.cratedigger.localImport = {
                enable = true;
                dir = "";
              };
            };
            slashDir = evaluate {
              services.cratedigger.localImport = {
                enable = true;
                dir = "/";
              };
            };
            trailingSlashDir = evaluate {
              services.cratedigger.localImport = {
                enable = true;
                dir = "/srv/imports/";
              };
            };
            validDir = evaluate {
              services.cratedigger.localImport = {
                enable = true;
                dir = "/srv/imports";
              };
            };
            disabledWithBadDir = evaluate {
              services.cratedigger.localImport = {
                enable = false;
                dir = "not/absolute";
              };
            };
          };

        # Issue #1355 Worth-exploring item 2: processingDir was checked
        # only with `hasPrefix "/"`, and its disjointness from
        # slskd.downloadDir was a plain lexical string comparison, so a
        # `.`/`..` component could pass evaluation while naming a tree
        # that physically overlaps slskd's download directory. Same
        # eval-safety property as localImportAssertions above (reads only
        # .config.assertions, never forces .system.build.toplevel).
        processingPathAssertions =
          let
            prefixes = [
              "services.cratedigger.processingDir"
              "services.cratedigger.slskd.downloadDir"
            ];
            evaluate = extra:
              let
                system = lib.nixosSystem {
                  system = builtins.currentSystem;
                  modules = [
                    { nixpkgs.pkgs = modulePkgs; }
                    f.nixosModules.default
                    ({ ... }: {
                      services.cratedigger = {
                        enable = true;
                        src = ./.;
                        slskd.apiKeyFile = "/run/secrets/slskd-key";
                        slskd.downloadDir = "/srv/slskd";
                        pipelineDb.createLocally = true;
                        beets.runtime = {
                          package = beetsPackage;
                          configDir = "/etc/beets";
                          expectedLibrary = "/srv/beets/beets-library.db";
                          expectedDirectory = "/srv/music";
                          expectedStateFile = "/var/lib/beets/state.pickle";
                          expectedSecretInclude = "/run/secrets/beets.yaml";
                        };
                      };
                    })
                    extra
                  ];
                };
              in map (assertion: assertion.message)
                (builtins.filter
                  (assertion:
                    !assertion.assertion
                    && lib.any
                      (p: lib.hasPrefix p assertion.message)
                      prefixes)
                  system.config.assertions);
          in {
            dotDotSegment = evaluate {
              services.cratedigger.processingDir =
                "/var/lib/cratedigger/processing/../processing";
            };
            dotSegment = evaluate {
              services.cratedigger.processingDir =
                "/var/lib/cratedigger/./processing";
            };
            doubledSlash = evaluate {
              services.cratedigger.processingDir =
                "/var/lib//cratedigger/processing";
            };
            trailingSlash = evaluate {
              services.cratedigger.processingDir =
                "/var/lib/cratedigger/processing/";
            };
            downloadDirDotDot = evaluate {
              services.cratedigger.slskd.downloadDir =
                lib.mkForce "/srv/slskd/../slskd";
            };
            # The concrete gap #1355 WE2 named: a `..` component makes
            # processingDir resolve to the SAME real directory as
            # downloadDir, but the plain lexical hasPrefix/equality
            # disjointness comparison never converges on the raw strings,
            # so the OLD code reported no violation at all. Only the new
            # normalization assertion on processingDir catches this.
            overlapEscapesLexicalCheck = evaluate {
              services.cratedigger.processingDir =
                "/data/cratedigger/processing/../../slskd";
              services.cratedigger.slskd.downloadDir = lib.mkForce "/data/slskd";
            };
            # Regression guard: a plainly nested, already-normalized pair
            # must still trip the (unchanged) disjointness assertion.
            disjointRegressionStillFires = evaluate {
              services.cratedigger.processingDir = "/srv/cratedigger/processing";
              services.cratedigger.slskd.downloadDir =
                lib.mkForce "/srv/cratedigger/processing/nested";
            };
            # Must-still-work: the live doc2 wrapper's exact shape
            # (dataDir default "/mnt/virtio/cratedigger" + "/processing",
            # slskd.downloadDir overridden to "/mnt/virtio/music/slskd").
            validLiveShape = evaluate {
              services.cratedigger.processingDir = "/mnt/virtio/cratedigger/processing";
              services.cratedigger.slskd.downloadDir =
                lib.mkForce "/mnt/virtio/music/slskd";
            };
          };
      }
    '''
    return _cached_nix_eval_json(expression)


class TestDefaultHeadlessComposition(unittest.TestCase):
    """The exported module keeps the direct CLI usable without the web."""

    def test_exported_module_installs_cli_without_web_units_or_sockets(
        self,
    ) -> None:
        composition = _shared_module_worlds_rest()["headlessComposition"]
        assert isinstance(composition, dict)
        self.assertFalse(composition["webEnabled"])
        self.assertIn("pipeline-cli", composition["systemPackages"])
        self.assertIn("decision-differential", composition["systemPackages"])
        self.assertFalse(composition["hasWebService"])
        self.assertEqual(composition["cratediggerSockets"], [])


class TestWebAuthenticationModuleContract(unittest.TestCase):
    """The enabled web surface has one fail-closed module-owned perimeter."""

    def test_basic_and_insecure_mode_matrix_is_evaluated_part1(self) -> None:
        """The ``missing``...``rootAccessGroup`` half of the webAuthMatrix
        assertion matrix (issue #1156 item 1).

        See :func:`_shared_module_worlds_web_auth_matrix_part1` for why this
        is now two methods/targets instead of one, and
        ``test_basic_and_insecure_mode_matrix_is_evaluated_part2`` for the
        rest. The ``serviceGroupOverlap``/``nginxGroupOverlap``/
        ``secretGroupOverlap`` and ``rootAccessGroup``/``wheelAccessGroup``
        checks straddled the ``rootAccessGroup``/``wheelAccessGroup``
        boundary in the original single method; each is now asserted once,
        in whichever half holds the world it names, on the identical
        message substring it always checked.
        """
        worlds = _shared_module_worlds_web_auth_matrix_part1()["webAuthMatrixPart1"]
        assert isinstance(worlds, dict)
        self.assertTrue(
            any("exactly one" in message for message in worlds["missing"])
        )
        self.assertEqual(worlds["basic"], [])
        self.assertEqual(worlds["insecure"], [])
        self.assertTrue(
            any("mutually exclusive" in message for message in worlds["conflict"])
        )
        self.assertTrue(
            any("outside /nix/store" in message for message in worlds["storeBasic"])
        )
        self.assertTrue(
            any("canonical DNS hostname" in message for message in worlds["badHost"])
        )
        self.assertTrue(
            any("lowercase canonical" in message for message in worlds["uppercaseHost"])
        )
        self.assertTrue(
            any("not an IP literal" in message for message in worlds["ipHost"])
        )
        self.assertTrue(
            any("nginx-token-safe" in message for message in worlds["injectedBasic"])
        )
        self.assertEqual(worlds["disabled"], [])
        self.assertTrue(
            any("inactive-mode residue" in message for message in worlds["disabledBasic"])
        )
        self.assertTrue(
            any(
                "inactive-mode residue" in message
                for message in worlds["disabledInsecure"]
            )
        )
        # External authorization is a first-class third mode: valid alone,
        # mutually exclusive with both others, and residue while disabled.
        self.assertEqual(worlds["external"], [])
        for world in (
            "externalBasicConflict",
            "externalInsecureConflict",
            "allThreeConflict",
        ):
            self.assertTrue(
                any(
                    "mutually exclusive" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertTrue(
            any(
                "inactive-mode residue" in message
                for message in worlds["disabledExternal"]
            )
        )
        for world in ("serviceGroupOverlap", "nginxGroupOverlap"):
            self.assertTrue(
                any("must be dedicated" in message for message in worlds[world]),
                (world, worlds[world]),
            )
        self.assertTrue(
            any(
                "forbidden authority group" in message
                for message in worlds["rootAccessGroup"]
            ),
            worlds["rootAccessGroup"],
        )

    def test_basic_and_insecure_mode_matrix_is_evaluated_part2(self) -> None:
        """The ``wheelAccessGroup``...``nginxRestartDisabled`` half of the
        webAuthMatrix assertion matrix (issue #1156 item 1).

        See ``test_basic_and_insecure_mode_matrix_is_evaluated_part1`` for
        the split rationale and the two straddling checks this method
        carries its half of.
        """
        worlds = _shared_module_worlds_web_auth_matrix_part2()["webAuthMatrixPart2"]
        assert isinstance(worlds, dict)
        self.assertTrue(
            any(
                "must be dedicated" in message
                for message in worlds["secretGroupOverlap"]
            ),
            worlds["secretGroupOverlap"],
        )
        self.assertTrue(
            any(
                "forbidden authority group" in message
                for message in worlds["wheelAccessGroup"]
            ),
            worlds["wheelAccessGroup"],
        )
        self.assertEqual(worlds["explicitOperatorGroup"], [])
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxAccountSecretGroup"]
            ),
            worlds["nginxAccountSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxReverseSecretGroup"]
            ),
            worlds["nginxReverseSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxAliasedReverseSecretGroup"]
            ),
            worlds["nginxAliasedReverseSecretGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxServiceMediaGroup"]
            ),
            worlds["nginxServiceMediaGroup"],
        )
        self.assertTrue(
            any(
                "forbids nginx account/service membership" in message
                for message in worlds["nginxServiceNumericMediaGroup"]
            ),
            worlds["nginxServiceNumericMediaGroup"],
        )
        self.assertTrue(
            any(
                "primary group" in message
                for message in worlds["nginxPrimaryServiceGroup"]
            ),
            worlds["nginxPrimaryServiceGroup"],
        )
        for world in (
            "nginxServiceRootUserOverride",
            "nginxServiceRootGroupOverride",
        ):
            self.assertTrue(
                any(
                    "final nginx.service User and Group" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertTrue(
            any(
                "membership in web.accessGroup" in message
                for message in worlds["nginxMissingAccessGroup"]
            ),
            worlds["nginxMissingAccessGroup"],
        )
        self.assertEqual(worlds["nginxNumericAccessGroup"], [])
        self.assertTrue(
            any(
                "cratedigger-web.service SupplementaryGroups" in message
                for message in worlds["webServiceCredentialGroup"]
            ),
            worlds["webServiceCredentialGroup"],
        )
        for world in (
            "webServiceRootOverride",
            "webServiceNginxGroupOverride",
        ):
            self.assertTrue(
                any(
                    "final cratedigger-web.service User and Group" in message
                    for message in worlds[world]
                ),
                (world, worlds[world]),
            )
        self.assertEqual(worlds["nginxReverseUnrelatedGroup"], [])
        self.assertTrue(
            any(
                "requires services.nginx.enableReload" in message
                for message in worlds["nginxReloadDisabled"]
            ),
            worlds["nginxReloadDisabled"],
        )
        self.assertTrue(
            any(
                "requires systemd.services.nginx.restartIfChanged"
                in message
                for message in worlds["nginxRestartDisabled"]
            ),
            worlds["nginxRestartDisabled"],
        )

    def test_injected_basic_path_cannot_render_toplevel(self) -> None:
        expression = r'''
          let
            f = builtins.getFlake (toString ./.);
            modulePkgs = import f.inputs.nixpkgs {
              system = builtins.currentSystem;
            };
            beetsPackage = import ./nix/beets.nix { pkgs = modulePkgs; };
            system = f.inputs.nixpkgs.lib.nixosSystem {
              system = builtins.currentSystem;
              modules = [
                { nixpkgs.pkgs = modulePkgs; }
                f.nixosModules.default
                ({ ... }: {
                  services.cratedigger = {
                    enable = true;
                    src = ./.;
                    user = "cratedigger";
                    group = "cratedigger";
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = {
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                    };
                    web = {
                      enable = true;
                      hostName = "music.example.test";
                      basicAuthFile =
                        "/run/secrets/file; satisfy any; allow all; #";
                    };
                  };
                })
              ];
            };
          in system.config.system.build.toplevel.drvPath
        '''
        result = subprocess.run(
            ["nix", "eval", "--impure", "--expr", expression],
            cwd=REPO_ROOT,
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("nginx-token-safe segments", result.stderr)

    def test_merged_basic_gateway_values_are_exact(self) -> None:
        worlds = _shared_module_worlds_rest()["mergedGateway"]
        assert isinstance(worlds, dict)
        dual = worlds["dualStack"]
        ipv4 = worlds["ipv4Only"]
        insecure = worlds["insecureRecovery"]
        alternate = worlds["alternateBasic"]
        external = worlds["externalMode"]
        self.assertEqual(dual["failures"], [])
        self.assertEqual(ipv4["failures"], [])
        self.assertEqual(insecure["failures"], [])
        self.assertEqual(alternate["failures"], [])
        self.assertEqual(external["failures"], [])
        self.assertEqual(
            dual["listen"],
            [
                {"addr": "127.0.0.1", "port": 8086},
                {"addr": "[::1]", "port": 8086},
            ],
        )
        self.assertEqual(
            ipv4["listen"], [{"addr": "127.0.0.1", "port": 8086}]
        )
        self.assertEqual(dual["hostName"], "music.example.test")
        self.assertIn("gateway_mode=basic", dual["gatewayPolicy"])
        self.assertTrue(dual["gatewayPolicy"].startswith("format=1\n"))
        self.assertIn(
            "gateway_credential_path=/run/secrets/cratedigger.htpasswd",
            dual["gatewayPolicy"],
        )
        self.assertIn("gateway_mode=insecure", insecure["gatewayPolicy"])
        self.assertIn(
            "gateway_credential_path=-", insecure["gatewayPolicy"]
        )
        self.assertIn(
            "gateway_marker_path=/run/cratedigger-web/gateway-policy-",
            insecure["gatewayPolicy"],
        )
        # External mode is its own policy identity: a distinct gateway_mode,
        # no credential, no Basic attachment, and a marker that cannot be
        # produced by either other mode's descriptor.
        self.assertIn("gateway_mode=external", external["gatewayPolicy"])
        self.assertIn("gateway_credential_path=-", external["gatewayPolicy"])
        self.assertEqual(external["basicAuthFile"], None)
        self.assertEqual(external["rootBasicAuthFile"], None)
        self.assertNotEqual(
            external["gatewayPolicy"], insecure["gatewayPolicy"]
        )
        marker_pattern = re.compile(
            r"if \(!-f "
            r"(/run/cratedigger-web/gateway-policy-[0-9a-f]{64})\)"
        )
        basic_marker = marker_pattern.search(dual["gatewayExtra"])
        ipv4_marker = marker_pattern.search(ipv4["gatewayExtra"])
        insecure_marker = marker_pattern.search(insecure["gatewayExtra"])
        alternate_marker = marker_pattern.search(alternate["gatewayExtra"])
        self.assertIsNotNone(basic_marker)
        self.assertIsNotNone(ipv4_marker)
        self.assertIsNotNone(insecure_marker)
        self.assertIsNotNone(alternate_marker)
        assert basic_marker is not None
        assert ipv4_marker is not None
        assert insecure_marker is not None
        assert alternate_marker is not None
        self.assertEqual(basic_marker.group(1), ipv4_marker.group(1))
        self.assertNotEqual(basic_marker.group(1), insecure_marker.group(1))
        self.assertNotEqual(basic_marker.group(1), alternate_marker.group(1))
        self.assertNotEqual(insecure_marker.group(1), alternate_marker.group(1))
        self.assertEqual(
            dual["basicAuthFile"], "/run/secrets/cratedigger.htpasswd"
        )
        self.assertEqual(dual["rootBasicAuthFile"], None)
        self.assertEqual(dual["mergedBasicAuthFile"], None)
        self.assertEqual(
            dual["proxyPass"], "http://unix:/run/cratedigger-web/web.sock:"
        )
        self.assertNotIn("proxy_read_timeout", dual["gatewayExtra"])
        self.assertIn(
            "proxy_pass_request_headers off;", dual["gatewayExtra"]
        )
        self.assertIn(
            "proxy_set_header X-Cratedigger-Request-Channel browser;",
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header Content-Security-Policy "frame-ancestors \'none\'" always;',
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header X-Frame-Options "DENY" always;',
            dual["gatewayExtra"],
        )
        self.assertIn(
            'add_header Cross-Origin-Resource-Policy "same-origin" always;',
            dual["gatewayExtra"],
        )
        self.assertEqual(
            dual["healthProxy"],
            "http://unix:/run/cratedigger-web/web.sock:/healthz",
        )
        self.assertIn('if ($request_uri != "/healthz")', dual["healthExtra"])
        self.assertIn("limit_except GET", dual["healthExtra"])
        self.assertIn("auth_basic off;", dual["healthExtra"])
        self.assertIn("proxy_http_version 1.0;", dual["healthExtra"])
        self.assertIn("proxy_pass_request_body off;", dual["healthExtra"])
        self.assertIn('proxy_set_header Connection close;', dual["healthExtra"])
        self.assertIn('proxy_set_header Content-Length "";', dual["healthExtra"])
        self.assertIn(
            'proxy_set_header Transfer-Encoding "";', dual["healthExtra"]
        )
        self.assertTrue(dual["rejectDefault"])
        self.assertEqual(dual["rejectConfig"], "return 444;")
        self.assertEqual(
            dual["socketListen"], ["/run/cratedigger-web/web.sock"]
        )
        self.assertEqual(dual["socketGroup"], "cratedigger-web")
        self.assertEqual(dual["socketMode"], "0660")
        for dependency in (
            "cratedigger-db-migrate.service",
            "cratedigger-web.socket",
        ):
            self.assertIn(dependency, dual["webAfter"])
            self.assertIn(dependency, dual["webRequires"])
        self.assertEqual(dual["webGroups"], ["cratedigger-web"])
        self.assertEqual(dual["webUser"], "cratedigger")
        self.assertEqual(dual["webGroup"], "cratedigger")
        self.assertTrue(dual["webStartPre"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-basic-auth-validate", dual["webStartPre"][0]
        )
        self.assertIn(
            "cratedigger-web-basic-auth-app-isolation",
            dual["webStartPre"][1],
        )
        self.assertFalse(dual["webStartPre"][1].startswith("+"))
        self.assertEqual(len(dual["webStartPre"]), 2)
        self.assertEqual(insecure["webStartPre"], [])
        self.assertTrue(dual["nginxEnableReload"])
        self.assertTrue(insecure["nginxEnableReload"])
        self.assertTrue(alternate["nginxEnableReload"])
        self.assertTrue(dual["nginxRestartIfChanged"])
        self.assertTrue(insecure["nginxRestartIfChanged"])
        self.assertTrue(alternate["nginxRestartIfChanged"])
        self.assertIn("cratedigger-web.socket", dual["nginxAfter"])
        self.assertIn("cratedigger-web.socket", dual["nginxWants"])
        self.assertNotIn("cratedigger-web.socket", dual["nginxRequires"])
        self.assertEqual(dual["nginxUnit"], ipv4["nginxUnit"])
        self.assertEqual(dual["nginxUnit"], insecure["nginxUnit"])
        self.assertEqual(dual["nginxUnit"], alternate["nginxUnit"])
        self.assertEqual(dual["nginxGroups"], ["cratedigger-web"])
        self.assertEqual(dual["nginxUser"], "nginx")
        self.assertEqual(dual["nginxGroup"], "nginx")
        self.assertIn("cratedigger-web", dual["nginxUserGroups"])
        self.assertIn("cratedigger-web", dual["applicationUserGroups"])
        self.assertTrue(dual["startPre"][0].startswith("+"))
        self.assertIn(
            "cratedigger-web-gateway-clear-start", dual["startPre"][0]
        )
        self.assertFalse(dual["startPre"][1].startswith("+"))
        self.assertIn(
            "cratedigger-web-nginx-effective-identity", dual["startPre"][1]
        )
        self.assertTrue(dual["startPre"][2].startswith("+"))
        self.assertIn("cratedigger-web-gateway-start", dual["startPre"][2])
        self.assertIn("nginx-pre-start", dual["startPre"][3])
        self.assertTrue(dual["reload"][0].startswith("+"))
        self.assertIn("cratedigger-web-gateway-prepare-reload", dual["reload"][0])
        self.assertIn("nginx", dual["reload"][1])
        self.assertIn("kill", dual["reload"][2])
        self.assertTrue(dual["reload"][3].startswith("+"))
        self.assertIn("cratedigger-web-gateway-finish-reload", dual["reload"][3])
        self.assertEqual(dual["startPre"], insecure["startPre"])
        self.assertEqual(dual["startPre"], alternate["startPre"])
        self.assertEqual(dual["reload"], insecure["reload"])
        self.assertEqual(dual["reload"], alternate["reload"])
        self.assertEqual(insecure["basicAuthFile"], None)
        self.assertIn(
            "cratedigger-web-gateway-clear-start", insecure["startPre"][0]
        )
        self.assertIn(
            "cratedigger-web-gateway-prepare-reload", insecure["reload"][0]
        )
        self.assertIn(
            "cratedigger-web-gateway-finish-reload", insecure["reload"][3]
        )

    def test_socket_activation_and_access_group_are_explicit(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn(
            'webRuntimeDirectory = "/run/cratedigger-web";',
            text,
        )
        self.assertIn(
            'webSocketPath = "${webRuntimeDirectory}/web.sock";',
            text,
        )
        self.assertIn("systemd.sockets.cratedigger-web", text)
        self.assertIn("listenStreams = [webSocketPath];", text)
        self.assertIn('SocketMode = "0660";', text)
        self.assertIn("SocketGroup = cfg.web.accessGroup;", text)
        self.assertIn(
            '"d /run/cratedigger-web 0750 root ${cfg.web.accessGroup} -"', text
        )
        self.assertIn('"cratedigger-web.socket"', text)
        self.assertIn(
            "${config.services.nginx.user}.extraGroups = "
            "[cfg.web.accessGroup];",
            text,
        )

    def test_gateway_uses_configured_addresses_and_default_reject(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertNotIn("cfg.web.port", text)
        self.assertIn("services.nginx.virtualHosts", text)
        self.assertIn("webGatewayListen = map", text)
        self.assertIn("inherit addr;", text)
        self.assertIn("cfg.web.gatewayAddresses;", text)
        self.assertIn(
            'default = ["127.0.0.1"] ++ optional config.networking.enableIPv6 "[::1]";',
            text,
        )
        self.assertIn("port = cfg.web.gatewayPort;", text)
        self.assertIn("serverName = webHostName;", text)
        self.assertIn("default = true;", text)
        self.assertIn("return 444;", text)
        self.assertIn('locations."= /healthz"', text)
        self.assertIn(':/healthz";', text)
        self.assertIn('if (\'\'$request_uri != "/healthz")', text)
        self.assertIn("limit_except GET", text)

    def test_gateway_reconstructs_only_reviewed_backend_headers(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("proxy_pass_request_headers off;", text)
        self.assertIn("proxy_set_header Host ${webHostName};", text)
        self.assertIn(
            "proxy_set_header X-Cratedigger-Request-Channel browser;", text
        )
        self.assertIn("proxy_set_header Content-Length ''$content_length;", text)
        self.assertIn("proxy_set_header Content-Type ''$content_type;", text)
        self.assertIn("proxy_set_header Accept ''$http_accept;", text)
        self.assertIn("proxy_set_header Range ''$http_range;", text)
        self.assertIn("proxy_set_header Origin ''$http_origin;", text)
        self.assertIn("proxy_set_header Referer ''$http_referer;", text)
        self.assertNotIn("proxy_set_header Authorization", text)
        self.assertNotIn("proxy_set_header Cookie", text)
        self.assertIn("Content-Security-Policy", text)
        self.assertIn("frame-ancestors 'none'", text)
        self.assertIn("X-Frame-Options", text)
        self.assertIn("Cross-Origin-Resource-Policy", text)

    def test_web_wrapper_uses_exact_canonical_https_origin(self) -> None:
        text = _nix_source(MODULE_NIX)
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_end = text.index(
            'writeShellScriptBin "cratedigger-youtube-ingest"', web_start
        )
        wrapper = text[web_start:web_end]
        self.assertIn(
            '--canonical-origin "https://${webHostName}"',
            wrapper,
        )

    def test_web_wrapper_passes_insecure_flag_only_for_explicit_mode(
        self,
    ) -> None:
        text = _nix_source(MODULE_NIX)
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_end = text.index(
            'writeShellScriptBin "cratedigger-youtube-ingest"', web_start
        )
        wrapper = text[web_start:web_end]

        self.assertIn(
            '${optionalString cfg.web.enableInsecure "--insecure-mode"}',
            wrapper,
        )
        self.assertEqual(wrapper.count("--insecure-mode"), 1)

    def test_web_wrapper_passes_external_flag_only_for_external_mode(
        self,
    ) -> None:
        text = _nix_source(MODULE_NIX)
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        web_end = text.index(
            'writeShellScriptBin "cratedigger-youtube-ingest"', web_start
        )
        wrapper = text[web_start:web_end]

        self.assertIn(
            '${optionalString cfg.web.externalAuth "--external-auth-mode"}',
            wrapper,
        )
        self.assertEqual(wrapper.count("--external-auth-mode"), 1)

    def test_gateway_policy_validator_admits_exactly_three_modes(self) -> None:
        """The policy descriptor grammar is the fail-closed mode authority.

        A mode the validator does not name must fall through to the invalid
        arm, so a descriptor can never select a policy nginx would not have
        been configured for.
        """
        text = _nix_source(MODULE_NIX)
        read_policy = text[
            text.index("webGatewayReadPolicy = ''") :
            text.index("webGatewayAssertPolicyUnchanged = ''")
        ]
        self.assertIn("basic)", read_policy)
        self.assertIn("insecure)", read_policy)
        self.assertIn("external)", read_policy)
        self.assertIn("policy descriptor has an invalid mode", read_policy)
        self.assertIn(
            "external policy must not name a credential", read_policy
        )

    def test_external_auth_option_is_declared_and_documented(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("externalAuth = mkOption", text)
        options = text[
            text.index("externalAuth = mkOption") :
            text.index("externalAuth = mkOption") + 1200
        ]
        self.assertIn("type = types.bool;", options)
        self.assertIn("default = false;", options)

    def test_basic_secret_is_runtime_only_and_checked_before_nginx_start(
        self,
    ) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("basicAuthFile = mkOption", text)
        self.assertNotIn("basicAuth = ", text)
        self.assertIn(
            'writeShellScript "cratedigger-web-basic-auth-validate"', text
        )
        self.assertIn(
            '"cratedigger-web-basic-auth-app-isolation"', text
        )
        self.assertIn(
            "the web application can read its gateway credential", text
        )
        self.assertIn("systemd.services.nginx = mkIf cfg.web.enable", text)
        self.assertIn("ExecStartPre = lib.mkBefore", text)
        self.assertIn("ExecReload = lib.mkBefore", text)
        self.assertIn("ExecReload = lib.mkAfter", text)
        marker_helpers = text[
            text.index("webGatewayClearMarkers =") :
            text.index('writeShellScript "cratedigger-web-gateway-start"')
        ]
        start_script = text[
            text.index('writeShellScript "cratedigger-web-gateway-start"') :
            text.index(
                'writeShellScript "cratedigger-web-gateway-prepare-reload"'
            )
        ]
        reload_prepare = text[
            text.index(
                'writeShellScript "cratedigger-web-gateway-prepare-reload"'
            ) :
            text.index(
                'writeShellScript "cratedigger-web-gateway-finish-reload"'
            )
        ]
        reload_finish = text[
            text.index(
                'writeShellScript "cratedigger-web-gateway-finish-reload"'
            ) :
            text.index("webNginxUserExtraGroups")
        ]
        self.assertLess(
            start_script.index("${webBasicAuthValidationScript}"),
            start_script.index("${webGatewayPublishMarker}"),
        )
        self.assertLess(
            reload_prepare.index("${webGatewayClearMarkers}"),
            reload_prepare.index("${webBasicAuthValidationScript}"),
        )
        self.assertNotIn("${webGatewayPublishMarker}", reload_prepare)
        self.assertIn("${webGatewayPublishMarker}", reload_finish)
        self.assertNotIn("webGatewayPendingMarker", text)
        self.assertNotIn("webGatewayStageMarker", text)
        self.assertIn("${pkgs.findutils}/bin/find", marker_helpers)

        self.assertIn(
            "${lib.escapeShellArg webRuntimeDirectory}", marker_helpers
        )
        self.assertIn("-maxdepth 1", marker_helpers)
        self.assertIn(
            '-name ${lib.escapeShellArg "gateway-policy-*"}',
            marker_helpers,
        )
        self.assertIn("-delete", marker_helpers)
        self.assertIn("-m 0440", marker_helpers)
        self.assertIn("-o root", marker_helpers)
        self.assertIn(
            "-g ${lib.escapeShellArg cfg.web.accessGroup}", marker_helpers
        )
        self.assertIn('"$gateway_marker_path"', marker_helpers)
        self.assertIn(
            "gateway_marker_path=${webGatewayActiveMarker}",
            text,
        )
        self.assertNotIn(
            ". ${lib.escapeShellArg webGatewayPolicyFile}", marker_helpers
        )
        self.assertIn("mapfile -t policy_lines", marker_helpers)
        self.assertIn("policy descriptor must contain exactly four lines", text)
        self.assertIn("gateway_policy_sha256", text)
        self.assertIn("webGatewayWriteReloadReceipt", text)
        self.assertIn("webGatewayReadReloadReceipt", text)
        self.assertIn("policy_sha256=", text)
        self.assertIn("gateway_credential_sha256=", text)
        self.assertIn("credential changed after reload validation", text)
        self.assertIn(
            "policy descriptor differs from the validated receipt",
            text,
        )
        self.assertIn(
            'configured_path="\'\'${1:-}"',
            text,
        )
        self.assertIn("realpath -e", text)
        self.assertIn("runuser -u", text)
        self.assertIn("${pkgs.acl}/bin/getfacl", text)
        self.assertIn("expected_target_acl", text)
        self.assertIn("only the base 0440 ACL", text)
        self.assertIn("must not be group/other writable", text)
        self.assertIn("must not have extended/default ACLs", text)
        self.assertIn("check_ancestors", text)
        self.assertIn("resolved credential target is inside /nix/store", text)

    def test_nginx_effective_identity_is_checked_before_gateway_readiness(
        self,
    ) -> None:
        text = _nix_source(MODULE_NIX)
        identity_start = text.index(
            '"cratedigger-web-nginx-effective-identity"'
        )
        identity_script = text[
            identity_start : text.index("webGatewayClearMarkers =")
        ]
        self.assertIn("${pkgs.coreutils}/bin/id -u", identity_script)
        self.assertIn("${pkgs.coreutils}/bin/id -g", identity_script)
        self.assertIn("${pkgs.coreutils}/bin/id -G", identity_script)
        self.assertIn("webForbiddenAuthorityGroups", identity_script)
        self.assertIn("effective nginx UID must not be 0", identity_script)
        self.assertIn(
            "effective nginx group set contains forbidden",
            identity_script,
        )
        self.assertIn(
            "effective nginx group set lacks required accessGroup",
            identity_script,
        )

    def test_vm_tls_private_key_is_generated_outside_tracked_source(
        self,
    ) -> None:
        text = _nix_source(MODULE_VM_NIX)
        self.assertNotRegex(
            text,
            r"-----BEGIN (?:EC |RSA |)PRIVATE KEY-----",
        )
        self.assertIn(
            'pkgs.runCommand "cratedigger-module-vm-tls"',
            text,
        )
        self.assertIn(
            "security.pki.certificateFiles = [publicTlsCertificate];",
            text,
        )


class TestModuleVmPerformanceContract(unittest.TestCase):
    def test_guest_reads_the_nix_store_from_a_local_image(self) -> None:
        text = _nix_source(MODULE_VM_NIX)
        self.assertIn("virtualisation.useNixStoreImage = true;", text)
        self.assertIn("virtualisation.writableStore = true;", text)

    def test_guest_declares_the_verified_core_count(self) -> None:
        """Issue #1131: an unset ``virtualisation.cores`` silently inherits
        the qemu-vm module's guest default of 1, serializing PostgreSQL,
        nginx, ~10 switch-to-configuration calls, and 2 reboots onto one
        emulated core. Pinned to the EXACT value that was actually run
        through ``nix build .#checks.x86_64-linux.moduleVm`` under KVM
        (4:33 wall vs 5:34 at the old default of 1) rather than a loose
        ``> 1`` bound — a future edit to e.g. 2 cores would pass a `> 1`
        check without ever being verified under the real VM check. Guest
        core count changes guest scheduling/timing, which can in principle
        surface a latent race in a test that happens to be sensitive to
        it, so a value change here should be a deliberate, re-verified
        decision under the real VM check, not a silent drift.
        """
        text = _nix_source(MODULE_VM_NIX)
        match = re.search(r"virtualisation\.cores\s*=\s*(\d+)\s*;", text)
        self.assertIsNotNone(match, "virtualisation.cores must be set explicitly")
        assert match is not None
        self.assertEqual(int(match.group(1)), 4)


class TestImporterServiceContract(unittest.TestCase):
    def test_importer_wrapper_and_service_are_defined(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('writeShellScriptBin "cratedigger-importer"', text)
        self.assertIn("${src}/scripts/importer.py", text)
        self.assertIn("systemd.services.cratedigger-importer", text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${importerPkg}/bin/cratedigger-importer"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${pipelineDsn}"', text)
        self.assertIn("WorkingDirectory = cfg.stateDir", text)

    def test_importer_service_restarts_on_switch(self) -> None:
        """Deploy should restart the importer worker.

        Launch-fence recovery handles mid-job kills at startup; leaving a worker
        dead after switch-to-configuration is worse than restarting it.
        """
        text = _nix_source(MODULE_NIX)
        # Find the importer service block and assert restartIfChanged=true
        # appears within it (not just somewhere in the file).
        importer_block_start = text.index("systemd.services.cratedigger-importer")
        importer_block_end = text.index(
            "systemd.services.cratedigger-import-preview-worker"
        )
        importer_block = text[importer_block_start:importer_block_end]
        self.assertIn("restartIfChanged = true", importer_block)

    def test_importer_service_kill_mode_is_mixed(self) -> None:
        """Issue #1089 B1: the graceful drain is only real under
        ``KillMode = "mixed"``. The systemd DEFAULT (``control-group``)
        signals every process in the cgroup at once — including the beets
        child subprocess, which has no handler and dies immediately
        regardless of the parent's own drain logic (the RCA's own <1s
        stop). ``mixed`` signals only the main PID, so a deploy stop lets
        the child actually finish before the bounded ``TimeoutStopSec``
        escalates to a cgroup-wide SIGKILL.
        """
        text = _nix_source(MODULE_NIX)
        importer_block_start = text.index("systemd.services.cratedigger-importer")
        importer_block_end = text.index(
            "systemd.services.cratedigger-import-preview-worker"
        )
        importer_block = text[importer_block_start:importer_block_end]
        self.assertIn('KillMode = "mixed"', importer_block)
        self.assertIn('TimeoutStopSec = "10min"', importer_block)

    def test_preview_worker_service_restarts_on_switch(self) -> None:
        """Same rationale as the importer worker.

        requeue_stale_import_preview_jobs handles mid-measurement kills at
        startup; deploy should not leave the preview worker dead.
        """
        text = _nix_source(MODULE_NIX)
        preview_block_start = text.index(
            "systemd.services.cratedigger-import-preview-worker"
        )
        # The next service definition or end of the systemd.services block
        # bounds the preview-worker block. Use a sentinel that's safe.
        preview_block = text[preview_block_start:preview_block_start + 4000]
        self.assertIn("restartIfChanged = true", preview_block)

    def test_services_consume_one_immutable_runtime_config(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('configTemplate = pkgs.writeText "cratedigger-config.ini"', text)
        self.assertNotIn("renderConfigScript", text)
        self.assertNotIn("systemd.services.cratedigger-config-render", text)

    def test_preview_worker_wrapper_service_and_worker_count_are_defined(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('writeShellScriptBin "cratedigger-import-preview-worker"', text)
        self.assertIn("${src}/scripts/import_preview_worker.py", text)
        self.assertIn("systemd.services.cratedigger-import-preview-worker", text)
        # Preview is mandatory: service gated only on importer.enable.
        self.assertIn("mkIf cfg.importer.enable", text)
        self.assertIn("previewWorkers", text)
        self.assertIn("default = 2", text)
        self.assertIn("cfg.importer.previewWorkers >= 1", text)
        self.assertIn("services.cratedigger.importer.previewWorkers must be at least 1", text)
        self.assertIn('--workers ${toString cfg.importer.previewWorkers}', text)
        self.assertIn('after = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('requires = ["cratedigger-db-migrate.service"]', text)
        self.assertIn('ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker"', text)
        self.assertIn('Environment = "PIPELINE_DB_DSN=${pipelineDsn}"', text)


class TestSearchSchedulerConfigContract(unittest.TestCase):
    def test_page_size_preserves_capacity_for_both_cohorts(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn(
            "cfg.searchSettings.numberOfAlbumsToGrab >= 2",
            text,
        )
        self.assertIn(
            "services.cratedigger.searchSettings.numberOfAlbumsToGrab "
            "must be at least 2",
            text,
        )


class TestPinnedPackageSetContract(unittest.TestCase):
    """The runtime closure builds from cratedigger's own flake.lock, not the
    consumer's nixpkgs (tier-2 plan U2, R1 / KTD1).

    ``nix/module.nix`` must build its python env from ``cfg.packageSet``
    (defaulting to the ambient ``pkgs`` so the file stays importable
    standalone), and ``flake.nix`` must export ``nixosModules.default`` as a
    wrapper that pins ``packageSet`` to the flake's own locked nixpkgs. A
    consumer setting ``packageSet`` explicitly is the deliberate escape
    hatch — it forfeits the tested-closure guarantee.
    """

    def test_module_builds_package_from_packageSet(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("packageSet = mkOption", text)
        self.assertIn("cratedigger = cfg.packageSet.callPackage ./package.nix", text)
        self.assertNotIn("pkgs.callPackage ./package.nix", text)

    def test_flake_export_pins_packageSet_to_own_lock(self) -> None:
        text = _nix_source(FLAKE_NIX)
        self.assertIn("nixosModules.default", text)
        self.assertIn("imports = [ ./nix/module.nix ];", text)
        self.assertIn(
            "services.cratedigger.packageSet = lib.mkDefault", text,
            "flake.nix must pin packageSet via mkDefault so a consumer's "
            "explicit packageSet (the escape hatch) still wins",
        )
        self.assertIn("pkgs.stdenv.hostPlatform.system", text)

    def test_moduleVm_consumes_the_wrapped_export(self) -> None:
        """The VM gate must exercise what consumers actually import."""
        text = _nix_source(FLAKE_NIX)
        self.assertIn("cratediggerModule = self.nixosModules.default;", text)


class TestExternalBeetsRuntimeCapability(unittest.TestCase):
    """The public module consumes one externally owned Beets capability."""

    RUNTIME_FIELDS = (
        "package",
        "configDir",
        "expectedLibrary",
        "expectedDirectory",
        "expectedStateFile",
        "expectedSecretInclude",
    )

    @staticmethod
    def _string_list(value: object) -> list[str]:
        if not isinstance(value, list):
            raise TypeError(value)
        strings = [item for item in value if isinstance(item, str)]
        if len(strings) != len(value):
            raise AssertionError(value)
        return strings

    def test_capability_assertions_cover_happy_missing_invalid_and_disabled(self) -> None:
        worlds = _shared_module_worlds_rest()["beetsCapability"]
        assert isinstance(worlds, dict)
        self.assertEqual(worlds["valid"], [])
        missing = worlds["missing"]
        assert isinstance(missing, dict)
        for field in self.RUNTIME_FIELDS:
            messages = self._string_list(missing[field])
            self.assertTrue(messages, field)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message and "required" in message
                    for message in messages
                ),
                (field, messages),
            )
        incompatible_messages = self._string_list(worlds["incompatiblePackage"])
        self.assertTrue(
            any(
                "package.pythonModule must match services.cratedigger.packageSet.python3"
                in message
                for message in incompatible_messages
            ),
            incompatible_messages,
        )
        ambient_mismatch = worlds["ambientPackageMismatch"]
        assert isinstance(ambient_mismatch, dict)
        self.assertIs(ambient_mismatch["distinct"], True)
        ambient_messages = self._string_list(ambient_mismatch["failures"])
        self.assertTrue(
            any(
                "package.pythonModule must match services.cratedigger.packageSet.python3"
                in message
                for message in ambient_messages
            ),
            ambient_messages,
        )
        invalid_paths = worlds["invalidPaths"]
        assert isinstance(invalid_paths, dict)
        for field, value in invalid_paths.items():
            messages = self._string_list(value)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message
                    and "absolute normalized path" in message
                    for message in messages
                ),
                (field, messages),
            )
        root_paths = worlds["rootPaths"]
        assert isinstance(root_paths, dict)
        for field, value in root_paths.items():
            messages = self._string_list(value)
            self.assertTrue(
                any(
                    f"beets.runtime.{field}" in message
                    and "must not be /" in message
                    for message in messages
                ),
                (field, messages),
            )
        root_identity = self._string_list(worlds["rootIdentity"])
        self.assertTrue(
            any("guarded application identity" in message for message in root_identity),
            root_identity,
        )
        numeric_identity = self._string_list(worlds["numericIdentity"])
        self.assertTrue(
            any("guarded application identity" in message for message in numeric_identity),
            numeric_identity,
        )
        self.assertEqual(
            worlds["defaultIdentity"],
            {
                "user": "cratedigger",
                "group": "cratedigger",
                "serviceUser": "cratedigger",
                "serviceGroup": "cratedigger",
            },
        )
        self.assertEqual(worlds["disabled"], {"assertions": [], "services": []})

    def test_readiness_and_role_state_capabilities_evaluate(self) -> None:
        units = _shared_module_worlds_rest()["beetsReadiness"]
        assert isinstance(units, dict)
        readiness = {
            "beets-config-ready.service",
            "beets-secret-ready.service",
        }
        typed_units: dict[str, dict[str, list[str]]] = {}
        for role, value in units.items():
            if not isinstance(value, dict):
                raise TypeError(value)
            unit = {
                field: self._string_list(value.get(field))
                for field in (
                    "after",
                    "wants",
                    "requires",
                    "bindReadOnlyPaths",
                    "bindPaths",
                    "readWritePaths",
                )
            }
            typed_units[role] = unit
            self.assertLessEqual(readiness, set(unit["after"]), role)
            if role in ("main", "census"):
                # Both are timer-driven, restartIfChanged=false oneshots
                # (CLAUDE.md's migration-hold rationale): they intentionally
                # use wants+after, never requires, so the readiness units'
                # own restart-on-deploy never SIGTERMs a mid-flight run.
                self.assertLessEqual(readiness, set(unit["wants"]), role)
                self.assertTrue(
                    readiness.isdisjoint(unit["requires"]),
                    (role, unit["requires"]),
                )
            else:
                self.assertLessEqual(readiness, set(unit["requires"]), role)
            self.assertNotIn("/etc/beets", unit["readWritePaths"], role)
        state = "/var/lib/beets/state.pickle"
        for role in ("main", "preview", "web", "census"):
            # #1142: the census oneshot calls enforce_beets_startup(role="web")
            # and must bind the state file read-only exactly like the other
            # web-role/observer callers, or the host-writable group
            # permission trips state_writable_by_reader at startup.
            self.assertIn(f"-{state}", typed_units[role]["bindReadOnlyPaths"], role)
            self.assertNotIn(state, typed_units[role]["bindPaths"], role)
        self.assertIn(f"-{state}", typed_units["importer"]["bindPaths"])
        self.assertIn(f"-{state}", typed_units["importer"]["readWritePaths"])
        for role in ("importer", "web"):
            self.assertIn("-/srv/music", typed_units[role]["readWritePaths"], role)
            self.assertIn("-/srv/beets", typed_units[role]["readWritePaths"], role)
        self.assertIn("-/srv/music", typed_units["main"]["bindReadOnlyPaths"])
        self.assertIn("-/srv/beets", typed_units["main"]["bindReadOnlyPaths"])
        for role in ("main", "preview", "census"):
            self.assertNotIn("/srv/music", typed_units[role]["readWritePaths"], role)
            self.assertNotIn("/srv/beets", typed_units[role]["readWritePaths"], role)
        text = _nix_source(MODULE_NIX)
        self.assertIn('missingOkExternalPath = path: map (value: "-${value}")', text)
        self.assertIn(
            'BindPaths = missingOkExternalPath cfg.beets.runtime.expectedStateFile;',
            text,
        )

    def test_closure_config_environment_and_removal_ratchets(self) -> None:
        text = _nix_source(MODULE_NIX)
        package = _nix_source(PACKAGE_NIX)
        shell = _nix_source(SHELL_NIX)
        self.assertIn("beetsPackage = cfg.beets.runtime.package;", text)
        self.assertIn(
            "cratedigger = cfg.packageSet.callPackage ./package.nix { inherit beetsPackage; };",
            text,
        )
        self.assertIn('configTemplate = pkgs.writeText "cratedigger-config.ini"', text)
        for line in (
            "directory = ${cfg.beets.runtime.expectedDirectory}",
            "library = ${cfg.beets.runtime.expectedLibrary}",
            "config_dir = ${cfg.beets.runtime.configDir}",
            "state_file = ${cfg.beets.runtime.expectedStateFile}",
            "python = ${pythonEnv}/bin/python",
            "secret_include = ${cfg.beets.runtime.expectedSecretInclude}",
        ):
            self.assertIn(line, text)
        self.assertEqual(
            text.count('export BEETSDIR="${cfg.beets.runtime.configDir}"'),
            1,
        )
        self.assertEqual(
            text.count('export CRATEDIGGER_RUNTIME_CONFIG="${configTemplate}"'),
            1,
        )
        self.assertEqual(text.count("${beetsRuntimeEnvironment}"), 11)
        for wrapper in (
            "cratedigger",
            "cratedigger-importer",
            "cratedigger-import-preview-worker",
            "cratedigger-web",
            "cratedigger-check-beets-config",
            # #1142 — the daily retag-divergence census oneshot admits the
            # runtime config the same explicit way (--config/--runtime-dir),
            # not the env-var-only shape cratedigger-unfindable/pipeline-cli
            # use.
            "cratedigger-retag-census",
            "cratedigger-library-completeness-census",
        ):
            start = text.index(f'writeShellScriptBin "{wrapper}"')
            block = text[start:start + 1800]
            self.assertIn("${beetsRuntimeEnvironment}", block)
            self.assertIn('--config "${configTemplate}"', block)
            self.assertIn('--runtime-dir "${cfg.stateDir}"', block)
        self.assertIn("{ pkgs, beetsPackage }:", package)
        self.assertNotIn("beetsPackage ?", package)
        self.assertIn("beetsPackage = import ./beets.nix", shell)
        self.assertIn("inherit pkgs beetsPackage", shell)
        for obsolete in (
            "cfg.beets.package",
            "cfg.beets.config",
            "beetsSettings",
            "beetsConfigTemplate",
            "cratediggerBeet",
            'writeShellScriptBin "cratedigger-beet"',
            "discogsTokenFile",
            "discogsOperatorGroup",
            "defaultBeetsDbDir",
            "systemd.services.cratedigger-config-render",
            "services.cratedigger.beets.config.musicbrainz",
        ):
            self.assertNotIn(obsolete, text)
        self.assertNotIn('mktemp "$config_dir/.config.ini.XXXXXX"', text)
        self.assertNotIn('mktemp "$beets_dir/.config.yaml.XXXXXX"', text)
        self.assertIsNone(
            re.search(
                r"ExecStartPre\s*=\s*[^;]*checkBeetsConfigPkg",
                text,
                re.DOTALL,
            ),
            "the local checker must remain operator-invoked, never a systemd prestart",
        )


class TestJellyfinNotifierConfigContract(unittest.TestCase):
    def test_library_id_option_stays_deleted(self) -> None:
        """Issue #1221 item 1: the refresh machinery is gone, and
        ``libraryId`` (its collection-wide fallback target, the option's
        only consumer) went with it."""
        text = _nix_source(MODULE_NIX)
        self.assertNotIn("libraryId", text)
        self.assertNotIn("library_id", text)


class TestCreateLocallyContract(unittest.TestCase):
    """pipelineDb.createLocally (tier-2 plan U7, R10/KTD5): local postgres
    with peer auth by construction — role + database named after cfg.user,
    socket DSN default, migrate unit ordered after NixOS setup completes."""

    def test_provisioning_block(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("services.postgresql = mkIf cfg.pipelineDb.createLocally", text)
        self.assertIn("ensureDatabases = [ cfg.user ];", text)
        self.assertIn("name = cfg.user;", text)
        self.assertIn("ensureDBOwnership = true;", text)
        self.assertIn('lib.mkDefault "postgresql:///${cfg.user}?host=/run/postgresql"', text)

    def test_migrate_ordered_after_local_postgres_setup(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn(
            'after = optional cfg.pipelineDb.createLocally "postgresql-setup.service";',
            text,
        )
        self.assertIn(
            'requires = optional cfg.pipelineDb.createLocally "postgresql-setup.service";',
            text,
        )

    def test_dsn_guard_gives_actionable_error(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("pipelineDsn =", text)
        self.assertIn("pipelineDb.createLocally = true", text)
        # No unit interpolates the raw nullable option.
        self.assertNotIn("${cfg.pipelineDb.dsn}", text)


class TestApiBaseThreading(unittest.TestCase):
    """One app API value; external Beets configuration is independent.

    Discogs is
    mirror-required with no public default (R13)."""

    def test_config_ini_renders_api_bases(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("[MusicBrainz]", text)
        self.assertIn("api_base = ${cfg.musicbrainz.apiBase}", text)
        self.assertIn("[Discogs]", text)

    def test_mb_default_is_public_and_discogs_has_none(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('default = "https://musicbrainz.org";', text)
        # discogs.apiBase: nullOr with null default — mirror-required.
        idx = text.index("discogs = {")
        self.assertIn("default = null;", text[idx:idx + 800])

    def test_web_wrapper_does_not_pass_api_base_flags(self) -> None:
        """Issue #497: config.ini is the ONE production source for the MB/
        Discogs API bases (read at startup via
        configure_api_bases_from_runtime_config()). The module must not also
        pass --mb-api/--discogs-api on the actual ExecStart invocation —
        that was a second path carrying the same two values, which is
        exactly the double-plumbing this consolidation removes. The flags
        themselves stay on web/server.py for a manual dev-only override,
        and a comment nearby is allowed to
        mention them by name — only the invocation argv is asserted here."""
        text = _nix_source(MODULE_NIX)
        web_start = text.index('writeShellScriptBin "cratedigger-web"')
        exec_start = text.index("exec ${pyRunner} ${src}/web/server.py", web_start)
        exec_end = text.index("'';", exec_start)
        exec_block = text[exec_start:exec_end]
        self.assertNotIn("--mb-api", exec_block)
        self.assertNotIn("--discogs-api", exec_block)

    def test_api_base_does_not_derive_external_beets_config(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertNotIn("services.cratedigger.beets.config", text)
        self.assertNotIn("mbHost = lib.removePrefix", text)


class TestLocalImportModuleContract(unittest.TestCase):
    """Issue #1176 PR2: the manual local-import lane's configuration
    surface — options, config.ini rendering, and the module assertion."""

    def test_config_ini_renders_local_import_section_unconditionally(self) -> None:
        """Both keys render via an unconditional Nix ternary — ``enabled``
        via ``${if cfg.localImport.enable then "True" else "False"}``,
        ``dir`` via its OWN, different ternary on a different predicate
        (``${if cfg.localImport.dir != null then toString ... else ""}``)
        — never ``optionalString cfg.localImport.enable`` gating either
        line or the whole section. So the ``[Local Import]`` section and
        both keys render REGARDLESS of ``enable``, which is what lets a
        disabled lane still ship ``enabled = False`` / an empty ``dir``
        rather than omitting the section outright (issue #1176 PR2 review
        finding 8)."""
        text = _nix_source(MODULE_NIX)
        self.assertIn("[Local Import]", text)
        self.assertIn(
            'enabled = ${if cfg.localImport.enable then "True" else "False"}',
            text)
        self.assertIn(
            'dir = ${if cfg.localImport.dir != null then toString cfg.localImport.dir else ""}',
            text)
        self.assertNotIn("optionalString cfg.localImport", text)

    def test_options_declared_with_no_working_dir_default(self) -> None:
        text = _nix_source(MODULE_NIX)
        idx = text.index("localImport = {")
        block = text[idx:idx + 1600]
        self.assertIn("enable = mkOption {", block)
        self.assertIn("dir = mkOption {", block)
        self.assertIn("type = types.nullOr types.str;", block)
        # Both options' own blocks default off/unset — `default = false;`
        # for enable, `default = null;` for dir. Neither carries a working
        # directory value anywhere in this block.
        self.assertIn("default = false;", block)
        self.assertIn("default = null;", block)

    def test_assertion_uses_the_shared_normalized_path_helper(self) -> None:
        """Issue #1176 PR2 review finding 2: a bare ``hasPrefix "/"`` check
        admits ``dir = "/srv/imports/"`` (trailing slash) — every candidate
        then dies inside ``open_directory_path`` with a containment
        verdict about a fault that is entirely in the operator's config.
        The module already has ``isAbsoluteNormalizedPath`` for exactly
        this; the assertion must use it, not a hand-rolled check."""
        text = _nix_source(MODULE_NIX)
        idx = text.index("services.cratedigger.localImport: enable requires")
        block = text[max(0, idx - 400):idx]
        self.assertIn("isAbsoluteNormalizedPath cfg.localImport.dir", block)
        self.assertIn('cfg.localImport.dir != "/"', block)

    def test_assertion_firing_matrix(self) -> None:
        """Issue #1176 PR2 review finding 8: evaluated-world coverage for
        the assertion firing on null/relative/empty/trailing-slash/`/`,
        staying silent on a valid dir, and staying silent when the lane is
        simply disabled regardless of ``dir``."""
        worlds = _shared_module_worlds_rest()["localImportAssertions"]
        assert isinstance(worlds, dict)
        expected_message = (
            "services.cratedigger.localImport: enable requires "
            "localImport.dir to be set, an absolute normalized path "
            "(no trailing slash, no . or .. components), and not /."
        )
        for bad_world in (
            "missingDir", "relativeDir", "emptyDir", "slashDir",
            "trailingSlashDir",
        ):
            with self.subTest(world=bad_world):
                self.assertEqual(worlds[bad_world], [expected_message])
        self.assertEqual(worlds["validDir"], [])
        self.assertEqual(worlds["disabledWithBadDir"], [])


class TestProcessingPathNormalizationContract(unittest.TestCase):
    """Issue #1355 Worth-exploring item 2.

    ``processingDir`` was checked only with a bare ``hasPrefix "/"``, and
    its disjointness from ``slskd.downloadDir`` was a plain lexical
    ``removeSuffix "/"`` + ``hasPrefix`` string comparison. A ``..``
    component could make two configured options resolve to the same real
    directory while module evaluation reported no violation at all —
    ``lib/fs_authority.py::open_private_processing_root`` still caught it
    at runtime via ``os.path.realpath``, but only after
    ``nixos-rebuild switch`` had already declared the configuration good.
    The fix reuses the module's existing ``isAbsoluteNormalizedPath``
    helper (already used for the Beets runtime paths and
    ``localImport.dir``) for both options, so the disjointness comparison
    that follows only ever sees inputs normalized by contract.
    """

    def test_both_options_are_checked_with_the_shared_normalized_path_helper(
        self,
    ) -> None:
        text = _nix_source(MODULE_NIX)
        idx = text.index(
            "services.cratedigger.processingDir must be lexically disjoint")
        block = text[max(0, idx - 1200):idx]
        self.assertIn("isAbsoluteNormalizedPath cfg.processingDir", block)
        self.assertIn(
            "isAbsoluteNormalizedPath cfg.slskd.downloadDir", block)
        # The disjointness assertion itself stays exactly as it was — a
        # plain lexical comparison — because its inputs are now guaranteed
        # normalized by the two assertions above.
        self.assertIn("removeSuffix \"/\" cfg.processingDir", text)
        self.assertIn("removeSuffix \"/\" cfg.slskd.downloadDir", text)

    def test_assertion_firing_matrix(self) -> None:
        """Evaluated-world coverage: a ``..`` segment, a ``.`` segment, a
        doubled slash, and a trailing slash each fire the new
        ``processingDir`` normalization clause; a ``..`` segment on
        ``slskd.downloadDir`` fires its own new clause; the concrete gap
        the issue named (a ``..`` segment making two options resolve to
        the same real directory while the lexical disjointness comparison
        sees no overlap) is caught ONLY by the new normalization clause;
        a plainly nested, already-normalized pair still trips the
        unchanged disjointness assertion; and the live doc2 wrapper's
        exact shape passes cleanly.
        """
        worlds = _shared_module_worlds_rest()["processingPathAssertions"]
        assert isinstance(worlds, dict)
        processing_dir_message = (
            "services.cratedigger.processingDir must be an absolute "
            "normalized path (no trailing slash, no . or .. components, "
            "no doubled slashes)."
        )
        download_dir_message = (
            "services.cratedigger.slskd.downloadDir must be an absolute "
            "normalized path when set (no trailing slash, no . or .. "
            "components, no doubled slashes)."
        )
        disjoint_message = (
            "services.cratedigger.processingDir must be lexically "
            "disjoint from services.cratedigger.slskd.downloadDir"
        )
        for bad_world in ("dotDotSegment", "dotSegment", "doubledSlash",
                          "trailingSlash", "overlapEscapesLexicalCheck"):
            with self.subTest(world=bad_world):
                self.assertEqual(worlds[bad_world], [processing_dir_message])
        self.assertEqual(
            worlds["downloadDirDotDot"], [download_dir_message])
        self.assertEqual(
            worlds["disjointRegressionStillFires"], [disjoint_message])
        self.assertEqual(worlds["validLiveShape"], [])


class TestOwnedRedisContract(unittest.TestCase):
    def test_cratedigger_owns_local_redis_server_by_default(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("redis = {", text)
        self.assertIn('default = true;', text)
        self.assertIn("services.redis.servers.cratedigger", text)
        self.assertIn("enable = cfg.redis.enable", text)
        self.assertIn("bind = cfg.redis.host", text)
        self.assertIn("port = cfg.redis.port", text)
        self.assertIn('default = "3gb";', text)
        self.assertIn('maxmemory = cfg.redis.maxmemory', text)
        self.assertIn('"maxmemory-policy" = "allkeys-lru"', text)

    def test_peer_cache_config_is_rendered(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn("[Peer Cache]", text)
        self.assertIn("redis_host = ${cfg.redis.host}", text)
        self.assertIn("redis_port = ${toString cfg.redis.port}", text)
        self.assertIn("ttl_seconds = ${toString cfg.peerCache.ttlSeconds}", text)
        self.assertIn("speed_ttl_seconds = ${toString cfg.peerCache.speedTtlSeconds}", text)
        self.assertIn("redis_connect_timeout_ms = ${toString cfg.peerCache.redisConnectTimeoutMs}", text)
        self.assertIn("redis_operation_timeout_ms = ${toString cfg.peerCache.redisOperationTimeoutMs}", text)

    def test_pipeline_and_web_are_ordered_after_owned_redis(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('redisServiceUnits = optional cfg.redis.enable "redis-cratedigger.service";', text)
        self.assertGreaterEqual(
            text.count("++ redisServiceUnits ++ beetsReadinessUnits"),
            2,
        )
        self.assertGreaterEqual(text.count("wants = redisServiceUnits;"), 1)

    def test_pipeline_wrapper_passes_redis_host_and_port(self) -> None:
        text = _nix_source(MODULE_NIX)
        self.assertIn('--redis-host "${cfg.redis.host}"', text)
        self.assertIn("--redis-port ${toString cfg.redis.port}", text)


class TestStandaloneCheckerPackageIdentity(unittest.TestCase):
    def test_wrapper_requires_and_threads_the_admitted_beets_package(self) -> None:
        wrappers = _nix_source(WRAPPERS_NIX)
        flake = _nix_source(FLAKE_NIX)
        self.assertIn("{ pkgs, beetsPackage,", wrappers)
        self.assertNotIn("beetsPackage ?", wrappers)
        self.assertIn("./package.nix { inherit beetsPackage; }", wrappers)
        self.assertIn("beetsPackage = import ./nix/beets.nix", flake)
        self.assertIn("inherit pkgs version beetsPackage;", flake)

    def test_wrapper_drops_inherited_pythonpath_and_flake_executes_checker(self) -> None:
        wrappers = _nix_source(WRAPPERS_NIX)
        flake = _nix_source(FLAKE_NIX)
        self.assertIn('export PYTHONPATH="${src}"', wrappers)
        self.assertNotIn('PYTHONPATH="${src}\'\'${PYTHONPATH', wrappers)
        self.assertIn("checkBeetsConfigPackageBoundary", flake)
        self.assertIn("cratedigger-check-beets-config-package-boundary", flake)
        self.assertIn("hostile inherited PYTHONPATH imported beets", flake)
        self.assertIn("/bin/cratedigger-check-beets-config", flake)


class TestQualityRankBandDefaultsMatchProduction(unittest.TestCase):
    """The module's band defaults ARE ``QualityRankConfig.defaults()``.

    ``nix/module.nix`` renders ``[Quality Ranks]`` into the immutable
    deployed ``config.ini``, so a band declared here that disagrees with the
    dataclass silently retunes live quality policy at the next switch, with
    nothing in the Python suite reading the deployed value. Issue #1145
    collapsed ``mp3_vbr``/``mp3_cbr`` into one ``mp3`` table on both sides;
    a mutant that reverted only the Nix half survived every existing
    ``tests/test_nix_module.py`` contract.

    The expectation is DERIVED from the production dataclass, never typed
    here, so retuning a band in one place fails until the other follows.
    """

    #: Nix option name -> ``QualityRankConfig`` attribute. Both directions are
    #: checked below, so a codec added to one side and not the other fails.
    _CODECS: ClassVar[dict[str, str]] = {
        "opus": "opus",
        "mp3": "mp3",
        "aac": "aac",
        "vorbis": "vorbis",
        "wma": "wma",
    }

    @staticmethod
    def _rendered_bands(source: str, codec: str) -> dict[str, int]:
        match = re.search(
            r"^\s*" + re.escape(codec) + r" = mkCodecBands \"[^\"]+\" \{"
            r"(?P<body>.*?)^\s*\};",
            source,
            re.DOTALL | re.MULTILINE,
        )
        assert match is not None, f"no mkCodecBands block for {codec!r}"
        return {
            name: int(value)
            for name, value in re.findall(
                r"(\w+)\s*=\s*(\d+);", match.group("body"))
        }

    def test_every_band_default_equals_the_dataclass(self) -> None:
        from lib.quality import QualityRankConfig

        source = _nix_source(MODULE_NIX)
        defaults = QualityRankConfig.defaults()
        for nix_name, attr in self._CODECS.items():
            with self.subTest(codec=nix_name):
                bands = getattr(defaults, attr)
                self.assertEqual(
                    self._rendered_bands(source, nix_name),
                    {
                        "transparent": bands.transparent,
                        "excellent": bands.excellent,
                        "good": bands.good,
                        "acceptable": bands.acceptable,
                    },
                )

    def test_the_declared_codec_set_is_exactly_the_dataclass_set(self) -> None:
        """No codec may exist on one side only — the #1145 failure shape.

        Renaming ``mp3_vbr``/``mp3_cbr`` to ``mp3`` in Python while the Nix
        option kept the old names would leave the deployed config naming a
        section the INI parser no longer reads.
        """
        from lib.quality import QualityRankConfig

        source = _nix_source(MODULE_NIX)
        bands_block = re.search(
            r"^      bands = \{(?P<body>.*?)^      \};",
            source, re.DOTALL | re.MULTILINE)
        assert bands_block is not None
        declared = set(re.findall(
            r"^\s*(\w+) = mkCodecBands ", bands_block.group("body"),
            re.MULTILINE))
        self.assertEqual(declared, set(self._CODECS))
        rendered = json.loads(QualityRankConfig.defaults().to_json())
        for attr in self._CODECS.values():
            self.assertIn(attr, rendered)
        self.assertNotIn("mp3_vbr", rendered)
        self.assertNotIn("mp3_cbr", rendered)


def _attrset_block(source: str, marker: str) -> str:
    """The next ``{ ... }`` attrset body starting at ``marker``, found via
    matching brace depth (nested attrsets inside are common — e.g.
    ``serviceConfig``/``timerConfig``).

    Comment lines are stripped from the result. Without that, every
    ``assertIn`` over this block is satisfied by the attribute appearing in a
    ``#`` comment — so commenting an attribute out, the single most likely way
    one of these gets disabled, would leave the pins green (issue #1161
    review).

    Callers now pass :func:`_nix_source`, which has already stripped comments,
    so brace matching runs over comment-free text and the old hazard of a
    comment carrying an unbalanced brace is gone. The strip here is retained
    and idempotent, so the helper stays correct if handed raw source.
    """
    start = source.index(marker)
    open_brace = source.index("{", start)
    depth = 0
    for i in range(open_brace, len(source)):
        if source[i] == "{":
            depth += 1
        elif source[i] == "}":
            depth -= 1
            if depth == 0:
                return _strip_comment_lines(source[open_brace:i + 1])
    raise AssertionError(f"unterminated block for {marker!r}")


class TestMigrateUnitCannotBeSwallowedByAConcurrentStart(unittest.TestCase):
    """#1161 — the migrate unit must be re-run by every switch even when an
    unrelated ``systemctl start`` lands mid-switch.

    These are source pins over the module's own attrset. The load-bearing
    proof is at the real adapter — the systemd unit file NixOS renders and
    switch-to-configuration actually parses — and lives in
    ``nix/tests/module-vm.nix`` (``X-StopIfChanged=false``, plus the
    start-is-a-no-op / restart-re-runs behaviour pair)."""

    def setUp(self) -> None:
        source = _nix_source(MODULE_NIX)
        self.service_block = _attrset_block(
            source, "systemd.services.cratedigger-db-migrate",
        )

    def test_switch_restarts_rather_than_stop_and_start(self) -> None:
        """stopIfChanged = false routes the unit to switch-to-configuration's
        restart list, whose JOB_RESTART absorbs a concurrent JOB_START instead
        of being replaced by it."""
        self.assertIn("stopIfChanged = false;", self.service_block)

    def test_deploys_still_re_run_the_migrator(self) -> None:
        self.assertIn("restartIfChanged = true;", self.service_block)

    def test_remain_after_exit_is_the_precondition_being_defended(self) -> None:
        """RemainAfterExit keeps the unit active(exited) between switches,
        which is exactly why a plain start returns -EALREADY and silently
        skips ExecStart. Losing it would change the failure mode this pin
        describes."""
        self.assertIn("RemainAfterExit = true;", self.service_block)


class TestRetagDivergenceCensusServiceShape(unittest.TestCase):
    """#1142 review N8 — the daily retag-divergence census oneshot/timer
    shape, mirroring cratedigger-unfindable's own systemd contract but
    with no pipeline-DB dependency."""

    def setUp(self) -> None:
        source = _nix_source(MODULE_NIX)
        self.service_block = _attrset_block(
            source, "systemd.services.cratedigger-retag-census",
        )
        self.timer_block = _attrset_block(
            source, "systemd.timers.cratedigger-retag-census",
        )

    def test_service_is_a_oneshot_that_never_restarts_on_deploy(self) -> None:
        self.assertIn('Type = "oneshot";', self.service_block)
        self.assertIn("restartIfChanged = false;", self.service_block)

    def test_service_runs_as_the_configured_cratedigger_user(self) -> None:
        self.assertIn("User = cfg.user;", self.service_block)
        self.assertIn("Group = cfg.group;", self.service_block)

    def test_service_has_a_bounded_timeout(self) -> None:
        self.assertIn("TimeoutStartSec = ", self.service_block)

    def test_service_has_no_pipeline_db_dependency(self) -> None:
        """Beets-only — unlike every other unit in this module, it must
        name neither the migration unit nor the pipeline DSN anywhere in
        its own block."""
        self.assertNotIn("cratedigger-db-migrate", self.service_block)
        self.assertNotIn("pipelineDsn", self.service_block)
        self.assertNotIn("PIPELINE_DB_DSN", self.service_block)

    def test_timer_fires_daily_with_persistence_and_jitter(self) -> None:
        self.assertIn('OnCalendar = "daily";', self.timer_block)
        self.assertIn("Persistent = true;", self.timer_block)
        self.assertIn("RandomizedDelaySec = ", self.timer_block)

    def test_timer_is_wanted_by_timers_target(self) -> None:
        self.assertIn('wantedBy = ["timers.target"];', self.timer_block)

    def test_service_binds_the_beets_state_file_read_only(self) -> None:
        """The runner calls enforce_beets_startup(role="web"); it owes the
        same beetsObserverReadOnlyPaths bind as the other web-role/observer
        callers (see the real-eval assertion in
        TestExternalBeetsRuntimeCapability.test_readiness_and_role_state_capabilities_evaluate
        for the rendered-path proof, not just this source-text check)."""
        self.assertIn(
            "BindReadOnlyPaths = beetsObserverReadOnlyPaths;", self.service_block,
        )


class TestLibraryCompletenessCensusServiceShape(unittest.TestCase):
    """#1149 is independently scheduled/read-only, not pipeline work."""

    def setUp(self) -> None:
        source = _nix_source(MODULE_NIX)
        self.service_block = _attrset_block(
            source, "systemd.services.cratedigger-library-completeness-census",
        )
        self.timer_block = _attrset_block(
            source, "systemd.timers.cratedigger-library-completeness-census",
        )

    def test_service_is_read_only_beets_oneshot_with_public_fallback_headroom(self) -> None:
        self.assertIn('Type = "oneshot";', self.service_block)
        self.assertIn("restartIfChanged = false;", self.service_block)
        self.assertIn("BindReadOnlyPaths = beetsObserverReadOnlyPaths;", self.service_block)
        self.assertIn('TimeoutStartSec = "4h";', self.service_block)
        self.assertNotIn("cratedigger-db-migrate", self.service_block)
        self.assertNotIn("PIPELINE_DB_DSN", self.service_block)

    def test_timer_is_daily_persistent_and_jittered(self) -> None:
        self.assertIn('wantedBy = ["timers.target"];', self.timer_block)
        self.assertIn('OnCalendar = "daily";', self.timer_block)
        self.assertIn("Persistent = true;", self.timer_block)
        self.assertIn("RandomizedDelaySec = ", self.timer_block)


if __name__ == "__main__":
    unittest.main()
