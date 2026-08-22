"""Generated proof that the web gateway admits exactly one authorization mode.

The invariant this patrols: for every combination of ``web.enable``,
``basicAuthFile``, ``enableInsecure``, and ``externalAuth``, the module either
evaluates cleanly and derives exactly one ``gateway_mode``, or it fails
evaluation with an assertion naming the conflict. There is no combination that
evaluates cleanly into an ambiguous or absent mode, and no mode may inherit
another's credential.

The domain is genuinely finite — four independent booleans, sixteen worlds — so
the property enumerates it exhaustively rather than sampling it. Every world is
evaluated by the REAL NixOS module through one ``nix eval``; nothing here
reimplements the module's mode arithmetic in Python, because a Python twin
would agree with itself while the module drifted.
"""

from __future__ import annotations

import functools
import itertools
import json
import subprocess
import unittest
from dataclasses import dataclass
from pathlib import Path

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from tests.finite_domain import finite_generated_domain

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ModeWorld:
    """One selection of the module's four authorization-surface booleans."""

    enable: bool
    basic: bool
    insecure: bool
    external: bool

    @property
    def key(self) -> str:
        return (
            f"w{int(self.enable)}{int(self.basic)}"
            f"{int(self.insecure)}{int(self.external)}"
        )

    @property
    def selected_count(self) -> int:
        return int(self.basic) + int(self.insecure) + int(self.external)

    @property
    def expected_mode(self) -> str | None:
        """The one ``gateway_mode`` a cleanly-evaluating world must derive."""
        if not self.enable or self.selected_count != 1:
            return None
        if self.basic:
            return "basic"
        if self.external:
            return "external"
        return "insecure"


MODE_WORLDS: tuple[ModeWorld, ...] = tuple(
    ModeWorld(enable=enable, basic=basic, insecure=insecure, external=external)
    for enable, basic, insecure, external in itertools.product(
        (True, False), repeat=4
    )
)
MODE_WORLD_COUNT = 16


def verify_mode_world_domain() -> None:
    """Independently prove the domain is the exact 2^4 boolean product."""
    if len(MODE_WORLDS) != MODE_WORLD_COUNT:
        raise AssertionError(
            f"mode domain must hold {MODE_WORLD_COUNT} worlds, "
            f"found {len(MODE_WORLDS)}"
        )
    if len({world.key for world in MODE_WORLDS}) != MODE_WORLD_COUNT:
        raise AssertionError("mode domain keys collapsed")
    reconstructed = {
        (world.enable, world.basic, world.insecure, world.external)
        for world in MODE_WORLDS
    }
    if reconstructed != set(itertools.product((True, False), repeat=4)):
        raise AssertionError("mode domain is not the full boolean product")


def _nix_web_attrs(world: ModeWorld) -> str:
    lines = [f"enable = {'true' if world.enable else 'lib.mkForce false'};"]
    if world.enable:
        lines.append('hostName = "music.example.test";')
    if world.basic:
        lines.append('basicAuthFile = "/run/secrets/cratedigger.htpasswd";')
    lines.append(f"enableInsecure = {'true' if world.insecure else 'false'};")
    lines.append(f"externalAuth = {'true' if world.external else 'false'};")
    return "\n                          ".join(lines)


@functools.cache
def _evaluate_mode_worlds() -> dict[str, dict[str, object]]:
    """Evaluate all sixteen worlds against the real module in one nix eval.

    Issue #1226: the sixteen worlds share ONE nixpkgs instance
    (``{ nixpkgs.pkgs = modulePkgs; }`` as each system's first module,
    ``flake.nix``'s own established idiom) rather than each
    ``lib.nixosSystem`` instantiating its own. Nothing about what is
    evaluated or asserted changes; it took this module from 54.0s to 21.9s
    in a real phase run. See
    ``tests/test_nix_module.py::_shared_module_worlds_web_auth_matrix_part1``
    for the mechanism and the byte-identical-output evidence.

    Issue #1248: ``builtins.getFlake`` unconditionally walks and NAR-hashes
    its ENTIRE source directory the moment it is called -- before any
    output is even referenced -- so pointing it at a bare ``./.`` names
    this live, possibly-dirty worktree and races any concurrent test
    worker's Python bytecode cache writes/removals under ``**/__pycache__``
    (gitignored, never part of the module's real input; a suite run
    observed this as ``path '.../tests/__pycache__/test_quality_generated.
    cpython-314.pyc.<n>' does not exist``, a directory-walk read of a file
    a sibling worker had already renamed away). ``repoSource`` filters that
    subtree out with a plain ``builtins.path`` -- a Nix builtin, not a
    nixpkgs helper, so it needs no ``lib`` and has no circular dependency
    on the very flake it is about to fetch. Filtering a DIRECTORY out of a
    ``builtins.path``/``filterSource`` walk means Nix never recurses into
    it at all, so this is not "less likely to race", it is structurally
    unable to: the walk never calls ``readdir``/``stat`` on anything under
    ``__pycache__/`` in the first place. Both raw-path references below
    (the old ``getFlake (toString ./.)`` and the module config's own
    ``src = ./.;``, which named the same live directory independently) now
    read the SAME filtered copy, so no live-tree read in this expression
    is left unfiltered. Nothing about which worlds are evaluated, what the
    module asserts, or the module's own source (every non-``__pycache__``
    file is still present, unchanged) is affected -- this only removes a
    subtree the module never references anyway.
    """
    worlds = "\n            ".join(
        f"{world.key} = evaluate {{ {_nix_web_attrs(world)} }};"
        for world in MODE_WORLDS
    )
    expression = f'''
      let
        repoSource = builtins.path {{
          path = toString ./.;
          filter = path: type: baseNameOf path != "__pycache__";
          name = "cratedigger-nix-eval-source";
        }};
        # builtins.path's result carries a store-path context (it tracks
        # that the string provenance is a store path), and getFlake
        # refuses a context-carrying argument ("... is not allowed to
        # refer to a store path"). The directory is already realized on
        # disk synchronously by builtins.path above, so there is no build
        # step being skipped -- discarding the context is safe here.
        f = builtins.getFlake (builtins.unsafeDiscardStringContext (toString repoSource));
        lib = f.inputs.nixpkgs.lib;
        modulePkgs = import f.inputs.nixpkgs {{
          system = builtins.currentSystem;
        }};
        beetsPackage = import ./nix/beets.nix {{ pkgs = modulePkgs; }};
        evaluate = web:
          let
            system = lib.nixosSystem {{
              system = builtins.currentSystem;
              modules = [
                {{ nixpkgs.pkgs = modulePkgs; }}
                f.nixosModules.default
                ({{ ... }}: {{
                  services.cratedigger = {{
                    enable = true;
                    src = repoSource;
                    user = "cratedigger";
                    group = "cratedigger";
                    slskd.apiKeyFile = "/run/secrets/slskd-key";
                    slskd.downloadDir = "/srv/slskd";
                    pipelineDb.createLocally = true;
                    beets.runtime = {{
                      package = beetsPackage;
                      configDir = "/etc/beets";
                      expectedLibrary = "/srv/beets/beets-library.db";
                      expectedDirectory = "/srv/music";
                      expectedStateFile = "/var/lib/beets/state.pickle";
                      expectedSecretInclude = "/run/secrets/beets.yaml";
                    }};
                    web = {{ enable = true; }} // web;
                  }};
                }})
              ];
            }};
          in {{
            failures = map (assertion: assertion.message)
              (builtins.filter
                (assertion:
                  !assertion.assertion
                  && lib.hasPrefix "services.cratedigger.web" assertion.message)
                system.config.assertions);
            policy =
              if system.config.services.cratedigger.web.enable
              then
                system.config.environment.etc
                  ."cratedigger/web-gateway-policy".text
              else null;
          }};
      in {{
            {worlds}
      }}
    '''
    result = subprocess.run(
        ["nix", "eval", "--impure", "--json", "--expr", expression],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(f"nix eval failed: {result.stderr}")
    return json.loads(result.stdout)


def parse_policy_field(policy: str | None, field: str) -> str | None:
    """Read one field out of the gateway policy descriptor."""
    if policy is None:
        return None
    for line in policy.splitlines():
        if line.startswith(f"{field}="):
            return line[len(field) + 1 :]
    return None


def mode_selection_violations(
    world: ModeWorld, failures: list[str], policy: str | None,
) -> list[str]:
    """Return every way this evaluated world breaks the mode invariant.

    Kept module-level so the known-bad self-tests below can drive it with
    planted results and prove it actually trips.
    """
    violations: list[str] = []
    conflicted = any("mutually exclusive" in message for message in failures)
    unselected = any("exactly one" in message for message in failures)
    residue = any("inactive-mode residue" in message for message in failures)

    if not world.enable:
        if world.selected_count > 0 and not residue:
            violations.append(
                "a disabled web surface kept authorization residue without "
                "failing evaluation"
            )
        if world.selected_count == 0 and failures:
            violations.append(
                f"a clean disabled world failed evaluation: {failures}"
            )
        return violations

    if world.selected_count == 1:
        if failures:
            violations.append(
                f"a single-mode world failed evaluation: {failures}"
            )
        mode = parse_policy_field(policy, "gateway_mode")
        if mode != world.expected_mode:
            violations.append(
                f"derived gateway_mode {mode!r}, expected "
                f"{world.expected_mode!r}"
            )
        credential = parse_policy_field(policy, "gateway_credential_path")
        if world.basic:
            if credential in (None, "-"):
                violations.append("Basic mode derived no credential path")
        elif credential != "-":
            violations.append(
                f"non-Basic mode {mode!r} named credential {credential!r}"
            )
        return violations

    if world.selected_count == 0 and not unselected:
        violations.append(
            "an enabled web surface with no mode selected evaluated cleanly"
        )
    if world.selected_count > 1 and not conflicted:
        violations.append(
            f"{world.selected_count} simultaneous modes evaluated without a "
            "mutual-exclusion failure"
        )
    return violations


class TestWebAuthorizationModeDomain(unittest.TestCase):
    """Every world in the exact mode domain resolves to one mode or fails."""

    @finite_generated_domain(
        cardinality=MODE_WORLD_COUNT,
        verify=verify_mode_world_domain,
    )
    @given(world=st.sampled_from(MODE_WORLDS))
    @example(world=ModeWorld(True, False, False, True))
    @example(world=ModeWorld(True, True, False, True))
    @example(world=ModeWorld(True, False, True, True))
    @example(world=ModeWorld(True, True, True, True))
    @example(world=ModeWorld(True, False, False, False))
    @example(world=ModeWorld(False, False, False, True))
    def test_every_world_selects_one_mode_or_fails_closed(
        self, world: ModeWorld,
    ) -> None:
        evaluated = _evaluate_mode_worlds()[world.key]
        failures = evaluated["failures"]
        policy = evaluated["policy"]
        assert isinstance(failures, list)
        assert policy is None or isinstance(policy, str)

        violations = mode_selection_violations(world, failures, policy)

        self.assertEqual(violations, [], (world, failures, policy))

    def test_every_mode_derives_a_distinct_policy_identity(self) -> None:
        """Two modes must never share a gateway-policy fingerprint.

        The marker filename is derived from the policy identity, so a shared
        identity would let one mode's published readiness marker satisfy
        another mode's gateway.
        """
        evaluated = _evaluate_mode_worlds()
        policies = {}
        for world in MODE_WORLDS:
            if world.expected_mode is None:
                continue
            policy = evaluated[world.key]["policy"]
            assert isinstance(policy, str)
            policies[world.expected_mode] = policy

        self.assertEqual(set(policies), {"basic", "insecure", "external"})
        markers = {
            mode: parse_policy_field(policy, "gateway_marker_path")
            for mode, policy in policies.items()
        }
        self.assertEqual(
            len(set(markers.values())), 3, markers,
        )


class TestModeInvariantCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: the checker must reject planted violations."""

    def test_clean_single_mode_world_passes(self) -> None:
        world = ModeWorld(True, False, False, True)
        policy = (
            "format=1\ngateway_mode=external\ngateway_credential_path=-\n"
            "gateway_marker_path=/run/cratedigger-web/gateway-policy-"
            + "a" * 64
        )

        self.assertEqual(mode_selection_violations(world, [], policy), [])

    def test_conflicting_world_that_evaluates_cleanly_is_rejected(
        self,
    ) -> None:
        world = ModeWorld(True, True, True, True)

        violations = mode_selection_violations(world, [], None)

        self.assertTrue(
            any("simultaneous modes" in item for item in violations),
            violations,
        )

    def test_unselected_world_that_evaluates_cleanly_is_rejected(self) -> None:
        world = ModeWorld(True, False, False, False)

        violations = mode_selection_violations(world, [], None)

        self.assertTrue(
            any("no mode selected" in item for item in violations), violations,
        )

    def test_wrong_derived_mode_is_rejected(self) -> None:
        world = ModeWorld(True, False, False, True)
        policy = "format=1\ngateway_mode=insecure\ngateway_credential_path=-"

        violations = mode_selection_violations(world, [], policy)

        self.assertTrue(
            any("derived gateway_mode" in item for item in violations),
            violations,
        )

    def test_non_basic_mode_naming_a_credential_is_rejected(self) -> None:
        world = ModeWorld(True, False, False, True)
        policy = (
            "format=1\ngateway_mode=external\n"
            "gateway_credential_path=/run/secrets/cratedigger.htpasswd"
        )

        violations = mode_selection_violations(world, [], policy)

        self.assertTrue(
            any("named credential" in item for item in violations), violations,
        )

    def test_disabled_world_keeping_residue_is_rejected(self) -> None:
        world = ModeWorld(False, False, False, True)

        violations = mode_selection_violations(world, [], None)

        self.assertTrue(
            any("residue" in item for item in violations), violations,
        )

    def test_single_mode_world_that_failed_evaluation_is_rejected(
        self,
    ) -> None:
        world = ModeWorld(True, False, False, True)

        violations = mode_selection_violations(
            world, ["services.cratedigger.web something broke"], None,
        )

        self.assertTrue(
            any("failed evaluation" in item for item in violations),
            violations,
        )


if __name__ == "__main__":
    unittest.main()
