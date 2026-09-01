"""Deterministic pins for cratedigger.py's startup / cycle hand-off (#1313).

Three claims here were bounded AST parses of ``cratedigger.main``'s source
until #1313 split its tail into ``run_startup_and_cycle`` and
``build_cycle_collaborators``: that ``main()`` hands off to ``run_cycle``
exactly once, that the ``--reconcile-dry-run`` gate sits ahead of that
hand-off, and that the owner context wires a real
``ClaimedQueueKeysRegistry()``. All three are executed here instead, over
the real functions, together with the Phase-1 call-site pin that used to
parse ``_run_phase1``.

Deterministic only, and deliberately NOT left in
``tests/test_convergence_runner_generated.py`` where the source pins lived:
a Hypothesis module cannot take part in the mutmut breadth pass (#1317),
and these are pins, not properties.
"""
from __future__ import annotations

import os
import sys
import unittest
from dataclasses import replace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import cratedigger
from lib.config import CratediggerConfig
from lib.context import CratediggerContext, CycleCollaborators
from lib.peer_cache import PeerCache
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
from tests.helpers import make_cycle_collaborators


class TestCycleHandoff(unittest.TestCase):
    """The cycle hand-off, as behaviour (issue #1313).

    Two claims here used to be bounded AST parses of ``cratedigger.main``'s
    source, because ``main()`` needs a live DB and slskd client to run at
    all: that it hands off to ``run_cycle`` exactly once, and that the
    ``--reconcile-dry-run`` gate sits ahead of that hand-off. #1313 split
    the tail of ``main()`` into ``run_startup_and_cycle``, which a test
    executes for real — so both are asserted by driving it rather than by
    comparing line numbers.

    Stronger in two ways the source pins could not reach: the dry-run case
    proves no cycle runs AND that the reconciliation's own exit code is
    what comes back, and the normal case proves the context handed to
    ``run_cycle`` carries the collaborators the factory built.
    """

    def _cfg(self) -> CratediggerConfig:
        return CratediggerConfig()

    def test_normal_run_hands_off_to_run_cycle_exactly_once(self):
        cfg = self._cfg()
        source = FakePipelineDBSource()
        collaborators = make_cycle_collaborators(
            cfg=cfg, slskd=FakeSlskdAPI(), pipeline_db_source=source)
        seen: list[CratediggerContext] = []
        built_from: list[tuple[object, object]] = []

        def _factory(
            factory_cfg: CratediggerConfig, factory_source: object,
        ) -> CycleCollaborators:
            built_from.append((factory_cfg, factory_source))
            return collaborators

        def _no_dry_run(_source: object) -> int:
            raise AssertionError(
                "reconciliation must not run without --reconcile-dry-run")

        code = cratedigger.run_startup_and_cycle(
            cfg,
            source,
            reconcile_dry_run=False,
            collaborators_factory=_factory,
            run_cycle_fn=seen.append,
            reconcile_dry_run_fn=_no_dry_run,
        )

        self.assertEqual(code, 0)
        self.assertEqual(len(seen), 1, "exactly one cycle per invocation")
        self.assertIs(seen[0].collaborators, collaborators)
        # The factory gets THIS call's cfg and source, not something else:
        # collaborators built from another config would wire the cycle's
        # ownership writer onto the wrong DSN.
        self.assertEqual(len(built_from), 1)
        self.assertIs(built_from[0][0], cfg)
        self.assertIs(built_from[0][1], source)

    def test_dry_run_returns_its_own_code_without_running_a_cycle(self):
        cfg = self._cfg()
        source = FakePipelineDBSource()
        reconciled: list[object] = []

        def _factory(
            _cfg: CratediggerConfig, _source: object,
        ) -> CycleCollaborators:
            return make_cycle_collaborators(cfg=_cfg)

        def _no_cycle(_ctx: CratediggerContext) -> None:
            raise AssertionError(
                "--reconcile-dry-run is read-only: no cycle may run")

        def _reconcile(source_arg: object) -> int:
            reconciled.append(source_arg)
            return 7

        code = cratedigger.run_startup_and_cycle(
            cfg,
            source,
            reconcile_dry_run=True,
            collaborators_factory=_factory,
            run_cycle_fn=_no_cycle,
            reconcile_dry_run_fn=_reconcile,
        )

        self.assertEqual(code, 7, "main returns the reconciliation's code")
        self.assertEqual(reconciled, [source])


class TestCycleCollaboratorWiring(unittest.TestCase):
    """What ``main()``'s owner collaborators are actually wired with.

    #1178 PR2 review F1 (mutant b) pinned one kwarg SPELLING in
    ``main()``'s source: that ``claimed_queue_keys_registry=`` constructed
    a real ``ClaimedQueueKeysRegistry()``. Dropping it would degrade the
    cross-request enqueue guard to its cross-cycle-only layer, which does
    not catch the #1178 same-cycle collision — neither sibling request has
    an accepted ledger row yet when the other's guard runs.

    #1313 lifted that wiring into ``build_cycle_collaborators``, so the
    claim is executed here instead of parsed: the registry is real AND
    fresh per cycle (a shared one would leak claims across cycles, which
    the spelling pin could not see), and the ownership writer — never
    covered by that pin at all — is a real ``DownloadOwnershipWriter`` on
    the configured DSN.
    """

    def _build(self, cfg: CratediggerConfig, source: FakePipelineDBSource):
        self.slskd = FakeSlskdAPI()
        # A real, cold PeerCache (client=None is exactly what
        # connect_from_config returns when Redis is unreachable).
        self.peer_cache = PeerCache(
            None, ttl_seconds=60, speed_ttl_seconds=10)
        # Both factories reach real infrastructure in production — the
        # slskd API key on disk, a Redis ping — so what config they are
        # handed decides which instance the cycle gets.
        self.factory_cfgs: list[object] = []

        def _slskd_factory(factory_cfg: CratediggerConfig) -> object:
            self.factory_cfgs.append(factory_cfg)
            return self.slskd

        def _peer_cache_factory(factory_cfg: CratediggerConfig) -> PeerCache:
            self.factory_cfgs.append(factory_cfg)
            return self.peer_cache

        return cratedigger.build_cycle_collaborators(
            cfg,
            source,
            slskd_factory=_slskd_factory,
            peer_cache_factory=_peer_cache_factory,
        )

    def test_wiring_is_a_fresh_registry_and_a_real_ownership_writer(self):
        from lib.download_ownership import DownloadOwnershipWriter
        from lib.enqueue import ClaimedQueueKeysRegistry

        # A DSN nothing else in the tree defaults to, so the assertion
        # below cannot pass by coincidence.
        cfg = replace(
            CratediggerConfig(),
            pipeline_db_dsn="postgresql://cycle-wiring-probe/db")
        source = FakePipelineDBSource()

        built = self._build(cfg, source)

        self.assertIsInstance(
            built.claimed_queue_keys_registry, ClaimedQueueKeysRegistry)
        writer = built.download_ownership
        assert isinstance(writer, DownloadOwnershipWriter)
        # The writer opens a fresh handle per operation on THIS cfg's DSN;
        # a writer built on some other DSN would prove nothing about the
        # cycle's own ledger.
        self.assertEqual(writer.dsn, cfg.pipeline_db_dsn)
        self.assertIs(built.cfg, cfg)
        self.assertIs(built.pipeline_db_source, source)
        self.assertIs(built.slskd, self.slskd)
        self.assertIs(built.peer_cache, self.peer_cache)
        # Both factories were handed THIS cfg — an slskd client built from
        # another config would talk to another host, and a peer cache from
        # another config to another Redis.
        self.assertEqual(self.factory_cfgs, [cfg, cfg])

    def test_each_cycle_gets_its_own_registry(self):
        cfg = CratediggerConfig()
        first = self._build(cfg, FakePipelineDBSource())
        second = self._build(cfg, FakePipelineDBSource())
        self.assertIsNot(
            first.claimed_queue_keys_registry,
            second.claimed_queue_keys_registry,
            "one registry per cycle: sharing one leaks same-cycle claims "
            "across cycles (#1178 PR2 review F7)")


class TestPhase1ContextCallSite(unittest.TestCase):
    """``_run_phase1`` builds Phase 1's context through the forwarding helper.

    PR #1280's shipped defect was an inline ``CratediggerContext(...)``
    here that omitted ``download_ownership``, turning every download-timeout
    cleanup into a logged no-op under the ownership gate, with the whole
    suite green. #1278 pinned it by parsing this function's source for a
    ``build_phase1_context`` call; #1313 drives the real function through
    its ``poll_fn`` seam and asserts what the context it builds actually
    carries — which catches an inline construction that names the helper's
    fields wrongly as well as one that skips the helper.
    """

    def test_phase1_context_carries_the_owners_collaborators(self):
        from lib.download_ownership import DownloadOwnershipWriter
        from lib.enqueue import ClaimedQueueKeysRegistry
        ledger = FakePipelineDB()
        writer = DownloadOwnershipWriter(
            db_factory=lambda: ledger, close_after_use=False)
        owner_source = FakePipelineDBSource()
        # A REAL registry, not the helper's ``None`` default: a forwarding
        # assertion against None passes whether the value is forwarded or
        # dropped.
        registry = ClaimedQueueKeysRegistry()
        owner = CratediggerContext(
            collaborators=make_cycle_collaborators(
                cfg=CratediggerConfig(),
                slskd=FakeSlskdAPI(),
                pipeline_db_source=owner_source,
                download_ownership=writer,
                claimed_queue_keys_registry=registry,
            ),
        )
        owner.cooled_down_users.add("grumpy-peer")
        phase1_source = FakePipelineDBSource()
        seen: list[CratediggerContext] = []
        source_factory_cfgs: list[object] = []

        def _source_factory(
            factory_cfg: CratediggerConfig,
        ) -> FakePipelineDBSource:
            source_factory_cfgs.append(factory_cfg)
            return phase1_source

        cratedigger._run_phase1(
            owner, _source_factory, poll_fn=seen.append)

        self.assertEqual(len(seen), 1)
        phase1_ctx = seen[0]
        self.assertIs(phase1_ctx.download_ownership, writer)
        # assertIs, not assertEqual: sharing the SET OBJECT is what lets a
        # cooldown Phase 1 discovers reach Phase 2's worker contexts.
        self.assertIs(phase1_ctx.cooled_down_users, owner.cooled_down_users)
        self.assertIs(phase1_ctx.pipeline_db_source, phase1_source)
        self.assertIsNot(
            phase1_ctx.pipeline_db_source, owner.pipeline_db_source)
        # The remaining four slots, so a derivation that drops ANY of them
        # is caught here and not only by the generated property.
        self.assertIs(phase1_ctx.cfg, owner.cfg)
        self.assertIs(phase1_ctx.slskd, owner.slskd)
        self.assertIs(phase1_ctx.claimed_queue_keys_registry, registry)
        self.assertIsNone(
            phase1_ctx.peer_cache,
            "Phase 1 takes no peer cache: the owner's instance is not "
            "shareable unforked across threads")
        # Phase 1's own source is opened from the OWNER's config: a factory
        # handed something else would open a connection to another DB.
        self.assertEqual(source_factory_cfgs, [owner.cfg])
        self.assertEqual(
            phase1_source.close_calls, 1,
            "Phase 1's own source is closed in the finally, always")


if __name__ == "__main__":
    unittest.main()
