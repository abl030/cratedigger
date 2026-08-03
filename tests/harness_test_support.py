"""Import harness modules against synthetic Beets modules without global leaks."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from types import ModuleType

_MISSING = object()
_HARNESS_MODULES = ("harness.beets_harness", "harness.beets_compat")


@contextmanager
def isolated_beets_harness(modules: Mapping[str, ModuleType]) -> Iterator[ModuleType]:
    """Return a fresh harness bound to mocks, restoring all import state after."""
    package = importlib.import_module("harness")
    module_names = (*modules, *_HARNESS_MODULES)
    prior_modules = {name: sys.modules.get(name, _MISSING) for name in module_names}
    prior_parent_attributes: list[tuple[ModuleType, str, object]] = []
    for name in modules:
        parent_name, _, attribute = name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if isinstance(parent, ModuleType):
            prior_parent_attributes.append(
                (parent, attribute, vars(parent).get(attribute, _MISSING)),
            )
    prior_attributes = {
        name.rpartition(".")[2]: vars(package).get(name.rpartition(".")[2], _MISSING)
        for name in _HARNESS_MODULES
    }
    for name in module_names:
        sys.modules.pop(name, None)
    sys.modules.update(modules)
    try:
        yield importlib.import_module("harness.beets_harness")
    finally:
        for name in module_names:
            previous = prior_modules[name]
            if previous is _MISSING:
                sys.modules.pop(name, None)
            else:
                assert isinstance(previous, ModuleType)
                sys.modules[name] = previous
        for parent, attribute, previous in prior_parent_attributes:
            if previous is _MISSING:
                vars(parent).pop(attribute, None)
            else:
                vars(parent)[attribute] = previous
        for attribute, previous in prior_attributes.items():
            if previous is _MISSING:
                vars(package).pop(attribute, None)
            else:
                vars(package)[attribute] = previous
