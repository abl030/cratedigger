"""Import harness modules against synthetic Beets modules without global leaks."""

from __future__ import annotations

import importlib
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from types import ModuleType

_HARNESS_MODULES = ("harness.beets_harness", "harness.beets_compat")


@contextmanager
def isolated_beets_harness(modules: Mapping[str, ModuleType]) -> Iterator[ModuleType]:
    """Return a fresh harness bound to mocks, restoring all import state after."""
    package_existed = "harness" in sys.modules
    package = importlib.import_module("harness")
    module_names = (*modules, *_HARNESS_MODULES)
    prior_modules = {name: sys.modules[name] for name in module_names if name in sys.modules}
    prior_parent_attributes: list[tuple[object, str, bool, object]] = []
    for name in modules:
        parent_name, _, attribute = name.rpartition(".")
        parent = sys.modules.get(parent_name)
        if parent is not None:
            parent_vars = vars(parent)
            prior_parent_attributes.append(
                (parent, attribute, attribute in parent_vars, parent_vars.get(attribute)),
            )
    prior_attributes = {
        name.rpartition(".")[2]: vars(package)[name.rpartition(".")[2]]
        for name in _HARNESS_MODULES
        if name.rpartition(".")[2] in vars(package)
    }
    for name in module_names:
        sys.modules.pop(name, None)
    for attribute in _HARNESS_MODULES:
        vars(package).pop(attribute.rpartition(".")[2], None)
    sys.modules.update(modules)
    try:
        yield importlib.import_module("harness.beets_harness")
    finally:
        for name in module_names:
            sys.modules.pop(name, None)
        sys.modules.update(prior_modules)
        for parent, attribute, existed, previous in prior_parent_attributes:
            if existed:
                setattr(parent, attribute, previous)
            else:
                vars(parent).pop(attribute, None)
        if package_existed:
            for attribute in _HARNESS_MODULES:
                vars(package).pop(attribute.rpartition(".")[2], None)
            vars(package).update(prior_attributes)
        else:
            sys.modules.pop("harness", None)
