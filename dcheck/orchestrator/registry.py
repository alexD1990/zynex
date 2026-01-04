from __future__ import annotations

from typing import Dict

from dcheck.common.interfaces import DCheckModule
from dcheck.modules.core_quality.module import CoreModule


def _load_entrypoint_modules() -> Dict[str, DCheckModule]:
    """
    Discover external modules via Python entry points.

    Expected entry point group:
      [project.entry-points."dcheck.modules"]
      gdpr = "dcheck_gdpr.module:GDPRModule"
    """
    modules: Dict[str, DCheckModule] = {}

    try:
        # Python 3.10+ (and newer) supports importlib.metadata in stdlib
        from importlib.metadata import entry_points
    except Exception:
        return modules  # No plugin support available

    try:
        eps = entry_points()

        # Compatibility across Python versions:
        # - New API: eps.select(group="dcheck.modules")
        # - Old API: eps.get("dcheck.modules", [])
        group = "dcheck.modules"
        if hasattr(eps, "select"):
            candidates = list(eps.select(group=group))
        else:
            candidates = list(eps.get(group, []))  # type: ignore[attr-defined]

        for ep in candidates:
            try:
                obj = ep.load()

                # Support either a module instance or a class
                if isinstance(obj, type):
                    inst = obj()
                else:
                    inst = obj

                if not isinstance(inst, DCheckModule):
                    # Wrong type; ignore
                    continue

                # Use module.name as the orchestrator key
                modules[inst.name] = inst

            except Exception:
                # Fail-open: never crash orchestrator due to a plugin
                continue

    except Exception:
        # Any discovery failure should not break core usage
        return modules

    return modules


def discover_modules() -> Dict[str, DCheckModule]:
    """
    Step 1: built-in modules.
    Step 2: plugin discovery via entry points.
    """
    builtins: Dict[str, DCheckModule] = {}
    core = CoreModule()
    builtins[core.name] = core

    plugins = _load_entrypoint_modules()

    # Avoid letting plugins shadow built-ins silently.
    # Built-ins win if a plugin uses the same name.
    for name, mod in plugins.items():
        if name not in builtins:
            builtins[name] = mod

    return builtins
