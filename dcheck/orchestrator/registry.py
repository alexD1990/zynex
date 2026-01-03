# dcheck/orchestrator/registry.py
from __future__ import annotations

from typing import Dict

from dcheck.common.interfaces import DCheckModule
from dcheck.modules.core_quality.module import CoreModule


def discover_modules() -> Dict[str, DCheckModule]:
    """
    Step 1 (OSS-only): return built-in modules.
    Step 2 (later): add plugin discovery via entry points.
    """
    core = CoreModule()
    return {core.name: core}
