# zynex/common/interfaces.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional

from zynex.common.types import CheckResult, ExecutionContext


class DCheckModule(ABC):
    """
    Contract all modules must implement.
    Orchestrator only depends on this interface.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    def description(self) -> str:
        return ""

    def preflight(self, ctx: ExecutionContext) -> Optional[CheckResult]:
        """
        Optional: return a single preflight result (e.g. small_files)
        to support the notebook preflight UX.
        """
        return None

    @abstractmethod
    def run(self, ctx: ExecutionContext) -> List[CheckResult]:
        raise NotImplementedError
