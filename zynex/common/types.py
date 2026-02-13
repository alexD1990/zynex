# zynex/common/types.py
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


@dataclass
class ExecutionContext:
    """
    Shared context passed to modules.
    Keep this import-light (no pyspark imports here).
    """
    df: Any
    table_name: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)


class CheckStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    ERROR = "error"
    SKIPPED = "skipped"


@dataclass
class CheckResult:
    """
    Standard result format across all modules.
    """
    check_id: str
    status: str
    message: str
    metrics: Dict[str, Any] = field(default_factory=dict)
    module_name: str = ""


@dataclass
class Report:
    """
    Aggregated results grouped by module (not mixed).
    """
    rows: int
    columns: int
    column_names: List[str]
    results_by_module: Dict[str, List[CheckResult]] = field(default_factory=dict)

    def all_results_flat(self) -> List[CheckResult]:
        out: List[CheckResult] = []
        for _, rs in self.results_by_module.items():
            out.extend(rs)
        return out
