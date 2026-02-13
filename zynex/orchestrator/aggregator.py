# zynex/orchestrator/aggregator.py
from __future__ import annotations

from typing import List, Optional

from zynex.common.types import CheckResult, Report


class ResultAggregator:
    def __init__(self, *, rows: int, columns: int, column_names: List[str]) -> None:
        self._report = Report(
            rows=rows,
            columns=columns,
            column_names=column_names,
            results_by_module={},
        )

    def add_results(self, module_name: str, results: List[CheckResult]) -> None:
        if module_name not in self._report.results_by_module:
            self._report.results_by_module[module_name] = []

        for r in results:
            # Ensure module_name is set consistently
            if not r.module_name:
                r.module_name = module_name
            self._report.results_by_module[module_name].append(r)

    def add_single(self, module_name: str, result: Optional[CheckResult]) -> None:
        if result is None:
            return
        self.add_results(module_name, [result])

    def build(self) -> Report:
        return self._report
