# zynex/modules/core_quality/module.py
from __future__ import annotations

from typing import List

from zynex.common.interfaces import DCheckModule
from zynex.common.types import CheckResult, ExecutionContext

from zynex.rules.performance import SmallFileRule
from zynex.rules.structural import DuplicateRowRule
from zynex.rules.quality import NullRatioRule
from zynex.rules.skewness import SkewnessRule


def _to_check_result(module_name: str, rule_result) -> CheckResult:
    """
    Adapter from existing RuleResult -> CheckResult.
    rule_result has: name, status, metrics, message
    """
    return CheckResult(
        check_id=f"{module_name}.{rule_result.name}",
        status=(rule_result.status or "").lower(),
        message=rule_result.message or "",
        metrics=rule_result.metrics or {},
        module_name=module_name,
    )


class CoreModule(DCheckModule):
    @property
    def name(self) -> str:
        return "core_quality"

    @property
    def description(self) -> str:
        return "Core data quality checks (OSS)."

    def preflight(self, ctx: ExecutionContext) -> CheckResult | None:
        """
        Metadata-only check: small files.
        Only meaningful when table_name is provided.
        """
        if not ctx.table_name:
            return None

        sf_rule = SmallFileRule(table_name=ctx.table_name)
        rule_res = sf_rule.apply(ctx.df, context={"table_name": ctx.table_name})
        return _to_check_result(self.name, rule_res)

    def run(self, ctx: ExecutionContext) -> List[CheckResult]:
        """
        Full scan checks: duplicates + null ratio + skewness.
        Rowcount is provided via ctx.rows (computed at most once for the whole run).
        """
        df = ctx.df
        rows = ctx.rows

        context = {"table_name": ctx.table_name, "rows": rows}

        thr = float((ctx.config or {}).get("extreme_values_threshold_stddev", 3.0))

        rules = [
            DuplicateRowRule(),
            NullRatioRule(),
            SkewnessRule(threshold_stddev=thr),
        ]

        out: List[CheckResult] = []

        for rule in rules:
            rule_res = rule.apply(df, context=context)
            out.append(_to_check_result(self.name, rule_res))

        return out