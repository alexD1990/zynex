# dcheck/modules/core/module.py
from __future__ import annotations

from typing import List

from dcheck.common.interfaces import DCheckModule
from dcheck.common.types import CheckResult, ExecutionContext

from dcheck.rules.performance import SmallFileRule
from dcheck.rules.structural import DuplicateRowRule
from dcheck.rules.quality import NullRatioRule
from dcheck.rules.skewness import SkewnessRule


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
        # Existing rule expects context dict
        rule_res = sf_rule.apply(ctx.df, context={"table_name": ctx.table_name})
        return _to_check_result(self.name, rule_res)

    def run(self, ctx: ExecutionContext) -> List[CheckResult]:
        """
        Full scan checks: count + duplicates + null ratio + skewness.
        """
        df = ctx.df

        # Optional caching controlled by config
        cache = bool(ctx.config.get("cache", False))
        persisted = False
        try:
            if cache:
                df = df.persist()
                persisted = True

            rows = int(df.count())
            # Push rows into metrics so report adapter can set report.rows
            # and downstream checks can use it.
            context = {"table_name": ctx.table_name, "rows": rows}

            rules = [
                DuplicateRowRule(),
                NullRatioRule(),
                SkewnessRule(threshold_stddev=5.0),
            ]

            out: List[CheckResult] = []
            # Put dataset rowcount as a pseudo-check so we can reconstruct report.rows later
            out.append(
                CheckResult(
                    check_id=f"{self.name}.rowcount",
                    status="ok",
                    message=f"Rowcount computed: {rows}",
                    metrics={"rows": rows},
                    module_name=self.name,
                )
            )

            for rule in rules:
                rule_res = rule.apply(df, context=context)
                out.append(_to_check_result(self.name, rule_res))

            return out

        finally:
            if persisted:
                df.unpersist()
