# dcheck/orchestrator/engine.py
from __future__ import annotations

from typing import Callable, List, Optional

from dcheck.common.types import CheckResult, ExecutionContext, Report
from dcheck.orchestrator.aggregator import ResultAggregator
from dcheck.orchestrator.registry import discover_modules


def run_orchestrator(
    df,
    *,
    table_name: Optional[str] = None,
    modules: Optional[List[str]] = None,
    config: Optional[dict] = None,
    on_preflight_done: Optional[Callable[[CheckResult], None]] = None,
) -> Report:
    """
    Orchestrator: finds modules, selects which to run, runs them,
    collects results in a module-isolated report.
    """
    config = config or {}
    available = discover_modules()

    selected = modules or ["core_quality"]
    # Keep only modules that exist
    selected = [m for m in selected if m in available]

    ctx = ExecutionContext(df=df, table_name=table_name, config=config)

    # NOTE: rows will be computed by core module today (df.count()).
    # But we keep report.rows initialized with 0 and update later in adapter/rendering.
    agg = ResultAggregator(rows=0, columns=len(df.columns), column_names=df.columns)

    for module_name in selected:
        module = available[module_name]

        # ---- Preflight (optional) ----
        if table_name:
            try:
                pre = module.preflight(ctx)
                if pre is not None:
                    agg.add_single(module_name, pre)
                    if on_preflight_done:
                        on_preflight_done(pre)
            except Exception as e:
                # Preflight failure should not stop full run
                err = CheckResult(
                    check_id=f"{module_name}.preflight",
                    status="error",
                    message=f"Preflight crashed: {type(e).__name__}: {e}",
                    metrics={},
                    module_name=module_name,
                )
                agg.add_single(module_name, err)
                if on_preflight_done:
                    on_preflight_done(err)

        # ---- Full run ----
        try:
            results = module.run(ctx)
            agg.add_results(module_name, results)
        except Exception as e:
            err = CheckResult(
                check_id=f"{module_name}.module_error",
                status="error",
                message=f"Module crashed: {type(e).__name__}: {e}",
                metrics={},
                module_name=module_name,
            )
            agg.add_results(module_name, [err])

    return agg.build()
