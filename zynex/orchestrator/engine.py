# zynex/orchestrator/engine.py
from __future__ import annotations

from typing import Callable, List, Optional

from zynex.common.types import CheckResult, ExecutionContext, Report
from zynex.orchestrator.aggregator import ResultAggregator
from zynex.orchestrator.registry import discover_modules


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

    Performance:
      - Rowcount is computed at most once via ctx.rows (lazy memoization).
      - Optional DataFrame caching is controlled by config (cache_df/cache).
    """
    config = config or {}
    available = discover_modules()

    selected = modules or ["core_quality"]
    # Keep only modules that exist
    selected = [m for m in selected if m in available]

    ctx = ExecutionContext(df=df, table_name=table_name, config=config)

    # If caching is enabled, persist once for the whole orchestrator run.
    # (ctx.rows will also call ensure_persisted() before its first action.)
    ctx.ensure_persisted()

    try:
        rows = ctx.rows
        agg = ResultAggregator(rows=rows, columns=len(df.columns), column_names=df.columns)

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
    finally:
        ctx.unpersist_if_needed()