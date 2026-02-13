# zynex/core/engine.py
from __future__ import annotations

from typing import Optional, Callable

from zynex.orchestrator.engine import run_orchestrator
from zynex.orchestrator.adapters import report_to_validation_report
from zynex.core.report import ValidationReport, RuleResult


def run_engine(
    df,
    table_name: Optional[str] = None,
    cache: bool = False,
    on_preflight_done: Optional[Callable[[RuleResult], None]] = None,
) -> ValidationReport:
    """
    Backwards-compatible wrapper.
    """
    # Convert old callback signature to new CheckResult callback
    def _cb(new_result):
        if on_preflight_done is None:
            return
        name = new_result.check_id.split(".", 1)[1] if "." in new_result.check_id else new_result.check_id
        rr = RuleResult(
            name=name,
            status=new_result.status,
            metrics=new_result.metrics or {},
            message=new_result.message,
        )
        on_preflight_done(rr)

    rep = run_orchestrator(
        df,
        table_name=table_name,
        modules=["core_quality"],
        config={"cache": cache},
        on_preflight_done=_cb,
    )
    return report_to_validation_report(rep)
