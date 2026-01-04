# dcheck/orchestrator/adapters.py
from __future__ import annotations

from typing import List

from dcheck.common.types import Report as NewReport, CheckResult as NewCheckResult
from dcheck.core.report import ValidationReport, RuleResult


def _short_name(check_id: str) -> str:
    """
    Convert 'module.check' -> 'check'
    If no dot, return as-is.
    """
    return check_id.split(".", 1)[1] if "." in check_id else check_id


def report_to_validation_report(r: NewReport) -> ValidationReport:
    """
    Flatten module-isolated results into the existing ValidationReport
    so render_report/tests continue to work.
    """
    flat: List[NewCheckResult] = r.all_results_flat()

    # Prefer orchestrator-provided dataset metadata.
    # Some legacy/core paths may emit a '*.rowcount' pseudo-check; keep that as an override if present.
    rows = int(getattr(r, "rows", 0) or 0)
    for cr in flat:
        if _short_name(cr.check_id) == "rowcount":
            try:
                rows = int((cr.metrics or {}).get("rows", 0))
            except Exception:
                # Fall back to orchestrator-provided value if parsing fails
                rows = int(getattr(r, "rows", 0) or 0)
            break

    vr = ValidationReport(
        rows=rows,
        columns=r.columns,
        column_names=r.column_names,
        results=[],
    )

    for cr in flat:
        # Hide rowcount pseudo-check from the old renderer/tests
        if _short_name(cr.check_id) == "rowcount":
            continue

        vr.results.append(
            RuleResult(
                name=_short_name(cr.check_id),
                status=cr.status,
                metrics=cr.metrics or {},
                message=cr.message or "",
            )
        )

    return vr


