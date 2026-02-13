# zynex/orchestrator/adapters.py
from __future__ import annotations

from typing import List

from zynex.common.types import Report as NewReport, CheckResult as NewCheckResult
from zynex.core.report import ValidationReport, RuleResult


_ALLOWED_STATUS = {"ok", "warning", "error", "skipped", "not_applicable"}


def _short_name(check_id: str) -> str:
    """
    Convert 'module.check' -> 'check'
    If no dot, return as-is.
    """
    return check_id.split(".", 1)[1] if "." in check_id else check_id


def _normalize_status(raw) -> str:
    s = str(raw or "").strip().lower()
    return s if s in _ALLOWED_STATUS else "error"


def report_to_validation_report(r: NewReport) -> ValidationReport:
    """
    Flatten module-isolated results into the existing ValidationReport
    so render_report/tests continue to work.

    Also enforces global status domain at the adapter boundary.
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

        raw_status = cr.status
        status = _normalize_status(raw_status)
        raw_norm = str(raw_status or "").strip().lower()

        message = cr.message or ""
        # If module emits invalid status, make it visible + normalize to error.
        if raw_norm not in _ALLOWED_STATUS:
            message = (
                f"Invalid status '{raw_status}' from module '{cr.module_name}' "
                f"(normalized to 'error'). "
                + message
            )

        vr.results.append(
            RuleResult(
                name=_short_name(cr.check_id),
                status=status,
                metrics=cr.metrics or {},
                message=message,
            )
        )

    return vr

