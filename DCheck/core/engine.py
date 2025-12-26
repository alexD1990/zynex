from typing import Optional, Callable
from DCheck.rules.structural import DuplicateRowRule
from DCheck.rules.quality import NullRatioRule
from DCheck.rules.performance import SmallFileRule
from DCheck.rules.skewness import SkewnessRule
from DCheck.core.report import ValidationReport, RuleResult

def run_engine(
    df,
    table_name: Optional[str] = None,
    cache: bool = False,
    on_preflight_done: Optional[Callable[[RuleResult], None]] = None,
) -> ValidationReport:
    """
    Core validation engine. Orchestrates the execution of rules.

    Args:
        df: Spark DataFrame to validate.
        table_name: Name/Path of the table (required for metadata checks).
        cache: Whether to persist the DataFrame in memory.
        on_preflight_done: Callback function triggered after metadata checks completes.
    """
    
    # Initialize empty report
    report = ValidationReport(
        rows=0,
        columns=len(df.columns),
        column_names=df.columns,
    )

    context = {"table_name": table_name}

    # =========================================================
    # PHASE 1: PRE-FLIGHT CHECKS (Metadata / Small Files)
    # =========================================================
    if table_name:
        # Run metadata-based rules
        sf_rule = SmallFileRule(table_name=table_name)
        sf_result = sf_rule.apply(df, context=context)
        
        report.results.append(sf_result)

        # Trigger callback for immediate UI feedback (if provided)
        if on_preflight_done:
            on_preflight_done(sf_result)

    # =========================================================
    # PHASE 2: CORE CHECKS (Full Data Scan)
    # =========================================================
    persisted = False
    try:
        if cache:
            df = df.persist()
            persisted = True

        # Perform heavy count operation
        rows = int(df.count())
        report.rows = rows
        context["rows"] = rows

        core_rules = [
            DuplicateRowRule(),
            NullRatioRule(),
            SkewnessRule(threshold_stddev=5.0),
        ]

        for rule in core_rules:
            result = rule.apply(df, context=context)
            report.results.append(result)

        return report

    finally:
        # Ensure resources are released
        if persisted:
            df.unpersist()